# Patrones concurrentes en Subastas Go

Este documento describe los cinco patrones de programación concurrente
agregados al proyecto: **Pipeline**, **Worker Pool**, **Fan-out/Fan-in**,
**Semáforos** y **Errgroup**. Cada patrón está implementado en código real,
integrado en el flujo de la aplicación y cubierto por pruebas.

Los patrones nuevos viven fuera de las estrategias `Mutex`, `CAS` y `Actor`.
No se agregó trabajo dentro de la sección crítica de `Bid`, por lo que la
comparación de rendimiento entre estrategias sigue midiendo lo mismo.

## Resumen de integración

| Patrón | Paquete / archivo | Funciones principales | Dónde se integra |
|---|---|---|---|
| Pipeline | `internal/pipeline/pipeline.go` | `Run`, `stageInput`, `stageProcess`, `stageOutput`, `accept` | `internal/web/server.go`, manejador SSE `/events` |
| Worker Pool | `internal/workpool/pool.go` | `New`, `Submit`, `Close`, `worker` | `AuctionManager.enqueueSettlement` y flag `-workers` |
| Fan-out/Fan-in | `internal/fanout/fanout.go` | `Start`, `Run` | `settlement.Validate` al cerrar una subasta |
| Semáforo | `internal/semaphore/semaphore.go` | `New`, `Acquire`, `TryAcquire`, `Release` | `fanout.Start`, para limitar comprobaciones simultáneas |
| Errgroup | `golang.org/x/sync/errgroup` | `WithContext`, `Go`, `Wait` | `fanout.Start`, coordinación de comprobaciones |

---

## 1. Pipeline

### Qué hace

El pipeline procesa los eventos de subasta en tres etapas conectadas por
canales:

1. **Entrada** (`stageInput`): recibe los eventos crudos de la suscripción del
   Broker.
2. **Procesamiento** (`stageProcess`): valida cada evento, descarta eventos
   inválidos y repeticiones exactas consecutivas, y conserva siempre el evento
   de cierre.
3. **Salida** (`stageOutput`): entrega los eventos procesados al consumidor
   SSE.

### Dónde está

- Implementación: `internal/pipeline/pipeline.go`.
- Integración: `internal/web/server.go`, en el manejador `/events`:

```go
sub := broker.Subscribe(1024)
defer broker.Unsubscribe(sub)

events := pipeline.Run(r.Context(), sub.C)
```

El bucle SSE lee de `events`, no directamente de `sub.C`.

### Goroutines

Por cada conexión SSE se crean tres goroutines:

| Goroutine | Quién la crea | Cuándo termina |
|---|---|---|
| `stageInput` | `pipeline.Run` | se cierra `sub.C` o se cancela el contexto |
| `stageProcess` | `pipeline.Run` | se cierra su canal de entrada o se cancela el contexto |
| `stageOutput` | `pipeline.Run` | se cierra su canal de entrada o se cancela el contexto |

### Canales

| Canal | Capacidad | Crea | Escribe | Lee | Cierra |
|---|---|---|---|---|---|
| `sub.C` | 1024 | `Broker.Subscribe` | `Broker.loop` | `stageInput` | `Broker` al desuscribir o cerrar |
| `input` | 0 | `pipeline.Run` | `stageInput` | `stageProcess` | `stageInput` |
| `processed` | 0 | `pipeline.Run` | `stageProcess` | `stageOutput` | `stageProcess` |
| `out` | 0 | `pipeline.Run` | `stageOutput` | manejador SSE | `stageOutput` |

Cada etapa es dueña de su canal de salida: solo esa goroutine lo escribe y lo
cierra. El canal `sub.C` pertenece al Broker y el pipeline nunca lo cierra.

### Cancelación

`pipeline.Run` recibe el contexto de la petición HTTP (`r.Context()`). Cuando
el cliente cierra la conexión SSE o el servidor se apaga, el contexto se
cancela y las tres etapas salen por sus `select` sobre `ctx.Done()`. Los
envíos entre etapas también seleccionan `ctx.Done()`, así que ninguna etapa
queda bloqueada intentando escribir a una etapa que ya terminó.

### Qué problema resuelve

- Desacopla la validación y el filtrado de eventos del Broker y del manejador
  SSE.
- El Broker sigue publicando de forma no bloqueante en la cola acotada del
  suscriptor. Un pipeline lento puede llenar esa cola y provocar descartes del
  Broker, pero **nunca bloquea la aceptación de pujas**.
- Evita duplicados exactos y eventos inválidos sin perder el cierre.
- Al cerrar los canales en orden, no deja goroutines filtradas.

### Cómo se comprueba

`internal/pipeline/pipeline_test.go` verifica:

- procesamiento y filtrado de eventos;
- conservación del evento de cierre;
- cierre del canal de salida cuando se cierra la entrada;
- cancelación del contexto y cierre de todas las etapas;
- ausencia de fugas con `goleak`.

---

## 2. Worker Pool

### Qué hace

Mantiene un número fijo de trabajadores que consumen trabajos de un canal
compartido. Se usa para ejecutar la **liquidación de subastas cerradas**
(validación concurrente del resultado) sin crear una goroutine por cada cierre.

### Dónde está

- Implementación: `internal/workpool/pool.go`.
- Integración: `internal/app/manager.go`.
  - `NewAuctionManager` crea el pool con tamaño `workers`.
  - `AuctionManager.run` llama a `enqueueSettlement` al cerrar la subasta.
  - `AuctionManager.Close` espera las subastas en curso y cierra el pool.
- Configuración: flag `-workers` en `cmd/auction/main.go` (por defecto `4`).

### Goroutines

`workpool.New(size)` crea `size` goroutines `worker`. Cada una ejecuta:

```go
for task := range p.jobs {
    if err := task.job(task.ctx); err != nil {
        log.Printf("worker pool: trabajo falló: %v", err)
    }
}
```

No se crea una goroutine por trabajo: el número de goroutines queda acotado
por el tamaño del pool.

### Canales

| Canal | Capacidad | Crea | Escribe | Lee | Cierra |
|---|---|---|---|---|---|
| `jobs` | `size` | `workpool.New` | `Submit` | los `worker` | `Close` |

El canal `jobs` está acotado para aplicar contrapresión: si la cola está
llena, `Submit` espera espacio o la cancelación de su contexto.

### Cancelación y cierre

- `Submit(ctx, job)` devuelve `ctx.Err()` si el contexto se cancela mientras
  espera espacio.
- El trabajo recibe el mismo contexto, así que puede observar la cancelación
  mientras se ejecuta.
- `Close()` cierra `jobs`, espera con `sync.WaitGroup` a que los trabajadores
  terminen los trabajos ya encolados y es idempotente.
- En `AuctionManager`, cada liquidación usa un `context.WithTimeout` de 5
  segundos para no quedar colgada.

### Qué problema resuelve

- Limita la concurrencia de liquidaciones cuando muchas subastas cierran a la
  vez.
- Evita la creación ilimitada de goroutines por cierre.
- Aplica contrapresión mediante la cola acotada.
- No toca el camino de aceptación de pujas: las liquidaciones se encolan
  después de que la subasta ya cerró.

### Cómo se comprueba

`internal/workpool/pool_test.go` verifica:

- límite de trabajadores y finalización de todos los trabajos;
- rechazo de trabajos después de `Close`;
- cancelación de `Submit` con contexto;
- cancelación visible dentro del trabajo;
- ausencia de fugas con `goleak`.

`internal/app/manager_test.go` comprueba la integración completa: inicio de
subasta, cierre, resultado y liquidación sin fugas.

---

## 3. Fan-out/Fan-in

### Qué hace

Distribuye un conjunto finito de trabajos independientes entre varias
goroutines (**fan-out**) y reúne todos los resultados en un único canal
(**fan-in**). Se usa para validar el cierre de una subasta con comprobaciones
independientes.

### Dónde está

- Implementación: `internal/fanout/fanout.go`.
- Integración: `internal/settlement/settlement.go`, función `Validate`, que
  ejecuta estas comprobaciones:
  - `precio-base`: el monto final no es menor que el precio base.
  - `ganador`: hay ganador si y solo si hubo pujas por encima del precio base.
  - `estado-final`: el resultado de `Close` coincide con `Highest`.
  - `producto`: el lote tiene identificador, nombre y duración válida.

### Goroutines

`fanout.Start` lanza una goroutine por elemento (`group.Go`) y una goroutine
adicional que espera al `errgroup` y cierra el canal de resultados. Las
goroutines de trabajo son de corta vida: terminan al procesar su elemento o al
cancelarse el contexto.

### Canales

| Canal | Capacidad | Crea | Escribe | Lee | Cierra |
|---|---|---|---|---|---|
| `out` (resultados) | `len(items)` | `fanout.Start` | goroutines de trabajo | `fanout.Run` | goroutine que espera a `errgroup.Wait` |
| `permits` (semáforo) | `limit` | `semaphore.New` dentro de `Start` | `Acquire` | `Release` | nadie |

El canal de resultados se cierra cuando todos los productores terminan. El
buffer tiene capacidad `len(items)`, por lo que un consumidor lento no bloquea
a los productores.

### Cancelación

- `errgroup.WithContext` crea un contexto derivado que se cancela al primer
  error.
- Cada goroutine adquiere el semáforo con `Acquire(groupCtx)`, así que la
  espera de permiso también respeta la cancelación.
- La función de trabajo recibe `groupCtx`.
- Si el contexto externo se cancela, `Run` devuelve `ctx.Err()`.

### Qué problema resuelve

- Reduce la latencia del análisis de cierre ejecutando comprobaciones
  independientes en paralelo.
- Reconstruye el orden original mediante `Result.Index`.
- Garantiza el cierre del canal de resultados cuando terminan todos los
  productores.
- No comparte estado mutable entre comprobaciones, por lo que no hay carreras.

### Cómo se comprueba

`internal/fanout/fanout_test.go` verifica:

- recepción de todos los resultados y cierre del canal;
- respeto del límite de concurrencia;
- cancelación del contexto y cierre del canal;
- propagación de errores y cancelación de las tareas restantes.

`internal/settlement/settlement_test.go` verifica las validaciones reales y la
cancelación.

---

## 4. Semáforos

### Qué hace

Limita el número de operaciones que pueden ejecutarse a la vez. La
implementación usa un canal con capacidad igual al límite: cada permiso ocupa
una posición y se libera al vaciarla.

### Dónde está

- Implementación: `internal/semaphore/semaphore.go`.
- Integración: `internal/fanout/fanout.go`, dentro de `Start`. El límite es el
  parámetro `concurrency`, que proviene del flag `-workers`.

```go
sem := semaphore.New(limit)
if err := sem.Acquire(groupCtx); err != nil {
    return err
}
defer sem.Release()
```

### Canales

| Canal | Capacidad | Crea | Escribe (adquiere) | Lee (libera) | Cierra |
|---|---|---|---|---|---|
| `permits` | `limit` | `semaphore.New` | `Acquire` / `TryAcquire` | `Release` | nadie |

### Cancelación y liberación

- `Acquire(ctx)` espera un permiso o devuelve `ctx.Err()` si el contexto
  termina.
- `Release` se llama siempre con `defer` en `fanout.Start`, incluso si la
  función de trabajo falla.
- No hay adquisiciones anidadas ni esperas circulares, por lo que no se
  produce interbloqueo.

### Qué problema resuelve

Evita que el fan-out de comprobaciones abra demasiadas ejecuciones simultáneas
si el conjunto de trabajos crece. Separa la política de concurrencia (límite)
de la creación de goroutines.

### Cómo se comprueba

`internal/semaphore/semaphore_test.go` verifica:

- respeto del límite;
- `TryAcquire` sin bloqueo;
- cancelación mientras se espera un permiso;
- límite bajo carga concurrente.

---

## 5. Errgroup

### Qué hace

Coordina un grupo de goroutines relacionadas, propaga el primer error y cancela
las tareas restantes cuando alguna falla. Se usa `golang.org/x/sync/errgroup`.

### Dónde está

- Implementación: `internal/fanout/fanout.go`.
- Integración: `internal/settlement/settlement.go`, a través de `fanout.Run`.

```go
group, groupCtx := errgroup.WithContext(ctx)

for i, item := range items {
    group.Go(func() error {
        // trabajo con groupCtx
    })
}

go func() {
    _ = group.Wait()
    close(out)
}()
```

### Goroutines

- Una goroutine por elemento, lanzada con `group.Go`.
- Una goroutine que ejecuta `group.Wait()` y cierra el canal de resultados.

### Propagación de errores y cancelación

- El primer error no nulo cancela `groupCtx`.
- Las goroutines restantes observan `groupCtx.Done()` al adquirir el semáforo
  o dentro de la función de trabajo.
- Cada resultado viaja con su error; `fanout.Run` devuelve el primer error no
  contextual, priorizando el error real sobre `context.Canceled`.
- `group.Wait()` se ejecuta una sola vez, en la goroutine que cierra el canal.

### Qué problema resuelve

- Sustituye la coordinación manual de errores y cancelación en una operación
  nueva (validación de cierre), sin reemplazar los `WaitGroup` existentes de
  postores y auditoría.
- Garantiza que una comprobación fallida no deje las demás colgadas.

### Cómo se comprueba

- `internal/fanout/fanout_test.go`, prueba
  `TestRunPropagatesErrorAndCancelsRest`.
- `internal/settlement/settlement_test.go`, pruebas de validación y
  cancelación.

---

## Diferencias entre Pipeline, Worker Pool y Fan-out/Fan-in

| Aspecto | Pipeline | Worker Pool | Fan-out/Fan-in |
|---|---|---|---|
| Flujo | Continuo y ordenado por etapas | Cola de trabajos independientes | Conjunto finito de trabajos |
| Canales | Varios canales entre etapas | Un canal de trabajos | Un canal de resultados |
| Goroutines | Una por etapa, de larga vida | N trabajadores fijos, de larga vida | Una por trabajo + cierre, de corta vida |
| Resultado | Eventos transformados | Sin resultado (efecto lateral) | Resultados reunidos y ordenados |
| Cierre | Cada etapa cierra su salida | `Close` cierra `jobs` | El canal se cierra al terminar todos |
| Uso en el proyecto | Eventos del Broker hacia SSE | Liquidación de subastas | Comprobaciones de cierre |
| Cancelación | Contexto de la petición SSE | Contexto de `Submit` y del trabajo | `errgroup` y contexto externo |

También conviene distinguir el semáforo del worker pool:

- El **semáforo** no crea ni posee goroutines; solo limita cuántas pueden
  entrar a una sección.
- El **worker pool** posee un conjunto fijo de goroutines y distribuye trabajos
  entre ellas.

Y `errgroup` de `WaitGroup`:

- `WaitGroup` solo espera; no propaga errores ni cancela.
- `errgroup` espera, propaga el primer error y cancela el contexto derivado.

---

## Integración con la arquitectura existente

1. `Notifying.Bid` publica en el Broker como antes. Las estrategias `Mutex`,
   `CAS` y `Actor` no cambiaron.
2. El Broker entrega eventos a la suscripción SSE con su política no
   bloqueante de siempre.
3. El pipeline procesa esa suscripción y entrega al manejador SSE.
4. Al cerrar una subasta, `AuctionManager.run` guarda el resultado y encola la
   liquidación en el worker pool.
5. La liquidación usa fan-out/fan-in, semáforo y errgroup para validar el
   cierre.
6. `AuctionManager.Close` espera las subastas y cierra el pool; luego se cierra
   el Broker y el servidor HTTP.

Ninguno de estos pasos se ejecuta dentro de `Bid`, por lo que la comparación
Mutex/CAS/Actor y los benchmarks existentes no se ven afectados.

---

## Comandos de verificación

```bash
gofmt -l .                       # no debe imprimir nada
go mod tidy
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/auction
```

Pruebas específicas por patrón:

```bash
go test ./internal/pipeline
go test ./internal/workpool
go test ./internal/fanout
go test ./internal/semaphore
go test ./internal/settlement
go test ./internal/app -run TestManagerRunsAuctionAndSettlement
```
