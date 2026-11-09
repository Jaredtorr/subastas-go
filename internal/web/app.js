
const connectionDot = document.querySelector("#connectionDot");
const connectionText = document.querySelector("#connectionText");

const catalogSection = document.querySelector("#catalogSection");
const productSection = document.querySelector("#productSection");
const catalogGrid = document.querySelector("#catalogGrid");
const backToCatalog = document.querySelector("#backToCatalog");

const productImage = document.querySelector("#productImage");
const productName = document.querySelector("#productName");
const productCategory = document.querySelector("#productCategory");
const productDescription = document.querySelector("#productDescription");

const startAuctionButton = document.querySelector("#startAuction");
const auctionMessage = document.querySelector("#auctionMessage");

const auctionStatus = document.querySelector("#auctionStatus");
const highestAmount = document.querySelector("#highestAmount");
const highestBidder = document.querySelector("#highestBidder");

const strategy = document.querySelector("#strategy");
const eventCount = document.querySelector("#eventCount");
const activityList = document.querySelector("#activityList");

const winner = document.querySelector("#winner");
const finalAmount = document.querySelector("#finalAmount");
const resultStatus = document.querySelector("#resultStatus");
const lastEvent = document.querySelector("#lastEvent");

const monitorGoroutines = document.querySelector("#monitorGoroutines");
const monitorBidders = document.querySelector("#monitorBidders");

const monitorPoolWorkers = document.querySelector("#monitorPoolWorkers");
const monitorPoolActive = document.querySelector("#monitorPoolActive");
const monitorPoolPending = document.querySelector("#monitorPoolPending");
const monitorPoolCompleted = document.querySelector("#monitorPoolCompleted");

const monitorFanoutStarted = document.querySelector("#monitorFanoutStarted");
const monitorFanoutCompleted = document.querySelector("#monitorFanoutCompleted");
const monitorFanoutPending = document.querySelector("#monitorFanoutPending");

const monitorSemCapacity = document.querySelector("#monitorSemCapacity");
const monitorSemMaxCapacity = document.querySelector("#monitorSemMaxCapacity");
const monitorSemOccupied = document.querySelector("#monitorSemOccupied");
const monitorSemAvailable = document.querySelector("#monitorSemAvailable");

const monitorErrgroupCompleted = document.querySelector("#monitorErrgroupCompleted");
const monitorErrgroupErrors = document.querySelector("#monitorErrgroupErrors");

const monitorPipelineReceived = document.querySelector("#monitorPipelineReceived");
const monitorPipelineProcessed = document.querySelector("#monitorPipelineProcessed");
const monitorPipelineSent = document.querySelector("#monitorPipelineSent");

const monitorPubsubPublished = document.querySelector("#monitorPubsubPublished");
const monitorPubsubDelivered = document.querySelector("#monitorPubsubDelivered");

let selectedProduct = null;
let totalEvents = 0;
let renderPending = false;

const MAX_VISIBLE_BIDS = 30;
const auctions = new Map();

const source = new EventSource("/events");

function getAuctionState(lotId) {
    if (!auctions.has(lotId)) {
        auctions.set(lotId, {
            active: false,
            closed: false,
            highest: 0,
            bidder: "",
            winner: "",
            result: 0,
            events: []
        });
    }

    return auctions.get(lotId);
}

function scheduleRender() {
    if (renderPending) return;

    renderPending = true;

    requestAnimationFrame(() => {
        renderPending = false;
        renderAuctionState();
    });
}

async function loadConfiguration() {
    try {
        const response = await fetch("/config");

        if (!response.ok) {
            throw new Error("No se pudo obtener la configuración");
        }

        const config = await response.json();
        strategy.textContent = config.strategy.toUpperCase();

    } catch (error) {
        console.error("Error de configuración:", error);
        strategy.textContent = "No disponible";
    }
}

async function loadCatalog() {
    try {
        const response = await fetch("/catalog");

        if (!response.ok) {
            throw new Error("No se pudo cargar el catálogo");
        }

        const products = await response.json();

        catalogGrid.replaceChildren();

        products.forEach(product => {
            const card = document.createElement("article");
            card.className = "product-card";
            card.tabIndex = 0;

            const image = document.createElement("img");
            image.src = product.image;
            image.alt = product.name;
            image.loading = "lazy";

            image.onerror = () => {
                image.removeAttribute("src");
                image.alt = "Imagen no disponible";
            };

            const content = document.createElement("div");
            content.className = "product-card-content";

            const category = document.createElement("span");
            category.className = "product-card-category";
            category.textContent = product.category;

            const name = document.createElement("h3");
            name.textContent = product.name;

            const description = document.createElement("p");
            description.textContent = product.description;

            const state = document.createElement("p");
            state.className = "product-card-category";

            const auction = auctions.get(product.id);

            state.textContent = auction
                ? auction.closed
                    ? "Subasta finalizada"
                    : "Subasta en curso"
                : "Disponible";

            const action = document.createElement("div");
            action.className = "product-card-action";
            action.textContent = "Consultar artículo →";

            content.append(category, name, description, state, action);
            card.append(image, content);

            card.addEventListener("click", () => {
                selectProduct(product);
            });

            card.addEventListener("keydown", event => {
                if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    selectProduct(product);
                }
            });

            catalogGrid.appendChild(card);
        });

    } catch (error) {
        console.error("Error al cargar catálogo:", error);
        catalogGrid.textContent =
            "No fue posible cargar los artículos. Comprueba el servidor.";
    }
}

function selectProduct(product) {
    selectedProduct = product;

    productName.textContent = product.name;
    productCategory.textContent = product.category;
    productDescription.textContent = product.description;

    productImage.src = product.image;
    productImage.alt = product.name;

    productImage.onerror = () => {
        productImage.removeAttribute("src");
        productImage.alt = "Imagen no disponible";
    };

    catalogSection.hidden = true;
    productSection.hidden = false;

    renderAuctionState();
    loadMetrics();

    window.scrollTo({
        top: 0,
        behavior: "smooth"
    });
}

backToCatalog.addEventListener("click", () => {
    productSection.hidden = true;
    catalogSection.hidden = false;

    loadCatalog();

    window.scrollTo({
        top: 0,
        behavior: "smooth"
    });
});

startAuctionButton.addEventListener("click", startAuction);

async function startAuction() {
    if (!selectedProduct) return;

    const state = getAuctionState(selectedProduct.id);

    if (state.active || state.closed) return;

    startAuctionButton.disabled = true;
    auctionMessage.textContent = "Iniciando subasta...";

    try {
        const response = await fetch("/auction/start", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                lotId: selectedProduct.id
            })
        });

        if (!response.ok) {
            const message = await response.text();
            throw new Error(message);
        }

        state.active = true;

        auctionMessage.textContent =
            "Subasta iniciada. Los postores están compitiendo.";

        scheduleRender();
        loadCatalog();

    } catch (error) {
        console.error("Error al iniciar subasta:", error);

        auctionMessage.textContent =
            "No se pudo iniciar: " + error.message;

        startAuctionButton.disabled = false;
    }
}

source.onopen = () => {
    connectionDot.parentElement.classList.add("connected");
    connectionText.textContent = "Conectado al servidor";
};

source.onerror = () => {
    connectionDot.parentElement.classList.remove("connected");

    if (source.readyState === EventSource.CONNECTING) {
        connectionText.textContent = "Reconectando...";
    } else if (source.readyState === EventSource.CLOSED) {
        connectionText.textContent = "Conexión cerrada";
    }
};

source.onmessage = message => {
    let event;

    try {
        event = JSON.parse(message.data);
    } catch (error) {
        console.error("Evento SSE inválido:", error);
        return;
    }

    if (!event.LotID) {
        console.warn("Evento sin LotID:", event);
        return;
    }

    const state = getAuctionState(event.LotID);

    totalEvents++;
    state.events.push(event);

    if (state.events.length > MAX_VISIBLE_BIDS) {
        state.events.splice(
            0,
            state.events.length - MAX_VISIBLE_BIDS
        );
    }

    if (event.Kind === 0) {
        handleNewBid(event, state);

    } else if (event.Kind === 1) {
        handleAuctionClosed(event, state);

    } else {
        console.warn("Tipo de evento desconocido:", event);
        return;
    }

    if (selectedProduct?.id === event.LotID) {
        scheduleRender();
    }
};

function handleNewBid(event, state) {
    const amount = Number(event.Amount);

    if (amount > state.highest) {
        state.highest = amount;
        state.bidder = event.Bidder;
    }
}

function handleAuctionClosed(event, state) {
    state.active = false;
    state.closed = true;
    state.winner = event.Bidder || "";
    state.result = Number(event.Amount) || 0;
    state.highest = Math.max(state.highest, state.result);

    console.log("SUBASTA FINALIZADA:", {
        producto: event.LotName,
        ganador: state.winner,
        monto: state.result
    });

    loadCatalog();
}

function renderAuctionState() {
    if (!selectedProduct) return;

    const state = getAuctionState(selectedProduct.id);

    highestAmount.textContent = formatAmount(state.highest);

    highestBidder.textContent = state.bidder
        ? `Postor: ${state.bidder}`
        : "Sin pujas registradas";

    eventCount.textContent = state.events.length;

    activityList.replaceChildren();

    const bids = state.events.filter(event => event.Kind === 0);

    if (bids.length === 0) {
        const empty = document.createElement("div");
        empty.className = "empty-state";
        empty.innerHTML =
            "<span class='empty-icon'>↗</span><p>Esperando la primera puja...</p>";

        activityList.appendChild(empty);

    } else {
        bids.slice().reverse().forEach(event => {
            const item = document.createElement("div");
            item.className = "activity-item";

            const information = document.createElement("div");

            const name = document.createElement("span");
            name.className = "activity-name";
            name.textContent = event.Bidder;

            const type = document.createElement("span");
            type.className = "activity-type";
            type.textContent = "Puja aceptada";

            const amount = document.createElement("strong");
            amount.className = "activity-amount";
            amount.textContent = formatAmount(event.Amount);

            information.append(name, type);
            item.append(information, amount);

            activityList.appendChild(item);
        });
    }

    if (state.closed) {
        auctionStatus.textContent = "Subasta finalizada";
        resultStatus.textContent = "Cerrada";
        lastEvent.textContent = "Cierre de subasta";

        winner.textContent = state.winner || "Sin ganador";

        finalAmount.textContent = state.winner
            ? formatAmount(state.result)
            : "La subasta terminó sin pujas";

        auctionMessage.textContent = "Esta subasta ha finalizado.";
        startAuctionButton.disabled = true;

    } else if (state.active) {
        auctionStatus.textContent = "Subasta en curso";
        resultStatus.textContent = "En curso";
        lastEvent.textContent = state.events.length
            ? "Nueva puja"
            : "Subasta iniciada";

        winner.textContent = "Pendiente";
        finalAmount.textContent = "Esperando cierre";

        auctionMessage.textContent =
            "Los postores están compitiendo.";

        startAuctionButton.disabled = true;

    } else {
        auctionStatus.textContent = "Esperando inicio";
        resultStatus.textContent = "En espera";
        lastEvent.textContent = "Ninguno";

        winner.textContent = "Pendiente";
        finalAmount.textContent = "Sin resultado final";

        auctionMessage.textContent =
            "Selecciona iniciar para comenzar la subasta.";

        startAuctionButton.disabled = false;
    }
}

function formatAmount(amount) {
    return "$" + new Intl.NumberFormat("es-MX", {
        maximumFractionDigits: 0
    }).format(amount);
}

async function loadMetrics() {
    if (productSection.hidden) return;

    try {
        const response = await fetch("/metrics");

        if (!response.ok) {
            throw new Error("No se pudo obtener el monitor de concurrencia");
        }

        renderMetrics(await response.json());

    } catch (error) {
        console.error("Error del monitor de concurrencia:", error);
    }
}

function renderMetrics(metrics) {
    monitorGoroutines.textContent = metrics.goroutines;
    monitorBidders.textContent = metrics.activeBidders;

    monitorPoolWorkers.textContent = metrics.workPool.workers;
    monitorPoolActive.textContent = metrics.workPool.active;
    monitorPoolPending.textContent = metrics.workPool.pending;
    monitorPoolCompleted.textContent = metrics.workPool.completed;

    monitorFanoutStarted.textContent = metrics.fanOut.started;
    monitorFanoutCompleted.textContent = metrics.fanOut.completed;
    monitorFanoutPending.textContent = metrics.fanOut.pending;

    monitorSemCapacity.textContent = metrics.semaphore.capacity;
    monitorSemMaxCapacity.textContent = metrics.semaphore.maxCapacity ?? 0;
    monitorSemOccupied.textContent = metrics.semaphore.occupied;
    monitorSemAvailable.textContent = metrics.semaphore.available;

    monitorErrgroupCompleted.textContent = metrics.errgroup.completed;
    monitorErrgroupErrors.textContent = metrics.errgroup.errors;

    monitorPipelineReceived.textContent = metrics.pipeline.received;
    monitorPipelineProcessed.textContent = metrics.pipeline.processed;
    monitorPipelineSent.textContent = metrics.pipeline.sent;

    monitorPubsubPublished.textContent = metrics.pubSub.published;
    monitorPubsubDelivered.textContent = metrics.pubSub.delivered;
}

loadConfiguration();
loadCatalog();
loadMetrics();

setInterval(loadMetrics, 1000);
