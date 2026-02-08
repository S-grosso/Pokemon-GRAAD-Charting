function getParam(name) {
  return new URLSearchParams(location.search).get(name);
}

function euro(n) {
  if (n == null || Number.isNaN(n)) return "—";
  return new Intl.NumberFormat("it-IT", { style: "currency", currency: "EUR" }).format(n);
}

async function init() {
  const id = getParam("id");
  if (!id) return;

  const [catR, priceR, metaR] = await Promise.all([
    fetch("data/catalog.json", { cache: "no-store" }),
    fetch("data/prices.json", { cache: "no-store" }),
    fetch("data/meta.json", { cache: "no-store" })
  ]);

  const catalog = await catR.json();
  const prices = await priceR.json();
  const meta = await metaR.json();

  const c = (catalog.cards || []).find(x => x.id === id);
  if (!c) {
    document.getElementById("title").innerHTML = "<h2>Carta non trovata</h2>";
    return;
  }

  document.title = `${c.name} — ${c.setName || c.setId} — ${c.numberFull || c.number || ""}`;

  document.getElementById("title").innerHTML = `
    <h2>${c.name}</h2>
    <div class="small">${c.setName || c.setId} — ${c.numberFull || c.number || ""} — ${c.lang?.toUpperCase() || ""}</div>
  `;

  document.getElementById("meta").innerHTML = `
    <span class="badge">${c.rarity || "—"}</span>
    ${c.features?.length ? `<span class="badge">${c.features.join(", ")}</span>` : ""}
    <span class="badge mono">${c.id}</span>
  `;

  if (c.imageLarge) {
    document.getElementById("img").innerHTML = `<img class="cardimg" src="${c.imageLarge}" alt="Artwork">`;
  } else {
    document.getElementById("img").innerHTML = `<div class="small">Immagine non disponibile</div>`;
  }

  const p = (prices.byCard && prices.byCard[id]) ? prices.byCard[id] : {};
  const buckets = [
    ["RAW", "raw"],
    ["GRAAD 7", "graad_7"],
    ["GRAAD 8", "graad_8"],
    ["GRAAD 9", "graad_9"],
    ["GRAAD 9.5", "graad_9_5"],
    ["GRAAD 10", "graad_10"]
  ];

  const root = document.getElementById("prices");
  root.innerHTML = "";

  for (const [label, key] of buckets) {
    const box = document.createElement("div");
    box.className = "pricebox";
    const val = p[key]?.median_eur ?? null;
    const n = p[key]?.n ?? 0;
    box.innerHTML = `<div class="small">${label}</div><div style="font-size:22px; font-weight:700;">${euro(val)}</div><div class="small">vendite: ${n}</div>`;
    root.appendChild(box);
  }

  document.getElementById("updated").textContent =
    `Ultimo aggiornamento: ${meta.updatedAt || "—"} • Finestra: ultimi 30 giorni`;
}

init();
