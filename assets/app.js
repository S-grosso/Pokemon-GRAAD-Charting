// assets/app.js
const $ = (id) => document.getElementById(id);

let catalog = [];
let sets = [];
let langs = [];

/* ---------------------------
   Normalizzazione ricerca
--------------------------- */
function norm(s) {
  let t = (s || "")
    .toString()
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();

  // token lingua comuni
  t = t
    .replace(/\b(jap|jpn|jp|giapponese)\b/g, " ja ")
    .replace(/\b(eng|en|english|inglese)\b/g, " en ");

  return t.replace(/\s+/g, " ").trim();
}

/* ---------------------------
   Stato URL (q/set/lang)
--------------------------- */
function getStateFromUI() {
  return {
    q: $("q").value || "",
    set: $("set").value || "",
    lang: $("lang").value || ""
  };
}

function applyStateToUI(state) {
  if (state.q != null) $("q").value = state.q;
  if (state.set != null) $("set").value = state.set;
  if (state.lang != null) $("lang").value = state.lang;
}

function readStateFromURL() {
  const p = new URLSearchParams(location.search);
  return {
    q: p.get("q") || "",
    set: p.get("set") || "",
    lang: p.get("lang") || ""
  };
}

function writeStateToURL(state) {
  const url = new URL(location.href);
  if (state.q) url.searchParams.set("q", state.q); else url.searchParams.delete("q");
  if (state.set) url.searchParams.set("set", state.set); else url.searchParams.delete("set");
  if (state.lang) url.searchParams.set("lang", state.lang); else url.searchParams.delete("lang");
  history.replaceState(null, "", url.toString());
}

/* ---------------------------
   UI helpers
--------------------------- */
function buildOptions(select, items, allLabel) {
  select.innerHTML = "";
  const opt0 = document.createElement("option");
  opt0.value = "";
  opt0.textContent = allLabel;
  select.appendChild(opt0);

  for (const it of items) {
    const o = document.createElement("option");
    o.value = it.value;
    o.textContent = it.label;
    select.appendChild(o);
  }
}

/* ---------------------------
   Filtri + rendering
--------------------------- */
function applyFilters({ writeURL = true } = {}) {
  const state = getStateFromUI();
  if (writeURL) writeStateToURL(state);

  const q = norm(state.q);
  const set = state.set;
  const lang = state.lang;

  let res = catalog;

  if (set) res = res.filter(c => c.setId === set);
  if (lang) res = res.filter(c => c.lang === lang);

  if (q) {
    res = res.filter(c => {
      const hay = norm([
        c.name,
        c.nameEn,
        c.nameJa,
        c.setId,
        c.setName,
        c.numberFull,
        c.number,
        c.lang,
        c.rarity,
        c.features?.join(" ")
      ].join(" "));
      return hay.includes(q);
    });
  }

  $("stats").textContent = `Risultati: ${res.length} carte`;
  render(res.slice(0, 200), state);
}

function render(cards, state) {
  const root = $("results");
  root.innerHTML = "";

  const q = encodeURIComponent(state.q || "");
  const set = encodeURIComponent(state.set || "");
  const lang = encodeURIComponent(state.lang || "");

  for (const c of cards) {
    const div = document.createElement("div");
    div.className = "card";

    const a = document.createElement("a");
    a.href = `card.html?id=${encodeURIComponent(c.id)}&q=${q}&set=${set}&lang=${lang}`;

    const displayName = (c.lang === "ja" && c.nameEn) ? c.nameEn : c.name;
    const subtitleExtra = (c.lang === "ja" && c.nameJa && c.nameEn) ? ` • ${c.nameJa}` : "";

    a.innerHTML = `
      <div><strong>${displayName}</strong></div>
      <div class="small">${c.setName || c.setId} — ${c.numberFull || c.number || ""} — ${c.lang?.toUpperCase() || ""}${subtitleExtra}</div>
      <div class="small">${c.rarity || ""} ${c.features?.length ? "• " + c.features.join(", ") : ""}</div>
    `;

    div.appendChild(a);
    root.appendChild(div);
  }
}

/* ---------------------------
   Init
--------------------------- */
async function init() {
  const r = await fetch("data/catalog.json", { cache: "no-store" });
  const j = await r.json();
  catalog = j.cards || [];

  const setMap = new Map();
  const langMap = new Map();
  for (const c of catalog) {
    if (c.setId) setMap.set(c.setId, c.setName || c.setId);
    if (c.lang) langMap.set(c.lang, c.lang.toUpperCase());
  }

  sets = [...setMap.entries()]
    .map(([value, label]) => ({ value, label }))
    .sort((a, b) => a.label.localeCompare(b.label));

  langs = [...langMap.entries()]
    .map(([value, label]) => ({ value, label }))
    .sort((a, b) => a.label.localeCompare(b.label));

  buildOptions($("set"), sets, "Tutte le espansioni");
  buildOptions($("lang"), langs, "Tutte le lingue");

  const urlState = readStateFromURL();
  applyStateToUI(urlState);

  $("q").addEventListener("input", () => applyFilters({ writeURL: true }));
  $("set").addEventListener("change", () => applyFilters({ writeURL: true }));
  $("lang").addEventListener("change", () => applyFilters({ writeURL: true }));

  $("stats").textContent = `Catalogo caricato: ${catalog.length} carte`;
  applyFilters({ writeURL: true });
}

init();
