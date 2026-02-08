const $ = (id) => document.getElementById(id);

let catalog = [];
let sets = [];
let langs = [];

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

function cardLabel(c) {
  const bits = [
    c.name,
    c.setName ? `(${c.setName})` : "",
    c.numberFull ? `#${c.numberFull}` : (c.number ? `#${c.number}` : ""),
    c.lang ? c.lang.toUpperCase() : ""
  ].filter(Boolean);
  return bits.join(" ");
}

function applyFilters() {
  const q = norm($("q").value);
  const set = $("set").value;
  const lang = $("lang").value;

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
  render(res.slice(0, 200));
}

function render(cards) {
  const root = $("results");
  root.innerHTML = "";
  for (const c of cards) {
    const div = document.createElement("div");
    div.className = "card";
    const a = document.createElement("a");
    a.href = `card.html?id=${encodeURIComponent(c.id)}`;
    a.innerHTML = `
      <div><strong>${c.name}</strong></div>
      <div class="small">${c.setName || c.setId} — ${c.numberFull || c.number || ""} — ${c.lang?.toUpperCase() || ""}</div>
      <div class="small">${c.rarity || ""} ${c.features?.length ? "• " + c.features.join(", ") : ""}</div>
    `;
    div.appendChild(a);
    root.appendChild(div);
  }
}

async function init() {
  const r = await fetch("data/catalog.json", { cache: "no-store" });
  const j = await r.json();
  catalog = j.cards || [];

  // sets/langs per dropdown
  const setMap = new Map();
  const langMap = new Map();
  for (const c of catalog) {
    if (c.setId) setMap.set(c.setId, c.setName || c.setId);
    if (c.lang) langMap.set(c.lang, c.lang.toUpperCase());
  }

  sets = [...setMap.entries()].map(([value, label]) => ({ value, label }))
    .sort((a,b) => a.label.localeCompare(b.label));
  langs = [...langMap.entries()].map(([value, label]) => ({ value, label }))
    .sort((a,b) => a.label.localeCompare(b.label));

  buildOptions($("set"), sets, "Tutte le espansioni");
  buildOptions($("lang"), langs, "Tutte le lingue");

  $("q").addEventListener("input", applyFilters);
  $("set").addEventListener("change", applyFilters);
  $("lang").addEventListener("change", applyFilters);

  $("stats").textContent = `Catalogo caricato: ${catalog.length} carte`;
  render(catalog.slice(0, 80));
}

init();
