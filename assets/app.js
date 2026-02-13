const $ = (id) => document.getElementById(id);
let catalog = [];
let sets = [];
let langs = [];

/* --------------------------- Normalizzazione ricerca --------------------------- */
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

/* --------------------------- Stato URL (q/set/lang) --------------------------- */
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
  if (state.q) url.searchParams.set("q", state.q);
  else url.searchParams.delete("q");
  if (state.set) url.searchParams.set("set", state.set);
  else url.searchParams.delete("set");
  if (state.lang) url.searchParams.set("lang", state.lang);
  else url.searchParams.delete("lang");
  history.replaceState(null, "", url.toString());
}

/* --------------------------- UI helpers --------------------------- */
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

/* --------------------------- Matching (PATCH short query exact match) --------------------------- */
function matchesCardForQuery(card, qn, isShort) {
  // Priorità: pokemonKey (già normalizzato lato catalogo)
  if (card.pokemonKey) {
    if (isShort) {
      // match ESATTO: "mew" non prende "mewtwo"
      if (card.pokemonKey === qn) return true;
    } else {
      if (card.pokemonKey.includes(qn)) return true;
    }
  }

  // Fallback: nome visualizzato e nameEn
  const nameN = norm(card.name || "");
  const enN = norm(card.nameEn || "");
  if (isShort) {
    // parola intera con boundary semplice (spazi/punteggiatura)
    const rx = new RegExp(`(^|[^a-z0-9])${qn}([^a-z0-9]|$)`, "i");
    return rx.test(nameN) || rx.test(enN);
  }
  return nameN.includes(qn) || enN.includes(qn);
}

/* --------------------------- Filtri + rendering --------------------------- */
function applyFilters({ writeURL = true } = {}) {
  const state = getStateFromUI();
  if (writeURL) writeStateToURL(state);

  const qn = norm(state.q);
  const set = state.set;
  const lang = state.lang;

  let res = catalog;
  if (set) res = res.filter(c => c.setId === set);
  if (lang) res = res.filter(c => c.lang === lang);

  if (qn) {
    // “short query”: 1 token, 1–4 char, solo lettere (evita numeri/codici)
    const isSingleToken = !qn.includes(" ");
    const isShort = isSingleToken && qn.length > 0 && qn.length <= 4 && !/[0-9]/.test(qn);

    if (isShort) {
      res = res.filter(c => matchesCardForQuery(c, qn, true));
    } else {
      // fallback: comportamento precedente (include anche set/numero/rarity ecc.)
      // + include pokemonKey nell'haystack (utile per JP con nameEn mappato)
      res = res.filter(c => {
        const hay = norm([
          c.pokemonKey,
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
        return hay.includes(qn);
      });
    }
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

    // Riga “compatta” coerente: setId + numberFull
    const lineSet = c.setId || (c.setName || "");
    const lineNum = c.numberFull || c.number || "";

    a.innerHTML = `
${c.name}

${lineSet}${lineNum ? " — " + lineNum : ""} — ${c.lang?.toUpperCase() || ""}

${c.rarity || ""} ${c.features?.length ? "• " + c.features.join(", ") : ""}
`;
    div.appendChild(a);
    root.appendChild(div);
  }
}

/* --------------------------- Init --------------------------- */
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
  sets = [...setMap.entries()]
    .map(([value, label]) => ({ value, label }))
    .sort((a, b) => a.label.localeCompare(b.label));
  langs = [...langMap.entries()]
    .map(([value, label]) => ({ value, label }))
    .sort((a, b) => a.label.localeCompare(b.label));

  buildOptions($("set"), sets, "Tutte le espansioni");
  buildOptions($("lang"), langs, "Tutte le lingue");

  // Ripristina stato dall’URL (se arrivi da card.html o da refresh)
  const urlState = readStateFromURL();
  applyStateToUI(urlState);

  $("q").addEventListener("input", () => applyFilters({ writeURL: true }));
  $("set").addEventListener("change", () => applyFilters({ writeURL: true }));
  $("lang").addEventListener("change", () => applyFilters({ writeURL: true }));

  $("stats").textContent = `Catalogo caricato: ${catalog.length} carte`;
  applyFilters({ writeURL: true });
}
init();
