import fs from "fs";
import fetch from "node-fetch";
import * as cheerio from "cheerio";

const DATA_DIR = "data";
const DAYS = 30;
const SKIP_CATALOG = process.env.SKIP_CATALOG === "1";
const CATALOG_STRATEGY = process.env.CATALOG_STRATEGY || "tcgdex"; // "tcgdex" | "split"
const MIN_CATALOG_CARDS = 12000;
const MIN_EN_CARDS = 8000;

// (TCGdex-only) abilita enrichment EN via dexId (molto più costoso: fa chiamate /cards/:id anche per EN)
const ENRICH_TCGDEX_EN_POKEMONKEY = process.env.ENRICH_TCGDEX_EN_POKEMONKEY === "1";

// cache locale per dexId -> nome inglese (per non martellare PokeAPI)
const DEX_CACHE_FILE = `${DATA_DIR}/dex_en_cache.json`;

// cache locale jaName -> { dexId, enName, pokemonKey }
const SPECIES_NAME_MAP_FILE = `${DATA_DIR}/poke_species_name_map.json`;

// eBay: categoria “Trading Card Singles”
const EBAY_CATEGORY_ID = "183454";
const USER_AGENT = "PokeGraadBot/0.6";

// Limitless JP (HTML)
const LIMITLESS_BASE = "https://limitlesstcg.com";
const LIMITLESS_JP_INDEX = `${LIMITLESS_BASE}/cards/jp`;

// --- Utils ---
function readJson(path, fallback) {
  try { return JSON.parse(fs.readFileSync(path, "utf8")); } catch { return fallback; }
}
function writeJson(path, obj) {
  fs.writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function todayISO() {
  return new Date().toISOString().slice(0, 19) + "Z";
}
function norm(s) {
  return (s || "")
    .toString()
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}
function median(nums) {
  const arr = nums
    .filter(n => typeof n === "number" && Number.isFinite(n))
    .sort((a, b) => a - b);
  if (!arr.length) return null;
  const mid = Math.floor(arr.length / 2);
  return arr.length % 2 ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2;
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function pickDexId(detail) {
  const d = detail?.dexId;
  if (Array.isArray(d) && d.length) return Number(d[0]) || null;
  if (typeof d === "number") return d;
  if (typeof d === "string" && d.trim()) {
    const n = Number(d.trim());
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function hasJapaneseChars(s) {
  const t = (s || "").toString();
  return /[\u3040-\u30ff\u3400-\u9fff]/.test(t);
}

function absUrl(maybeRelative) {
  if (!maybeRelative) return "";
  try { return new URL(maybeRelative, LIMITLESS_BASE).toString(); } catch { return ""; }
}

// TCGdex assets: {base}/{quality}.{ext}
function tcgdexImg(imageBase, quality = "high", ext = "webp") {
  if (!imageBase) return "";
  return `${imageBase}/${quality}.${ext}`;
}

// fetch JSON con un minimo di resilienza
async function fetchJson(url, opts = {}, retries = 4) {
  for (let i = 0; i <= retries; i++) {
    try {
      const resp = await fetch(url, opts);
      if (!resp.ok) {
        // retry su 429/5xx
        if ((resp.status === 429 || resp.status >= 500) && i < retries) {
          await sleep(400 * (i + 1));
          continue;
        }
        return null;
      }
      return await resp.json();
    } catch (e) {
      if (i < retries) {
        await sleep(400 * (i + 1));
        continue;
      }
      return null;
    }
  }
  return null;
}

// fetch HTML con resilienza
async function fetchHtml(url, opts = {}, retries = 4) {
  for (let i = 0; i <= retries; i++) {
    try {
      const resp = await fetch(url, opts);
      if (!resp.ok) {
        if ((resp.status === 429 || resp.status >= 500) && i < retries) {
          await sleep(500 * (i + 1));
          continue;
        }
        return null;
      }
      return await resp.text();
    } catch (e) {
      if (i < retries) {
        await sleep(500 * (i + 1));
        continue;
      }
      return null;
    }
  }
  return null;
}

async function fetchTCGdexCardDetail(lang, cardId) {
  const url = `https://api.tcgdex.net/v2/${lang}/cards/${encodeURIComponent(cardId)}`;
  return await fetchJson(url, { headers: { "user-agent": USER_AGENT } });
}

/* -------------------------------------------------------
   PokémonTCG.io EN (primary) con fallback
   - Prima era “silenzioso”: se payload.data manca, usciva e tornava []
   - Ora: logga status, gestisce 403/429 e fallisce esplicitamente
-------------------------------------------------------- */
async function fetchPokemonTcgEnCards() {
  const all = [];
  const pageSize = 250;
  let page = 1;
  let totalCount = Number.POSITIVE_INFINITY;

  while (all.length < totalCount) {
    const params = new URLSearchParams({
      page: String(page),
      pageSize: String(pageSize),
      q: "supertype:pokemon"
    });

    const url = `https://api.pokemontcg.io/v2/cards?${params.toString()}`;

    let lastStatus = null;
    let payload = null;

    // retry “manuale” qui, perché vogliamo distinguere bene 403/429
    for (let attempt = 0; attempt < 3; attempt++) {
      let resp;
      try {
        resp = await fetch(url, { headers: { "user-agent": USER_AGENT } });
      } catch (e) {
        lastStatus = "network_error";
        await sleep(400 * (attempt + 1));
        continue;
      }

      lastStatus = resp.status;

      if (resp.ok) {
        try {
          payload = await resp.json();
        } catch (e) {
          payload = null;
        }
        break;
      }

      // 429/5xx: retry con backoff
      if (resp.status === 429 || resp.status >= 500) {
        await sleep(700 * (attempt + 1));
        continue;
      }

      // 401/403/4xx: non ha senso insistere
      break;
    }

    if (!payload?.data) {
      console.error(`[PokemonTCG.io] EN fetch failed at page=${page} status=${lastStatus}`);
      throw new Error("PokemonTCG.io EN fetch failed");
    }

    totalCount = Number(payload.totalCount || 0);
    all.push(...payload.data);

    if (!payload.data.length) break;
    page += 1;

    if (page % 6 === 0) await sleep(200);
  }

  return all;
}

async function getPokemonNameEnByDexId(dexId) {
  const cache = readJson(DEX_CACHE_FILE, {});
  if (cache[dexId]) return cache[dexId];

  const url = `https://pokeapi.co/api/v2/pokemon-species/${dexId}/`;
  const j = await fetchJson(url, { headers: { "user-agent": USER_AGENT } }, 2);
  if (!j) return null;

  const nameEn =
    (j.names || []).find(x => x.language?.name === "en")?.name ||
    j.name ||
    null;

  if (nameEn) {
    cache[dexId] = nameEn;
    writeJson(DEX_CACHE_FILE, cache);
  }
  return nameEn;
}

// costruisce/legge la mappa jaName -> enName/pokemonKey (1 volta, poi cache su file)
async function getOrBuildSpeciesNameMap() {
  const cached = readJson(SPECIES_NAME_MAP_FILE, null);
  if (cached && typeof cached === "object" && Object.keys(cached).length > 0) return cached;

  const map = {}; // key: jaName (esatto) -> { dexId, enName, pokemonKey }
  let url = "https://pokeapi.co/api/v2/pokemon-species?limit=200&offset=0";

  while (url) {
    const page = await fetchJson(url, { headers: { "user-agent": USER_AGENT } }, 2);
    if (!page?.results?.length) break;

    for (const r of page.results) {
      const detail = await fetchJson(r.url, { headers: { "user-agent": USER_AGENT } }, 2);
      if (!detail) continue;

      const dexId = Number(detail.id) || null;
      const enName =
        (detail.names || []).find(x => x.language?.name === "en")?.name ||
        detail.name ||
        null;
      const jaName =
        (detail.names || []).find(x => x.language?.name === "ja")?.name ||
        null;

      if (dexId && enName && jaName) {
        map[jaName] = { dexId, enName, pokemonKey: norm(enName) };
      }
    }

    url = page.next || null;
    await sleep(150); // leggero throttle
  }

  writeJson(SPECIES_NAME_MAP_FILE, map);
  return map;
}

// --- helper: cardKey + pokemonKey ---
function makeCardKey(setId, number, printingLang) {
  return `${setId}|${number}|${printingLang}`;
}
async function getPokemonKeyEnByDexId(dexId) {
  const en = await getPokemonNameEnByDexId(dexId);
  return en ? norm(en) : null;
}

/* -------------------------------------------------------
   Limitless JP adapter + TCGdex image preference (PATCH)
   - Prima: era facile finire a fare /cards/:id per migliaia di carte (costosissimo)
   - Ora: crea una “image map” per setId|localId prendendo SOLO /ja/sets/:id
-------------------------------------------------------- */

async function buildTcgDexJaImageMapAll(setIds) {
  const out = new Map(); // setId -> Map(localId -> imageLarge)
  const base = "https://api.tcgdex.net/v2/ja";

  const unique = [...new Set((setIds || []).map(s => (s || "").toString().trim()).filter(Boolean))];
  let n = 0;

  for (const setId of unique) {
    n += 1;

    const set = await fetchJson(`${base}/sets/${encodeURIComponent(setId)}`, { headers: { "user-agent": USER_AGENT } }, 2);
    const map = new Map();

    if (set?.serie?.id === "tcgp") {
      out.set(setId, map);
      continue;
    }

    if (Array.isArray(set?.cards)) {
      for (const c of set.cards) {
        const localId = (c.localId ?? c.number ?? "").toString().trim();
        if (!localId) continue;
        const img = c.image ? tcgdexImg(c.image, "high", "webp") : "";
        if (img) map.set(localId, img);
      }
    }

    out.set(setId, map);

    if (n % 10 === 0) await sleep(250);
  }

  return out;
}

async function fetchLimitlessJpSetsIndex() {
  const html = await fetchHtml(LIMITLESS_JP_INDEX, { headers: { "user-agent": USER_AGENT } }, 3);
  if (!html) return [];

  const $ = cheerio.load(html);
  const seen = new Map(); // setId -> setName

  // Link tipo /cards/jp/SV2a
  $('a[href^="/cards/jp/"]').each((_, el) => {
    const href = $(el).attr("href") || "";
    const m = href.match(/^\/cards\/jp\/([^\/\?#]+)$/i);
    if (!m) return;

    const setId = (m[1] || "").trim();
    if (!setId) return;

    const label = ($(el).text() || "").replace(/\s+/g, " ").trim();
    let setName = label || setId;

    if (label.toLowerCase().startsWith(setId.toLowerCase())) {
      setName = label.slice(setId.length).replace(/^[\s—-]+/, "").trim() || setId;
    }

    if (!seen.has(setId)) seen.set(setId, setName);
  });

  // fallback se la struttura pagina cambia
  if (seen.size === 0) {
    $("table a").each((_, el) => {
      const href = $(el).attr("href") || "";
      const m = href.match(/^\/cards\/jp\/([^\/\?#]+)$/i);
      if (!m) return;

      const setId = (m[1] || "").trim();
      if (!setId) return;

      const label = ($(el).text() || "").replace(/\s+/g, " ").trim();
      if (!seen.has(setId)) seen.set(setId, label || setId);
    });
  }

  return [...seen.entries()].map(([setId, setName]) => ({ setId, setName }));
}

function parseLimitlessSetCards(html, setId, setName) {
  const $ = cheerio.load(html);
  const tmp = [];

  // Link carta: /cards/jp/{setId}/{number}
  $(`a[href^="/cards/jp/${setId}/"]`).each((_, a) => {
    const href = $(a).attr("href") || "";
    const m = href.match(new RegExp(`^\\/cards\\/jp\\/${setId}\\/([^\\/?#]+)$`, "i"));
    if (!m) return;

    const number = (m[1] || "").toString().trim();
    if (!number) return;

    const row = $(a).closest("tr");

    // Nome: prova in ordine (aria/attr -> testo link -> testo riga) e scegli quello con giapponese se presente
    const cand = [];
    const t1 = (($(a).attr("title") || "")).replace(/\s+/g, " ").trim();
    if (t1) cand.push(t1);

    const t2 = (($(a).attr("aria-label") || "")).replace(/\s+/g, " ").trim();
    if (t2) cand.push(t2);

    const t3 = (($(a).text() || "")).replace(/\s+/g, " ").trim();
    if (t3) cand.push(t3);

    if (row && row.length) {
      const t4 = (row.find("td").toArray().map(td => $(td).text().replace(/\s+/g, " ").trim()).join(" | ") || "").trim();
      if (t4) cand.push(t4);
    }

    let nameJa = null;
    for (const c of cand) {
      // “pulizia” euristica: prendi token più plausibile
      const parts = c.split("|").map(x => x.trim()).filter(Boolean);
      for (const p of parts) {
        if (hasJapaneseChars(p) && p.length <= 40) { nameJa = p; break; }
      }
      if (nameJa) break;
    }

    // se non trovato un “vero” giapponese, fallback al testo del link (può essere romaji)
    if (!nameJa) nameJa = t3 || null;

    // immagine dalla riga
    let imageLarge = "";
    if (row && row.length) {
      const img = row.find("img").first();
      const src = img.attr("src") || img.attr("data-src") || "";
      imageLarge = absUrl(src);
    }

    const sourceId = absUrl(href);

    tmp.push({ setId, setName, number, nameJa, imageLarge, sourceId });
  });

  // dedup per number
  const byKey = new Map();
  for (const c of tmp) {
    const k = `${c.setId}|${c.number}`;
    if (!byKey.has(k)) byKey.set(k, c);
    else {
      const prev = byKey.get(k);
      if ((!prev.imageLarge && c.imageLarge) || (!prev.nameJa && c.nameJa) || (!hasJapaneseChars(prev.nameJa) && hasJapaneseChars(c.nameJa))) {
        byKey.set(k, c);
      }
    }
  }

  return [...byKey.values()];
}

async function fetchLimitlessCardDetail(cardUrl) {
  const html = await fetchHtml(cardUrl, { headers: { "user-agent": USER_AGENT } }, 2);
  if (!html) return { dexId: null, nameJa: null, imageLarge: "" };

  const $ = cheerio.load(html);

  // Nome: h1 spesso contiene il nome. Se contiene kana/kanji, ottimo.
  const h1 = ($("h1").first().text() || "").replace(/\s+/g, " ").trim();
  let nameJa = hasJapaneseChars(h1) ? h1 : null;

  // fallback: primo testo breve che contenga caratteri giapponesi
  if (!nameJa) {
    const cand = $("body").find("*").toArray()
      .map(el => ($(el).text() || "").replace(/\s+/g, " ").trim())
      .find(t => t && t.length <= 40 && hasJapaneseChars(t));
    if (cand) nameJa = cand;
  }

  // DexId: regex su testo pagina
  const bodyText = $("body").text().replace(/\s+/g, " ");
  let dexId = null;

  const m1 = bodyText.match(/Pok[ée]dex\s*[:#]?\s*(\d{1,4})/i);
  if (m1) dexId = Number(m1[1]) || null;

  if (!dexId) {
    const m2 = bodyText.match(/National\s+Pok[ée]dex\s*[:#]?\s*(\d{1,4})/i);
    if (m2) dexId = Number(m2[1]) || null;
  }

  // Immagine: og:image se presente
  let imageLarge = "";
  const og = $('meta[property="og:image"]').attr("content") || "";
  if (og) imageLarge = absUrl(og);

  if (!imageLarge) {
    const img = $("img").toArray()
      .map(el => $(el).attr("src") || $(el).attr("data-src") || "")
      .map(src => absUrl(src))
      .find(src => src && /card|cards|image|img/i.test(src));
    if (img) imageLarge = img;
  }

  return { dexId, nameJa, imageLarge };
}

async function fetchJapaneseCatalogCards() {
  const sets = await fetchLimitlessJpSetsIndex();
  if (!sets.length) return [];

  const out = [];
  let i = 0;

  for (const s of sets) {
    i += 1;
    const setId = s.setId;
    const setName = s.setName || setId;

    const url = `${LIMITLESS_JP_INDEX}/${encodeURIComponent(setId)}`;
    const html = await fetchHtml(url, { headers: { "user-agent": USER_AGENT } }, 3);
    if (!html) continue;

    const cards = parseLimitlessSetCards(html, setId, setName);
    out.push(...cards);

    if (i % 10 === 0) await sleep(350);
  }

  return out;
}

/* -------------------------------------------------------
   1) Catalogo (TCGdex)
   - include EN + JA
   - esclude TCG Pocket (serie tcgp)
   - per JP-only prova a valorizzare nameEn + pokemonKey via dexId
   - opzionale: EN pokemonKey via dexId (flag ENRICH_TCGDEX_EN_POKEMONKEY=1)
   - NEW: jpSetIds (deriva lingua di stampa dal fatto che il set esiste sotto /ja)
   - NEW: fallback JP da nameJa tramite SPECIES_NAME_MAP_FILE quando dexId manca
-------------------------------------------------------- */
async function buildCatalogFromTCGdex() {
  const langs = ["en", "ja"];
  const agg = new Map(); // key = setId|localId
  const jpSetIds = new Set(); // set visti sotto /ja (stampa JP)

  for (const lang of langs) {
    const base = `https://api.tcgdex.net/v2/${lang}`;

    const sets = await fetchJson(`${base}/sets`, { headers: { "user-agent": USER_AGENT } }, 2);
    if (!sets) throw new Error(`TCGdex sets (${lang}) failed`);

    for (const s of sets) {
      const set = await fetchJson(`${base}/sets/${encodeURIComponent(s.id)}`, { headers: { "user-agent": USER_AGENT } }, 2);
      if (!set) continue;

      // IMPORTANT: escludi TCG Pocket
      if (set.serie?.id === "tcgp") continue;

      const setId = set.id || s.id;
      const setName = set.name || s.name || setId;

      // marca i set che compaiono sotto /ja
      if (lang === "ja" && setId) jpSetIds.add(setId);

      const total =
        set.cardCount?.official ??
        set.cardCount?.total ??
        null;

      if (!Array.isArray(set.cards)) continue;

      for (const c of set.cards) {
        const name = c.name || "Unknown";
        const localId = (c.localId ?? c.number ?? "").toString().trim();
        if (!setId || !localId) continue;

        const key = `${setId}|${localId}`;

        const img = c.image
          ? tcgdexImg(c.image, "high", "webp")
          : (c.imageLarge || c.images?.large || "");

        const prev = agg.get(key) ?? {
          setId,
          setName,
          number: localId,
          numberFull: total ? `${localId}/${total}` : null,
          rarity: c.rarity || null,
          features: c.rarity ? [c.rarity] : [],
          imageLarge: img || "",
          nameEn: null,
          nameJa: null,
          cardIdEn: null,
          cardIdJa: null,
          pokemonKey: null
        };

        if (lang === "en") {
          prev.nameEn = name;
          prev.cardIdEn = c.id || prev.cardIdEn;
        } else if (lang === "ja") {
          prev.nameJa = name;
          prev.cardIdJa = c.id || prev.cardIdJa;
        }

        if (!prev.imageLarge && img) prev.imageLarge = img;

        if (!prev.rarity && c.rarity) prev.rarity = c.rarity;
        if ((!prev.features || !prev.features.length) && c.rarity) prev.features = [c.rarity];

        agg.set(key, prev);
      }
    }
  }

  // helper locale: lingua di stampa stimata (prima jpSetIds, poi fallback inferLangFromSetCode)
  function inferPrintingLangFromSetId(setId) {
    if (jpSetIds.has(setId)) return "ja";
    return inferLangFromSetCode(setId); // fallback euristico dove serve
  }

  // Enrichment: immagini via detail se mancanti; pokemonKey/nameEn via dexId (JP always; EN opzionale)
  let detailFetches = 0;

  // species map caricata una volta
  const speciesMap = await getOrBuildSpeciesNameMap();

  for (const v of agg.values()) {
    const inferred = inferPrintingLangFromSetId(v.setId); // "ja" | null

    // se manca immagine, tenta dal dettaglio della lingua disponibile
    if (!v.imageLarge) {
      const lang = v.cardIdJa ? "ja" : (v.cardIdEn ? "en" : null);
      const id = v.cardIdJa || v.cardIdEn;
      if (lang && id) {
        const detail = await fetchTCGdexCardDetail(lang, id);
        detailFetches++;
        if (detail?.image) v.imageLarge = tcgdexImg(detail.image, "high", "webp");
        if (detailFetches % 40 === 0) await sleep(700);
      }
    }

    // JP sets: riempi nameEn + pokemonKey via dexId; fallback da nameJa->speciesMap se dexId manca
    if (inferred === "ja" && v.cardIdJa && (!v.nameEn || !v.pokemonKey)) {
      const detail = await fetchTCGdexCardDetail("ja", v.cardIdJa);
      detailFetches++;

      const dex = pickDexId(detail);

      if (dex != null) {
        if (!v.nameEn) {
          const enName = await getPokemonNameEnByDexId(dex);
          if (enName) v.nameEn = enName;
        }
        if (!v.pokemonKey) {
          const pk = await getPokemonKeyEnByDexId(dex);
          if (pk) v.pokemonKey = pk;
        }
      } else {
        const jaName = v.nameJa || null;
        const hit = jaName ? speciesMap[jaName] : null;
        if (hit) {
          if (!v.nameEn) v.nameEn = hit.enName;
          if (!v.pokemonKey) v.pokemonKey = hit.pokemonKey;
        }
      }

      if (detailFetches % 40 === 0) await sleep(700);
    }

    // EN (non-JP sets): opzionale -> pokemonKey via dexId (costoso)
    if (inferred !== "ja" && ENRICH_TCGDEX_EN_POKEMONKEY && v.cardIdEn && !v.pokemonKey) {
      const detail = await fetchTCGdexCardDetail("en", v.cardIdEn);
      detailFetches++;

      const dex = pickDexId(detail);
      if (dex != null) {
        const pk = await getPokemonKeyEnByDexId(dex);
        if (pk) v.pokemonKey = pk;
      }

      if (detailFetches % 40 === 0) await sleep(700);
    }
  }

  // Esplodi in record per lingua (lang = lingua di stampa)
  const out = [];

  for (const v of agg.values()) {
    const inferred = inferPrintingLangFromSetId(v.setId); // "ja" | null

    // Caso: set JP -> UNA SOLA entry, lang=ja
    if (inferred === "ja") {
      const displayJa = v.nameJa || v.nameEn;
      if (!displayJa) continue;

      out.push({
        id: `${v.setId}-${v.number}-${norm(v.nameEn || displayJa)}-ja`,
        cardKey: makeCardKey(v.setId, v.number, "ja"),
        name: displayJa,
        nameEn: v.nameEn || null,
        nameJa: v.nameJa || null,
        pokemonKey: v.pokemonKey || (v.nameEn ? norm(v.nameEn) : null),
        lang: "ja",
        setId: v.setId,
        setName: v.setName,
        number: v.number,
        numberFull: v.numberFull,
        rarity: v.rarity,
        features: v.features,
        imageLarge: v.imageLarge
      });

      continue;
    }

    // Caso: non-JP -> puoi avere davvero EN e/o JA distinti
    if (v.nameEn) {
      out.push({
        id: `${v.setId}-${v.number}-${norm(v.nameEn)}-en`,
        cardKey: makeCardKey(v.setId, v.number, "en"),
        name: v.nameEn,
        nameEn: v.nameEn,
        nameJa: v.nameJa || null,
        pokemonKey: v.pokemonKey || norm(v.nameEn),
        lang: "en",
        setId: v.setId,
        setName: v.setName,
        number: v.number,
        numberFull: v.numberFull,
        rarity: v.rarity,
        features: v.features,
        imageLarge: v.imageLarge
      });
    }

    if (v.nameJa) {
      out.push({
        id: `${v.setId}-${v.number}-${norm(v.nameEn || v.nameJa)}-ja`,
        cardKey: makeCardKey(v.setId, v.number, "ja"),
        name: v.nameJa,
        nameEn: v.nameEn || null,
        nameJa: v.nameJa,
        pokemonKey: v.pokemonKey || (v.nameEn ? norm(v.nameEn) : null),
        lang: "ja",
        setId: v.setId,
        setName: v.setName,
        number: v.number,
        numberFull: v.numberFull,
        rarity: v.rarity,
        features: v.features,
        imageLarge: v.imageLarge
      });
    }
  }

  return { cards: out };
}

/* -------------------------------------------------------
   EN fallback: TCGdex (solo EN, senza per-card detail)
-------------------------------------------------------- */
async function buildEnglishCatalogFromTcgdexFallback() {
  const base = "https://api.tcgdex.net/v2/en";
  const sets = await fetchJson(`${base}/sets`, { headers: { "user-agent": USER_AGENT } }, 2);
  if (!sets) throw new Error("TCGdex EN sets failed (fallback)");

  const out = [];
  let n = 0;

  for (const s of sets) {
    n += 1;
    const set = await fetchJson(`${base}/sets/${encodeURIComponent(s.id)}`, { headers: { "user-agent": USER_AGENT } }, 2);
    if (!set) continue;

    if (set.serie?.id === "tcgp") continue;
    if (!Array.isArray(set.cards)) continue;

    const setId = (set.id || s.id || "").toString().trim();
    const setName = set.name || s.name || setId || "Unknown";
    if (!setId) continue;

    const total = set.cardCount?.official ?? set.cardCount?.total ?? null;

    for (const c of set.cards) {
      const localId = (c.localId ?? c.number ?? "").toString().trim();
      if (!localId || !c.name) continue;

      const img = c.image ? tcgdexImg(c.image, "high", "webp") : "";
      out.push({
        id: `${setId}-${localId}-${norm(c.name)}-en`,
        cardKey: makeCardKey(setId, localId, "en"),
        name: c.name,
        nameEn: c.name,
        nameJa: null,
        pokemonKey: norm(c.name), // niente dexId qui: è solo fallback di copertura
        lang: "en",
        setId,
        setName,
        number: localId,
        numberFull: total ? `${localId}/${total}` : null,
        rarity: c.rarity || null,
        features: c.rarity ? [c.rarity] : [],
        imageLarge: img || ""
      });
    }

    if (n % 8 === 0) await sleep(250);
  }

  return out;
}

/* -------------------------------------------------------
   Split sources (PATCH)
   - EN: PokémonTCG.io (primary) + fallback TCGdex EN se fallisce
   - JP: Limitless (/cards/jp) pagine set + (on-demand) pagine carta
   - Immagini JP: preferisci TCGdex image map, altrimenti Limitless
   - IMPORTANTE: niente più “TCGdex card detail per ogni carta JP”
-------------------------------------------------------- */
async function buildCatalogFromSplitSources() {
  // speciesMap una volta
  const speciesMap = await getOrBuildSpeciesNameMap();

  const out = [];

  // --- EN (primary PokémonTCG.io, fallback TCGdex EN) ---
  try {
    const fromEnApi = await fetchPokemonTcgEnCards();

    for (const c of fromEnApi) {
      const setId = (c.set?.id || c.set?.ptcgoCode || "").toString().toLowerCase();
      const setName = c.set?.name || setId || "Unknown";
      const number = (c.number || "").toString().trim();
      if (!setId || !number || !c.name) continue;

      const dexId = Array.isArray(c.nationalPokedexNumbers) && c.nationalPokedexNumbers.length
        ? Number(c.nationalPokedexNumbers[0])
        : null;

      const pokemonKey = dexId ? await getPokemonKeyEnByDexId(dexId) : norm(c.name);

      const total = c.set?.printedTotal ?? c.set?.total ?? null;
      out.push({
        id: `${setId}-${number}-${norm(c.name)}-en`,
        cardKey: makeCardKey(setId, number, "en"),
        name: c.name,
        nameEn: c.name,
        nameJa: null,
        pokemonKey,
        lang: "en",
        setId,
        setName,
        number,
        numberFull: total ? `${number}/${total}` : null,
        rarity: c.rarity || null,
        features: c.rarity ? [c.rarity] : [],
        imageLarge: c.images?.large || ""
      });
    }
  } catch (e) {
    console.error(`[Split] EN primary failed (${e.message}). Falling back to TCGdex EN.`);
    const fallbackEn = await buildEnglishCatalogFromTcgdexFallback();
    out.push(...fallbackEn);
  }

  // --- JP via Limitless ---
  const jpRaw = await fetchJapaneseCatalogCards();
  if (!jpRaw.length) return { cards: out };

  // Pre-build image maps una sola volta (per set)
  const jpSetIds = [...new Set(jpRaw.map(x => (x.setId || "").toString().trim()).filter(Boolean))];
  const tcgdexJaImgMaps = await buildTcgDexJaImageMapAll(jpSetIds);

  let detailFetches = 0;
  let tcgdexImgHits = 0;
  let tcgdexImgMiss = 0;

  for (const c of jpRaw) {
    const setId = (c.setId || "").toString().trim();
    const setName = c.setName || setId || "Unknown";
    const number = (c.number || "").toString().trim();
    if (!setId || !number) continue;

    // nameJa dal listing: dovrebbe essere giapponese; se è romaji, lo gestiamo dopo
    let nameJa = c.nameJa || null;

    // immagini: preferisci TCGdex se esiste per quel set/numero
    let imageLarge = c.imageLarge || "";
    const m = tcgdexJaImgMaps.get(setId);
    const tcgImg = m ? m.get(number) : null;
    if (tcgImg) {
      imageLarge = tcgImg;
      tcgdexImgHits++;
    } else {
      tcgdexImgMiss++;
    }

    // enrichment: cerchiamo di valorizzare nameEn/pokemonKey senza fare fetch a raffica
    let dexId = null;
    let nameEn = null;
    let pokemonKey = null;

    // 1) se nameJa è già giapponese -> speciesMap subito
    if (nameJa && hasJapaneseChars(nameJa)) {
      const hit = speciesMap[nameJa];
      if (hit) {
        nameEn = hit.enName;
        pokemonKey = hit.pokemonKey;
      }
    }

    // 2) se non risolto, proviamo pagina carta SOLO se serve davvero
    //    Nota: questo rimane potenzialmente costoso, ma dovrebbe attivarsi solo quando il listing non ha il nome giapponese vero.
    if ((!nameEn || !pokemonKey) && c.sourceId) {
      // riduci pressione: se già abbiamo nameJa giapponese ma non matcha speciesMap, non ha senso fare detail per tutti.
      // facciamolo solo se nameJa NON contiene caratteri giapponesi (romaji) oppure è proprio vuoto.
      const mustDetail = !nameJa || !hasJapaneseChars(nameJa);

      if (mustDetail) {
        const detail = await fetchLimitlessCardDetail(c.sourceId);
        detailFetches++;

        if (detail?.dexId) dexId = detail.dexId;

        if (detail?.nameJa && hasJapaneseChars(detail.nameJa)) {
          nameJa = detail.nameJa;
        }

        if (!imageLarge && detail?.imageLarge) imageLarge = detail.imageLarge;

        // retry speciesMap
        const hit2 = nameJa && hasJapaneseChars(nameJa) ? speciesMap[nameJa] : null;
        if (hit2) {
          nameEn = hit2.enName;
          pokemonKey = hit2.pokemonKey;
        }

        if (detailFetches % 50 === 0) await sleep(650);
      }
    }

    // 3) se abbiamo dexId, enrichment “forte”
    if (dexId != null && (!nameEn || !pokemonKey)) {
      if (!nameEn) {
        const enName = await getPokemonNameEnByDexId(dexId);
        if (enName) nameEn = enName;
      }
      if (!pokemonKey) {
        const pk = await getPokemonKeyEnByDexId(dexId);
        if (pk) pokemonKey = pk;
      }
    }

    out.push({
      id: `${setId}-${number}-${norm(nameEn || nameJa || `${setId}-${number}`)}-ja`,
      cardKey: makeCardKey(setId, number, "ja"),
      name: nameJa || nameEn || `${setId} ${number}`,
      nameEn: nameEn || null,
      nameJa: nameJa || null,
      pokemonKey: pokemonKey || (nameEn ? norm(nameEn) : null),
      lang: "ja",
      setId,
      setName,
      number,
      numberFull: null,
      rarity: null,
      features: [],
      imageLarge,
      sourceId: c.sourceId || null
    });
  }

  console.log(
    `JP(Limitless) cards=${jpRaw.length} tcgdexImgHits=${tcgdexImgHits} tcgdexImgMiss=${tcgdexImgMiss} cardDetailFetches=${detailFetches}`
  );

  return { cards: out };
}

function validateCatalogShape(catalog) {
  const cards = Array.isArray(catalog?.cards) ? catalog.cards : [];
  const langCounts = cards.reduce((acc, c) => {
    const k = c?.lang || "unknown";
    acc[k] = (acc[k] || 0) + 1;
    return acc;
  }, {});

  if (cards.length < MIN_CATALOG_CARDS) {
    throw new Error(`Catalog too small: ${cards.length} cards`);
  }

  if ((langCounts.en || 0) < MIN_EN_CARDS) {
    throw new Error(`Catalog EN coverage too low: ${langCounts.en || 0}`);
  }

  return langCounts;
}

/* -------------------------------------------------------
   2) Scraping vendite eBay.it
-------------------------------------------------------- */
function isLikelyLot(title) {
  const t = norm(title);
  return (
    /\blot\b/.test(t) ||
    /\bbundle\b/.test(t) ||
    /\bplayset\b/.test(t) ||
    /\bchoose\b/.test(t) ||
    /\bseleziona\b/.test(t) ||
    /\b(\d+)\s*(cards|carte)\b/.test(t)
  );
}

function parseEurPrice(text) {
  const m = (text || "").replace(/\./g, "").match(/(\d+,\d{1,2}|\d+)(?=\s*€|\s*eur)/i);
  if (!m) return null;
  const v = Number(m[1].replace(",", "."));
  return Number.isFinite(v) ? v : null;
}

function detectLangFromTitle(title) {
  const t = norm(title);
  if (/\b(jap|jpn|jp|giapponese)\b/.test(t)) return "ja";
  if (/\b(eng|english|en|inglese)\b/.test(t)) return "en";
  return null;
}

// --- fallback euristico (usato SOLO dove non hai jpSetIds)
function inferLangFromSetCode(setCode) {
  const code = norm(setCode || "");

  // JP mainline special sets spesso finiscono con "a"
  // Esempi: sv2a, sv9a, s8a, s12a, sm12a, xy8a
  if (/^(sv|s|sm|bw|xy)\d{1,3}a$/.test(code)) return "ja";

  return null;
}

function detectGraadBucket(title) {
  const t = norm(title);
  if (!t.includes("graad")) return null;

  const m = t.match(/graad\s*([0-9]{1,2}(?:[.,]5)?)/);
  if (!m) return "graad_unknown";

  const g = Number(m[1].replace(",", "."));
  if (g === 10) return "graad_10";
  if (g === 9.5) return "graad_9_5";
  if (g === 9) return "graad_9";
  if (g === 8) return "graad_8";
  if (g === 7) return "graad_7";

  if (g > 9 && g < 9.5) return "graad_9";
  if (g > 8 && g < 9) return "graad_8";
  if (g > 7 && g < 8) return "graad_7";

  return "graad_unknown";
}

function extractSetCode(title) {
  const t = norm(title);
  // sv2a / sv9a / sv4a ecc, e anche codici tipo mbd
  const m = t.match(/\b(sv\d{1,2}[a-z]?|m[a-z]{1,3})\b/);
  return m ? m[0] : null;
}

function extractLocalId(title) {
  const raw = title || "";

  // 181/165 -> 181 (caso più affidabile)
  const m1 = raw.match(/(\d{1,3})\s*\/\s*(\d{1,3})/);
  if (m1) return m1[1];

  // promo/seriale tipo BW68 / SVP123 / mBD022
  const mPromo = raw.match(/\b([A-Z]{1,4}\d{1,4})\b/);
  if (mPromo) return mPromo[1];

  // evita di prendere il voto GRAAD (es. 9.5, 10)
  const rawWithoutGrade = raw.replace(/graad\s*[0-9]{1,2}(?:[.,]5)?/ig, " ");

  // #181 o 181 (fallback)
  const m2 = rawWithoutGrade.match(/\b#?\s*(\d{2,3})\b/);
  if (m2) return m2[1];

  return null;
}

function titleHasName(tNorm, card) {
  // per JA spesso nel titolo c'è l'inglese, quindi matchiamo anche nameEn
  return (
    tNorm.includes(norm(card.name)) ||
    (card.nameEn && tNorm.includes(norm(card.nameEn)))
  );
}

function bestMatchCard(catalog, title) {
  const t = norm(title);
  if (isLikelyLot(t)) return { card: null, confidence: 0 };

  const lang = detectLangFromTitle(t);
  const setCode = extractSetCode(title);   // può essere sbagliato nel titolo

  // qui NON hai jpSetIds: fallback euristico ok
  const langBySet = inferLangFromSetCode(setCode);

  const finalLang = lang || langBySet;
  const localId = extractLocalId(title);
  const cards = catalog.cards || [];

  if (!localId) {
    // Fallback: alcuni titoli eBay non riportano il numero carta.
    const byName = cards.filter(c => {
      if (lang && c.lang !== lang) return false;
      if (!lang && finalLang && c.lang !== finalLang) return false;
      if (setCode && norm(c.setId) !== norm(setCode)) return false;
      return titleHasName(t, c);
    });

    if (!byName.length) return { card: null, confidence: 0 };

    let best = byName[0];
    for (const c of byName) if (c.imageLarge && !best.imageLarge) best = c;

    let conf = 0.72;
    if (setCode) conf += 0.05;
    if (finalLang) conf += 0.03;
    return { card: best, confidence: Math.min(conf, 0.82), mode: "name_only" };
  }

  // PASS 1 (strict): lingua + setCode + numero + nome
  let strict = cards.filter(c => {
    if (lang && c.lang !== lang) return false;
    if (!lang && finalLang && c.lang !== finalLang) return false;
    if (setCode && norm(c.setId) !== norm(setCode)) return false;
    if (c.number && norm(c.number) !== norm(localId)) return false;
    return titleHasName(t, c);
  });

  if (strict.length) {
    let best = strict[0];
    for (const c of strict) if (c.imageLarge && !best.imageLarge) best = c;

    let conf = 0.86;
    if (finalLang) conf += 0.04;
    return { card: best, confidence: Math.min(conf, 1.0), mode: "strict" };
  }

  // PASS 2 (loose): numero + nome + (lingua se presente)
  let loose = cards.filter(c => {
    if (lang && c.lang !== lang) return false;
    if (!lang && finalLang && c.lang !== finalLang) return false;
    if (c.number && norm(c.number) !== norm(localId)) return false;
    return titleHasName(t, c);
  });

  if (!loose.length) return { card: null, confidence: 0 };

  let best = loose[0];

  if (setCode) {
    const sameFamily = loose.find(c => norm(c.setId).startsWith(norm(setCode).slice(0, 2)));
    if (sameFamily) best = sameFamily;
  }

  for (const c of loose) if (c.imageLarge && !best.imageLarge) best = c;

  let conf = 0.80;
  if (finalLang) conf += 0.05;
  return { card: best, confidence: Math.min(conf, 0.90), mode: "loose" };
}

async function fetchEbaySoldPageHTML({ keyword, page = 1, categoryId = EBAY_CATEGORY_ID, gradedOnly = false }) {
  const params = new URLSearchParams({
    _nkw: keyword,
    LH_Sold: "1",
    LH_Complete: "1",
    rt: "nc",
    _pgn: String(page),
    _sacat: categoryId
  });

  // Condizione “Graded” (se disponibile in quella categoria)
  if (gradedOnly) params.set("LH_ItemCondition", "2750");

  const url = `https://www.ebay.it/sch/i.html?${params.toString()}`;

  const resp = await fetch(url, {
    headers: { "user-agent": `Mozilla/5.0 (compatible; ${USER_AGENT})` }
  });
  if (!resp.ok) throw new Error(`eBay fetch failed: ${resp.status}`);
  return await resp.text();
}

function parseEbaySearchItems(html) {
  const $ = cheerio.load(html);
  const items = [];
  $(".s-item").each((_, el) => {
    const title = $(el).find(".s-item__title").text().trim();
    const priceText = $(el).find(".s-item__price").text().trim();
    const link = $(el).find(".s-item__link").attr("href");

    if (!title || title === "Shop on eBay") return;
    const price = parseEurPrice(priceText);
    if (!link || price == null) return;

    items.push({ title, price_eur: price, url: link });
  });
  return items;
}

/* -------------------------------------------------------
   Pipeline principale
-------------------------------------------------------- */
async function main() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

  // crea file cache se mancanti (workflow committa data/)
  if (!fs.existsSync(DEX_CACHE_FILE)) writeJson(DEX_CACHE_FILE, {});
  if (!fs.existsSync(SPECIES_NAME_MAP_FILE)) writeJson(SPECIES_NAME_MAP_FILE, {});

  // A) catalogo
  const catalogFile = `${DATA_DIR}/catalog.json`;
  let catalog = readJson(catalogFile, { cards: [] });

  if (!SKIP_CATALOG) {
    try {
      catalog = CATALOG_STRATEGY === "split"
        ? await buildCatalogFromSplitSources()
        : await buildCatalogFromTCGdex();

      const langCounts = validateCatalogShape(catalog);
      console.log(`Catalog rebuilt: total=${catalog.cards.length} en=${langCounts.en || 0} ja=${langCounts.ja || 0}`);
      writeJson(catalogFile, catalog);
    } catch (e) {
      console.error("Catalog build failed:", e.message);
      if (process.env.STRICT_CATALOG === "1") process.exit(1);
      catalog = readJson(catalogFile, { cards: [] });
    }
  } else {
    if (!catalog.cards?.length) {
      console.warn("SKIP_CATALOG=1 ma catalog.json è vuoto: rigenero comunque.");
      catalog = await buildCatalogFromTCGdex();
      writeJson(catalogFile, catalog);
    }
  }

  // B) storico vendite rolling 30 giorni
  const salesFile = `${DATA_DIR}/sales_30d.json`;
  const salesObj = readJson(salesFile, { sales: [] });

  const cutoff = Date.now() - DAYS * 24 * 3600 * 1000;
  const kept = (salesObj.sales || []).filter(s => new Date(s.collectedAt).getTime() >= cutoff);

  // C) raccolta vendite: query mirate (eBay sold è rumoroso)
  const collectedAt = todayISO();

  const gradedQueries = [
    `"GRAAD" pokemon -psa -bgs -bsg -cgc`,
    `"GRAAD" sv2a -psa -bgs -bsg -cgc`,
    `"GRAAD" sv9a -psa -bgs -bsg -cgc`,
    `"GRAAD" jap -psa -bgs -bsg -cgc`
  ];

  const rawQueries = [
    `pokemon sv2a 181/165 -psa -bgs -bsg -cgc -graad -graded`,
    `pokemon meloetta 022/021 -psa -bgs -bsg -cgc -graad -graded`
  ];

  const queries = [
    ...gradedQueries.map(q => ({ keyword: q, gradedOnly: true })),
    ...rawQueries.map(q => ({ keyword: q, gradedOnly: false }))
  ];

  const PAGES_PER_QUERY = 2;

  const newSales = [];
  for (const q of queries) {
    for (let page = 1; page <= PAGES_PER_QUERY; page++) {
      let html;
      try {
        html = await fetchEbaySoldPageHTML({
          keyword: q.keyword,
          page,
          categoryId: EBAY_CATEGORY_ID,
          gradedOnly: q.gradedOnly
        });
      } catch (e) {
        console.error("eBay fetch error:", q.keyword, e.message);
        continue;
      }

      const items = parseEbaySearchItems(html);

      for (const it of items) {
        if (isLikelyLot(it.title)) continue;

        const bucket = detectGraadBucket(it.title);
        const graded = bucket && bucket.startsWith("graad_");
        const priceBucket = graded ? bucket : "raw";

        // Se la query è “gradedOnly”, ma il titolo non contiene graad, skip:
        if (q.gradedOnly && !graded) continue;

        const match = bestMatchCard(catalog, it.title);
        if (!match.card || match.confidence < 0.72) continue;

        newSales.push({
          collectedAt,
          source: "ebay.it",
          title: it.title,
          url: it.url,
          price_eur: it.price_eur,
          cardId: match.card.id,
          bucket: priceBucket
        });
      }
    }
  }

  const matchedCount = newSales.length;
  console.log(`Sales collected: matched=${matchedCount} kept_before_dedup=${kept.length}`);

  // dedup su (url + price + cardId + bucket)
  const seen = new Set(kept.map(s => `${s.url}|${s.price_eur}|${s.cardId}|${s.bucket}`));
  for (const s of newSales) {
    const k = `${s.url}|${s.price_eur}|${s.cardId}|${s.bucket}`;
    if (!seen.has(k)) {
      seen.add(k);
      kept.push(s);
    }
  }

  writeJson(salesFile, { sales: kept });

  // D) mediane 30 giorni per carta/bucket
  const byCard = {};
  for (const s of kept) {
    byCard[s.cardId] ??= {
      raw: [],
      graad_7: [],
      graad_8: [],
      graad_9: [],
      graad_9_5: [],
      graad_10: []
    };
    if (byCard[s.cardId][s.bucket]) byCard[s.cardId][s.bucket].push(s.price_eur);
  }

  const priceOut = { byCard: {} };
  for (const [cardId, buckets] of Object.entries(byCard)) {
    priceOut.byCard[cardId] = {};
    for (const [bucket, arr] of Object.entries(buckets)) {
      priceOut.byCard[cardId][bucket] = { median_eur: median(arr), n: arr.length };
    }
  }

  writeJson(`${DATA_DIR}/prices.json`, priceOut);
  writeJson(`${DATA_DIR}/meta.json`, { updatedAt: collectedAt });
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
