import fs from "fs";
import fetch from "node-fetch";
import * as cheerio from "cheerio";

const DATA_DIR = "data";
const DAYS = 30;
const SKIP_CATALOG = process.env.SKIP_CATALOG === "1";

// cache locale per dexId -> nome inglese (per non martellare PokeAPI)
const DEX_CACHE_FILE = `${DATA_DIR}/dex_en_cache.json`;

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

// TCGdex assets: {base}/{quality}.{ext}
function tcgdexImg(imageBase, quality = "high", ext = "webp") {
  if (!imageBase) return "";
  return `${imageBase}/${quality}.${ext}`;
}

async function fetchJson(url, opts = {}) {
  const resp = await fetch(url, opts);
  if (!resp.ok) return null;
  return await resp.json();
}

async function fetchTCGdexCardDetail(lang, cardId) {
  const url = `https://api.tcgdex.net/v2/${lang}/cards/${encodeURIComponent(cardId)}`;
  return await fetchJson(url, { headers: { "user-agent": "PokeGraadBot/0.3" } });
}

async function getPokemonNameEnByDexId(dexId) {
  const cache = readJson(DEX_CACHE_FILE, {});
  if (cache[dexId]) return cache[dexId];

  const url = `https://pokeapi.co/api/v2/pokemon-species/${dexId}/`;
  const j = await fetchJson(url, { headers: { "user-agent": "PokeGraadBot/0.3" } });
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

// --- 1) Catalogo (TCGdex) ---
// - include EN + JA
// - esclude TCG Pocket (serie tcgp)
// - per JP-only prova a valorizzare nameEn via dexId
async function buildCatalogFromTCGdex() {
  const langs = ["en", "ja"];
  const agg = new Map(); // key = setId|localId

  for (const lang of langs) {
    const base = `https://api.tcgdex.net/v2/${lang}`;

    const sets = await fetchJson(`${base}/sets`, { headers: { "user-agent": "PokeGraadBot/0.3" } });
    if (!sets) throw new Error(`TCGdex sets (${lang}) failed`);

    for (const s of sets) {
      const set = await fetchJson(`${base}/sets/${encodeURIComponent(s.id)}`, { headers: { "user-agent": "PokeGraadBot/0.3" } });
      if (!set) continue;

      // IMPORTANT: escludi TCG Pocket
      if (set.serie?.id === "tcgp") continue;

      const setId = set.id || s.id;
      const setName = set.name || s.name || setId;

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
        const img = c.image ? tcgdexImg(c.image, "high", "webp") : (c.imageLarge || c.images?.large || "");

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
          cardIdJa: null
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

  // Enrichment: JP-only -> nameEn via dexId; immagini via detail (solo quando serve)
  let detailFetches = 0;

  for (const v of agg.values()) {
    // Se manca immagine, prova dal detail della lingua disponibile
    if (!v.imageLarge) {
      const lang = v.cardIdJa ? "ja" : (v.cardIdEn ? "en" : null);
      const id = v.cardIdJa || v.cardIdEn;
      if (lang && id) {
        const detail = await fetchTCGdexCardDetail(lang, id);
        detailFetches++;
        if (detail?.image) v.imageLarge = tcgdexImg(detail.image, "high", "webp");
        if (detailFetches % 80 === 0) await sleep(300);
      }
    }

    // JP-only: riempi nameEn via dexId
    if (v.nameJa && !v.nameEn && v.cardIdJa) {
      const detail = await fetchTCGdexCardDetail("ja", v.cardIdJa);
      detailFetches++;

      const dex = Array.isArray(detail?.dexId) ? detail.dexId[0] : null;
      if (dex) {
        const enName = await getPokemonNameEnByDexId(dex);
        if (enName) v.nameEn = enName;
      }

      if (detailFetches % 80 === 0) await sleep(300);
    }
  }

  // Esplosione record per lingua
  const out = [];
  for (const v of agg.values()) {
    if (v.nameEn) {
      out.push({
        id: `${v.setId}-${v.number}-${norm(v.nameEn)}-en`,
        name: v.nameEn,
        nameEn: v.nameEn,
        nameJa: v.nameJa,
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
        name: v.nameJa,
        nameEn: v.nameEn || null,
        nameJa: v.nameJa,
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

// --- 2) Scraping vendite eBay.it ---
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
  const m = text.replace(/\./g, "").match(/(\d+,\d{1,2}|\d+)(?=\s*€|\s*eur)/i);
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
  const m = t.match(/\bsv\d{1,2}[a-z]?\b/);
  return m ? m[0] : null;
}

function extractLocalId(title) {
  const raw = title || "";

  // 181/165 -> 181
  const m1 = raw.match(/(\d{1,3})\s*\/\s*(\d{1,3})/);
  if (m1) return m1[1];

  // promo / codici tipo BW68, SVP123, ecc.
  const mPromo = raw.match(/\b([A-Z]{1,4}\d{1,4})\b/);
  if (mPromo) return mPromo[1];

  // #181 o 181
  const m2 = raw.match(/\b#?\s*(\d{1,3})\b/);
  if (m2) return m2[1];

  return null;
}

function bestMatchCard(catalog, title) {
  const t = norm(title);
  if (isLikelyLot(t)) return { card: null, confidence: 0 };

  const lang = detectLangFromTitle(t);
  const setCode = extractSetCode(title);
  const localId = extractLocalId(title);

  if (!localId) return { card: null, confidence: 0 };

  const candidates = (catalog.cards || []).filter(c => {
    if (lang && c.lang !== lang) return false;
    if (setCode && norm(c.setId) !== norm(setCode)) return false;

    if (c.number && norm(c.number) !== norm(localId)) return false;

    // nome: match su name oppure nameEn (fondamentale per JA)
    const okName =
      t.includes(norm(c.name)) ||
      (c.nameEn && t.includes(norm(c.nameEn)));

    return okName;
  });

  if (!candidates.length) return { card: null, confidence: 0 };

  let best = candidates[0];
  let conf = 0.65;
  if (setCode) conf += 0.15;
  if (lang) conf += 0.10;
  if (localId) conf += 0.10;

  // preferisci con immagine
  for (const c of candidates) {
    if (c.imageLarge && !best.imageLarge) best = c;
  }

  return { card: best, confidence: Math.min(conf, 1.0) };
}

async function fetchEbaySoldPageHTML({ keyword, page = 1, categoryId = "183454", gradedOnly = false }) {
  // 183454 = Trading Card Singles (di solito include le carte Pokémon singole)
  // gradedOnly -> LH_ItemCondition=2750 (graded)
  const params = new URLSearchParams({
    _nkw: keyword,
    LH_Sold: "1",
    LH_Complete: "1",
    rt: "nc",
    _pgn: String(page),
    _sacat: categoryId,

    // più risultati per pagina, meno pagine
    _ipg: "240",

    // sort: spesso 13 = "End Time: newly listed" / "recent first" (eBay cambia, ma non rompe se ignorato)
    _sop: "13"
  });

  if (gradedOnly) params.set("LH_ItemCondition", "2750");

  const url = `https://www.ebay.it/sch/i.html?${params.toString()}`;

  const resp = await fetch(url, {
    headers: { "user-agent": "Mozilla/5.0 (compatible; PokeGraadBot/0.3)" }
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

// --- Pipeline principale ---
async function main() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

  // cache dex
  if (!fs.existsSync(DEX_CACHE_FILE)) writeJson(DEX_CACHE_FILE, {});

  // A) catalogo
  const catalogFile = `${DATA_DIR}/catalog.json`;
  let catalog = readJson(catalogFile, { cards: [] });

  if (!SKIP_CATALOG) {
    try {
      catalog = await buildCatalogFromTCGdex();
      writeJson(catalogFile, catalog);
    } catch (e) {
      console.error("Catalog build failed:", e.message);
      catalog = readJson(catalogFile, { cards: [] });
    }
  } else {
    if (!catalog.cards?.length) {
      console.warn("SKIP_CATALOG=1 ma catalog.json è vuoto: rigenero comunque.");
      catalog = await buildCatalogFromTCGdex();
      writeJson(catalogFile, catalog);
    }
  }

  // B) rolling 30 giorni (basato su collectedAt)
  const salesFile = `${DATA_DIR}/sales_30d.json`;
  const salesObj = readJson(salesFile, { sales: [] });

  const cutoff = Date.now() - DAYS * 24 * 3600 * 1000;
  const kept = (salesObj.sales || []).filter(s => new Date(s.collectedAt).getTime() >= cutoff);

  // C) raccolta vendite: query mirate + esclusioni grading USA
  const collectedAt = todayISO();

  const gradedQueries = [
    `"GRAAD" pokemon -psa -bgs -bsg -cgc -ace`,
    `"GRAAD" jap pokemon -psa -bgs -bsg -cgc -ace`,
    `"GRAAD" sv9a pokemon -psa -bgs -bsg -cgc -ace`,
    `"GRAAD" "pokemon 151" -psa -bgs -bsg -cgc -ace`
  ];

  // RAW “mirate” (non serve gradedOnly)
  // Nota: queste servono soprattutto a popolare subito qualche card popolare, ma non farle troppo generiche.
  const rawQueries = [
    `pokemon sv9a 181/165 jap -psa -bgs -bsg -cgc -ace -graad -graded`,
    `pokemon meloetta jap -psa -bgs -bsg -cgc -ace -graad -graded`
  ];

  const PAGES_PER_QUERY = 2; // tienilo basso, ma _ipg alto

  const newSales = [];

  // Helper per evitare di salvare vendite “non graad” quando stai facendo query graad
  function isActuallyGraad(title) {
    return /\bgraad\b/i.test(title || "");
  }

  for (const kw of gradedQueries) {
    for (let page = 1; page <= PAGES_PER_QUERY; page++) {
      let html;
      try {
        html = await fetchEbaySoldPageHTML({ keyword: kw, page, gradedOnly: true });
      } catch (e) {
        console.error("eBay fetch error (graded):", kw, e.message);
        continue;
      }

      const items = parseEbaySearchItems(html);
      for (const it of items) {
        if (!isActuallyGraad(it.title)) continue;
        if (isLikelyLot(it.title)) continue;

        const bucket = detectGraadBucket(it.title) ?? "graad_unknown";
        const priceBucket = bucket.startsWith("graad_") ? bucket : "graad_unknown";

        const match = bestMatchCard(catalog, it.title);
        if (!match.card || match.confidence < 0.80) continue;

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

  for (const kw of rawQueries) {
    for (let page = 1; page <= PAGES_PER_QUERY; page++) {
      let html;
      try {
        html = await fetchEbaySoldPageHTML({ keyword: kw, page, gradedOnly: false });
      } catch (e) {
        console.error("eBay fetch error (raw):", kw, e.message);
        continue;
      }

      const items = parseEbaySearchItems(html);
      for (const it of items) {
        if (isLikelyLot(it.title)) continue;

        // Se trovi graad in una query raw, lo classifichi comunque come graad
        const bucketMaybe = detectGraadBucket(it.title);
        const priceBucket = bucketMaybe && bucketMaybe.startsWith("graad_") ? bucketMaybe : "raw";

        const match = bestMatchCard(catalog, it.title);
        if (!match.card || match.confidence < 0.80) continue;

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
      graad_10: [],
      graad_unknown: []
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
