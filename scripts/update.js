import fs from "fs";
import fetch from "node-fetch";
import * as cheerio from "cheerio";

const DATA_DIR = "data";
const DAYS = 30;

// Se 1: non ricostruisce il catalogo, usa quello esistente
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
    .filter(n => typeof n === "number" && !Number.isNaN(n))
    .sort((a, b) => a - b);
  if (!arr.length) return null;
  const mid = Math.floor(arr.length / 2);
  return arr.length % 2 ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

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
  return await fetchJson(url);
}

async function getPokemonNameEnByDexId(dexId) {
  const cache = readJson(DEX_CACHE_FILE, {});
  if (cache[dexId]) return cache[dexId];

  const url = `https://pokeapi.co/api/v2/pokemon-species/${dexId}/`;
  const j = await fetchJson(url, {
    headers: { "user-agent": "PokeGraadBot/0.1" }
  });
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

// --- 1) Catalogo carte (TCGdex) ---
async function buildCatalogFromTCGdex() {
  const langs = ["en", "ja"];
  const agg = new Map(); // key = setId|localId

  for (const lang of langs) {
    const base = `https://api.tcgdex.net/v2/${lang}`;

    const sets = await fetchJson(`${base}/sets`);
    if (!sets) throw new Error(`TCGdex sets (${lang}) failed`);

    for (const s of sets) {
      const set = await fetchJson(`${base}/sets/${encodeURIComponent(s.id)}`);
      if (!set) continue;

      const setId = set.id || s.id;
      const setName = set.name || s.name || setId;

      const total =
        set.cardCount?.official ??
        set.cardCount?.total ??
        set.cardCount?.count ??
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
          cardIdJa: null
        };

        if (lang === "en") {
          prev.nameEn = name;
          prev.cardIdEn = c.id || prev.cardIdEn;
        }
        if (lang === "ja") {
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

  // Enrichment: JP-only -> nameEn da dexId
  let detailFetches = 0;

  for (const v of agg.values()) {
    if (v.nameJa && !v.nameEn && v.cardIdJa) {
      const detail = await fetchTCGdexCardDetail("ja", v.cardIdJa);
      detailFetches++;

      if (detail) {
        if (!v.imageLarge && detail.image) v.imageLarge = tcgdexImg(detail.image, "high", "webp");

        const dex = Array.isArray(detail.dexId) ? detail.dexId[0] : null;
        if (dex) {
          const enName = await getPokemonNameEnByDexId(dex);
          if (enName) v.nameEn = enName;
        }
      }

      if (detailFetches % 80 === 0) await sleep(300);
    }

    if (v.nameEn && !v.imageLarge && v.cardIdEn) {
      const detail = await fetchTCGdexCardDetail("en", v.cardIdEn);
      detailFetches++;
      if (detail?.image) v.imageLarge = tcgdexImg(detail.image, "high", "webp");
      if (detailFetches % 80 === 0) await sleep(300);
    }
  }

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
    /\bchoose\b/.test(t) ||
    /\bseleziona\b/.test(t) ||
    /\b(\d+)\s*(cards|carte)\b/.test(t) ||
    /\bplayset\b/.test(t)
  );
}

function parseEurPrice(text) {
  const m = text.replace(/\./g, "").match(/(\d+,\d{1,2}|\d+)(?=\s*€|\s*eur)/i);
  if (!m) return null;
  const n = m[1].replace(",", ".");
  const v = Number(n);
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
  const raw = m[1].replace(",", ".");
  const g = Number(raw);

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

function extractCardNumber(title) {
  const t = title;
  const m1 = t.match(/(\d{1,3})\s*\/\s*(\d{1,3})/);
  if (m1) return { number: m1[1], total: m1[2], full: `${m1[1]}/${m1[2]}` };

  const m2 = t.match(/\b#?\s*(\d{1,3})\b/);
  if (m2) return { number: m2[1], total: null, full: null };

  return { number: null, total: null, full: null };
}

function extractSetCode(title) {
  const t = norm(title);
  const m = t.match(/\bsv\d{1,2}[a-z]?\b/);
  return m ? m[0] : null;
}

function bestMatchCard(catalog, title) {
  const t = norm(title);
  if (isLikelyLot(t)) return { card: null, confidence: 0 };

  const lang = detectLangFromTitle(t);
  const setCode = extractSetCode(title);
  const num = extractCardNumber(title);

  const candidates = catalog.cards.filter(c => {
    if (lang && c.lang !== lang) return false;
    if (setCode && norm(c.setId) !== norm(setCode)) return false;
    if (num.number && c.number && c.number.toString() !== num.number.toString()) return false;

    const nameOk =
      t.includes(norm(c.name)) ||
      (c.nameEn && t.includes(norm(c.nameEn)));

    return nameOk;
  });

  if (!candidates.length) return { card: null, confidence: 0 };

  let best = candidates[0];
  let conf = 0.6;
  if (setCode) conf += 0.2;
  if (lang) conf += 0.1;
  if (num.number) conf += 0.1;

  for (const c of candidates) {
    if (c.imageLarge && !best.imageLarge) best = c;
  }

  if (!num.number) return { card: null, confidence: 0 };

  return { card: best, confidence: Math.min(conf, 1.0), num };
}

function buildEbaySoldUrl(keyword, page = 1) {
  return (
    `https://www.ebay.it/sch/i.html` +
    `?_nkw=${encodeURIComponent(keyword)}` +
    `&LH_Sold=1&LH_Complete=1` +
    `&rt=nc` +
    `&_pgn=${page}`
  );
}

async function fetchEbaySoldPageHTML(keyword, page = 1) {
  const url = buildEbaySoldUrl(keyword, page);

  const resp = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0 (compatible; PokeGraadBot/0.1; +https://example.invalid)"
    }
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

  // cache dex: assicurati esista (almeno vuota)
  if (!fs.existsSync(DEX_CACHE_FILE)) {
    writeJson(DEX_CACHE_FILE, {});
  }

  // A) catalogo (SKIP_CATALOG=1 -> usa quello esistente)
  let catalog;

  if (SKIP_CATALOG) {
    catalog = readJson(`${DATA_DIR}/catalog.json`, { cards: [] });

    // fallback: se non esiste o è vuoto, rigenera comunque
    if (!catalog.cards || catalog.cards.length === 0) {
      console.error("SKIP_CATALOG=1 ma catalog.json è vuoto/mancante: rigenero catalogo.");
      catalog = await buildCatalogFromTCGdex();
      writeJson(`${DATA_DIR}/catalog.json`, catalog);
    }
  } else {
    try {
      catalog = await buildCatalogFromTCGdex();
    } catch (e) {
      console.error("Catalog build failed:", e.message);
      catalog = readJson(`${DATA_DIR}/catalog.json`, { cards: [] });
    }
    writeJson(`${DATA_DIR}/catalog.json`, catalog);
  }

  // B) storico vendite rolling 30 giorni
  const salesFile = `${DATA_DIR}/sales_30d.json`;
  const salesObj = readJson(salesFile, { sales: [] });

  const cutoff = Date.now() - DAYS * 24 * 3600 * 1000;
  const kept = (salesObj.sales || []).filter(s => new Date(s.collectedAt).getTime() >= cutoff);

  // C) raccogli vendite nuove (4 query mirate, 2 pagine ciascuna)
  const collectedAt = todayISO();
  const newSales = [];

  const queries = [
    // 1) focus grading
    { label: "graad", keyword: "pokemon graad", pages: 2 },
    // 2) grading + jap (utile per JP)
    { label: "graad-jap", keyword: "pokemon graad jap", pages: 2 },
    // 3) set moderno JP molto comune in Italia
    { label: "sv9a", keyword: "pokemon sv9a", pages: 2 },
    // 4) sv9a + jap (ulteriore segnale lingua)
    { label: "sv9a-jap", keyword: "pokemon sv9a jap", pages: 2 }
  ];

  for (const q of queries) {
    for (let page = 1; page <= q.pages; page++) {
      let html;
      try {
        html = await fetchEbaySoldPageHTML(q.keyword, page);
      } catch (e) {
        console.error(`eBay fetch error (${q.label} p${page}):`, e.message);
        continue;
      }

      const items = parseEbaySearchItems(html);

      for (const it of items) {
        if (isLikelyLot(it.title)) continue;

        const bucket = detectGraadBucket(it.title);
        const graded = bucket && bucket.startsWith("graad_");

        const match = bestMatchCard(catalog, it.title);
        if (!match.card || match.confidence < 0.85) continue;

        const priceBucket = graded ? bucket : "raw";

        newSales.push({
          collectedAt,
          source: "ebay.it",
          query: q.label,
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
      graad_10: []
    };
    if (byCard[s.cardId][s.bucket]) byCard[s.cardId][s.bucket].push(s.price_eur);
  }

  const priceOut = { byCard: {} };
  for (const [cardId, buckets] of Object.entries(byCard)) {
    priceOut.byCard[cardId] = {};
    for (const [bucket, arr] of Object.entries(buckets)) {
      priceOut.byCard[cardId][bucket] = {
        median_eur: median(arr),
        n: arr.length
      };
    }
  }

  writeJson(`${DATA_DIR}/prices.json`, priceOut);
  writeJson(`${DATA_DIR}/meta.json`, { updatedAt: collectedAt });
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
