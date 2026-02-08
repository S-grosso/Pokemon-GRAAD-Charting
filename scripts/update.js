import fs from "fs";
import fetch from "node-fetch";
import * as cheerio from "cheerio";

const DATA_DIR = "data";
const DAYS = 30;

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
  const arr = nums.filter(n => typeof n === "number" && !Number.isNaN(n)).sort((a,b)=>a-b);
  if (!arr.length) return null;
  const mid = Math.floor(arr.length / 2);
  return arr.length % 2 ? arr[mid] : (arr[mid-1] + arr[mid]) / 2;
}

// --- 1) Catalogo carte (TCGdex) ---
// Per restare “gratis” e multilingua, TCGdex è la scelta più comoda. :contentReference[oaicite:4]{index=4}
async function buildCatalogFromTCGdex() {
  // Strategia MVP:
  // - prendiamo set + carte in EN e JA
  // - teniamo solo campi minimi per ricerca + immagine (se disponibile)
  //
  // Nota: TCGdex ha endpoint per lingua; per semplicità usiamo due fetch separati.
  const langs = ["en", "ja"];
  const cards = [];

  for (const lang of langs) {
    const base = `https://api.tcgdex.net/v2/${lang}`;

    // sets
    const setsResp = await fetch(`${base}/sets`);
    if (!setsResp.ok) throw new Error(`TCGdex sets (${lang}) failed: ${setsResp.status}`);
    const sets = await setsResp.json();

    // Per non esplodere i tempi: in MVP scarichiamo solo i set “sv”/moderni?
    // Qui lasciamo TUTTI i set; se diventa pesante, si mette un filtro.
    for (const s of sets) {
      // recupero dettagli set (contiene lista carte)
      const setResp = await fetch(`${base}/sets/${encodeURIComponent(s.id)}`);
      if (!setResp.ok) continue;
      const set = await setResp.json();

      // set.cards può essere enorme; prendiamo i riferimenti e poi dettagli per carta?
      // TCGdex spesso include già info utili nei dettagli set; gestiamo entrambe.
      const setName = set.name || s.name || s.id;

      if (Array.isArray(set.cards)) {
        for (const c of set.cards) {
          // c può essere oggetto ricco o un riferimento; proviamo a leggere campi base.
          const name = c.name || null;
          const localId = c.localId || c.number || null;     // dipende dai dati
          const id = c.id || null;

          // Se manca l’id carta, saltiamo (in pratica succede raramente).
          if (!id) continue;

          // Creiamo un id interno stabile per il sito:
          // setId + localId + lang + nome normalizzato.
          const num = (localId || "").toString();
          const internalId = `${set.id}-${num}-${norm(name || "card")}-${lang}`;

          cards.push({
            id: internalId,
            name: name || "Unknown",
            lang,
            setId: set.id,
            setName,
            number: num,
            numberFull: null, // lo riempiamo se lo ricaviamo dal title eBay, o da altre fonti
            rarity: c.rarity || null,
            features: c.rarity ? [c.rarity] : [],
            imageLarge: c.image ? (c.image + "/high.webp") : (c.imageLarge || c.images?.large || "")
          });
        }
      }
    }
  }

  // Dedup grezzo (se stesso id interno appare doppio)
  const uniq = new Map();
  for (const c of cards) {
    if (!uniq.has(c.id)) uniq.set(c.id, c);
  }

  return { cards: [...uniq.values()] };
}

// --- 2) Scraping vendite eBay.it ---
// NB: accesso ai “sold” via API ufficiale è in gran parte ristretto; per MVP facciamo scraping. :contentReference[oaicite:5]{index=5}
//
// Strategia MVP:
// - cerchiamo in sold listings in una query “larga” (keyword Pokémon) e filtri che favoriscano singole carte
// - estraiamo (title, price, url)
// - applichiamo filtri anti-lotto
// - proviamo a matchare a catalogo su: nome + lingua (JAP/JP, ENG/EN) + numero carta (es. 181/165 o 181) + set (es. SV9A)
// - classifichiamo RAW vs GRAAD e bucket grade
//
// Limite: eBay non garantisce “data sold” in HTML. In MVP usiamo la data di raccolta;
// con update giornaliero è una buona approssimazione per finestra rolling 30 giorni.
function isLikelyLot(title) {
  const t = norm(title);
  return (
    /\blot\b/.test(t) ||
    /\bbundle\b/.test(t) ||
    /\bchoose\b/.test(t) ||
    /\bseleziona\b/.test(t) ||
    /\b(\\d+)\s*(cards|carte)\b/.test(t) ||
    /\bplayset\b/.test(t)
  );
}

function parseEurPrice(text) {
  // gestisce "12,34 EUR" o "12,34€"
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

  // Cerca "GRAAD 9.5" ecc
  const m = t.match(/graad\s*([0-9]{1,2}(?:[.,]5)?)/);
  if (!m) return "graad_unknown";
  const raw = m[1].replace(",", ".");
  const g = Number(raw);

  if (g === 10) return "graad_10";
  if (g === 9.5) return "graad_9_5";
  if (g === 9) return "graad_9";
  if (g === 8) return "graad_8";
  if (g === 7) return "graad_7";

  // MVP: se arriva un 8.5 lo buttiamo nel bucket più vicino verso il basso
  if (g > 9 && g < 9.5) return "graad_9";
  if (g > 8 && g < 9) return "graad_8";
  if (g > 7 && g < 8) return "graad_7";

  return "graad_unknown";
}

function extractCardNumber(title) {
  const t = title;
  // 181/165
  const m1 = t.match(/(\d{1,3})\s*\/\s*(\d{1,3})/);
  if (m1) return { number: m1[1], total: m1[2], full: `${m1[1]}/${m1[2]}` };

  // solo numero (es. " #181 ")
  const m2 = t.match(/\b#?\s*(\d{1,3})\b/);
  if (m2) return { number: m2[1], total: null, full: null };

  return { number: null, total: null, full: null };
}

function extractSetCode(title) {
  const t = norm(title);
  // sv9a, sv4a, sv8, ecc. (MVP)
  const m = t.match(/\bsv\d{1,2}[a-z]?\b/);
  return m ? m[0] : null;
}

function bestMatchCard(catalog, title) {
  // restituisce { card, confidence }
  const t = norm(title);
  if (isLikelyLot(t)) return { card: null, confidence: 0 };

  const lang = detectLangFromTitle(t);      // ja/en/ null
  const setCode = extractSetCode(title);    // es. sv9a
  const num = extractCardNumber(title);     // 181/165 etc.

  // nome: prendiamo la parola prima forte; MVP: match su substring nome carta
  // Qui facciamo un match “conservativo”: se non c’è almeno set+numero oppure nome+numero+lingua, scartiamo.
  const candidates = catalog.cards.filter(c => {
    if (lang && c.lang !== lang) return false;
    if (setCode && norm(c.setId) !== norm(setCode)) return false;
    if (num.number && c.number && c.number.toString() !== num.number.toString()) return false;
    // nome presente nel titolo
    const nameOk = t.includes(norm(c.name));
    return nameOk;
  });

  if (!candidates.length) return { card: null, confidence: 0 };

  // confidenza: set+numero+lang+nome = 1.0; senza set 0.8; senza lang 0.7; senza numero 0.0 (scartato)
  let best = candidates[0];
  let conf = 0.6;
  if (setCode) conf += 0.2;
  if (lang) conf += 0.1;
  if (num.number) conf += 0.1;

  // preferisci quello con set match (già filtrato) e magari con immagine disponibile
  for (const c of candidates) {
    if (c.imageLarge && !best.imageLarge) best = c;
  }

  // se manca numero o manca qualsiasi segnale forte, scartiamo per evitare mismatch
  if (!num.number) return { card: null, confidence: 0 };

  return { card: best, confidence: Math.min(conf, 1.0), num };
}

async function fetchEbaySoldPageHTML(page = 1) {
  // Query “larga” ma orientata a carte singole.
  // NB: parametri eBay possono cambiare; MVP.
  const url =
    `https://www.ebay.it/sch/i.html` +
    `?_nkw=${encodeURIComponent("pokemon")}` +
    `&LH_Sold=1&LH_Complete=1` +
    `&rt=nc` +
    `&_pgn=${page}`;

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

  // A) catalogo (se già grande, puoi rigenerarlo meno spesso; per MVP lo rigeneriamo sempre)
  let catalog;
  try {
    catalog = await buildCatalogFromTCGdex();
  } catch (e) {
    // Se fallisce TCGdex, usiamo l’ultimo catalogo esistente
    console.error("Catalog build failed:", e.message);
    catalog = readJson(`${DATA_DIR}/catalog.json`, { cards: [] });
  }

  writeJson(`${DATA_DIR}/catalog.json`, catalog);

  // B) storico vendite rolling 30 giorni
  const salesFile = `${DATA_DIR}/sales_30d.json`;
  const salesObj = readJson(salesFile, { sales: [] });

  // pulizia vecchie vendite (basata su timestamp raccolta)
  const cutoff = Date.now() - DAYS * 24 * 3600 * 1000;
  const kept = (salesObj.sales || []).filter(s => new Date(s.collectedAt).getTime() >= cutoff);

  // C) raccogli vendite nuove (MVP: prime 2 pagine)
  const collectedAt = todayISO();
  const newSales = [];

  for (let page = 1; page <= 2; page++) {
    let html;
    try {
      html = await fetchEbaySoldPageHTML(page);
    } catch (e) {
      console.error("eBay fetch error:", e.message);
      continue;
    }

    const items = parseEbaySearchItems(html);

    for (const it of items) {
      if (isLikelyLot(it.title)) continue;

      const bucket = detectGraadBucket(it.title);
      const graded = bucket && bucket.startsWith("graad_");

      const match = bestMatchCard(catalog, it.title);
      if (!match.card || match.confidence < 0.85) continue; // conservativo

      // RAW se non graded
      const priceBucket = graded ? bucket : "raw";

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

  // dedup: per semplicità dedup su (url + price + cardId)
  const seen = new Set(kept.map(s => `${s.url}|${s.price_eur}|${s.cardId}|${s.bucket}`));
  for (const s of newSales) {
    const k = `${s.url}|${s.price_eur}|${s.cardId}|${s.bucket}`;
    if (!seen.has(k)) {
      seen.add(k);
      kept.push(s);
    }
  }

  writeJson(salesFile, { sales: kept });

  // D) aggregati mediane 30 giorni per carta/bucket
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
