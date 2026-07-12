import fs from "fs";
import fetch from "node-fetch";
import * as cheerio from "cheerio";

const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36";
const EBAY_CATEGORY_ID = "183454";

async function probe(keyword, { gradedOnly = false, dump = false } = {}) {
  const params = new URLSearchParams({
    _nkw: keyword,
    LH_Sold: "1",
    LH_Complete: "1",
    rt: "nc",
    _pgn: "1",
    _sacat: EBAY_CATEGORY_ID
  });
  if (gradedOnly) params.set("LH_ItemCondition", "2750");

  const url = `https://www.ebay.it/sch/i.html?${params.toString()}`;
  console.log(`\n--- ${keyword} (gradedOnly=${gradedOnly})`);
  console.log(url);

  let resp;
  try {
    resp = await fetch(url, {
      headers: {
        "user-agent": USER_AGENT,
        "accept": "text/html,application/xhtml+xml",
        "accept-language": "it-IT,it;q=0.9"
      }
    });
  } catch (e) {
    console.log(`NETWORK ERROR: ${e.message}`);
    return;
  }

  console.log(`status=${resp.status}`);
  const html = await resp.text();
  console.log(`html=${html.length} bytes`);

  // segnali di blocco
  const blocked = /captcha|are you a robot|unusual traffic|verify you are human|pardon our interruption/i.test(html);
  console.log(`blocked_markers=${blocked}`);

  const $ = cheerio.load(html);
  const sItems = $(".s-item").length;
  const liItems = $("li.s-item").length;
  const resultsCount = $(".srp-controls__count-heading").text().trim();
  console.log(`.s-item=${sItems}  li.s-item=${liItems}  heading="${resultsCount}"`);

  // primi 3 titoli+prezzi, per vedere se il parsing regge
  $(".s-item").slice(0, 4).each((i, el) => {
    const t = $(el).find(".s-item__title").text().trim();
    const p = $(el).find(".s-item__price").text().trim();
    if (t) console.log(`  [${i}] ${p} | ${t.slice(0, 70)}`);
  });

  if (dump) {
    const f = `debug-${keyword.replace(/\W+/g, "_").slice(0, 30)}.html`;
    fs.writeFileSync(f, html);
    console.log(`dumped -> ${f}`);
  }
}

const DUMP = process.env.DUMP === "1";

await probe(`"GRAAD" pokemon -psa -bgs -cgc`, { gradedOnly: true, dump: DUMP });
await probe(`"GRAAD" pokemon`, { gradedOnly: false, dump: DUMP });
await probe(`pokemon charizard`, { gradedOnly: false, dump: DUMP });
