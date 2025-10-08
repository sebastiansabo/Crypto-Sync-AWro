/*
 * Cloudflare Worker: Crypto Rate Sync
 *
 * This worker fetches the latest cryptocurrency prices for Bitcoin (BTC) and
 * MultiversX (EGLD) from the CoinMarketCap API and stores them in Shopify
 * metafields. It mirrors the structure of the existing BNR EUR→RON worker,
 * including holiday/weekend skipping and optional forced runs via a `/run`
 * endpoint. Rates are written into the `custom` namespace under the keys
 * defined below, along with separate date metafields tracking when the
 * values were last updated. Prices are fetched in the fiat currency
 * specified by the `CMC_CONVERT` environment variable (default USD).
 *
 * Required environment variables:
 *   - SHOP_URL:    your-shop.myshopify.com (without protocol)
 *   - SHOP_TOKEN:  Admin API access token with read/write metafields scope
 *   - CMC_API_KEY: CoinMarketCap API key (pro API)
 * Optional environment variables:
 *   - CMC_CONVERT: Fiat currency to convert to (e.g. "USD", "RON"). Defaults to "USD".
 *   - API_VERSION: Shopify API version (default "2024-04")
 *   - RUN_KEY:     Pass ?key=<RUN_KEY> to /run to authorize manual runs
 */

const CMC_URL     = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest";
const NS          = "custom";
const BTC_KEY     = "custom_crypto_btc";   // number_decimal
const EGLD_KEY    = "crypto_egld";        // number_decimal
const BTC_DATE_KEY  = "custom_crypto_btc_date"; // single_line_text_field
const EGLD_DATE_KEY = "crypto_egld_date";      // single_line_text_field
const EPS         = 1e-6; // minimum change required to trigger an update

export default {
  /**
   * Scheduled handler invoked by Cloudflare Cron Triggers. It simply
   * delegates to the run() function and swallows exceptions so the cron
   * scheduler doesn’t retry endlessly.
   */
  async scheduled(evt, env, ctx) {
    ctx.waitUntil(run(env, { force: false }).catch(() => {}));
  },

  /**
   * HTTP handler. Visiting `/run` triggers an immediate update. If a
   * RUN_KEY is defined, the caller must provide ?key=RUN_KEY. You can
   * override weekend/holiday skipping by appending `force=1` to the query.
   */
  async fetch(req, env) {
    const { pathname, searchParams } = new URL(req.url);
    if (pathname === "/run") {
      if (env.RUN_KEY && searchParams.get("key") !== env.RUN_KEY)
        return new Response("Unauthorized", { status: 401 });
      const force = searchParams.get("force") === "1";
      try {
        const out = await run(env, { force });
        return new Response(JSON.stringify(out, null, 2), {
          headers: { "content-type": "application/json" }
        });
      } catch (e) {
        return new Response(String(e), { status: 500 });
      }
    }
    return new Response("Crypto Worker OK. Use /run to trigger.", { status: 200 });
  }
};

/**
 * Main routine. Determines whether to skip based on weekends/holidays,
 * fetches rates, reads existing metafields, and writes updated values.
 */
async function run(env, { force }) {
  const v = env.API_VERSION || "2024-04";
  if (!env.SHOP_URL || !env.SHOP_TOKEN)
    throw new Error("Missing SHOP_URL or SHOP_TOKEN.");
  if (!env.CMC_API_KEY)
    throw new Error("Missing CMC_API_KEY.");

  // Determine current date in Romania (YYYY-MM-DD)
  const todayRO  = romaniaISODate(new Date());
  const weekday  = isoWeekday(todayRO); // Monday=1, Sunday=7
  const holInfo  = isRomaniaHolidayInfo(todayRO);

  // Skip weekends and official holidays unless forced
  if (!force && (weekday >= 6 || holInfo.isHoliday)) {
    return {
      ok: true,
      skipped: true,
      reason: weekday >= 6 ? "weekend" : `holiday:${holInfo.name || ''}`,
      dateRO: todayRO
    };
  }

  // Fetch latest rates for BTC and EGLD from CoinMarketCap
  const convert = env.CMC_CONVERT || "USD";
  const rates   = await fetchCMC(env, ["BTC", "EGLD"], convert);
  const btcVal  = rates.BTC;
  const egldVal = rates.EGLD;

  // Resolve shop ID once
  const shopId  = await getShopId(env, v);
  const shopGID = `gid://shopify/Shop/${shopId}`;

  // Read existing metafields (both values and dates)
  const meta = await readCryptoMetafields(env, v, NS);
  const oldBTC      = meta.btc?.value ? parseFloat(meta.btc.value) : null;
  const oldBTCDate  = meta.btcDate?.value || null;
  const oldEGLD     = meta.egld?.value ? parseFloat(meta.egld.value) : null;
  const oldEGLDDate = meta.egldDate?.value || null;

  let wrote = false;
  const items = [];
  // Compare BTC rate and queue updates if it changed
  if (oldBTC === null || Math.abs(btcVal - oldBTC) >= EPS) {
    items.push({ namespace: NS, key: BTC_KEY, type: "number_decimal", value: btcVal.toFixed(6) });
    items.push({ namespace: NS, key: BTC_DATE_KEY, type: "single_line_text_field", value: todayRO });
  }
  // Compare EGLD rate and queue updates if it changed
  if (oldEGLD === null || Math.abs(egldVal - oldEGLD) >= EPS) {
    items.push({ namespace: NS, key: EGLD_KEY, type: "number_decimal", value: egldVal.toFixed(6) });
    items.push({ namespace: NS, key: EGLD_DATE_KEY, type: "single_line_text_field", value: todayRO });
  }

  if (items.length > 0) {
    await metafieldsSet(env, v, shopGID, items);
    wrote = true;
  }

  return {
    ok: true,
    skipped: false,
    wrote,
    btc: btcVal,
    egld: egldVal,
    dateRO: todayRO,
    lastBTCDate: oldBTCDate,
    lastEGLDDate: oldEGLDDate,
    shopId
  };
}

/* --------- Shopify helpers --------- */

// Build standard headers for Shopify REST/GraphQL calls
function shopHeaders(env) {
  return {
    "X-Shopify-Access-Token": env.SHOP_TOKEN,
    "Content-Type": "application/json"
  };
}

// Query shop ID from Shopify REST API
async function getShopId(env, v) {
  const r = await fetch(`https://${env.SHOP_URL}/admin/api/${v}/shop.json`, {
    headers: shopHeaders(env)
  });
  if (!r.ok) throw new Error(`Get shop failed: ${r.status} ${await r.text()}`);
  return (await r.json()).shop.id;
}

// Read crypto-related metafields using a single GraphQL query
async function readCryptoMetafields(env, v, ns) {
  const q = `
    query ReadCrypto($ns: String!, $btcKey: String!, $egldKey: String!, $btcDateKey: String!, $egldDateKey: String!) {
      shop {
        id
        btc: metafield(namespace: $ns, key: $btcKey) { id type value }
        egld: metafield(namespace: $ns, key: $egldKey) { id type value }
        btcDate: metafield(namespace: $ns, key: $btcDateKey) { id type value }
        egldDate: metafield(namespace: $ns, key: $egldDateKey) { id type value }
      }
    }
  `;
  const variables = {
    ns,
    btcKey: BTC_KEY,
    egldKey: EGLD_KEY,
    btcDateKey: BTC_DATE_KEY,
    egldDateKey: EGLD_DATE_KEY
  };
  const r = await fetch(`https://${env.SHOP_URL}/admin/api/${v}/graphql.json`, {
    method: "POST",
    headers: shopHeaders(env),
    body: JSON.stringify({ query: q, variables })
  });
  if (!r.ok) throw new Error(`GraphQL read failed: ${r.status} ${await r.text()}`);
  const data = await r.json();
  if (data.errors) throw new Error("GraphQL read errors: " + JSON.stringify(data.errors));
  return {
    btc: data.data.shop.btc || null,
    egld: data.data.shop.egld || null,
    btcDate: data.data.shop.btcDate || null,
    egldDate: data.data.shop.egldDate || null
  };
}

// Write one or more metafields via GraphQL mutation
async function metafieldsSet(env, v, ownerId, items) {
  const m = `mutation Set($metafields:[MetafieldsSetInput!]!){
    metafieldsSet(metafields:$metafields){ userErrors{ field message code } }
  }`;
  const r = await fetch(`https://${env.SHOP_URL}/admin/api/${v}/graphql.json`, {
    method: "POST",
    headers: shopHeaders(env),
    body: JSON.stringify({ query: m, variables: { metafields: items.map(x => ({ ...x, ownerId })) } })
  });
  if (!r.ok) throw new Error(`GraphQL write failed: ${r.status} ${await r.text()}`);
  const j = await r.json();
  const errs = j?.data?.metafieldsSet?.userErrors || [];
  if (errs.length) throw new Error("metafieldsSet errors: " + JSON.stringify(errs));
}

/* --------- CoinMarketCap helper --------- */

/**
 * Fetch latest quotes for the provided symbols from CoinMarketCap.
 * Accepts a list of symbols (e.g. ["BTC","EGLD"]) and returns a map
 * symbol → price (in the specified conversion currency). Prices are
 * returned as numbers; missing or unknown symbols throw an error.
 */
async function fetchCMC(env, symbols, convert) {
  const params = new URLSearchParams();
  params.set("symbol", symbols.join(","));
  if (convert) params.set("convert", convert);
  const url = `${CMC_URL}?${params.toString()}`;
  const r = await fetch(url, {
    headers: {
      "X-CMC_PRO_API_KEY": env.CMC_API_KEY,
      "Accept": "application/json"
    },
    // Cache for one minute to reduce API load
    cf: { cacheTtl: 60, cacheEverything: true }
  });
  if (!r.ok) throw new Error(`CMC fetch failed: ${r.status}`);
  const json = await r.json();
  if (!json.data) throw new Error("CMC response missing data");
  const result = {};
  for (const sym of symbols) {
    const entry = json.data[sym];
    if (!entry) throw new Error(`CMC data missing symbol ${sym}`);
    const quote = entry.quote?.[convert];
    if (!quote || typeof quote.price !== 'number')
      throw new Error(`CMC data missing price for ${sym} (${convert})`);
    result[sym] = quote.price;
  }
  return result;
}

/* --------- Date & holiday helpers (Romania) --------- */

// Return YYYY-MM-DD in Europe/Bucharest local time
function romaniaISODate(d) {
  const tz = 'Europe/Bucharest';
  const y  = new Intl.DateTimeFormat('ro-RO',{ timeZone:tz, year:'numeric'}).format(d);
  const m  = new Intl.DateTimeFormat('ro-RO',{ timeZone:tz, month:'2-digit'}).format(d);
  const day = new Intl.DateTimeFormat('ro-RO',{ timeZone:tz, day:'2-digit'}).format(d);
  return `${y}-${m}-${day}`;
}

// Monday=1 .. Sunday=7 for a calendar ISO date (timezone-agnostic)
function isoWeekday(iso) {
  const [y, m, d] = iso.split('-').map(Number);
  const dow = new Date(Date.UTC(y, m - 1, d)).getUTCDay(); // 0..6, Sun=0
  return dow === 0 ? 7 : dow;
}

// Determine if a date string (YYYY-MM-DD) is a public holiday in Romania
function isRomaniaHolidayInfo(iso) {
  const y = parseInt(iso.slice(0, 4), 10);
  const set = romaniaPublicHolidaysISO(y);
  return set.has(iso)
    ? { isHoliday: true, name: holidayName(y, iso) }
    : { isHoliday: false };
}

// Generate a set of Romania public holidays for a given year
function romaniaPublicHolidaysISO(year) {
  const fixed = new Set([
    `${year}-01-01`, `${year}-01-02`,
    `${year}-01-06`, `${year}-01-07`,
    `${year}-01-24`,
    `${year}-05-01`,
    `${year}-06-01`,
    `${year}-08-15`,
    `${year}-11-30`,
    `${year}-12-01`,
    `${year}-12-25`, `${year}-12-26`
  ]);
  const easter = orthodoxEasterGregorian(year); // Sunday (Gregorian)
  const goodFri     = addDays(easter, -2);
  const easterMon   = addDays(easter, 1);
  const pentecostSun = addDays(easter, 49);
  const pentecostMon = addDays(easter, 50);
  [goodFri, easter, easterMon, pentecostSun, pentecostMon]
    .forEach(d => fixed.add(isoOf(d)));
  return fixed;
}

// Return human-readable holiday names (Romanian)
function holidayName(y, iso) {
  const names = {
    [`${y}-01-01`]: "Anul Nou (Ziua 1)",
    [`${y}-01-02`]: "Anul Nou (Ziua 2)",
    [`${y}-01-06`]: "Boboteaza",
    [`${y}-01-07`]: "Sf. Ioan Botezătorul",
    [`${y}-01-24`]: "Unirea Principatelor",
    [`${y}-05-01`]: "Ziua Muncii",
    [`${y}-06-01`]: "Ziua Copilului",
    [`${y}-08-15`]: "Adormirea Maicii Domnului",
    [`${y}-11-30`]: "Sf. Andrei",
    [`${y}-12-01`]: "Ziua Națională",
    [`${y}-12-25`]: "Crăciun",
    [`${y}-12-26`]: "A doua zi de Crăciun"
  };
  return names[iso] || "Sărbătoare legală";
}

// Calculate Orthodox Easter in Gregorian calendar (for years 1900–2099)
function orthodoxEasterGregorian(year) {
  const a = year % 4;
  const b = year % 7;
  const c = year % 19;
  const d = (19 * c + 15) % 30;
  const e = (2 * a + 4 * b - d + 34) % 7;
  const monthJul = Math.floor((d + e + 114) / 31); // 3=Mar, 4=Apr (Julian)
  const dayJul   = ((d + e + 114) % 31) + 1;
  const jul      = new Date(Date.UTC(year, monthJul - 1, dayJul));
  return addDays(jul, 13); // Convert Julian to Gregorian (+13 days)
}

// Add n days to a Date instance (UTC)
function addDays(d, n) {
  const x = new Date(d.getTime());
  x.setUTCDate(x.getUTCDate() + n);
  return x;
}

// Format Date → YYYY-MM-DD
function isoOf(d) {
  return d.toISOString().slice(0, 10);
}