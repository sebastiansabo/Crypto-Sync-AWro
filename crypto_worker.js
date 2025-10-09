/*
 * Cloudflare Worker: Crypto Rate Sync
 *
 * This worker fetches the latest cryptocurrency prices for Bitcoin (BTC) and
 * MultiversX (EGLD) from the CoinMarketCap API and stores them in Shopify
 * metafields. It mirrors the structure of the existing BNR EUR→RON worker,
 * but does **not** skip weekends or holidays—crypto trading is continuous.
 * Rates are written into the `custom` namespace under the keys defined below,
 * along with separate date metafields tracking when the values were last updated.
 * Prices are fetched in the fiat currency specified by the `CMC_CONVERT`
 * environment variable (default EUR).
 *
 * Required environment variables:
 *   - SHOP_URL:    your-shop.myshopify.com (without protocol)
 *   - SHOP_TOKEN:  Admin API access token with read/write metafields scope
 *   - CMC_API_KEY: CoinMarketCap API key (pro API)
 * Optional environment variables:
 *   - CMC_CONVERT: Fiat currency to convert to (e.g. "EUR", "RON"). Defaults to "EUR".
 *   - API_VERSION: Shopify API version (default "2024-04")
 *   - RUN_KEY:     Pass ?key=<RUN_KEY> to /run to authorize manual runs
 */

const CMC_URL       = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest";
const NS            = "custom";
const BTC_KEY       = "custom_crypto_btc";       // number_decimal
const EGLD_KEY      = "crypto_egld";            // number_decimal
const BTC_DATE_KEY  = "custom_crypto_btc_date"; // single_line_text_field
const EGLD_DATE_KEY = "crypto_egld_date";       // single_line_text_field
const EPS           = 1e-6; // minimum change required to trigger an update

export default {
  async scheduled(evt, env, ctx) {
    ctx.waitUntil(run(env, { force: false }).catch(() => {}));
  },

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
 * Main routine. Always executes (no weekend/holiday skipping), fetches rates,
 * reads existing metafields, and writes updated values.
 */
async function run(env, { force }) {
  const v = env.API_VERSION || "2024-04";
  if (!env.SHOP_URL || !env.SHOP_TOKEN)
    throw new Error("Missing SHOP_URL or SHOP_TOKEN.");
  if (!env.CMC_API_KEY)
    throw new Error("Missing CMC_API_KEY.");

  // Current date in Romania; used only for the _date metafields
  const todayRO = romaniaISODate(new Date());

  // Fetch latest BTC and EGLD prices
  const convert  = env.CMC_CONVERT || "EUR";
  const rates    = await fetchCMC(env, ["BTC", "EGLD"], convert);
  const btcVal   = rates.BTC;
  const egldVal  = rates.EGLD;

  // Shopify shop ID
  const shopId  = await getShopId(env, v);
  const shopGID = `gid://shopify/Shop/${shopId}`;

  // Read existing metafields
  const meta        = await readCryptoMetafields(env, v, NS);
  const oldBTC      = meta.btc?.value ? parseFloat(meta.btc.value) : null;
  const oldBTCDate  = meta.btcDate?.value || null;
  const oldEGLD     = meta.egld?.value ? parseFloat(meta.egld.value) : null;
  const oldEGLDDate = meta.egldDate?.value || null;

  let wrote = false;
  const items = [];
  // Update BTC if changed
  if (oldBTC === null || Math.abs(btcVal - oldBTC) >= EPS) {
    items.push({ namespace: NS, key: BTC_KEY, type: "number_decimal", value: btcVal.toFixed(6) });
    items.push({ namespace: NS, key: BTC_DATE_KEY, type: "single_line_text_field", value: todayRO });
  }
  // Update EGLD if changed
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

function shopHeaders(env) {
  return {
    "X-Shopify-Access-Token": env.SHOP_TOKEN,
    "Content-Type": "application/json"
  };
}

async function getShopId(env, v) {
  const r = await fetch(`https://${env.SHOP_URL}/admin/api/${v}/shop.json`, {
    headers: shopHeaders(env)
  });
  if (!r.ok) throw new Error(`Get shop failed: ${r.status} ${await r.text()}`);
  return (await r.json()).shop.id;
}

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

/* --------- Date helper --------- */

// Return YYYY-MM-DD in Europe/Bucharest local time.
function romaniaISODate(d) {
  const tz  = 'Europe/Bucharest';
  const y   = new Intl.DateTimeFormat('ro-RO',{ timeZone: tz, year: 'numeric' }).format(d);
  const m   = new Intl.DateTimeFormat('ro-RO',{ timeZone: tz, month: '2-digit' }).format(d);
  const day = new Intl.DateTimeFormat('ro-RO',{ timeZone: tz, day: '2-digit' }).format(d);
  return `${y}-${m}-${day}`;
}
