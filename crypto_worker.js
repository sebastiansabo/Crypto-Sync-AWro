/*
 * Cloudflare Worker: Crypto Rate Sync (24/7)
 *
 * This worker fetches the latest cryptocurrency prices for Bitcoin (BTC) and
 * MultiversX (EGLD) from the CoinMarketCap API and stores them in Shopify
 * metafields. It mirrors the structure of the existing BNR EUR→RON worker,
 * but **does not skip weekends or holidays**, because crypto trading never stops.
 * Rates are written into the `custom` namespace under the keys defined below,
 * along with separate date metafields tracking when the values were last updated.
 * Prices are fetched in the fiat currency specified by the `CMC_CONVERT`
 * environment variable (default USD).
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

const CMC_URL       = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest";
const NS            = "custom";
const BTC_KEY       = "custom_crypto_btc";     // number_decimal
const EGLD_KEY      = "crypto_egld";          // number_decimal
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

async function run(env, { force }) {
  const v = env.API_VERSION || "2024-04";
  if (!env.SHOP_URL || !env.SHOP_TOKEN)
    throw new Error("Missing SHOP_URL or SHOP_TOKEN.");
  if (!env.CMC_API_KEY)
    throw new Error("Missing CMC_API_KEY.");

  // Always run (crypto markets are open 24/7). We still capture the current date
  // so that the `_date` metafields reflect when the update occurred.
  const todayRO = romaniaISODate(new Date());

  // Fetch latest rates for BTC and EGLD from CoinMarketCap
  const convert  = env.CMC_CONVERT || "USD";
  const rates    = await fetchCMC(env, ["BTC", "EGLD"], convert);
  const btcVal   = rates.BTC;
  const egldVal  = rates.EGLD;

  // Resolve shop ID once
  const shopId  = await getShopId(env, v);
  const shopGID = `gid://shopify/Shop/${shopId}`;

  // Read existing metafields (both values and dates)
  const meta        = await readCryptoMetafields(env, v, NS);
  const oldBTC      = meta.btc?.value ? parseFloat(meta.btc.value) : null;
  const oldBTCDate  = meta.btcDate?.value || null;
  const oldEGLD     = meta.egld?.value ? parseFloat(meta.egld.value) : null;
  const oldEGLDDate = meta.egldDate?.value || null;

  let wrote = false;
  const items = [];
  if (oldBTC === null || Math.abs(btcVal - oldBTC) >= EPS) {
    items.push({ namespace: NS, key: BTC_KEY, type: "number_decimal", value: btcVal.toFixed(6) });
    items.push({ namespace: NS, key: BTC_DATE_KEY, type: "single_line_text_field", value: todayRO });
  }
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

/* Shopify and helper functions remain unchanged: shopHeaders(), getShopId(),
   readCryptoMetafields(), metafieldsSet(), fetchCMC(),
   romaniaISODate(), isoWeekday(), isRomaniaHolidayInfo(), romaniaPublicHolidaysISO(),
   holidayName(), orthodoxEasterGregorian(), addDays(), isoOf() */
