import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import minimist from "minimist";
import { chromium } from "playwright";

import { SimilarwebHttpClient } from "./lib/http_client.js";
import { createBigQueryClient, ensureTable, insertRows, sha256Hex, truncateForBigQueryString } from "./lib/bq.js";
import { appendLine } from "./lib/logging.js";
import { monthFromYyyyMm, previousFullMonthUtc, monthRangeUtc, formatDateUTC, formatMonthYyyyMm } from "./lib/dates.js";
import {
  parseDownloadsMetrics,
  parseRevenueMetrics,
  parseOverviewMetrics,
  extractGooglePackageFromText,
} from "./lib/parsers.js";
import { readJsonIfExists, writeJsonAtomic } from "./lib/state.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATASET_ID = process.env.BQ_DATASET || "appstore_eu";

const TABLES = {
  map: "similarweb_app_map",
  alerts: "similarweb_alerts",
  apple: {
    overview: "similarweb_appstore_overview",
    downloads: "similarweb_appstore_store_downloads",
    revenue: "similarweb_appstore_revenue",
  },
  google: {
    overview: "similarweb_googleplay_overview",
    downloads: "similarweb_googleplay_store_downloads",
    revenue: "similarweb_googleplay_revenue",
  },
};

function mustInt(v, name) {
  const n = Number(v);
  if (!Number.isFinite(n) || !Number.isInteger(n)) throw new Error(`Invalid ${name}: ${v}`);
  return n;
}

function buildSimilarwebUrl(routePath, query) {
  const u = new URL(`https://apps.similarweb.com${routePath}`);
  for (const [k, v] of Object.entries(query)) u.searchParams.set(k, String(v));
  return u.toString();
}

function nowIso() {
  return new Date().toISOString();
}

async function ensureSchema(bq) {
  const monthPartitioning = { type: "MONTH", field: "month" };

  await ensureTable(bq, DATASET_ID, TABLES.apple.overview, {
    schema: [
      { name: "month", type: "DATE", mode: "REQUIRED" },
      { name: "country", type: "INT64", mode: "REQUIRED" },
      { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "app_id", type: "INT64", mode: "REQUIRED" },
      { name: "page_url", type: "STRING", mode: "REQUIRED" },
      { name: "raw_text", type: "STRING", mode: "NULLABLE" },
      { name: "total_downloads", type: "FLOAT64", mode: "NULLABLE" },
      { name: "total_revenue", type: "FLOAT64", mode: "NULLABLE" },
    ],
    timePartitioning: monthPartitioning,
    clustering: { fields: ["app_id"] },
  });

  await ensureTable(bq, DATASET_ID, TABLES.apple.downloads, {
    schema: [
      { name: "month", type: "DATE", mode: "REQUIRED" },
      { name: "country", type: "INT64", mode: "REQUIRED" },
      { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "app_id", type: "INT64", mode: "REQUIRED" },
      { name: "page_url", type: "STRING", mode: "REQUIRED" },
      { name: "raw_text", type: "STRING", mode: "NULLABLE" },
      { name: "total_downloads", type: "FLOAT64", mode: "NULLABLE" },
    ],
    timePartitioning: monthPartitioning,
    clustering: { fields: ["app_id"] },
  });

  await ensureTable(bq, DATASET_ID, TABLES.apple.revenue, {
    schema: [
      { name: "month", type: "DATE", mode: "REQUIRED" },
      { name: "country", type: "INT64", mode: "REQUIRED" },
      { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "app_id", type: "INT64", mode: "REQUIRED" },
      { name: "page_url", type: "STRING", mode: "REQUIRED" },
      { name: "raw_text", type: "STRING", mode: "NULLABLE" },
      { name: "total_revenue", type: "FLOAT64", mode: "NULLABLE" },
    ],
    timePartitioning: monthPartitioning,
    clustering: { fields: ["app_id"] },
  });

  for (const tab of ["overview", "downloads", "revenue"]) {
    const tableId = TABLES.google[tab];
    const common = [
      { name: "month", type: "DATE", mode: "REQUIRED" },
      { name: "country", type: "INT64", mode: "REQUIRED" },
      { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "app_id", type: "INT64", mode: "REQUIRED" },
      { name: "google_package", type: "STRING", mode: "REQUIRED" },
      { name: "page_url", type: "STRING", mode: "REQUIRED" },
      { name: "raw_text", type: "STRING", mode: "NULLABLE" },
    ];
    let schema = common;
    if (tab === "overview") {
      schema = schema.concat([
        { name: "total_downloads", type: "FLOAT64", mode: "NULLABLE" },
        { name: "total_revenue", type: "FLOAT64", mode: "NULLABLE" },
      ]);
    } else if (tab === "downloads") {
      schema = schema.concat([{ name: "total_downloads", type: "FLOAT64", mode: "NULLABLE" }]);
    } else if (tab === "revenue") {
      schema = schema.concat([{ name: "total_revenue", type: "FLOAT64", mode: "NULLABLE" }]);
    }

    await ensureTable(bq, DATASET_ID, tableId, {
      schema,
      timePartitioning: monthPartitioning,
      clustering: { fields: ["app_id", "google_package"] },
    });
  }

  await ensureTable(bq, DATASET_ID, TABLES.map, {
    schema: [
      { name: "app_id", type: "INT64", mode: "REQUIRED" },
      { name: "google_package", type: "STRING", mode: "REQUIRED" },
      { name: "first_seen", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "last_seen", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "source_url", type: "STRING", mode: "NULLABLE" },
    ],
    clustering: { fields: ["app_id"] },
  });

  await ensureTable(bq, DATASET_ID, TABLES.alerts, {
    schema: [
      { name: "app_id", type: "INT64", mode: "NULLABLE" },
      { name: "google_package", type: "STRING", mode: "NULLABLE" },
      { name: "tab", type: "STRING", mode: "NULLABLE" },
      { name: "store", type: "STRING", mode: "NULLABLE" },
      { name: "stage", type: "STRING", mode: "NULLABLE" },
      { name: "error_type", type: "STRING", mode: "NULLABLE" },
      { name: "error_message", type: "STRING", mode: "NULLABLE" },
      { name: "page_url", type: "STRING", mode: "NULLABLE" },
      { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
    ],
    timePartitioning: { type: "DAY", field: "pulled_at" },
    clustering: { fields: ["store", "tab", "error_type"] },
  });
}

async function insertAlert(bq, alertRow, alertsLogPath) {
  const pulledAt = new Date().toISOString();
  const row = { ...alertRow, pulled_at: pulledAt };

  try {
    const table = bq.dataset(DATASET_ID).table(TABLES.alerts);
    await insertRows(table, [row]);
  } catch (err) {
    await appendLine(alertsLogPath, JSON.stringify({ ...row, bq_insert_error: String(err) }));
    throw err;
  }

  await appendLine(alertsLogPath, JSON.stringify(row));
}

async function querySelectedAppIds({ bq, monthDate, country, limit, mode }) {
  const monthStr = formatDateUTC(monthDate);
  const limitInt = mustInt(limit, "--limit");
  const countryInt = mustInt(country, "--country");

  const baseCte = `
    WITH base AS (
      SELECT DISTINCT app_id
      FROM \`esoteric-parsec-147012.appstore_eu.app_urls_raw\`
      WHERE app_id IS NOT NULL
    ),
    meta AS (
      SELECT
        app_id,
        MAX(CAST(user_rating_count AS INT64)) AS max_rating_count,
        MAX(IF(country = 'us', 1, 0)) AS has_us
      FROM \`esoteric-parsec-147012.appstore_eu.app_metadata_by_country\`
      GROUP BY app_id
    ),
    joined AS (
      SELECT
        b.app_id,
        COALESCE(m.max_rating_count, 0) AS max_rating_count,
        COALESCE(m.has_us, 0) AS has_us
      FROM base b
      LEFT JOIN meta m USING (app_id)
    ),
    already_month AS (
      SELECT DISTINCT app_id
      FROM \`esoteric-parsec-147012.appstore_eu.${TABLES.apple.overview}\`
      WHERE month = @month AND country = @country
    )
  `;

  let whereClause = "WHERE app_id NOT IN (SELECT app_id FROM already_month)";
  let orderClause = "ORDER BY has_us DESC, max_rating_count DESC";

  if (mode === "weekly") {
    whereClause = `
      WHERE app_id NOT IN (SELECT app_id FROM already_month)
        AND app_id NOT IN (SELECT app_id FROM \`esoteric-parsec-147012.appstore_eu.${TABLES.map}\`)
    `;
    orderClause = "ORDER BY has_us DESC, max_rating_count DESC";
  }

  const query = `
    ${baseCte}
    SELECT app_id
    FROM joined
    ${whereClause}
    ${orderClause}
    LIMIT @limit
  `;

  const [rows] = await bq.query({
    query,
    params: { month: monthStr, country: countryInt, limit: limitInt },
  });
  return rows.map((r) => Number(r.app_id)).filter((n) => Number.isFinite(n));
}

async function lookupGooglePackageFromMap(bq, appId) {
  const query = `
    SELECT google_package
    FROM \`esoteric-parsec-147012.appstore_eu.${TABLES.map}\`
    WHERE app_id = @app_id
    ORDER BY last_seen DESC
    LIMIT 1
  `;
  const [rows] = await bq.query({ query, params: { app_id: appId } });
  return rows[0]?.google_package || null;
}

async function upsertMapRow(bq, { appId, googlePackage, sourceUrl }) {
  const query = `
    MERGE \`esoteric-parsec-147012.appstore_eu.${TABLES.map}\` T
    USING (SELECT @app_id AS app_id, @google_package AS google_package, @source_url AS source_url) S
    ON T.app_id = S.app_id
    WHEN MATCHED THEN
      UPDATE SET
        google_package = S.google_package,
        last_seen = CURRENT_TIMESTAMP(),
        source_url = COALESCE(S.source_url, T.source_url)
    WHEN NOT MATCHED THEN
      INSERT (app_id, google_package, first_seen, last_seen, source_url)
      VALUES (S.app_id, S.google_package, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), S.source_url)
  `;
  await bq.query({
    query,
    params: { app_id: appId, google_package: googlePackage, source_url: sourceUrl || null },
  });
}

async function resolveGooglePackageViaPlaywright({ appId, country, fromDate, toDate }) {
  const storagePath = path.join(__dirname, "storageState.json");

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ storageState: storagePath });
  const page = await context.newPage();

  const appleUrl = buildSimilarwebUrl(`/app-analysis/overview/apple/${appId}`, {
    country,
    from: formatDateUTC(fromDate),
    to: formatDateUTC(toDate),
    window: "false",
  });

  await page.goto(appleUrl, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(1500);

  const href = await page.evaluate(() => {
    const anchors = Array.from(document.querySelectorAll("a[href]"));
    const link = anchors.find((a) => /\/app-analysis\/overview\/google\//i.test(a.getAttribute("href") || ""));
    return link ? link.getAttribute("href") : null;
  });

  await browser.close();

  if (!href) return null;
  const m = String(href).match(/\/app-analysis\/overview\/google\/([^?"'\s]+)/i);
  if (!m) return null;
  return m[1];
}

async function scrapeOneTab({
  http,
  store,
  tab,
  id,
  monthStr,
  country,
  fromStr,
  toStr,
  bqTable,
  appId,
  googlePackage,
  alertsLogPath,
  bq,
}) {
  const route = `/app-analysis/${tab}/${store}/${id}`;
  const pageUrl = buildSimilarwebUrl(route, { country, from: fromStr, to: toStr, window: "false" });

  try {
    const { text } = await http.fetchRscText(pageUrl);

    const rawText = truncateForBigQueryString(text);
    let metrics = {};
    if (tab === "store-downloads") metrics = parseDownloadsMetrics(text);
    else if (tab === "revenue") metrics = parseRevenueMetrics(text);
    else if (tab === "overview") metrics = parseOverviewMetrics(text);

    const row = {
      month: monthStr,
      country,
      pulled_at: new Date().toISOString(),
      app_id: appId,
      page_url: pageUrl,
      raw_text: rawText,
      ...metrics,
    };
    if (googlePackage) row.google_package = googlePackage;

    await insertRows(bqTable, [row]);
    return { pageUrl, text };
  } catch (err) {
    await insertAlert(
      bq,
      {
        app_id: appId,
        google_package: googlePackage || null,
        tab,
        store,
        stage: "fetch_or_insert",
        error_type: err?.code || "error",
        error_message: String(err?.message || err),
        page_url: pageUrl,
      },
      alertsLogPath
    );
    throw err;
  }
}

async function main() {
  const argv = minimist(process.argv.slice(2), {
    string: ["month", "mode", "country"],
    default: { limit: 150, mode: "backfill", country: "999" },
  });

  const mode = String(argv.mode || "backfill");
  if (!["backfill", "weekly"].includes(mode)) throw new Error(`Invalid --mode: ${mode}`);
  const limit = mustInt(argv.limit ?? 150, "--limit");
  const country = mustInt(argv.country ?? 999, "--country");

  const monthDate = argv.month ? monthFromYyyyMm(argv.month) : previousFullMonthUtc();
  const monthStr = formatDateUTC(monthDate);
  const { from, to } = monthRangeUtc(monthDate);
  const fromStr = formatDateUTC(from);
  const toStr = formatDateUTC(to);

  const runDate = new Date();
  const yyyymmdd = runDate.toISOString().slice(0, 10);
  const alertsLogPath = path.join(__dirname, "..", "logs", `similarweb_alerts_${yyyymmdd}.log`);
  const runLogPath = path.join(__dirname, "..", "logs", `similarweb_run_${yyyymmdd}.log`);
  const statePath = path.join(__dirname, "state_similarweb.json");
  const cookiesPath = path.join(__dirname, "cookies.json");
  const storageStatePath = path.join(__dirname, "storageState.json");

  try {
    await fs.access(cookiesPath);
    await fs.access(storageStatePath);
  } catch {
    throw new Error("Missing Similarweb session files. Run `node tools/similarweb_login.js` first (creates tools/cookies.json + tools/storageState.json).");
  }

  const bq = createBigQueryClient();
  await ensureSchema(bq);

  const http = new SimilarwebHttpClient({ cookiesPath });

  const state = (await readJsonIfExists(statePath)) || null;
  const stateMatches =
    state &&
    state.month === monthStr &&
    state.mode === mode &&
    Number(state.country) === country &&
    Array.isArray(state.selected_app_ids);

  let selectedAppIds = [];
  if (stateMatches) {
    selectedAppIds = state.selected_app_ids;
  } else {
    selectedAppIds = await querySelectedAppIds({ bq, monthDate, country, limit, mode });
  }

  const selectedHash = sha256Hex(JSON.stringify({ month: monthStr, mode, country, selectedAppIds }));
  const resumeIndex = stateMatches ? Math.max(0, mustInt(state.last_index || 0, "state.last_index")) : 0;

  await writeJsonAtomic(statePath, {
    month: monthStr,
    mode,
    country,
    limit,
    selected_app_ids_hash: selectedHash,
    selected_app_ids: selectedAppIds,
    last_index: resumeIndex,
    processed_count: stateMatches ? mustInt(state.processed_count || 0, "state.processed_count") : 0,
    started_at: stateMatches ? state.started_at : nowIso(),
    updated_at: nowIso(),
  });

  await appendLine(
    runLogPath,
    JSON.stringify({
      event: "start",
      at: nowIso(),
      month: monthStr,
      month_yyyy_mm: formatMonthYyyyMm(monthDate),
      mode,
      country,
      limit,
      selected_count: selectedAppIds.length,
      resume_index: resumeIndex,
      selected_hash: selectedHash,
    })
  );

  const appleTables = {
    overview: bq.dataset(DATASET_ID).table(TABLES.apple.overview),
    downloads: bq.dataset(DATASET_ID).table(TABLES.apple.downloads),
    revenue: bq.dataset(DATASET_ID).table(TABLES.apple.revenue),
  };
  const googleTables = {
    overview: bq.dataset(DATASET_ID).table(TABLES.google.overview),
    downloads: bq.dataset(DATASET_ID).table(TABLES.google.downloads),
    revenue: bq.dataset(DATASET_ID).table(TABLES.google.revenue),
  };

  const t0 = Date.now();

  for (let i = resumeIndex; i < selectedAppIds.length; i += 1) {
    const appId = selectedAppIds[i];
    const appStart = Date.now();
    const idx1 = i + 1;

    try {
      const appleOverview = await scrapeOneTab({
        http,
        store: "apple",
        tab: "overview",
        id: String(appId),
        monthStr,
        country,
        fromStr,
        toStr,
        bqTable: appleTables.overview,
        appId,
        googlePackage: null,
        alertsLogPath,
        bq,
      });

      await scrapeOneTab({
        http,
        store: "apple",
        tab: "store-downloads",
        id: String(appId),
        monthStr,
        country,
        fromStr,
        toStr,
        bqTable: appleTables.downloads,
        appId,
        googlePackage: null,
        alertsLogPath,
        bq,
      });

      await scrapeOneTab({
        http,
        store: "apple",
        tab: "revenue",
        id: String(appId),
        monthStr,
        country,
        fromStr,
        toStr,
        bqTable: appleTables.revenue,
        appId,
        googlePackage: null,
        alertsLogPath,
        bq,
      });

      let googlePackage = await lookupGooglePackageFromMap(bq, appId);
      if (!googlePackage) googlePackage = extractGooglePackageFromText(appleOverview.text);

      if (!googlePackage) {
        try {
          googlePackage = await resolveGooglePackageViaPlaywright({
            appId,
            country,
            fromDate: from,
            toDate: to,
          });
        } catch (err) {
          await insertAlert(
            bq,
            {
              app_id: appId,
              google_package: null,
              tab: "overview",
              store: "apple",
              stage: "resolve_google_package",
              error_type: err?.code || "error",
              error_message: String(err?.message || err),
              page_url: null,
            },
            alertsLogPath
          );
        }
      }

      if (googlePackage) {
        await upsertMapRow(bq, { appId, googlePackage, sourceUrl: appleOverview.pageUrl });

        await scrapeOneTab({
          http,
          store: "google",
          tab: "overview",
          id: googlePackage,
          monthStr,
          country,
          fromStr,
          toStr,
          bqTable: googleTables.overview,
          appId,
          googlePackage,
          alertsLogPath,
          bq,
        });

        await scrapeOneTab({
          http,
          store: "google",
          tab: "store-downloads",
          id: googlePackage,
          monthStr,
          country,
          fromStr,
          toStr,
          bqTable: googleTables.downloads,
          appId,
          googlePackage,
          alertsLogPath,
          bq,
        });

        await scrapeOneTab({
          http,
          store: "google",
          tab: "revenue",
          id: googlePackage,
          monthStr,
          country,
          fromStr,
          toStr,
          bqTable: googleTables.revenue,
          appId,
          googlePackage,
          alertsLogPath,
          bq,
        });
      } else {
        await insertAlert(
          bq,
          {
            app_id: appId,
            google_package: null,
            tab: "overview",
            store: "google",
            stage: "resolve_google_package",
            error_type: "missing_google_package",
            error_message: "Could not determine google_package from RSC or mapping",
            page_url: null,
          },
          alertsLogPath
        );
      }
    } catch (err) {
      if (err?.code === "SW_LOGIN_EXPIRED" || err?.code === "SW_BLOCKED") {
        await appendLine(
          runLogPath,
          JSON.stringify({ event: "fatal", at: nowIso(), app_id: appId, error: String(err?.message || err) })
        );
        throw err;
      }
    } finally {
      const prevState = (await readJsonIfExists(statePath)) || {};
      const processedCount = Number(prevState.processed_count || 0) + 1;
      await writeJsonAtomic(statePath, {
        ...prevState,
        last_index: i + 1,
        processed_count: processedCount,
        updated_at: nowIso(),
      });

      const elapsedMs = Date.now() - t0;
      if (idx1 % 10 === 0) {
        const hours = elapsedMs / (1000 * 60 * 60);
        const rate = hours > 0 ? (idx1 / hours).toFixed(1) : "inf";
        process.stdout.write(`Progress ${idx1}/${selectedAppIds.length} | ${rate} apps/hour\n`);
      }
      const appMs = Date.now() - appStart;
      await appendLine(runLogPath, JSON.stringify({ event: "app_done", at: nowIso(), app_id: appId, ms: appMs }));
    }
  }

  await appendLine(runLogPath, JSON.stringify({ event: "done", at: nowIso() }));
}

main().catch((err) => {
  process.stderr.write(`${err?.stack || err}\n`);
  process.exitCode = 1;
});



