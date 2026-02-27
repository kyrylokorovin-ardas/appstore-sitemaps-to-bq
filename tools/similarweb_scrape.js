import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import minimist from "minimist";
import { chromium } from "playwright";

import { SimilarwebHttpClient } from "./lib/httpClient.js";
import { createBigQueryClient, ensureTable, insertRows, sha256Hex, truncateForBigQueryString } from "./lib/bq.js";
import { appendLine } from "./lib/logging.js";
import { monthFromYyyyMm, previousFullMonthUtc, monthRangeUtc, formatDateUTC, formatMonthYyyyMm } from "./lib/dates.js";
import {
  extractGooglePackageFromRsc,
  parseAudience,
  parseOverviewPerformance,
  parseRevenueTotal,
  parseReviewsReplyRate,
  parseTechnographicsOverview,
  parseTechnographicsSdks,
  parseUsageSessions,
} from "./lib/parser.js";
import { readJsonIfExists, writeJsonAtomic } from "./lib/state.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_ID = process.env.GCP_PROJECT || "esoteric-parsec-147012";
const DATASET_ID = process.env.BQ_DATASET || "appstore_eu";

const COUNTRY_DEFAULT = 999;

const TABLES = {
  map: "similarweb_app_map",
  alerts: "similarweb_alerts",
  apple: {
    overview: "similarweb_appstore_overview",
    reviews: "similarweb_appstore_reviews",
    usage_sessions: "similarweb_appstore_usage_sessions",
    technographics_overview: "similarweb_appstore_technographics_overview",
    technographics_sdks: "similarweb_appstore_technographics_sdks",
    revenue: "similarweb_appstore_revenue",
    audience: "similarweb_appstore_audience",
  },
  google: {
    overview: "similarweb_googleplay_overview",
    reviews: "similarweb_googleplay_reviews",
    usage_sessions: "similarweb_googleplay_usage_sessions",
    technographics_overview: "similarweb_googleplay_technographics_overview",
    technographics_sdks: "similarweb_googleplay_technographics_sdks",
    revenue: "similarweb_googleplay_revenue",
    audience: "similarweb_googleplay_audience",
  },
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function jitter(minMs, maxMs) {
  return minMs + Math.floor(Math.random() * (maxMs - minMs + 1));
}

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

function todayYyyyMmDdUtc() {
  const d = new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

async function ensureSchemas(bq) {
  const monthPartitioning = { type: "MONTH", field: "month" };

  const overviewSchema = (withGooglePackage) =>
    [
      { name: "month", type: "DATE", mode: "REQUIRED" },
      { name: "country", type: "INT64", mode: "REQUIRED" },
      { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "app_id", type: "INT64", mode: "REQUIRED" },
      ...(withGooglePackage
        ? [{ name: "google_package", type: "STRING", mode: "REQUIRED" }]
        : [{ name: "google_package", type: "STRING", mode: "NULLABLE" }]),
      { name: "store_downloads", type: "FLOAT64", mode: "NULLABLE" },
      { name: "ranking_text", type: "STRING", mode: "NULLABLE" },
      { name: "rating_avg", type: "FLOAT64", mode: "NULLABLE" },
      { name: "ratings_count", type: "FLOAT64", mode: "NULLABLE" },
      { name: "analyzed_reviews_total", type: "FLOAT64", mode: "NULLABLE" },
      { name: "analyzed_reviews_negative", type: "FLOAT64", mode: "NULLABLE" },
      { name: "analyzed_reviews_mixed", type: "FLOAT64", mode: "NULLABLE" },
      { name: "analyzed_reviews_positive", type: "FLOAT64", mode: "NULLABLE" },
      { name: "mau", type: "FLOAT64", mode: "NULLABLE" },
      { name: "daily_stickiness_pct", type: "FLOAT64", mode: "NULLABLE" },
      { name: "revenue_usd", type: "FLOAT64", mode: "NULLABLE" },
      { name: "revenue_text", type: "STRING", mode: "NULLABLE" },
      { name: "page_url", type: "STRING", mode: "REQUIRED" },
      { name: "raw_rsc_text", type: "STRING", mode: "NULLABLE" },
    ];

  const reviewsSchema = (withGooglePackage) =>
    [
      { name: "month", type: "DATE", mode: "REQUIRED" },
      { name: "country", type: "INT64", mode: "REQUIRED" },
      { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "app_id", type: "INT64", mode: "REQUIRED" },
      ...(withGooglePackage
        ? [{ name: "google_package", type: "STRING", mode: "REQUIRED" }]
        : [{ name: "google_package", type: "STRING", mode: "NULLABLE" }]),
      { name: "reply_rate_pct", type: "FLOAT64", mode: "NULLABLE" },
      { name: "page_url", type: "STRING", mode: "REQUIRED" },
      { name: "raw_rsc_text", type: "STRING", mode: "NULLABLE" },
    ];

  const usageSchema = (withGooglePackage) =>
    [
      { name: "month", type: "DATE", mode: "REQUIRED" },
      { name: "country", type: "INT64", mode: "REQUIRED" },
      { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "app_id", type: "INT64", mode: "REQUIRED" },
      ...(withGooglePackage
        ? [{ name: "google_package", type: "STRING", mode: "REQUIRED" }]
        : [{ name: "google_package", type: "STRING", mode: "NULLABLE" }]),
      { name: "mau", type: "FLOAT64", mode: "NULLABLE" },
      { name: "wau", type: "FLOAT64", mode: "NULLABLE" },
      { name: "dau", type: "FLOAT64", mode: "NULLABLE" },
      { name: "daily_stickiness_pct", type: "FLOAT64", mode: "NULLABLE" },
      { name: "avg_sessions_per_user", type: "FLOAT64", mode: "NULLABLE" },
      { name: "avg_session_time_sec", type: "INT64", mode: "NULLABLE" },
      { name: "avg_session_time_text", type: "STRING", mode: "NULLABLE" },
      { name: "avg_total_time_sec", type: "INT64", mode: "NULLABLE" },
      { name: "avg_total_time_text", type: "STRING", mode: "NULLABLE" },
      { name: "page_url", type: "STRING", mode: "REQUIRED" },
      { name: "raw_rsc_text", type: "STRING", mode: "NULLABLE" },
    ];

  const technoOverviewSchema = (withGooglePackage) =>
    [
      { name: "month", type: "DATE", mode: "REQUIRED" },
      { name: "country", type: "INT64", mode: "REQUIRED" },
      { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "app_id", type: "INT64", mode: "REQUIRED" },
      ...(withGooglePackage
        ? [{ name: "google_package", type: "STRING", mode: "REQUIRED" }]
        : [{ name: "google_package", type: "STRING", mode: "NULLABLE" }]),
      { name: "sdks_total", type: "FLOAT64", mode: "NULLABLE" },
      { name: "page_url", type: "STRING", mode: "REQUIRED" },
      { name: "raw_rsc_text", type: "STRING", mode: "NULLABLE" },
    ];

  const technoSdksSchema = (withGooglePackage) =>
    [
      { name: "month", type: "DATE", mode: "REQUIRED" },
      { name: "country", type: "INT64", mode: "REQUIRED" },
      { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "app_id", type: "INT64", mode: "REQUIRED" },
      ...(withGooglePackage
        ? [{ name: "google_package", type: "STRING", mode: "REQUIRED" }]
        : [{ name: "google_package", type: "STRING", mode: "NULLABLE" }]),
      { name: "sdk_name", type: "STRING", mode: "REQUIRED" },
      { name: "sdk_category", type: "STRING", mode: "NULLABLE" },
      { name: "installed", type: "BOOL", mode: "NULLABLE" },
      { name: "installed_date", type: "DATE", mode: "NULLABLE" },
      { name: "page_url", type: "STRING", mode: "REQUIRED" },
      { name: "raw_rsc_text", type: "STRING", mode: "NULLABLE" },
    ];

  const revenueSchema = (withGooglePackage) =>
    [
      { name: "month", type: "DATE", mode: "REQUIRED" },
      { name: "country", type: "INT64", mode: "REQUIRED" },
      { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "app_id", type: "INT64", mode: "REQUIRED" },
      ...(withGooglePackage
        ? [{ name: "google_package", type: "STRING", mode: "REQUIRED" }]
        : [{ name: "google_package", type: "STRING", mode: "NULLABLE" }]),
      { name: "total_revenue_usd", type: "FLOAT64", mode: "NULLABLE" },
      { name: "page_url", type: "STRING", mode: "REQUIRED" },
      { name: "raw_rsc_text", type: "STRING", mode: "NULLABLE" },
    ];

  const audienceSchema = (withGooglePackage) =>
    [
      { name: "month", type: "DATE", mode: "REQUIRED" },
      { name: "country", type: "INT64", mode: "REQUIRED" },
      { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "app_id", type: "INT64", mode: "REQUIRED" },
      ...(withGooglePackage
        ? [{ name: "google_package", type: "STRING", mode: "REQUIRED" }]
        : [{ name: "google_package", type: "STRING", mode: "NULLABLE" }]),
      { name: "gender_male_pct", type: "FLOAT64", mode: "NULLABLE" },
      { name: "gender_female_pct", type: "FLOAT64", mode: "NULLABLE" },
      { name: "age_18_24_pct", type: "FLOAT64", mode: "NULLABLE" },
      { name: "age_25_34_pct", type: "FLOAT64", mode: "NULLABLE" },
      { name: "age_35_44_pct", type: "FLOAT64", mode: "NULLABLE" },
      { name: "age_45_54_pct", type: "FLOAT64", mode: "NULLABLE" },
      { name: "age_55_plus_pct", type: "FLOAT64", mode: "NULLABLE" },
      { name: "page_url", type: "STRING", mode: "REQUIRED" },
      { name: "raw_rsc_text", type: "STRING", mode: "NULLABLE" },
    ];

  const alertSchema = [
    { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
    { name: "store", type: "STRING", mode: "NULLABLE" },
    { name: "tab", type: "STRING", mode: "NULLABLE" },
    { name: "stage", type: "STRING", mode: "NULLABLE" },
    { name: "app_id", type: "INT64", mode: "NULLABLE" },
    { name: "google_package", type: "STRING", mode: "NULLABLE" },
    { name: "page_url", type: "STRING", mode: "NULLABLE" },
    { name: "error_type", type: "STRING", mode: "NULLABLE" },
    { name: "error_message", type: "STRING", mode: "NULLABLE" },
  ];

  const mapSchema = [
    { name: "app_id", type: "INT64", mode: "REQUIRED" },
    { name: "google_package", type: "STRING", mode: "REQUIRED" },
    { name: "first_seen", type: "TIMESTAMP", mode: "REQUIRED" },
    { name: "last_seen", type: "TIMESTAMP", mode: "REQUIRED" },
    { name: "source_url", type: "STRING", mode: "NULLABLE" },
  ];

  const appleClustering = { fields: ["app_id"] };
  const googleClustering = { fields: ["app_id", "google_package"] };

  await ensureTable(bq, DATASET_ID, TABLES.apple.overview, {
    schema: overviewSchema(false),
    timePartitioning: monthPartitioning,
    clustering: appleClustering,
  });
  await ensureTable(bq, DATASET_ID, TABLES.google.overview, {
    schema: overviewSchema(true),
    timePartitioning: monthPartitioning,
    clustering: googleClustering,
  });

  await ensureTable(bq, DATASET_ID, TABLES.apple.reviews, {
    schema: reviewsSchema(false),
    timePartitioning: monthPartitioning,
    clustering: appleClustering,
  });
  await ensureTable(bq, DATASET_ID, TABLES.google.reviews, {
    schema: reviewsSchema(true),
    timePartitioning: monthPartitioning,
    clustering: googleClustering,
  });

  await ensureTable(bq, DATASET_ID, TABLES.apple.usage_sessions, {
    schema: usageSchema(false),
    timePartitioning: monthPartitioning,
    clustering: appleClustering,
  });
  await ensureTable(bq, DATASET_ID, TABLES.google.usage_sessions, {
    schema: usageSchema(true),
    timePartitioning: monthPartitioning,
    clustering: googleClustering,
  });

  await ensureTable(bq, DATASET_ID, TABLES.apple.technographics_overview, {
    schema: technoOverviewSchema(false),
    timePartitioning: monthPartitioning,
    clustering: appleClustering,
  });
  await ensureTable(bq, DATASET_ID, TABLES.google.technographics_overview, {
    schema: technoOverviewSchema(true),
    timePartitioning: monthPartitioning,
    clustering: googleClustering,
  });

  await ensureTable(bq, DATASET_ID, TABLES.apple.technographics_sdks, {
    schema: technoSdksSchema(false),
    timePartitioning: monthPartitioning,
    clustering: { fields: ["app_id", "sdk_name"] },
  });
  await ensureTable(bq, DATASET_ID, TABLES.google.technographics_sdks, {
    schema: technoSdksSchema(true),
    timePartitioning: monthPartitioning,
    clustering: { fields: ["app_id", "google_package", "sdk_name"] },
  });

  await ensureTable(bq, DATASET_ID, TABLES.apple.revenue, {
    schema: revenueSchema(false),
    timePartitioning: monthPartitioning,
    clustering: appleClustering,
  });
  await ensureTable(bq, DATASET_ID, TABLES.google.revenue, {
    schema: revenueSchema(true),
    timePartitioning: monthPartitioning,
    clustering: googleClustering,
  });

  await ensureTable(bq, DATASET_ID, TABLES.apple.audience, {
    schema: audienceSchema(false),
    timePartitioning: monthPartitioning,
    clustering: appleClustering,
  });
  await ensureTable(bq, DATASET_ID, TABLES.google.audience, {
    schema: audienceSchema(true),
    timePartitioning: monthPartitioning,
    clustering: googleClustering,
  });

  await ensureTable(bq, DATASET_ID, TABLES.map, {
    schema: mapSchema,
    clustering: { fields: ["app_id"] },
  });

  await ensureTable(bq, DATASET_ID, TABLES.alerts, {
    schema: alertSchema,
    timePartitioning: { type: "DAY", field: "pulled_at" },
    clustering: { fields: ["store", "tab", "error_type"] },
  });
}

async function insertAlert(bq, row, alertsLogPath) {
  const record = { ...row, pulled_at: new Date().toISOString() };
  await appendLine(alertsLogPath, JSON.stringify(record));
  try {
    const table = bq.dataset(DATASET_ID).table(TABLES.alerts);
    await insertRows(table, [record]);
  } catch (err) {
    await appendLine(alertsLogPath, JSON.stringify({ ...record, bq_insert_error: String(err) }));
  }
}

async function upsertMapRow(bq, { appId, googlePackage, sourceUrl }) {
  const query = `
    MERGE \`${PROJECT_ID}.${DATASET_ID}.${TABLES.map}\` T
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

async function lookupGooglePackageFromMap(bq, appId) {
  const query = `
    SELECT google_package
    FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLES.map}\`
    WHERE app_id = @app_id
    ORDER BY last_seen DESC
    LIMIT 1
  `;
  const [rows] = await bq.query({ query, params: { app_id: appId } });
  return rows[0]?.google_package || null;
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
  return m ? m[1] : null;
}

async function querySelectedAppIds({ bq, mode, limit, monthStr, country }) {
  const limitInt = mustInt(limit, "--limit");
  const countryInt = mustInt(country, "--country");

  const sourceCte =
    mode === "weekly"
      ? "SELECT DISTINCT app_id FROM `esoteric-parsec-147012.appstore_eu.app_urls_raw` WHERE app_id IS NOT NULL"
      : "SELECT DISTINCT app_id FROM `esoteric-parsec-147012.appstore_eu.app_metadata_by_country` WHERE app_id IS NOT NULL";

  const query = `
    WITH base AS (
      ${sourceCte}
    ),
    meta AS (
      SELECT
        app_id,
        MAX(CAST(user_rating_count AS INT64)) AS user_rating_count_max,
        MAX(IF(country = 'us', 1, 0)) AS has_us,
        MAX(
          COALESCE(
            SAFE_CAST(current_version_release_date AS DATE),
            DATE(SAFE_CAST(current_version_release_date AS TIMESTAMP))
          )
        ) AS release_date
      FROM \`esoteric-parsec-147012.appstore_eu.app_metadata_by_country\`
      GROUP BY app_id
    ),
    filtered AS (
      SELECT
        b.app_id,
        m.user_rating_count_max,
        m.has_us,
        m.release_date,
        DATE_DIFF(CURRENT_DATE('UTC'), m.release_date, DAY) AS days_ago,
        CASE
          WHEN DATE_DIFF(CURRENT_DATE('UTC'), m.release_date, DAY) BETWEEN 0 AND 30 THEN 1
          WHEN DATE_DIFF(CURRENT_DATE('UTC'), m.release_date, DAY) BETWEEN 31 AND 365 THEN 2
          ELSE 99
        END AS tier
      FROM base b
      JOIN meta m USING (app_id)
      WHERE m.release_date IS NOT NULL
        AND DATE_DIFF(CURRENT_DATE('UTC'), m.release_date, DAY) BETWEEN 0 AND 365
    ),
    already_month AS (
      SELECT DISTINCT app_id
      FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLES.apple.overview}\`
      WHERE month = @month AND country = @country
    ),
    mapped AS (
      SELECT DISTINCT app_id
      FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLES.map}\`
    )
    SELECT app_id
    FROM filtered
    WHERE tier IN (1, 2)
      AND app_id NOT IN (SELECT app_id FROM already_month)
      ${mode === "weekly" ? "AND app_id NOT IN (SELECT app_id FROM mapped)" : ""}
    ORDER BY tier ASC, has_us DESC, user_rating_count_max DESC
    LIMIT @limit
  `;

  const [rows] = await bq.query({
    query,
    params: { month: monthStr, country: countryInt, limit: limitInt },
  });
  return rows.map((r) => Number(r.app_id)).filter((n) => Number.isFinite(n));
}

async function rowExists(bq, tableId, { monthStr, country, appId, googlePackage }) {
  const where = ["month = @month", "country = @country", "app_id = @app_id"];
  const params = { month: monthStr, country, app_id: appId };
  if (googlePackage != null) {
    where.push("google_package = @google_package");
    params.google_package = googlePackage;
  } else {
    where.push("google_package IS NULL");
  }

  const query = `
    SELECT 1
    FROM \`${PROJECT_ID}.${DATASET_ID}.${tableId}\`
    WHERE ${where.join(" AND ")}
    LIMIT 1
  `;
  const [rows] = await bq.query({ query, params });
  return rows.length > 0;
}

async function existingSdkNames(bq, tableId, { monthStr, country, appId, googlePackage }) {
  const where = ["month = @month", "country = @country", "app_id = @app_id"];
  const params = { month: monthStr, country, app_id: appId };
  if (googlePackage != null) {
    where.push("google_package = @google_package");
    params.google_package = googlePackage;
  } else {
    where.push("google_package IS NULL");
  }
  const query = `
    SELECT sdk_name
    FROM \`${PROJECT_ID}.${DATASET_ID}.${tableId}\`
    WHERE ${where.join(" AND ")}
  `;
  const [rows] = await bq.query({ query, params });
  return new Set(rows.map((r) => String(r.sdk_name)));
}

async function fetchAndInsert({
  http,
  bq,
  tableId,
  store,
  tab,
  route,
  query,
  monthStr,
  country,
  appId,
  googlePackage,
  parser,
  alertsLogPath,
}) {
  const pageUrl = buildSimilarwebUrl(route, query);

  try {
    const already = await rowExists(bq, tableId, { monthStr, country, appId, googlePackage });
    if (already) return { skipped: true, pageUrl, text: null, parsed: null };

    const { text } = await http.fetchRscText(pageUrl);
    const rawRsc = truncateForBigQueryString(text);

    const parsed = parser ? parser(text) : {};
    const row = {
      month: monthStr,
      country,
      pulled_at: new Date().toISOString(),
      app_id: appId,
      google_package: googlePackage || null,
      ...parsed,
      page_url: pageUrl,
      raw_rsc_text: rawRsc,
    };

    const table = bq.dataset(DATASET_ID).table(tableId);
    await insertRows(table, [row]);

    return { skipped: false, pageUrl, text, parsed };
  } catch (err) {
    await insertAlert(
      bq,
      {
        store,
        tab,
        stage: "fetch_or_insert",
        app_id: appId,
        google_package: googlePackage || null,
        page_url: pageUrl,
        error_type: err?.code || "error",
        error_message: String(err?.message || err),
      },
      alertsLogPath
    );
    throw err;
  }
}

async function scrapeTechnographicsSdks({
  http,
  bq,
  store,
  id,
  monthStr,
  country,
  fromStr,
  toStr,
  appId,
  googlePackage,
  tableId,
  alertsLogPath,
}) {
  const route = `/app-analysis/technographics/${store}/${id}`;
  const pageUrl = buildSimilarwebUrl(route, { country, from: fromStr, to: toStr, window: "false" });

  try {
    const { text } = await http.fetchRscText(pageUrl);
    const rawRsc = truncateForBigQueryString(text);
    const rows = parseTechnographicsSdks(text);
    if (!rows.length) return { inserted: 0, pageUrl };

    const existing = await existingSdkNames(bq, tableId, { monthStr, country, appId, googlePackage });
    const newRows = rows
      .filter((r) => r.sdk_name && !existing.has(r.sdk_name))
      .map((r) => ({
        month: monthStr,
        country,
        pulled_at: new Date().toISOString(),
        app_id: appId,
        google_package: googlePackage || null,
        sdk_name: r.sdk_name,
        sdk_category: r.sdk_category || null,
        installed: r.installed == null ? null : Boolean(r.installed),
        installed_date: r.installed_date || null,
        page_url: pageUrl,
        raw_rsc_text: rawRsc,
      }));

    if (!newRows.length) return { inserted: 0, pageUrl };

    const table = bq.dataset(DATASET_ID).table(tableId);
    await insertRows(table, newRows);
    return { inserted: newRows.length, pageUrl };
  } catch (err) {
    await insertAlert(
      bq,
      {
        store,
        tab: "technographics",
        stage: "fetch_or_insert_sdks",
        app_id: appId,
        google_package: googlePackage || null,
        page_url: pageUrl,
        error_type: err?.code || "error",
        error_message: String(err?.message || err),
      },
      alertsLogPath
    );
    throw err;
  }
}

async function main() {
  const argv = minimist(process.argv.slice(2), {
    boolean: ["dry_run"],
    string: ["month", "mode", "country"],
    default: { limit: 150, mode: "backfill", country: String(COUNTRY_DEFAULT), dry_run: false },
  });

  const mode = String(argv.mode || "backfill");
  if (!["backfill", "weekly"].includes(mode)) throw new Error(`Invalid --mode: ${mode}`);

  const limit = mustInt(argv.limit ?? 150, "--limit");
  const country = mustInt(argv.country ?? COUNTRY_DEFAULT, "--country");

  const monthDate = argv.month ? monthFromYyyyMm(argv.month) : previousFullMonthUtc();
  const monthStr = formatDateUTC(monthDate);
  const { from, to } = monthRangeUtc(monthDate);
  const fromStr = formatDateUTC(from);
  const toStr = formatDateUTC(to);

  const yyyymmdd = todayYyyyMmDdUtc();
  const alertsLogPath = path.join(__dirname, "..", "logs", `similarweb_alerts_${yyyymmdd}.log`);
  const runLogPath = path.join(__dirname, "..", "logs", `similarweb_run_${yyyymmdd}.log`);
  const statePath = path.join(__dirname, "state_similarweb.json");
  const cookiesPath = path.join(__dirname, "cookies.json");
  const storageStatePath = path.join(__dirname, "storageState.json");

  try {
    await fs.access(cookiesPath);
    await fs.access(storageStatePath);
  } catch {
    throw new Error("Missing Similarweb session files. Run `node tools/similarweb_login.js` first.");
  }

  const bq = createBigQueryClient();
  await ensureSchemas(bq);

  const state = (await readJsonIfExists(statePath)) || null;
  const stateMatches =
    state &&
    state.month === monthStr &&
    state.mode === mode &&
    Number(state.country) === country &&
    Number(state.limit) === limit &&
    Array.isArray(state.selected_app_ids);

  let selectedAppIds;
  if (stateMatches) selectedAppIds = state.selected_app_ids;
  else selectedAppIds = await querySelectedAppIds({ bq, mode, limit, monthStr, country });

  const selectedHash = sha256Hex(JSON.stringify({ month: monthStr, mode, country, limit, selectedAppIds }));
  const resumeIndex = stateMatches ? Math.max(0, mustInt(state.last_index || 0, "state.last_index")) : 0;

  if (argv.dry_run) {
    process.stdout.write(
      JSON.stringify(
        {
          month: monthStr,
          mode,
          country,
          limit,
          selected_count: selectedAppIds.length,
          selected_hash: selectedHash,
          sample_app_ids: selectedAppIds.slice(0, 20),
        },
        null,
        2
      ) + "\n"
    );
    return;
  }

  await writeJsonAtomic(statePath, {
    month: monthStr,
    mode,
    country,
    limit,
    selected_hash: selectedHash,
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

  const http = new SimilarwebHttpClient({ cookiesPath });
  const t0 = Date.now();

  for (let i = resumeIndex; i < selectedAppIds.length; i += 1) {
    const appId = selectedAppIds[i];
    const idx1 = i + 1;
    const appStart = Date.now();

    const commonQuery = { country, from: fromStr, to: toStr, window: "false" };

    try {
      const appleOverview = await fetchAndInsert({
        http,
        bq,
        tableId: TABLES.apple.overview,
        store: "apple",
        tab: "overview",
        route: `/app-analysis/overview/apple/${appId}`,
        query: commonQuery,
        monthStr,
        country,
        appId,
        googlePackage: null,
        parser: (t) => parseOverviewPerformance(t),
        alertsLogPath,
      });

      await fetchAndInsert({
        http,
        bq,
        tableId: TABLES.apple.reviews,
        store: "apple",
        tab: "reviews",
        route: `/app-analysis/reviews/apple/${appId}`,
        query: commonQuery,
        monthStr,
        country,
        appId,
        googlePackage: null,
        parser: (t) => parseReviewsReplyRate(t),
        alertsLogPath,
      });

      await fetchAndInsert({
        http,
        bq,
        tableId: TABLES.apple.usage_sessions,
        store: "apple",
        tab: "usage-and-engagement",
        route: `/app-analysis/usage-and-engagement/apple/${appId}`,
        query: commonQuery,
        monthStr,
        country,
        appId,
        googlePackage: null,
        parser: (t) => parseUsageSessions(t),
        alertsLogPath,
      });

      await fetchAndInsert({
        http,
        bq,
        tableId: TABLES.apple.technographics_overview,
        store: "apple",
        tab: "technographics",
        route: `/app-analysis/technographics/apple/${appId}`,
        query: commonQuery,
        monthStr,
        country,
        appId,
        googlePackage: null,
        parser: (t) => parseTechnographicsOverview(t),
        alertsLogPath,
      });

      await scrapeTechnographicsSdks({
        http,
        bq,
        store: "apple",
        id: String(appId),
        monthStr,
        country,
        fromStr,
        toStr,
        appId,
        googlePackage: null,
        tableId: TABLES.apple.technographics_sdks,
        alertsLogPath,
      });

      await fetchAndInsert({
        http,
        bq,
        tableId: TABLES.apple.revenue,
        store: "apple",
        tab: "revenue",
        route: `/app-analysis/revenue/apple/${appId}`,
        query: commonQuery,
        monthStr,
        country,
        appId,
        googlePackage: null,
        parser: (t) => parseRevenueTotal(t),
        alertsLogPath,
      });

      await fetchAndInsert({
        http,
        bq,
        tableId: TABLES.apple.audience,
        store: "apple",
        tab: "audience-analysis",
        route: `/app-analysis/audience-analysis/apple/${appId}`,
        query: commonQuery,
        monthStr,
        country,
        appId,
        googlePackage: null,
        parser: (t) => parseAudience(t),
        alertsLogPath,
      });

      let googlePackage = await lookupGooglePackageFromMap(bq, appId);
      if (!googlePackage && appleOverview?.text) googlePackage = extractGooglePackageFromRsc(appleOverview.text);
      if (!googlePackage) {
        try {
          googlePackage = await resolveGooglePackageViaPlaywright({ appId, country, fromDate: from, toDate: to });
        } catch (err) {
          await insertAlert(
            bq,
            {
              store: "apple",
              tab: "overview",
              stage: "resolve_google_package",
              app_id: appId,
              google_package: null,
              page_url: null,
              error_type: err?.code || "error",
              error_message: String(err?.message || err),
            },
            alertsLogPath
          );
        }
      }

      if (googlePackage) {
        await upsertMapRow(bq, { appId, googlePackage, sourceUrl: appleOverview?.pageUrl || null });

        await fetchAndInsert({
          http,
          bq,
          tableId: TABLES.google.overview,
          store: "google",
          tab: "overview",
          route: `/app-analysis/overview/google/${googlePackage}`,
          query: commonQuery,
          monthStr,
          country,
          appId,
          googlePackage,
          parser: (t) => parseOverviewPerformance(t),
          alertsLogPath,
        });

        await fetchAndInsert({
          http,
          bq,
          tableId: TABLES.google.reviews,
          store: "google",
          tab: "reviews",
          route: `/app-analysis/reviews/google/${googlePackage}`,
          query: commonQuery,
          monthStr,
          country,
          appId,
          googlePackage,
          parser: (t) => parseReviewsReplyRate(t),
          alertsLogPath,
        });

        await fetchAndInsert({
          http,
          bq,
          tableId: TABLES.google.usage_sessions,
          store: "google",
          tab: "usage-and-engagement",
          route: `/app-analysis/usage-and-engagement/google/${googlePackage}`,
          query: commonQuery,
          monthStr,
          country,
          appId,
          googlePackage,
          parser: (t) => parseUsageSessions(t),
          alertsLogPath,
        });

        await fetchAndInsert({
          http,
          bq,
          tableId: TABLES.google.technographics_overview,
          store: "google",
          tab: "technographics",
          route: `/app-analysis/technographics/google/${googlePackage}`,
          query: commonQuery,
          monthStr,
          country,
          appId,
          googlePackage,
          parser: (t) => parseTechnographicsOverview(t),
          alertsLogPath,
        });

        await scrapeTechnographicsSdks({
          http,
          bq,
          store: "google",
          id: googlePackage,
          monthStr,
          country,
          fromStr,
          toStr,
          appId,
          googlePackage,
          tableId: TABLES.google.technographics_sdks,
          alertsLogPath,
        });

        await fetchAndInsert({
          http,
          bq,
          tableId: TABLES.google.revenue,
          store: "google",
          tab: "revenue",
          route: `/app-analysis/revenue/google/${googlePackage}`,
          query: commonQuery,
          monthStr,
          country,
          appId,
          googlePackage,
          parser: (t) => parseRevenueTotal(t),
          alertsLogPath,
        });

        await fetchAndInsert({
          http,
          bq,
          tableId: TABLES.google.audience,
          store: "google",
          tab: "audience-analysis",
          route: `/app-analysis/audience-analysis/google/${googlePackage}`,
          query: commonQuery,
          monthStr,
          country,
          appId,
          googlePackage,
          parser: (t) => parseAudience(t),
          alertsLogPath,
        });
      } else {
        await insertAlert(
          bq,
          {
            store: "google",
            tab: "overview",
            stage: "resolve_google_package",
            app_id: appId,
            google_package: null,
            page_url: null,
            error_type: "missing_google_package",
            error_message: "Could not determine google_package from RSC or mapping",
          },
          alertsLogPath
        );
      }
    } catch (err) {
      if (
        err?.code === "SW_LOGIN_EXPIRED" ||
        err?.code === "SW_BLOCKED" ||
        err?.code === "SW_TOO_MANY_429" ||
        err?.code === "SW_TOO_MANY_403"
      ) {
        await insertAlert(
          bq,
          {
            store: null,
            tab: null,
            stage: "fatal",
            app_id: appId,
            google_package: null,
            page_url: null,
            error_type: err?.code,
            error_message: String(err?.message || err),
          },
          alertsLogPath
        );
        await appendLine(runLogPath, JSON.stringify({ event: "fatal", at: nowIso(), app_id: appId, error: String(err) }));
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

      if (idx1 % 10 === 0) {
        const elapsedMs = Date.now() - t0;
        const hours = elapsedMs / (1000 * 60 * 60);
        const rate = hours > 0 ? (idx1 / hours).toFixed(2) : "inf";
        process.stdout.write(`Progress ${idx1}/${selectedAppIds.length} | ${rate} apps/hour\n`);
      }

      await appendLine(
        runLogPath,
        JSON.stringify({ event: "app_done", at: nowIso(), app_id: appId, ms: Date.now() - appStart })
      );

      await sleep(jitter(2000, 4000));
    }
  }

  await appendLine(runLogPath, JSON.stringify({ event: "done", at: nowIso() }));
}

main().catch((err) => {
  process.stderr.write(`${err?.stack || err}\n`);
  process.exitCode = 1;
});
