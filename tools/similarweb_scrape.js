import fs from "node:fs/promises";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import minimist from "minimist";
import { chromium } from "playwright";

import { ensureSimilarwebAuth } from "./lib/similarweb_auth.js";
import { SimilarwebHttpClient } from "./lib/httpClient.js";
import { createBigQueryClient, ensureTable, insertRows, sha256Hex, truncateForBigQueryString } from "./lib/bq.js";
import { appendLine } from "./lib/logging.js";
import { monthFromYyyyMm, previousFullMonthUtc, monthRangeUtc, formatDateUTC, formatMonthYyyyMm } from "./lib/dates.js";
import {
  extractGooglePackageFromRsc,
  normalizeCurrencyUsd,
  normalizeNumberText,
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
  audit: "similarweb_app_audit",
  apple: {
    overview: "similarweb_appstore_overview",
    reviews: "similarweb_appstore_reviews",
    usage_sessions: "similarweb_appstore_usage_sessions",
    technographics_overview: "similarweb_appstore_technographics_overview",
    technographics_sdks: "similarweb_appstore_technographics_sdks",
    revenue: "similarweb_appstore_revenue",
    audience: "similarweb_appstore_audience",
    audit: "similarweb_appstore_audit",
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

function isPlaywrightTimeout(err) {
  const msg = String(err?.message || err || "");
  return err?.name === "TimeoutError" || /timeout/i.test(msg);
}

async function sniffAuthOrBlocked(page) {
  try {
    const u = String(page?.url?.() || "");
    if (/\/login|signin|sign-in|\/auth/i.test(u)) return "login";
  } catch {}
  try {
    const t = await page.evaluate(() => (document && document.body ? document.body.innerText : ""));
    const low = String(t || "").toLowerCase();
    if (low.includes("access denied") || low.includes("captcha") || low.includes("verify you are human")) return "blocked";
    if (low.includes("sign in") || low.includes("log in") || low.includes("login")) return "login";
  } catch {}
  return "unknown";
}

function shardBucketForAppId(appId, workers) {
  const hex = createHash("md5").update(String(appId)).digest("hex");
  const first32 = parseInt(hex.slice(0, 8), 16) >>> 0;
  return workers > 0 ? first32 % workers : 0;
}

async function exportCookiesFromStorageState(storageStatePath, cookiesPath) {
  try {
    const raw = await fs.readFile(storageStatePath, "utf8");
    const parsed = JSON.parse(raw);
    const cookies = Array.isArray(parsed?.cookies) ? parsed.cookies : null;
    if (!cookies || cookies.length === 0) return false;
    await fs.mkdir(path.dirname(cookiesPath), { recursive: true });
    await fs.writeFile(cookiesPath, JSON.stringify(cookies, null, 2) + "\n", "utf8");
    return true;
  } catch {
    return false;
  }
}
function hasAnyNonNullValue(obj) {
  if (!obj) return false;
  for (const v of Object.values(obj)) {
    if (v == null) continue;
    if (typeof v === "number" && Number.isNaN(v)) continue;
    if (typeof v === "string" && v.trim() === "") continue;
    return true;
  }
  return false;
}

function tabParsedHasData(tabName, parsed) {
  const p = parsed || {};
  if (tabName === "reviews") return p.reply_rate_pct != null;
  if (tabName === "usage_sessions") return p.mau != null || p.wau != null || p.dau != null;
  if (tabName === "technographics") return p.sdks_total != null;
  if (tabName === "revenue") return p.total_revenue_usd != null;
  if (tabName === "audience") {
    return (
      p.gender_male_pct != null ||
      p.gender_female_pct != null ||
      p.age_18_24_pct != null ||
      p.age_25_34_pct != null ||
      p.age_35_44_pct != null ||
      p.age_45_54_pct != null ||
      p.age_55_plus_pct != null
    );
  }
  return hasAnyNonNullValue(p);
}

function domWaitPatternsForTab(tabName) {
  if (tabName === "reviews") return [/Reply rate/i, /Reply Rate/i];
  if (tabName === "usage_sessions") return [/MAU/i, /Daily Stickiness/i, /Active Users/i];
  if (tabName === "technographics") return [/SDKs/i, /Total SDKs/i];
  if (tabName === "revenue") return [/Total Revenue/i, /Revenue/i];
  if (tabName === "audience") return [/Gender/i, /Age/i, /Distribution/i];
  return [];
}


function addLangPrefixToSimilarwebUrl(url, lang = "en") {
  try {
    const u = new URL(url);
    const p = u.pathname || "";
    if (!p.startsWith("/app-analysis/")) return null;
    if (p.startsWith(`/${lang}/app-analysis/`)) return null;
    u.pathname = `/${lang}` + p;
    return u.toString();
  } catch {
    return null;
  }
}

async function fetchDomTextForTab(page, pageUrl, tabName) {
  let currentUrl = pageUrl;
  let resp = await page.goto(currentUrl, { waitUntil: "domcontentloaded" }).catch(() => null);
  await page.waitForTimeout(1200).catch(() => {});
  let status = resp ? resp.status() : null;
  if (status != null && status >= 500) {
    const alt = addLangPrefixToSimilarwebUrl(currentUrl, "en");
    if (alt) {
      currentUrl = alt;
      resp = await page.goto(currentUrl, { waitUntil: "domcontentloaded" }).catch(() => null);
      await page.waitForTimeout(1200).catch(() => {});
      status = resp ? resp.status() : null;
    }
    if (status != null && status >= 500) {
      const e = new Error(`Similarweb returned HTTP ${status} for page`);
      e.code = "SW_HTTP_ERROR";
      e.httpStatus = status;
      throw e;
    }
  }

  const hint = await sniffAuthOrBlocked(page);
  if (hint === "login") {
    const e = new Error("Similarweb session appears expired.");
    e.code = "SW_LOGIN_EXPIRED";
    throw e;
  }
  if (hint === "blocked") {
    const e = new Error("Similarweb access blocked (captcha / access denied).");
    e.code = "SW_BLOCKED";
    throw e;
  }

  for (const re of domWaitPatternsForTab(tabName)) {
    await page.getByText(re).first().waitFor({ timeout: 20000 }).catch(() => {});
  }
  await page.waitForTimeout(700).catch(() => {});
  const text = await page.evaluate(() => (document && document.body ? document.body.innerText : ""));
  const textStr = String(text || "");
  if (/internal server error/i.test(textStr)) {
    const alt = addLangPrefixToSimilarwebUrl(currentUrl, "en");
    if (alt) {
      currentUrl = alt;
      resp = await page.goto(currentUrl, { waitUntil: "domcontentloaded" }).catch(() => null);
      await page.waitForTimeout(1200).catch(() => {});
      const t2 = await page.evaluate(() => (document && document.body ? document.body.innerText : ""));
      const s2 = String(t2 || "");
      if (!/internal server error/i.test(s2)) return s2;
    }
    const e = new Error("Similarweb page shows Internal Server Error");
    e.code = "SW_HTTP_ERROR";
    e.httpStatus = 500;
    e.bodySnippet = textStr.slice(0, 400);
    throw e;
  }
  return textStr;
}
async function writeDomNoDataDebug({ appId, store, tabName, pageUrl, text }) {
  try {
    const yyyymmdd = todayYyyyMmDdUtc();
    const file = path.join(__dirname, "..", "logs", `dom_no_data_${yyyymmdd}_${store}_${tabName}_${appId}.txt`);
    await fs.mkdir(path.dirname(file), { recursive: true });
    const payload = `page_url=${pageUrl}\nstore=${store}\ntab=${tabName}\napp_id=${appId}\n\n` + String(text || "").slice(0, 20000);
    await fs.writeFile(file, payload, "utf8");
    return file;
  } catch {
    return null;
  }
}


let requestBlockingLogged = false;
const REQUEST_BLOCKED_RESOURCE_TYPES = new Set(["image", "font", "media"]);
const REQUEST_BLOCKED_URL_RE = /googletagmanager|google-analytics|doubleclick|segment|hotjar|mixpanel|sentry|datadog|amplitude/i;

async function createReusablePlaywrightPage({ storageStatePath, userDataDir, headful }) {
  let browser = null;
  let context = null;
  let page = null;

  if (userDataDir) {
    context = await chromium.launchPersistentContext(userDataDir, { headless: !headful });
  } else {
    browser = await chromium.launch({ headless: !headful });
    context = await browser.newContext({ storageState: storageStatePath });
  }

  context.setDefaultTimeout(45_000);

  await context.route("**/*", (route) => {
    try {
      const req = route.request();
      const url = req.url();
      const rt = req.resourceType();
      if (REQUEST_BLOCKED_RESOURCE_TYPES.has(rt) || REQUEST_BLOCKED_URL_RE.test(url)) return route.abort();
      return route.continue();
    } catch {
      return route.continue();
    }
  });

  if (!requestBlockingLogged) {
    requestBlockingLogged = true;
    process.stdout.write("Request blocking enabled\n");
  }

  page = await context.newPage();

  async function recreatePage() {
    try {
      if (page) await page.close().catch(() => {});
    } finally {
      page = await context.newPage();
    }
    return page;
  }

  async function close() {
    if (page) await page.close().catch(() => {});
    if (context) await context.close().catch(() => {});
    if (browser) await browser.close().catch(() => {});
  }

  return {
    get page() {
      return page;
    },
    recreatePage,
    close,
  };
}
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

function formatErrorMessage(err) {
  const msg = String((err && err.message) || err);
  const status = err && err.httpStatus ? " status=" + err.httpStatus : "";
  const loc = err && err.location ? " location=" + String(err.location).slice(0, 200) : "";
  const snippet = err && err.bodySnippet ? " snippet=" + String(err.bodySnippet).replace(/\s+/g, " ").slice(0, 500) : "";
  return (msg + status + loc + snippet).trim();
}

function isBigQueryInsertError(err) {
  if (!err) return false;
  if (String(err.name || "") === "PartialFailureError") return true;
  if (Array.isArray(err.errors) && err.errors.length) return true;
  const msg = String(err.message || err);
  return /bigquery/i.test(msg) || /insert/i.test(msg);
}

function classifyAuditStatus(err) {
  const code = err && err.code ? String(err.code) : '';
  if (code === 'SW_NO_DATA') return 'NO_DATA';
  if (code === 'SW_ACCESS_DENIED') return 'ACCESS_DENIED';
  if (code === 'SW_LOGIN_EXPIRED') return 'LOGIN_EXPIRED';
  if (code === 'SW_BLOCKED') return 'CAPTCHA';
  if (code === 'SW_TOO_MANY_429' || code === 'SW_TOO_MANY_403') return 'HTTP_ERROR';
  if (isPlaywrightTimeout(err)) return 'NAV_TIMEOUT';
  if (isBigQueryInsertError(err)) return 'BQ_ERROR';
  if (err && (err.httpStatus || err.status)) return 'HTTP_ERROR';
  return 'PARSE_ERROR';
}

function generateRunId({ worker, workers }) {
  const base = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
  const rand = Math.random().toString(16).slice(2, 10);
  return `${base}_w${worker}of${workers}_${rand}`;
}

function truncateDetails(s, maxChars = 800) {
  if (s == null) return null;
  const str = String(s);
  return str.length <= maxChars ? str : str.slice(0, maxChars);
}
function computeVerdictForTab(stats) {
  const s = stats || { success: 0, http_error: 0, access_denied: 0 };
  if (s.access_denied > 0) return "NOT_READY";
  if (s.http_error > 0 && s.success === 0) return "NOT_READY";
  if (s.success > 0) return "READY_FOR_MASS_RUN";
  return "NOT_READY";
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

  const auditSchema = [
    { name: "run_id", type: "STRING", mode: "REQUIRED" },
    { name: "app_id", type: "STRING", mode: "REQUIRED" },
    { name: "country", type: "INT64", mode: "REQUIRED" },
    { name: "month", type: "DATE", mode: "REQUIRED" },
    { name: "status", type: "STRING", mode: "REQUIRED" },
    { name: "details", type: "STRING", mode: "NULLABLE" },
    { name: "started_at", type: "TIMESTAMP", mode: "REQUIRED" },
    { name: "finished_at", type: "TIMESTAMP", mode: "REQUIRED" },
    { name: "duration_ms", type: "INT64", mode: "NULLABLE" },
    { name: "worker", type: "INT64", mode: "REQUIRED" },
    { name: "workers", type: "INT64", mode: "REQUIRED" },
    { name: "attempt_num", type: "INT64", mode: "REQUIRED" },
  ];
  const appAuditSchema = [
    { name: "run_id", type: "STRING", mode: "REQUIRED" },
    { name: "pulled_at", type: "TIMESTAMP", mode: "REQUIRED" },
    { name: "store", type: "STRING", mode: "REQUIRED" },
    { name: "tab", type: "STRING", mode: "REQUIRED" },
    { name: "month", type: "DATE", mode: "REQUIRED" },
    { name: "country", type: "INT64", mode: "REQUIRED" },
    { name: "app_id", type: "INT64", mode: "REQUIRED" },
    { name: "google_package", type: "STRING", mode: "NULLABLE" },
    { name: "status", type: "STRING", mode: "REQUIRED" },
    { name: "details", type: "STRING", mode: "NULLABLE" },
    { name: "page_url", type: "STRING", mode: "NULLABLE" },
    { name: "duration_ms", type: "INT64", mode: "NULLABLE" },
    { name: "attempt_num", type: "INT64", mode: "REQUIRED" },
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


  await ensureTable(bq, DATASET_ID, TABLES.apple.audit, {
    schema: auditSchema,
    timePartitioning: monthPartitioning,
    clustering: { fields: ["run_id", "status", "app_id"] },
  });

  await ensureTable(bq, DATASET_ID, TABLES.audit, {
    schema: appAuditSchema,
    timePartitioning: { type: "DAY", field: "pulled_at" },
    clustering: { fields: ["run_id", "store", "tab", "app_id"] },
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
    types: { app_id: "INT64", google_package: "STRING", source_url: "STRING" },
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
  const [rows] = await bq.query({ query, params: { app_id: appId }, types: { app_id: "INT64" } });
  return rows[0]?.google_package || null;
}

async function resolveGooglePackageViaPlaywright({ appId, country, fromDate, toDate, headful, storageStatePath, userDataDir }) {
  let browser = null;
  let context = null;

  try {
    if (userDataDir) {
      context = await chromium.launchPersistentContext(userDataDir, { headless: !headful });
    } else {
      browser = await chromium.launch({ headless: !headful });
      context = await browser.newContext({ storageState: storageStatePath });
    }

    context.setDefaultTimeout(30_000);
    const page = await context.newPage();

    const appleUrl = buildSimilarwebUrl(`/app-analysis/overview/apple/${appId}`, {
      country,
      from: formatDateUTC(fromDate),
      to: formatDateUTC(toDate),
      window: "false",
    });

    await page.goto(appleUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForTimeout(1500);

    const href = await page.evaluate(() => {
      const anchors = Array.from(document.querySelectorAll("a[href]"));
      const link = anchors.find((a) => /\/app-analysis\/overview\/google\//i.test(a.getAttribute("href") || ""));
      return link ? link.getAttribute("href") : null;
    });

    if (!href) return null;
    const m2 = String(href).match(/\/app-analysis\/overview\/google\/([^?"'\s]+)/i);
    return m2 ? m2[1] : null;
  } finally {
    if (context) await context.close().catch(() => {});
    if (browser) await browser.close().catch(() => {});
  }
}
async function queryProcessedAlreadyCount({ bq, monthStr, country, tab }) {
  const countryInt = mustInt(country, "--country");
  const tables = appleDestTablesForTab(String(tab || "overview"));
  const tableId = tables[0] || TABLES.apple.overview;
  const query = `
    SELECT COUNT(DISTINCT app_id) AS c
    FROM \`${PROJECT_ID}.${DATASET_ID}.${tableId}\`
    WHERE month = CAST(@month AS DATE)
      AND country = CAST(@country AS INT64)
      AND google_package IS NULL
  `;
  const [rows] = await bq.query({ query, params: { month: monthStr, country: countryInt }, types: { month: "DATE", country: "INT64" } });
  return Number(rows?.[0]?.c ?? 0);
}

function appleDestTablesForTab(tab) {
  switch (tab) {
    case "overview":
      return [TABLES.apple.overview];
    case "reviews":
      return [TABLES.apple.reviews];
    case "usage_sessions":
      return [TABLES.apple.usage_sessions];
    case "technographics":
      return [TABLES.apple.technographics_overview];
    case "revenue":
      return [TABLES.apple.revenue];
    case "audience":
      return [TABLES.apple.audience];
    case "all":
      return [
        TABLES.apple.overview,
        TABLES.apple.reviews,
        TABLES.apple.usage_sessions,
        TABLES.apple.technographics_overview,
        TABLES.apple.revenue,
        TABLES.apple.audience,
      ];
    default:
      return [TABLES.apple.overview];
  }
}

async function querySelectionStats({ bq, monthStr, country, tab }) {
  const countryInt = mustInt(country, "--country");
  const tables = appleDestTablesForTab(String(tab || "overview"));

  const joins = tables
    .map(
      (tableId, i) => `
      LEFT JOIN (
        SELECT DISTINCT app_id
        FROM \`${PROJECT_ID}.${DATASET_ID}.${tableId}\`
        WHERE month = CAST(@month AS DATE)
          AND country = CAST(@country AS INT64)
          AND google_package IS NULL
      ) t${i} ON t${i}.app_id = c.app_id`
    )
    .join("\n");

  const missingCond = tables.map((_, i) => `t${i}.app_id IS NULL`).join(" OR ");

  const query = `
    WITH base AS (
      SELECT DISTINCT SAFE_CAST(app_id AS INT64) AS app_id
      FROM \`${PROJECT_ID}.${DATASET_ID}.app_urls_raw\`
      WHERE SAFE_CAST(app_id AS INT64) IS NOT NULL
    ),
    meta AS (
      SELECT
        SAFE_CAST(app_id AS INT64) AS app_id,
        MAX(IF(LOWER(CAST(country AS STRING)) = 'us', 1, 0)) AS has_us,
        MAX(SAFE_CAST(user_rating_count AS INT64)) AS user_rating_count_max
      FROM \`${PROJECT_ID}.${DATASET_ID}.app_metadata_by_country\`
      WHERE SAFE_CAST(app_id AS INT64) IS NOT NULL
      GROUP BY app_id
    ),
    candidates AS (
      SELECT
        b.app_id,
        COALESCE(m.has_us, 0) AS has_us,
        COALESCE(m.user_rating_count_max, 0) AS user_rating_count_max
      FROM base b
      LEFT JOIN meta m
        ON m.app_id = b.app_id
    ),
    joined AS (
      SELECT
        c.*,
        (${missingCond}) AS is_missing
      FROM candidates c
      ${joins}
    )
    SELECT
      COUNT(*) AS candidates_total,
      COUNTIF(is_missing) AS missing_total,
      ARRAY_AGG(CAST(app_id AS INT64) ORDER BY has_us DESC, user_rating_count_max DESC, app_id ASC LIMIT 10) AS candidates_sample,
      ARRAY_AGG(IF(is_missing, CAST(app_id AS INT64), NULL) IGNORE NULLS ORDER BY has_us DESC, user_rating_count_max DESC, app_id ASC LIMIT 10) AS missing_sample
    FROM joined
  `;

  const [rows] = await bq.query({ query, params: { month: monthStr, country: countryInt }, types: { month: "DATE", country: "INT64" } });
  const r = rows?.[0] || {};
  return {
    candidates_total: Number(r.candidates_total || 0),
    missing_total: Number(r.missing_total || 0),
    candidates_sample: Array.isArray(r.candidates_sample) ? r.candidates_sample.map(Number) : [],
    missing_sample: Array.isArray(r.missing_sample) ? r.missing_sample.map(Number) : [],
  };
}

async function querySelectedAppIds({ bq, mode, limit, monthStr, country, tab }) {
  void mode;
  const limitInt = mustInt(limit, "--limit");
  const countryInt = mustInt(country, "--country");
  const tables = appleDestTablesForTab(String(tab || "overview"));

  const joins = tables
    .map(
      (tableId, i) => `
      LEFT JOIN (
        SELECT DISTINCT app_id
        FROM \`${PROJECT_ID}.${DATASET_ID}.${tableId}\`
        WHERE month = CAST(@month AS DATE)
          AND country = CAST(@country AS INT64)
          AND google_package IS NULL
      ) t${i} ON t${i}.app_id = c.app_id`
    )
    .join("\n");

  const missingCond = tables.map((_, i) => `t${i}.app_id IS NULL`).join(" OR ");

  const query = `
    WITH base AS (
      SELECT DISTINCT SAFE_CAST(app_id AS INT64) AS app_id
      FROM \`${PROJECT_ID}.${DATASET_ID}.app_urls_raw\`
      WHERE SAFE_CAST(app_id AS INT64) IS NOT NULL
    ),
    meta AS (
      SELECT
        SAFE_CAST(app_id AS INT64) AS app_id,
        MAX(IF(LOWER(CAST(country AS STRING)) = 'us', 1, 0)) AS has_us,
        MAX(SAFE_CAST(user_rating_count AS INT64)) AS user_rating_count_max
      FROM \`${PROJECT_ID}.${DATASET_ID}.app_metadata_by_country\`
      WHERE SAFE_CAST(app_id AS INT64) IS NOT NULL
      GROUP BY app_id
    ),
    candidates AS (
      SELECT
        b.app_id,
        COALESCE(m.has_us, 0) AS has_us,
        COALESCE(m.user_rating_count_max, 0) AS user_rating_count_max
      FROM base b
      LEFT JOIN meta m
        ON m.app_id = b.app_id
    ),
    joined AS (
      SELECT
        c.*,
        (${missingCond}) AS is_missing
      FROM candidates c
      ${joins}
    )
    SELECT app_id
    FROM joined
    WHERE is_missing
    ORDER BY has_us DESC, user_rating_count_max DESC, app_id ASC
    LIMIT @limit
  `;

  const [rows] = await bq.query({ query, params: { month: monthStr, country: countryInt, limit: limitInt }, types: { month: "DATE", country: "INT64", limit: "INT64" } });
  return rows.map((r) => Number(r.app_id)).filter((n) => Number.isFinite(n));
}

async function rowExists(bq, tableId, { monthStr, country, appId, googlePackage }) {
  const where = ["month = CAST(@month AS DATE)", "country = CAST(@country AS INT64)", "app_id = @app_id"];
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
  const types = { month: "DATE", country: "INT64", app_id: "INT64" };
  if (googlePackage != null) types.google_package = "STRING";
  const [rows] = await bq.query({ query, params, types });
  return rows.length > 0;
}

function isOverviewEmpty(m) {
  return (
    (m?.store_downloads == null) &&
    (m?.mau == null) &&
    (m?.rating_avg == null) &&
    (m?.ratings_count == null) &&
    (m?.ranking_text == null)
  );
}

async function deleteSingleRowByKey(bq, tableId, { monthStr, country, appId, googlePackage }) {
  const where = ["month = CAST(@month AS DATE)", "country = CAST(@country AS INT64)", "app_id = @app_id"];
  const params = { month: monthStr, country, app_id: appId };
  if (googlePackage != null) {
    where.push("google_package = @google_package");
    params.google_package = googlePackage;
  } else {
    where.push("google_package IS NULL");
  }

  const query = `
    DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.${tableId}\`
    WHERE ${where.join(" AND ")}
  `;

  const types = { month: "DATE", country: "INT64", app_id: "INT64" };
  if (googlePackage != null) types.google_package = "STRING";

  await bq.query({ query, params, types });
}


async function existingSdkNames(bq, tableId, { monthStr, country, appId, googlePackage }) {
  const where = ["month = CAST(@month AS DATE)", "country = CAST(@country AS INT64)", "app_id = @app_id"];
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
  const types = { month: "DATE", country: "INT64", app_id: "INT64" };
  if (googlePackage != null) types.google_package = "STRING";
  const [rows] = await bq.query({ query, params, types });
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
  runLogPath = null,
  pwPage = null,
  domTabName = null,
}) {
  const pageUrl = buildSimilarwebUrl(route, query);
  const effectiveTab = domTabName || tab;

  let text = null;
  let parsed = {};
  let usedDomFallback = false;

  try {
    try {
      const r = await http.fetchRscText(pageUrl, { maxAttempts: pwPage ? 1 : 3 });
      text = r && r.text != null ? String(r.text) : null;
    } catch (err) {
    if (effectiveTab === "reviews" && runLogPath) {
      await appendLine(
        runLogPath,
        JSON.stringify({
          event: "reviews_http_fail",
          at: nowIso(),
          store,
          app_id: appId,
          google_package: googlePackage || null,
          page_url: pageUrl,
          code: err?.code || null,
          httpStatus: err?.httpStatus || null,
          location: err?.location || null,
          body_snippet: err?.bodySnippet ? String(err.bodySnippet).slice(0, 300) : null,
          message: String(err?.message || err).slice(0, 200),
        })
      );
    }
      // Fallback to DOM if HTTP/RSC fails (Similarweb sometimes returns 500 for RSC routes).
      if (pwPage) {
        text = await fetchDomTextForTab(pwPage, pageUrl, effectiveTab);
        usedDomFallback = true;
        if (runLogPath) await appendLine(runLogPath, JSON.stringify({ event: "dom_fallback", at: nowIso(), store, tab: effectiveTab, app_id: appId, page_url: pageUrl, reason: err?.code || err?.httpStatus || "http_error" }));
      } else {
        throw err;
      }
    }

    if (text == null) {
      const e = new Error("No response text");
      e.code = "SW_NO_DATA";
      throw e;
    }

    parsed = parser ? parser(text) : {};

    // If RSC text is only a shell/loading stream, try DOM.
    if (!tabParsedHasData(effectiveTab, parsed) && pwPage && !usedDomFallback) {
      const domText = await fetchDomTextForTab(pwPage, pageUrl, effectiveTab);
      const domParsed = parser ? parser(domText) : {};
      if (tabParsedHasData(effectiveTab, domParsed)) {
        text = domText;
        parsed = domParsed;
        usedDomFallback = true;
        if (runLogPath) await appendLine(runLogPath, JSON.stringify({ event: "dom_fallback_shell", at: nowIso(), store, tab: effectiveTab, app_id: appId, page_url: pageUrl }));
      }
    }

    if (!tabParsedHasData(effectiveTab, parsed)) {
      const e = new Error("No parsable data for tab");
      e.code = "SW_NO_DATA";
      if (pwPage) {
        const dbg = await writeDomNoDataDebug({ appId, store, tabName: effectiveTab, pageUrl, text });
        if (runLogPath) await appendLine(runLogPath, JSON.stringify({ event: "dom_no_data", at: nowIso(), store, tab: effectiveTab, app_id: appId, page_url: pageUrl, debug_file: dbg }));
      }
      throw e;
    }

    const rawRsc = truncateForBigQueryString(text);
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
    await deleteSingleRowByKey(bq, tableId, { monthStr, country, appId, googlePackage });
    await insertRows(table, [row]);
    if (runLogPath) await appendLine(runLogPath, JSON.stringify({ event: "tab_insert", at: nowIso(), store, tab: effectiveTab, table: tableId, rows: 1, app_id: appId, google_package: googlePackage || null, dom_fallback: usedDomFallback }));

    return { skipped: false, pageUrl, text, parsed, usedDomFallback };
  } catch (err) {
    if (err?.code !== "SW_NO_DATA") {
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
          error_message: formatErrorMessage(err),
        },
        alertsLogPath
      );
    }
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
        error_message: formatErrorMessage(err),
      },
      alertsLogPath
    );
    throw err;
  }
}


function safeStringify(value, maxChars = 2_000_000) {
  try {
    const s = JSON.stringify(value);
    return s.length <= maxChars ? s : s.slice(0, maxChars);
  } catch {
    return null;
  }
}

function isPlainObject(v) {
  return v != null && typeof v === "object" && !Array.isArray(v);
}

function deepCollectKeyValues(root, wantedKeysLower, maxNodes = 50000) {
  const out = new Map();
  const stack = [root];
  let nodes = 0;

  while (stack.length) {
    const cur = stack.pop();
    nodes += 1;
    if (nodes > maxNodes) break;

    if (Array.isArray(cur)) {
      for (const v of cur) stack.push(v);
      continue;
    }
    if (!isPlainObject(cur)) continue;

    for (const [k, v] of Object.entries(cur)) {
      const lk = String(k).toLowerCase();
      if (wantedKeysLower.has(lk)) {
        if (!out.has(lk)) out.set(lk, []);
        out.get(lk).push(v);
      }
      if (v != null && (typeof v === "object" || Array.isArray(v))) stack.push(v);
    }
  }

  return out;
}

function bestNumeric(values, { min = null, max = null } = {}) {
  let best = null;
  for (const v of values || []) {
    let n = null;
    if (typeof v === "number") n = v;
    else if (typeof v === "string") n = normalizeNumberText(v);
    if (!Number.isFinite(n)) continue;
    if (min != null && n < min) continue;
    if (max != null && n > max) continue;
    if (best == null || n > best) best = n;
  }
  return best;
}

function bestCurrency(values) {
  let best = null;
  for (const v of values || []) {
    if (typeof v === "number" && Number.isFinite(v)) {
      if (best == null || v > best) best = v;
      continue;
    }
    if (typeof v === "string") {
      const { usd } = normalizeCurrencyUsd(v);
      if (usd != null && (best == null || usd > best)) best = usd;
    }
  }
  return best;
}

function firstString(values) {
  for (const v of values || []) {
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return null;
}

function extractOverviewMetricsFromJson(body) {
  const keys = [
    "downloads",
    "store_downloads",
    "storedownloads",
    "totaldownloads",
    "mau",
    "monthlyactiveusers",
    "revenue",
    "totalrevenue",
    "revenueusd",
    "rank",
    "ranking",
    "ranktext",
    "rating",
    "ratingavg",
    "ratingscount",
    "ratings_count",
    "engagement",
  ];
  const wanted = new Set(keys.map((k) => k.toLowerCase()));
  const found = deepCollectKeyValues(body, wanted);

  const storeDownloads =
    bestNumeric(found.get("downloads")) ??
    bestNumeric(found.get("store_downloads")) ??
    bestNumeric(found.get("storedownloads")) ??
    bestNumeric(found.get("totaldownloads"));

  const mau = bestNumeric(found.get("mau")) ?? bestNumeric(found.get("monthlyactiveusers"));

  const revenueUsd = bestCurrency(found.get("revenue")) ?? bestCurrency(found.get("totalrevenue")) ?? bestCurrency(found.get("revenueusd"));

  const rankText = firstString(found.get("ranktext"));
  const rankNum = bestNumeric(found.get("rank")) ?? bestNumeric(found.get("ranking"));
  const rankingText = rankText || (rankNum != null ? `#${rankNum}` : null);

  const ratingAvg = bestNumeric(found.get("ratingavg"), { min: 0, max: 5 }) ?? bestNumeric(found.get("rating"), { min: 0, max: 5 });

  const ratingsCount = bestNumeric(found.get("ratingscount")) ?? bestNumeric(found.get("ratings_count"));

  const revenueText = firstString(found.get("revenue")) || firstString(found.get("totalrevenue")) || null;

  return {
    store_downloads: storeDownloads ?? null,
    ranking_text: rankingText,
    rating_avg: ratingAvg ?? null,
    ratings_count: ratingsCount ?? null,
    mau: mau ?? null,
    revenue_usd: revenueUsd ?? null,
    revenue_text: revenueText,
  };
}

function extractOverviewMetricsFromText(rscText) {
  return parseOverviewPerformance(rscText);
}

function extractOverviewMetricsFromDomText(domText) {
  const text = String(domText || "");
  if (!text.trim()) {
    return {
      store_downloads: null,
      ranking_text: null,
      rating_avg: null,
      ratings_count: null,
      analyzed_reviews_total: null,
      analyzed_reviews_negative: null,
      analyzed_reviews_mixed: null,
      analyzed_reviews_positive: null,
      mau: null,
      daily_stickiness_pct: null,
      revenue_usd: null,
      revenue_text: null,
    };
  }

  const markerRe = /Performance Overview[\s\S]{0,600}?\bApp\b[\s\S]{0,200}?\bStore\s+Downloads\b[\s\S]{0,200}?\bRanking\b[\s\S]{0,200}?\bRatings\b[\s\S]{0,200}?\bAnalyzed\s+Reviews\b[\s\S]{0,200}?\bMAU\b[\s\S]{0,200}?\bDaily\s+Stickiness\b[\s\S]{0,200}?\bRevenue\b/i;
  const m = text.match(markerRe);
  if (m && m.index != null) {
    const headerEnd = m.index + m[0].length;
    const block = text.slice(headerEnd, Math.min(text.length, headerEnd + 6000));

    const numTokenRe = /\b\d[\d,]*(?:\.\d+)?\s*[KMB]?\b/g;
    const pctTokenRe = /\b\d[\d,]*(?:\.\d+)?\s*%/g;
    const kmbTokenRe = /\b\d[\d,]*(?:\.\d+)?\s*[KMB]\b/g;
    const ratingTokenRe = /\b[0-5](?:\.\d{1,2})\b/g;

    // 1) Store downloads: first non-percent number token after headers.
    let storeDownloads = null;
    for (const t of block.matchAll(numTokenRe)) {
      const raw = t[0];
      const pos = t.index == null ? -1 : t.index;
      if (pos < 0) continue;
      const around = block.slice(Math.max(0, pos - 1), Math.min(block.length, pos + raw.length + 1));
      if (/%/.test(around)) continue;
      const n = normalizeNumberText(raw);
      if (Number.isFinite(n) && n >= 1) {
        storeDownloads = n;
        break;
      }
    }

    // 2) Daily stickiness: first percent token.
    let stickiness = null;
    for (const t of block.matchAll(pctTokenRe)) {
      const n = normalizeNumberText(String(t[0]).replace(/%/g, ""));
      if (Number.isFinite(n) && n >= 0 && n <= 100) {
        stickiness = n;
        break;
      }
    }

    // 3) Ranking text: first "Ranked in ..." sentence.
    const rankingSentence = (block.match(/Ranked in[^.]+/i) || [])[0] || null;

    // 4) Rating avg: first 0-5.x number after ranking sentence.
    let ratingAvg = null;
    const fromRating = rankingSentence ? Math.max(0, block.indexOf(rankingSentence)) : 0;
    for (const t of block.slice(fromRating).matchAll(ratingTokenRe)) {
      const n = normalizeNumberText(t[0]);
      if (Number.isFinite(n) && n >= 0 && n <= 5) {
        ratingAvg = n;
        break;
      }
    }

    // 5) Ratings count: first K/M/B number after rating avg.
    let ratingsCount = null;
    for (const t of block.slice(fromRating).matchAll(kmbTokenRe)) {
      const n = normalizeNumberText(t[0]);
      if (!Number.isFinite(n) || n < 1) continue;
      if (storeDownloads != null && Math.abs(n - storeDownloads) < 0.0001) continue;
      ratingsCount = n;
      break;
    }

    // 6) Analyzed reviews total: first small-ish integer not equal to other metrics.
    let analyzedTotal = null;
    const intRe = /\b\d{1,7}\b/g;
    for (const t of block.matchAll(intRe)) {
      const n = Number(t[0]);
      if (!Number.isFinite(n)) continue;
      if (storeDownloads != null && Math.abs(n - storeDownloads) < 0.0001) continue;
      if (ratingsCount != null && Math.abs(n - ratingsCount) < 0.0001) continue;
      if (n >= 0 && n <= 5_000_000) {
        analyzedTotal = n;
        break;
      }
    }

    // 7) MAU: largest remaining K/M/B token (usually distinct from downloads + ratings).
    let mau = null;
    for (const t of block.matchAll(kmbTokenRe)) {
      const n = normalizeNumberText(t[0]);
      if (!Number.isFinite(n) || n < 1) continue;
      if (storeDownloads != null && Math.abs(n - storeDownloads) < 0.0001) continue;
      if (ratingsCount != null && Math.abs(n - ratingsCount) < 0.0001) continue;
      if (mau == null || n > mau) mau = n;
    }

    // 8) Revenue: look for $... token; otherwise N/A.
    const mCur = block.match(/\$\s*-?\d[\d,]*(?:\.\d+)?\s*[KMB]?/);
    const rev = mCur ? normalizeCurrencyUsd(mCur[0].replace(/\s+/g, "")) : { usd: null, text: null };
    const revText = rev.text || ((block.match(/\bN\/A\b/i) || [])[0] || null);

    return {
      store_downloads: storeDownloads ?? null,
      ranking_text: rankingSentence,
      rating_avg: ratingAvg ?? null,
      ratings_count: ratingsCount ?? null,
      analyzed_reviews_total: analyzedTotal ?? null,
      analyzed_reviews_negative: null,
      analyzed_reviews_mixed: null,
      analyzed_reviews_positive: null,
      mau: mau ?? null,
      daily_stickiness_pct: stickiness ?? null,
      revenue_usd: rev.usd ?? null,
      revenue_text: revText,
    };
  }

  // Fallback (best-effort label regex)
  const storeDownloads = normalizeNumberText((text.match(/Store Downloads\s+([\d,.]+\s*[KMB]?)/i) || [])[1]);
  const mau = normalizeNumberText((text.match(/\bMAU\b\s+([\d,.]+\s*[KMB]?)/) || [])[1]);
  const stick = normalizeNumberText(((text.match(/Daily Stickiness\s+([\d,.]+)\s*%/i) || [])[1] || "").replace(/%/g, ""));
  const ratingAvg = normalizeNumberText((text.match(/\bRating\b\s+([0-5](?:\.\d{1,2})?)/i) || [])[1]);
  const ratingsCount = normalizeNumberText((text.match(/Ratings\s+([\d,.]+\s*[KMB]?)/i) || [])[1]);
  const cur = (text.match(/\$\s*-?\d[\d,]*(?:\.\d+)?\s*[KMB]?/) || [])[0] || null;
  const rev = cur ? normalizeCurrencyUsd(cur.replace(/\s+/g, "")) : { usd: null, text: null };

  return {
    store_downloads: storeDownloads ?? null,
    ranking_text: (text.match(/Ranked in[^.]+/i) || [])[0] || null,
    rating_avg: ratingAvg ?? null,
    ratings_count: ratingsCount ?? null,
    analyzed_reviews_total: null,
    analyzed_reviews_negative: null,
    analyzed_reviews_mixed: null,
    analyzed_reviews_positive: null,
    mau: mau ?? null,
    daily_stickiness_pct: stick ?? null,
    revenue_usd: rev.usd ?? null,
    revenue_text: rev.text ?? null,
  };
}

function scoreOverview(m) {
  let s = 0;
  for (const k of ["store_downloads", "mau", "ranking_text", "rating_avg", "ratings_count"]) {
    if (m && m[k] != null) s += 1;
  }
  return s;
}

function hasAnyHeuristicKey(body) {
  const keys = ["downloads", "store_downloads", "storedownloads", "totaldownloads", "mau", "monthlyactiveusers", "revenue", "totalrevenue", "revenueusd", "rank", "ranking", "ranktext", "rating", "ratingavg", "ratingscount", "ratings_count", "engagement"];
  const wanted = new Set(keys.map((k) => k.toLowerCase()));
  const stack = [body];
  let nodes = 0;
  while (stack.length) {
    const cur = stack.pop();
    nodes += 1;
    if (nodes > 20000) return false;
    if (Array.isArray(cur)) {
      for (const v of cur) stack.push(v);
      continue;
    }
    if (!isPlainObject(cur)) continue;
    for (const [k, v] of Object.entries(cur)) {
      if (wanted.has(String(k).toLowerCase())) return true;
      if (v != null && (typeof v === "object" || Array.isArray(v))) stack.push(v);
    }
  }
  return false;
}

function pickBestJsonPayload(payloads) {
  let best = null;
  for (const p of payloads) {
    if (!p || !p.body) continue;
    if (!hasAnyHeuristicKey(p.body)) continue;
    const metrics = extractOverviewMetricsFromJson(p.body);
    const score = scoreOverview(metrics);
    if (!best || score > best.score) best = { payload: p, metrics, score };
  }
  return best;
}

async function scrapeOverviewWithNetwork({
  bq,
  tableId,
  writeToBq = true,
  store,
  id,
  monthStr,
  country,
  fromStr,
  toStr,
  appId,
  googlePackage,
  pw,
    debugNetwork,
  alertsLogPath,
}) {
  const pageUrl = buildSimilarwebUrl(`/app-analysis/overview/${store}/${id}`, {
    country,
    from: fromStr,
    to: toStr,
    window: "false",
  });
  const page = pw.page;

  const jsonPayloads = [];
  const rscPayloads = [];
  const responsesMeta = [];
  const requestsMeta = [];
  const responseTasks = new Set();

  try {
    const onRequest = (req) => {

      try {
        const url = req.url();
        if (/datadoghq|browser-intake|mpps\.similarweb\.com\/track/i.test(url)) return;
        if (requestsMeta.length < 400) {
          requestsMeta.push({ url, method: req.method(), resource_type: req.resourceType() });
        }
      } catch {
        // ignore
      }
    };
    page.on("request", onRequest);

    const onResponse = (resp) => {

      const task = (async () => {
        const url = resp.url();
        const status = resp.status();
        const rt = resp.request().resourceType();
        const headers = resp.headers();
        const ct = String(headers["content-type"] || headers["Content-Type"] || "");
        const ctLower = ct.toLowerCase();

        if (/datadoghq|browser-intake|mpps\.similarweb\.com\/track/i.test(url)) return;

        if (responsesMeta.length < 200) {
          responsesMeta.push({ url, status, resource_type: rt, content_type: ct });
        }

        const looksJson = ctLower.includes("json") || ctLower.includes("graphql") || /\.json(\\?|$)/i.test(url);
        const looksRsc = ctLower.includes("text/x-component");
        const isFetchLike = rt === "xhr" || rt === "fetch";

        if ((looksJson || isFetchLike) && jsonPayloads.length < 60) {
          let body = null;
          try {
            body = await resp.json();
          } catch {
            try {
              const t = await resp.text();
              const trimmed = t.trim();
              if (trimmed.startsWith("{") || trimmed.startsWith("[")) body = JSON.parse(trimmed);
            } catch {
              body = null;
            }
          }
          if (body != null) jsonPayloads.push({ url, status, body });
        }

        if (looksRsc && rscPayloads.length < 30) {
          try {
            const t = await resp.text();
            if (t && t.length) rscPayloads.push({ url, status, text: t });
          } catch {
            // ignore
          }
        }
      })();
      responseTasks.add(task);
      void task.finally(() => responseTasks.delete(task)).catch(() => {});
    };
    page.on("response", onResponse);

    const gotoBackoffs = [5000, 15000];
    let navigated = false;

    for (let attempt = 0; attempt < 3; attempt += 1) {
      try {
        await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 90_000 });
        await page.waitForSelector("text=Performance Overview", { timeout: 45_000 }).catch(() => {});
        await page.waitForSelector("text=Store Downloads", { timeout: 45_000 }).catch(() => {});
        await page.waitForTimeout(6000);
        await Promise.allSettled(Array.from(responseTasks));
        navigated = true;
        break;
      } catch (err) {
        if (!isPlaywrightTimeout(err)) throw err;

        // Retry timeouts a couple times with backoff.
        if (attempt < 2) {
          await sleep(gotoBackoffs[attempt]);
          continue;
        }

        const hint = await sniffAuthOrBlocked(page);
        if (hint === "login") {
          const e = new Error("Similarweb session appears expired.");
          e.code = "SW_LOGIN_EXPIRED";
          throw e;
        }
        if (hint === "blocked") {
          const e = new Error("Similarweb access blocked (captcha / access denied).");
          e.code = "SW_BLOCKED";
          throw e;
        }

        await insertAlert(
          bq,
          {
            store,
            tab: "overview",
            stage: "playwright_navigation_timeout",
            app_id: appId,
            google_package: googlePackage || null,
            page_url: pageUrl,
            error_type: "timeout",
            error_message: "Timeout loading Similarweb overview page after retries",
          },
          alertsLogPath
        );
        await pw.recreatePage().catch(() => {});
        await pw.recreatePage().catch(() => {});
      return { ok: false, status: "NAV_TIMEOUT", details: "timeout", pageUrl, googlePackageHint: null }
      }
    }

    if (!navigated) {
      await insertAlert(
        bq,
        {
          store,
          tab: "overview",
          stage: "playwright_navigation_timeout",
          app_id: appId,
          google_package: googlePackage || null,
          page_url: pageUrl,
          error_type: "timeout",
          error_message: "Failed to navigate Similarweb overview page",
        },
        alertsLogPath
      );
      await pw.recreatePage().catch(() => {});
      return { ok: false, status: "NAV_TIMEOUT", details: "timeout", pageUrl, googlePackageHint: null }
    }


    const finalUrl = page.url();
    if (/\/login|signin|sign-in|\/auth/i.test(finalUrl)) {
      const err = new Error("Similarweb session appears expired.");
      err.code = "SW_LOGIN_EXPIRED";
      throw err;
    }

    let googlePackageHint = null;
    if (store === "apple") {
      const href = await page.evaluate(() => {
        const anchors = Array.from(document.querySelectorAll("a[href]"));
        const link = anchors.find((a) => /\/app-analysis\/overview\/google\//i.test(a.getAttribute("href") || ""));
        return link ? link.getAttribute("href") : null;
      });
      if (href) {
        const m = String(href).match(/\/app-analysis\/overview\/google\/([^?"'\s]+)/i);
        googlePackageHint = m ? m[1] : null;
      }
    }

    // Also capture embedded Next.js payloads (often contains the actual metrics without XHR/JSON endpoints).
    try {
      const nextText = await page.evaluate(() => {
        const el = document.querySelector("script#__NEXT_DATA__");
        return el ? el.textContent : null;
      });
      if (nextText) {
        const parsed = JSON.parse(nextText);
        if (parsed && jsonPayloads.length < 60) jsonPayloads.push({ url: "__NEXT_DATA__", status: 200, body: parsed });
      }
    } catch {
      // ignore
    }
    let domText = null;
    let bestDom = null;
    try {
      domText = await page.evaluate(() => (document && document.body ? document.body.innerText : ""));
      const metrics = extractOverviewMetricsFromDomText(domText);
      const score = scoreOverview(metrics);
      bestDom = { payload: { url: "__DOM_INNERTEXT__", status: 200, text: domText }, metrics, score };
    } catch {
      bestDom = null;
    }
    const bestJson = pickBestJsonPayload(jsonPayloads);

    let bestRsc = null;
    for (const p of rscPayloads) {
      const metrics = extractOverviewMetricsFromText(p.text);
      const score = scoreOverview(metrics);
      if (!bestRsc || score > bestRsc.score) bestRsc = { payload: p, metrics, score };
    }

    let best = null;
    let bestKind = null;
    const candidates = [
      bestJson ? { kind: "json", ...bestJson } : null,
      bestRsc ? { kind: "rsc", ...bestRsc } : null,
      bestDom ? { kind: "dom", ...bestDom } : null,
    ].filter(Boolean);
    for (const c of candidates) {
      if (!best || c.score > best.score) {
        best = c;
        bestKind = c.kind;
      }
    }
    if (debugNetwork) {
      const debugPath = path.join(__dirname, "..", "logs", `debug_${store}_${appId}.json`);
      const record = {
        at: new Date().toISOString(),
        store,
        app_id: appId,
        google_package: googlePackage || null,
        page_url: pageUrl,
        requests_meta: requestsMeta,
        responses_meta: responsesMeta,
        captured_json_urls: jsonPayloads.map((p) => p.url),
        captured_rsc_urls: rscPayloads.map((p) => p.url),
        best_kind: bestKind,
        best_url: best?.payload?.url || null,
        best_status: best?.payload?.status || null,
        best_metrics: best?.metrics || null,
        best_body_snippet:
          bestKind === "json"
            ? (best?.payload?.body ? safeStringify(best.payload.body, 2000) : null)
            : (best?.payload?.text ? String(best.payload.text).slice(0, 2000) : null),
      };
      await fs.writeFile(debugPath, JSON.stringify(record, null, 2) + "", "utf8");
    }

    if (!best || isOverviewEmpty(best.metrics)) {
      const urls = jsonPayloads.map((p) => p.url).slice(0, 10);
      const snippet = jsonPayloads[0]?.body ? safeStringify(jsonPayloads[0].body, 2000) : null;
      await insertAlert(
        bq,
        {
          store,
          tab: "overview",
          stage: "extract_json_metrics",
          app_id: appId,
          google_package: googlePackage || null,
          page_url: pageUrl,
          error_type: "metrics_not_found",
          error_message: JSON.stringify({ json_urls: urls, sample_json_snippet: snippet }),
        },
        alertsLogPath
      );

      return { ok: false, status: "NO_DATA", details: "metrics_not_found", pageUrl, googlePackageHint }
    }

    const rawText =
      bestKind === "json" ? safeStringify(best.payload.body) : best.payload.text ? String(best.payload.text) : null;

    if (writeToBq) {
      const table = bq.dataset(DATASET_ID).table(tableId);
      const row = {
        month: monthStr,
        country,
        pulled_at: new Date().toISOString(),
        app_id: appId,
        google_package: googlePackage || null,
        ...best.metrics,
        page_url: pageUrl,
        raw_rsc_text: truncateForBigQueryString(rawText),
      };
      await deleteSingleRowByKey(bq, tableId, { monthStr, country, appId, googlePackage });
      await insertRows(table, [row]);
    }

    return { ok: true, status: "SUCCESS", details: writeToBq ? (bestKind ?? null) : "no_write", pageUrl, googlePackageHint }
  } catch (err) {

    await insertAlert(
      bq,
      {
        store,
        tab: "overview",
        stage: "playwright_network_capture",
        app_id: appId,
        google_package: googlePackage || null,
        page_url: pageUrl,
        error_type: err?.code || "error",
        error_message: formatErrorMessage(err),
      },
      alertsLogPath
    );
    throw err;
  } finally {
    try {
      page.off("request", onRequest);
      page.off("response", onResponse);
    } catch {
      // ignore
    }
  }
}
async function main() {
  const argv = minimist(process.argv.slice(2), {
    boolean: ["dry_run", "headful", "debug_network"],
    string: ["month", "mode", "country", "tab", "profile_dir"],
    default: { limit: 150, mode: "backfill", tab: "overview", country: String(COUNTRY_DEFAULT), dry_run: false, headful: false, debug_network: false, workers: 1, worker: 0 },
  });

  const mode = String(argv.mode || "backfill");
  if (!["backfill", "weekly"].includes(mode)) throw new Error(`Invalid --mode: ${mode}`);

  const tab = String(argv.tab || "overview");
  const supportedTabs = new Set(["overview", "reviews", "usage_sessions", "technographics", "revenue", "audience", "all"]);
  if (!supportedTabs.has(tab)) throw new Error(`Invalid --tab: ${tab}`);

  const limit = mustInt(argv.limit ?? 150, "--limit");
  const country = mustInt(argv.country ?? COUNTRY_DEFAULT, "--country");
  if (typeof country !== "number") throw new Error("--country must parse to a number (INT64). Got: " + (typeof country));

  const workers = mustInt(argv.workers ?? 1, "--workers");
  const worker = mustInt(argv.worker ?? 0, "--worker");
  if (workers < 1) throw new Error(`Invalid --workers: ${workers}`);
  if (worker < 0 || worker >= workers) throw new Error(`Invalid --worker: ${worker} (must be 0..${workers - 1})`);

  const headful = Boolean(argv.headful);
  const debugNetwork = Boolean(argv.debug_network);

  const wantTab = (name) => tab === "all" || tab === name;
  const profileDirArg = argv.profile_dir != null ? String(argv.profile_dir) : null;

  const monthDate = argv.month ? monthFromYyyyMm(argv.month) : previousFullMonthUtc();
  const monthStr = formatDateUTC(monthDate);
  const { from, to } = monthRangeUtc(monthDate);
  const fromStr = formatDateUTC(from);
  const toStr = formatDateUTC(to);

  process.stdout.write("Parsed types: typeof country=" + (typeof country) + ", monthDate=" + monthDate.toISOString().slice(0, 10) + "\\n");
  process.stdout.write("BQ params: month=" + monthStr + " (DATE), country=" + country + " (INT64)\\n");


  const yyyymmdd = todayYyyyMmDdUtc();
  const alertsLogPath = path.join(__dirname, "..", "logs", `similarweb_alerts_${yyyymmdd}.log`);
  const runLogPath = path.join(__dirname, "..", "logs", `similarweb_run_${yyyymmdd}.log`);
  const baseSessionDir = profileDirArg ? path.resolve(__dirname, "..", profileDirArg) : __dirname;
  if (profileDirArg) await fs.mkdir(baseSessionDir, { recursive: true });
  const cookiesPath = path.join(baseSessionDir, "cookies.json");
  const storageStatePath = path.join(baseSessionDir, "storageState.json");
  const userDataDir = profileDirArg ? baseSessionDir : null;

  const runId = generateRunId({ worker, workers });
  process.stdout.write('Run id: ' + runId + String.fromCharCode(10));
  process.stdout.write(`Shard worker ${worker}/${workers}\n`);

  // Ensure Similarweb session is valid (auto relogin headful if expired).
  const authCheckUrl = "https://apps.similarweb.com/app-analysis/overview/apple/835599320?country=999&from=2026-01-01&to=2026-01-31&window=false";
  await ensureSimilarwebAuth({ urlToCheck: authCheckUrl, headfulOnRelogin: true, checkHeadful: headful, userDataDir, storageStatePath, cookiesPath });
  await exportCookiesFromStorageState(storageStatePath, cookiesPath);

  let pw = null;
  try {
    await fs.access(cookiesPath);
    await fs.access(storageStatePath);
    pw = await createReusablePlaywrightPage({ storageStatePath, userDataDir, headful });
  } catch {
    throw new Error("Missing Similarweb session files. Run `node tools/similarweb_login.js` first (or pass --profile_dir to use a separate session)." );
  }

  const bq = createBigQueryClient();
  await ensureSchemas(bq);
  const selectionStats = await querySelectionStats({ bq, monthStr, country, tab });
  const processedAlreadyCount = await queryProcessedAlreadyCount({ bq, monthStr, country, tab });
  const selectedAppIdsAll = await querySelectedAppIds({ bq, mode, limit, monthStr, country, tab });
  const shardSkippedCount = selectedAppIdsAll.reduce((acc, appId) => acc + (shardBucketForAppId(appId, workers) !== worker ? 1 : 0), 0);
  const selectedAppIds = selectedAppIdsAll.filter((appId) => shardBucketForAppId(appId, workers) === worker);


  if (argv.dry_run) {
    process.stdout.write(
      JSON.stringify(
        {
          month: monthStr,
          mode,
          country,
          limit,
          workers,
          worker,
      selection_candidates_total: selectionStats.candidates_total,
      selection_missing_total: selectionStats.missing_total,
      selection_candidates_sample: selectionStats.candidates_sample,
      selection_missing_sample: selectionStats.missing_sample,
      candidates_total: selectedAppIdsAll.length,
          skipped_by_shard: shardSkippedCount,
          selected_count: selectedAppIds.length,
          processed_already_count: processedAlreadyCount,
          sample_app_ids: selectedAppIds.slice(0, 20),
        },
        null,
        2
      ) + ""
    );
    return;
  }

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
      workers,
      worker,
      selection_candidates_total: selectionStats.candidates_total,
      selection_missing_total: selectionStats.missing_total,
      selection_candidates_sample: selectionStats.candidates_sample,
      selection_missing_sample: selectionStats.missing_sample,
      candidates_total: selectedAppIdsAll.length,
      skipped_by_shard: shardSkippedCount,
      selected_count: selectedAppIds.length,
      processed_already_count: processedAlreadyCount,
    })
  );

  let http = new SimilarwebHttpClient({ cookiesPath, runLogPath });
  const reloginRetriedAppIds = new Set();
  const t0 = Date.now();

  let attemptNum = 0;

  const counters = {
    candidates_total: selectionStats.candidates_total,
    missing_total: selectionStats.missing_total,
    selected_total: selectedAppIdsAll.length,
    shard_selected_total: selectedAppIds.length,
    skipped_by_shard: shardSkippedCount,
    attempted_apps: 0,
    attempted_tabs: 0,
    success: 0,
    no_data: 0,
    skipped_exists: 0,
    errors: 0,
    parse_error: 0,
    nav_timeout: 0,
    login_expired: 0,
    captcha: 0,
    bq_error: 0,
    http_error: 0,
    access_denied: 0,
  };
  const tabStats = new Map();

  const auditTable = bq.dataset(DATASET_ID).table(TABLES.audit);
  const auditBuffer = [];
  const AUDIT_BATCH_SIZE = 300;

  async function flushAudit() {
    if (!auditBuffer.length) return;
    const batch = auditBuffer.splice(0, auditBuffer.length);
    try {
      await insertRows(auditTable, batch);
    } catch (err) {
      await appendLine(runLogPath, JSON.stringify({ event: 'audit_insert_error', at: nowIso(), run_id: runId, rows: batch.length, error: formatErrorMessage(err) }));
    }
  }

  async function pushAuditRow(row) {
    auditBuffer.push(row);
    if (auditBuffer.length >= AUDIT_BATCH_SIZE) await flushAudit();
  }

  async function recordAudit({ store, tabName, appId, googlePackage, status, details, pageUrl, durationMs, attemptNum }) {
    counters.attempted_tabs += 1;
    if (status === "SUCCESS") counters.success += 1;
    else if (status === "NO_DATA") counters.no_data += 1;
    else if (status === "SKIPPED_ALREADY_EXISTS") counters.skipped_exists += 1;
    else {
      counters.errors += 1;
      if (status === "LOGIN_EXPIRED") counters.login_expired += 1;
      else if (status === "CAPTCHA") counters.captcha += 1;
      else if (status === "NAV_TIMEOUT") counters.nav_timeout += 1;
      else if (status === "BQ_ERROR") counters.bq_error += 1;
      else if (status === "HTTP_ERROR") counters.http_error += 1;
      else if (status === "ACCESS_DENIED") counters.access_denied += 1;
      else counters.parse_error += 1;
    }

    const key = String(store || "") + ":" + String(tabName || "");
    const cur = tabStats.get(key) || { success: 0, http_error: 0, access_denied: 0, no_data: 0, other_error: 0 };
    if (status === "SUCCESS") cur.success += 1;
    else if (status === "ACCESS_DENIED") cur.access_denied += 1;
    else if (status === "HTTP_ERROR") cur.http_error += 1;
    else if (status === "NO_DATA") cur.no_data += 1;
    else if (status === "SKIPPED_ALREADY_EXISTS") {}
    else cur.other_error += 1;
    tabStats.set(key, cur);

    await pushAuditRow({
      run_id: runId,
      pulled_at: nowIso(),
      store,
      tab: tabName,
      month: monthStr,
      country,
      app_id: appId,
      google_package: googlePackage || null,
      status,
      details: truncateDetails(details, 2000),
      page_url: pageUrl || null,
      duration_ms: durationMs == null ? null : Math.trunc(durationMs),
      attempt_num: attemptNum,
    });
  }


  function isFatalSimilarwebError(err) {
    const code = err?.code || null;
    return code === "SW_LOGIN_EXPIRED" || code === "SW_BLOCKED" || code === "SW_TOO_MANY_429" || code === "SW_TOO_MANY_403";
  }

  async function runOverviewTab({ store, tableId, id, appId, googlePackage }) {
    const pageUrl = buildSimilarwebUrl(`/app-analysis/overview/${store}/${id}`, {
      country,
      from: fromStr,
      to: toStr,
      window: "false",
    });
    const started = Date.now();

    try {
      const exists = await rowExists(bq, tableId, { monthStr, country, appId, googlePackage });
      if (exists) {
        await recordAudit({ store, tabName: "overview", appId, googlePackage, status: "SKIPPED_ALREADY_EXISTS", details: "exists", pageUrl, durationMs: Date.now() - started, attemptNum });
        return { skipped: true };
      }

      const res = await scrapeOverviewWithNetwork({
        bq,
        tableId,
        store,
        id,
        monthStr,
        country,
        fromStr,
        toStr,
        appId,
        googlePackage,
        writeToBq: true,
        pw,
        debugNetwork,
        alertsLogPath,
      });

      if (!res?.ok) {
        await recordAudit({ store, tabName: "overview", appId, googlePackage, status: res?.status || "NO_DATA", details: res?.details || "no_data", pageUrl, durationMs: Date.now() - started, attemptNum });
        return { ok: false };
      }

      await recordAudit({ store, tabName: "overview", appId, googlePackage, status: "SUCCESS", details: res?.details || null, pageUrl, durationMs: Date.now() - started, attemptNum });
      return { ok: true, pageUrl, googlePackageHint: res?.googlePackageHint || null };
    } catch (err) {
      const status = classifyAuditStatus(err);
      await recordAudit({ store, tabName: "overview", appId, googlePackage, status, details: formatErrorMessage(err), pageUrl, durationMs: Date.now() - started, attemptNum });
      if (isFatalSimilarwebError(err)) throw err;
      return { ok: false };
    }
  }

  async function runRscTab({ store, tabName, alertsTab = null, tableId, route, query, appId, googlePackage, parser }) {
    const pageUrl = buildSimilarwebUrl(route, query);
    const started = Date.now();

    try {
      const exists = await rowExists(bq, tableId, { monthStr, country, appId, googlePackage });
      if (exists) {
        await recordAudit({ store, tabName, appId, googlePackage, status: "SKIPPED_ALREADY_EXISTS", details: "exists", pageUrl, durationMs: Date.now() - started, attemptNum });
        return { skipped: true };
      }

      await fetchAndInsert({
        http,
        bq,
        tableId,
        store,
        tab: alertsTab || tabName,
        route,
        query,
        monthStr,
        country,
        appId,
        googlePackage,
        parser,
        alertsLogPath,
        runLogPath,
        pwPage: pw?.page,
        domTabName: tabName,
      });

      await recordAudit({ store, tabName, appId, googlePackage, status: "SUCCESS", details: null, pageUrl, durationMs: Date.now() - started, attemptNum });
      return { ok: true };
    } catch (err) {
      const status = classifyAuditStatus(err);
      await recordAudit({ store, tabName, appId, googlePackage, status, details: formatErrorMessage(err), pageUrl, durationMs: Date.now() - started, attemptNum });
      if (isFatalSimilarwebError(err)) throw err;
      return { ok: false };
    }
  }

  async function runTechnographicsTab({ store, id, appId, googlePackage }) {
    const route = `/app-analysis/technographics/${store}/${id}`;
    const query = { country, from: fromStr, to: toStr, window: "false" };
    const pageUrl = buildSimilarwebUrl(route, query);
    const started = Date.now();

    const tableId = store === "apple" ? TABLES.apple.technographics_overview : TABLES.google.technographics_overview;
    const sdksTableId = store === "apple" ? TABLES.apple.technographics_sdks : TABLES.google.technographics_sdks;

    try {
      const exists = await rowExists(bq, tableId, { monthStr, country, appId, googlePackage });
      if (exists) {
        await recordAudit({ store, tabName: "technographics", appId, googlePackage, status: "SKIPPED_ALREADY_EXISTS", details: "exists", pageUrl, durationMs: Date.now() - started, attemptNum });
        return { skipped: true };
      }

      await fetchAndInsert({
        http,
        bq,
        tableId,
        store,
        tab: "technographics",
        route,
        query,
        monthStr,
        country,
        appId,
        googlePackage,
        parser: (t) => parseTechnographicsOverview(t),
        alertsLogPath,
        runLogPath,
        pwPage: pw?.page,
        domTabName: "technographics",
      });

      await scrapeTechnographicsSdks({
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
        tableId: sdksTableId,
        alertsLogPath,
      });

      await recordAudit({ store, tabName: "technographics", appId, googlePackage, status: "SUCCESS", details: null, pageUrl, durationMs: Date.now() - started, attemptNum });
      return { ok: true };
    } catch (err) {
      const status = classifyAuditStatus(err);
      await recordAudit({ store, tabName: "technographics", appId, googlePackage, status, details: formatErrorMessage(err), pageUrl, durationMs: Date.now() - started, attemptNum });
      if (isFatalSimilarwebError(err)) throw err;
      return { ok: false };
    }
  }
  const attemptByAppId = new Map();

  appsLoop: for (let i = 0; i < selectedAppIds.length; i += 1) {
    const appId = selectedAppIds[i];
    const idx1 = i + 1;
    const appStart = Date.now();
    attemptNum = (attemptByAppId.get(appId) || 0) + 1;
    attemptByAppId.set(appId, attemptNum);

    counters.attempted_apps += 1;

    const commonQuery = { country, from: fromStr, to: toStr, window: "false" };
    const reviewsQuery = { country, window: "false" };

    let retrySameApp = false;

    try {
      let googlePackage = await lookupGooglePackageFromMap(bq, appId);

      let appleOverview = null;
      if (wantTab("overview")) {
        appleOverview = await runOverviewTab({ store: "apple", tableId: TABLES.apple.overview, id: String(appId), appId, googlePackage: null });
      } else if (!googlePackage) {
        appleOverview = await scrapeOverviewWithNetwork({
          bq,
          tableId: TABLES.apple.overview,
          store: "apple",
          id: String(appId),
          monthStr,
          country,
          fromStr,
          toStr,
          appId,
          googlePackage: null,
          writeToBq: false,
          pw,
          debugNetwork,
          alertsLogPath,
        });
      }

      if (!googlePackage && appleOverview?.googlePackageHint) googlePackage = appleOverview.googlePackageHint;

      if (wantTab("reviews")) {
        await runRscTab({
          store: "apple",
          tabName: "reviews",
          tableId: TABLES.apple.reviews,
          route: `/app-analysis/reviews/apple/${appId}`,
          query: reviewsQuery,
          appId,
          googlePackage: null,
          parser: (t) => parseReviewsReplyRate(t),
        });
      }

      if (wantTab("usage_sessions")) {
        await runRscTab({
          store: "apple",
          tabName: "usage_sessions",
          alertsTab: "usage-and-engagement",
          tableId: TABLES.apple.usage_sessions,
          route: `/app-analysis/usage-and-engagement/apple/${appId}`,
          query: commonQuery,
          appId,
          googlePackage: null,
          parser: (t) => parseUsageSessions(t),
        });
      }

      if (wantTab("technographics")) {
        await runTechnographicsTab({ store: "apple", id: String(appId), appId, googlePackage: null });
      }

      if (wantTab("revenue")) {
        await runRscTab({
          store: "apple",
          tabName: "revenue",
          tableId: TABLES.apple.revenue,
          route: `/app-analysis/revenue/apple/${appId}`,
          query: commonQuery,
          appId,
          googlePackage: null,
          parser: (t) => parseRevenueTotal(t),
        });
      }

      if (wantTab("audience")) {
        await runRscTab({
          store: "apple",
          tabName: "audience",
          alertsTab: "audience-analysis",
          tableId: TABLES.apple.audience,
          route: `/app-analysis/audience-analysis/apple/${appId}`,
          query: commonQuery,
          appId,
          googlePackage: null,
          parser: (t) => parseAudience(t),
        });
      }
      if (!googlePackage) {
        try {
          googlePackage = await resolveGooglePackageViaPlaywright({ appId, country, fromDate: from, toDate: to, headful, storageStatePath, userDataDir });
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
              error_message: formatErrorMessage(err),
            },
            alertsLogPath
          );
        }
      }

      if (googlePackage) {
        await upsertMapRow(bq, { appId, googlePackage, sourceUrl: appleOverview?.pageUrl || null });

        if (wantTab("overview")) {
          await runOverviewTab({ store: "google", tableId: TABLES.google.overview, id: googlePackage, appId, googlePackage });
        }

        if (wantTab("reviews")) {
          await runRscTab({
            store: "google",
            tabName: "reviews",
            tableId: TABLES.google.reviews,
            route: `/app-analysis/reviews/google/${googlePackage}`,
            query: reviewsQuery,
            appId,
            googlePackage,
            parser: (t) => parseReviewsReplyRate(t),
          });
        }

        if (wantTab("usage_sessions")) {
          await runRscTab({
            store: "google",
            tabName: "usage_sessions",
            alertsTab: "usage-and-engagement",
            tableId: TABLES.google.usage_sessions,
            route: `/app-analysis/usage-and-engagement/google/${googlePackage}`,
            query: commonQuery,
            appId,
            googlePackage,
            parser: (t) => parseUsageSessions(t),
          });
        }

        if (wantTab("technographics")) {
          await runTechnographicsTab({ store: "google", id: googlePackage, appId, googlePackage });
        }

        if (wantTab("revenue")) {
          await runRscTab({
            store: "google",
            tabName: "revenue",
            tableId: TABLES.google.revenue,
            route: `/app-analysis/revenue/google/${googlePackage}`,
            query: commonQuery,
            appId,
            googlePackage,
            parser: (t) => parseRevenueTotal(t),
          });
        }

        if (wantTab("audience")) {
          await runRscTab({
            store: "google",
            tabName: "audience",
            alertsTab: "audience-analysis",
            tableId: TABLES.google.audience,
            route: `/app-analysis/audience-analysis/google/${googlePackage}`,
            query: commonQuery,
            appId,
            googlePackage,
            parser: (t) => parseAudience(t),
          });
        }
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
      if (err?.code === "SW_LOGIN_EXPIRED") {
        if (reloginRetriedAppIds.has(appId)) {
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
              error_message: formatErrorMessage(err),
            },
            alertsLogPath
          );
          await appendLine(
            runLogPath,
            JSON.stringify({ event: "fatal", at: nowIso(), app_id: appId, error: String(err) })
          );
          throw err;
        }

        reloginRetriedAppIds.add(appId);
        await insertAlert(
          bq,
          {
            store: null,
            tab: null,
            stage: "auth_relogin",
            app_id: appId,
            google_package: null,
            page_url: null,
            error_type: err?.code,
            error_message: "Session expired mid-run; triggering headful relogin and retrying this app once.",
          },
          alertsLogPath
        );
        await appendLine(
          runLogPath,
          JSON.stringify({ event: "relogin", at: nowIso(), app_id: appId, reason: "SW_LOGIN_EXPIRED" })
        );

        await ensureSimilarwebAuth({ urlToCheck: authCheckUrl, headfulOnRelogin: true, checkHeadful: headful, userDataDir, storageStatePath, cookiesPath });
        await exportCookiesFromStorageState(storageStatePath, cookiesPath);
        if (pw) await pw.close().catch(() => {});
        pw = await createReusablePlaywrightPage({ storageStatePath, userDataDir, headful });
        http = new SimilarwebHttpClient({ cookiesPath, runLogPath });
        retrySameApp = true;
      } else if (
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
            error_message: formatErrorMessage(err),
          },
          alertsLogPath
        );
        await appendLine(runLogPath, JSON.stringify({ event: "fatal", at: nowIso(), app_id: appId, error: String(err) }));
        throw err;
      }
    } finally {
      if (retrySameApp) {
        i -= 1;
        await sleep(jitter(1500, 2500));
        continue appsLoop;
      }

      if (idx1 % 10 === 0) {
        const elapsedMs = Date.now() - t0;
        const hours = elapsedMs / (1000 * 60 * 60);
        const attemptedApps = counters.attempted_apps ?? idx1;
        const rate = hours > 0 ? (attemptedApps / hours).toFixed(2) : 'inf';
        const avgSecPerApp = attemptedApps > 0 ? (elapsedMs / 1000 / attemptedApps).toFixed(1) : 'inf';
        console.log(
          'Progress ' +
            idx1 +
            '/' +
            selectedAppIds.length +
            ' | attempted=' +
            attemptedApps +
            ' success=' +
            counters.success +
            ' errors=' +
            counters.errors +
            ' | ' +
            rate +
            ' apps/hour | avg ' +
            avgSecPerApp +
            's/app'
        );
      }

      await appendLine(
        runLogPath,
        JSON.stringify({ event: "app_done", at: nowIso(), app_id: appId, ms: Date.now() - appStart })
      );

      await flushAudit();

      await sleep(jitter(2000, 4000));
    }
  }

  await flushAudit();

  await appendLine(
    runLogPath,
    JSON.stringify({ event: 'summary', at: nowIso(), run_id: runId, tab, month: monthStr, country, worker, workers, counters })
  );

  {
    const elapsedMs = Date.now() - t0;
    const hours = elapsedMs / (1000 * 60 * 60);
    const attemptedApps = counters.attempted_apps ?? 0;
    const rate = hours > 0 ? (attemptedApps / hours).toFixed(2) : 'inf';
    process.stdout.write(
      String.fromCharCode(10) +
        'Summary: ' +
        JSON.stringify({ ...counters, elapsed_ms: elapsedMs, apps_per_hour: Number(rate) }) +
        String.fromCharCode(10)
    );
  }

  // Tab readiness verdicts (for mass runs)
  {
    const verdicts = {};
    for (const [k, v] of tabStats.entries()) {
      verdicts[k] = { ...v, verdict: computeVerdictForTab(v) };
    }
    process.stdout.write("Tab verdicts: " + JSON.stringify(verdicts) + String.fromCharCode(10));
  }


  await appendLine(runLogPath, JSON.stringify({ event: 'done', at: nowIso(), run_id: runId }));

  if (pw) await pw.close().catch(() => {});
}

main().catch((err) => {
  process.stderr.write(`${err?.stack || err}
`);
  process.exitCode = 1;
});















































