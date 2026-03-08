import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import minimist from "minimist";
import { chromium } from "playwright";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function nowStampUtc() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return (
    d.getUTCFullYear() +
    pad(d.getUTCMonth() + 1) +
    pad(d.getUTCDate()) +
    "_" +
    pad(d.getUTCHours()) +
    pad(d.getUTCMinutes()) +
    pad(d.getUTCSeconds())
  );
}

function safeStringify(v, space = 2) {
  try {
    return JSON.stringify(v, null, space);
  } catch {
    return null;
  }
}

function monthToRange(monthStr) {
  const m = String(monthStr || "").trim();
  if (!/^\d{4}-\d{2}$/.test(m)) throw new Error(`Invalid --month=${monthStr}; expected YYYY-MM`);
  const [y, mo] = m.split("-").map((x) => Number(x));
  const from = new Date(Date.UTC(y, mo - 1, 1));
  const to = new Date(Date.UTC(y, mo, 0));
  const iso = (dt) => dt.toISOString().slice(0, 10);
  return { from: iso(from), to: iso(to) };
}

function buildTabUrl({ tab, store, id, country, from, to }) {
  const tabToRoute = {
    overview: "overview",
    usage_sessions: "usage-and-engagement",
    reviews: "reviews",
    revenue: "revenue",
    audience: "audience-analysis",
    technographics: "technographics",
  };
  const route = tabToRoute[tab];
  if (!route) throw new Error(`Unknown tab=${tab}`);
  const base = `https://apps.similarweb.com/app-analysis/${route}/${store}/${id}`;
  const u = new URL(base);
  u.searchParams.set("country", String(country));
  u.searchParams.set("from", from);
  u.searchParams.set("to", to);
  u.searchParams.set("window", "false");
  return u.toString();
}

function isProbablyUsefulHost(host) {
  const h = String(host || "").toLowerCase();
  return Boolean(h) && h.endsWith("similarweb.com");
}

function isTrackerUrl(url) {
  const u = String(url || "").toLowerCase();
  return (
    u.includes("datadoghq.com") ||
    u.includes("doubleclick") ||
    u.includes("google-analytics") ||
    u.includes("googletagmanager") ||
    u.includes("segment") ||
    u.includes("mixpanel") ||
    u.includes("hotjar") ||
    u.includes("sentry") ||
    u.includes("datadog") ||
    u.includes("amplitude")
  );
}

function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function computeDataScore({ url, contentType, bodySnippet, tabScore }) {
  const u = String(url || "").toLowerCase();
  const ct = String(contentType || "").toLowerCase();
  const snip = String(bodySnippet || "");
  const snipLower = snip.toLowerCase();

  let score = Number.isFinite(tabScore) ? tabScore : 0;

  // Prefer machine payloads over HTML documents.
  if (ct.includes("application/json")) score += 50;
  if (ct.includes("text/x-component")) score += 25;
  if (ct.includes("application/graphql")) score += 30;

  // URL hints.
  if (u.includes("graphql")) score += 20;
  if (u.includes("_rsc=")) score += 15;
  if (u.includes("/_next/data")) score += 12;
  if (u.includes("/api/")) score += 12;

  // Penalize HTML shell.
  const isHtml = snipLower.startsWith("<!doctype html") || snipLower.startsWith("<html");
  if (ct.includes("text/html")) score -= 40;
  if (isHtml) score -= 40;

  // Penalize common RSC loading-only shells.
  const looksLikeRsc = snip.includes('$Sreact.fragment') || snip.includes('"$Sreact.fragment"');
  const looksLikeLoading = snipLower.includes("/loading-") || snipLower.includes("loading-");
  if (looksLikeRsc && looksLikeLoading && (Number.isFinite(tabScore) ? tabScore : 0) <= 2) score -= 25;

  // Reward presence of digits (likely real values).
  if (/\d/.test(snip)) score += 2;

  return score;
}
function scoreBodyForTab(bodyText, tab) {
  const t = String(bodyText || "");
  const lower = t.toLowerCase();

  const needlesCommon = [
    "mau",
    "monthlyactive",
    "dau",
    "wau",
    "downloads",
    "revenue",
    "rank",
    "rating",
    "reply_rate",
    "replyrate",
    "sdk",
    "audience",
    "gender",
    "age",
    "sessions",
    "stickiness",
  ];

  const needlesByTab = {
    overview: ["store downloads", "performance", "rank", "rating"],
    usage_sessions: ["active users", "sessions", "stickiness", "dau", "wau", "mau"],
    reviews: ["reply", "reviews"],
    revenue: ["total revenue", "revenue"],
    audience: ["gender", "age distribution", "female", "male"],
    technographics: ["sdk", "installed", "technographics"],
  };

  const needles = [...needlesCommon, ...(needlesByTab[tab] || [])];
  let score = 0;
  const hits = [];
  for (const n of needles) {
    if (lower.includes(n)) {
      score += 1;
      hits.push(n);
    }
  }
  return { score, hits: hits.slice(0, 25) };
}

async function ensureDir(p) {
  await fs.mkdir(p, { recursive: true });
}

function pickSubsetHeaders(headers) {
  const h = headers || {};
  const out = {};
  const keys = ["content-type", "cache-control", "location", "set-cookie", "x-request-id", "cf-ray", "server"];
  for (const k of keys) {
    const v = h[k] || h[k.toLowerCase()];
    if (v != null) out[k] = v;
  }
  return out;
}

function requestHeaderSubset(headers) {
  const h = headers || {};
  const keys = [
    "accept",
    "rsc",
    "referer",
    "origin",
    "next-url",
    "next-router-state-tree",
    "next-router-prefetch",
    "next-router-segment-prefetch",
  ];
  const out = {};
  for (const k of keys) {
    const v = h[k] || h[k.toLowerCase()];
    if (v != null) out[k] = v;
  }
  return out;
}

function buildCapture() {
  const entries = [];
  const byId = new Map();
  const idByReq = new WeakMap();
  let nextId = 1;

  function onRequest(req) {
    const url = req.url();
    const rt = req.resourceType();
    const method = req.method();

    let host = null;
    try {
      host = new URL(url).host;
    } catch {
      host = null;
    }

    const isDoc = rt === "document";
    const isX = rt === "xhr" || rt === "fetch";
    const isWs = rt === "websocket";

    const isOtherMaybeData =
      rt === "other" &&
      isProbablyUsefulHost(host) &&
      (url.toLowerCase().includes("app-analysis") ||
        url.toLowerCase().includes("graphql") ||
        url.toLowerCase().includes("/api/") ||
        url.toLowerCase().includes("_rsc=") ||
        url.toLowerCase().includes("_next/data"));

    const isScriptMaybeData =
      rt === "script" &&
      isProbablyUsefulHost(host) &&
      (url.includes("_next/data") || url.toLowerCase().includes("graphql") || url.toLowerCase().includes("/api/") || url.toLowerCase().endsWith(".json"));

    if (!isDoc && !isX && !isWs && !isOtherMaybeData && !isScriptMaybeData) return;

    if (isTrackerUrl(url) && !String(host || "").toLowerCase().endsWith("similarweb.com")) return;

    const id = nextId++;
    idByReq.set(req, id);

    const h = req.headers();
    const e = {
      id,
      url,
      host,
      method,
      resource_type: rt,
      request_headers: requestHeaderSubset(h),
      postDataSnippet: (() => {
        try {
          const pd = req.postData();
          if (!pd) return null;
          return pd.length > 2000 ? pd.slice(0, 2000) : pd;
        } catch {
          return null;
        }
      })(),
      started_at: new Date().toISOString(),
      status: null,
      response_content_type: null,
      response_headers: null,
      location: null,
      body_snippet: null,
      body_sha1: null,
      body_truncated: null,
      tab_score: null,
      tab_hits: null,
      data_score: null,
      is_html_snippet: null,
      rsc_request_hint: null,
    };
    entries.push(e);
    byId.set(id, e);
  }

  function onResponse(resp, { tab } = {}) {
    const task = (async () => {
      const req = resp.request();
      const id = idByReq.get(req);
      if (!id) return;
      const e = byId.get(id);
      if (!e) return;

      const status = resp.status();
      const headers = resp.headers();
      const ct = String(headers["content-type"] || headers["Content-Type"] || "");
      const location = headers["location"] || headers["Location"] || null;

      e.status = status;
      e.response_content_type = ct || null;
      e.location = location;
      e.response_headers = pickSubsetHeaders(headers);

      const hostLower = String(e.host || "").toLowerCase();
      const shouldRead = isProbablyUsefulHost(hostLower);
      if (!shouldRead) return;

      const ctLower = ct.toLowerCase();
      const isTextLike =
        ctLower.includes("application/json") ||
        ctLower.includes("text/") ||
        ctLower.includes("text/x-component") ||
        ctLower.includes("application/graphql") ||
        e.url.toLowerCase().includes("graphql") ||
        e.url.toLowerCase().includes("_rsc=") ||
        e.url.toLowerCase().includes("_next/data") ||
        e.url.toLowerCase().includes("/api/") ||
        e.url.toLowerCase().endsWith(".json");

      if (!isTextLike) return;

      let text = null;
      try {
        text = await resp.text();
      } catch {
        text = null;
      }
      if (!text) return;

      e.body_sha1 = sha1(text);
      e.body_truncated = text.length > 300;
      e.body_snippet = text.slice(0, 300);
      const snipLower = String(e.body_snippet || "").toLowerCase();
      e.is_html_snippet = snipLower.startsWith("<!doctype html") || snipLower.startsWith("<html");

      if (tab) {
        const scored = scoreBodyForTab(text, tab);
        e.tab_score = scored.score;
        e.tab_hits = scored.hits;
      }

      e.data_score = computeDataScore({ url: e.url, contentType: e.response_content_type, bodySnippet: e.body_snippet, tabScore: e.tab_score });
    })();

    return task;
  }

  return { entries, onRequest, onResponse };
}

async function runOneTab({ context, tab, url, logsDir, stamp, headless }) {
  const page = await context.newPage();
  page.setDefaultTimeout(60_000);

  const capture = buildCapture();
  const responseTasks = new Set();

  page.on("request", (req) => {
    try {
      capture.onRequest(req);
      const u = req.url();
      const rt = req.resourceType();
      const host = (() => {
        try {
          return new URL(u).host;
        } catch {
          return "";
        }
      })();
      const lower = u.toLowerCase();
      const interesting =
        isProbablyUsefulHost(host) &&
        (rt === "xhr" || rt === "fetch" || rt === "document") &&
        (lower.includes("app-analysis") || lower.includes("_rsc=") || lower.includes("graphql") || lower.includes("/api/") || lower.includes("_next/data"));
      if (interesting) process.stdout.write(`[req:${tab}] ${rt} ${req.method()} ${u}\n`);
    } catch {}
  });

  page.on("response", (resp) => {
    const t = capture.onResponse(resp, { tab });
    if (t) responseTasks.add(t);
    if (t) t.finally(() => responseTasks.delete(t));
  });

  const debugBase = `debug_${tab}_${stamp}`;
  const logPath = path.join(logsDir, `${debugBase}.log`);
  const htmlPath = path.join(logsDir, `${debugBase}.html`);
  const pngPath = path.join(logsDir, `${debugBase}.png`);
  const netPath = path.join(logsDir, `xhr_fetch_${stamp}_${tab}.json`);

  const stages = [];
  stages.push({ event: "debug_start", at: new Date().toISOString(), tab, url, headless });

  let gotoStatus = null;
  let finalUrl = null;
  let title = null;
  let markers = null;

  try {
    stages.push({ event: "goto_start", at: new Date().toISOString(), url });
    const resp = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90_000 });
    gotoStatus = resp ? resp.status() : null;
    await page.waitForLoadState("networkidle", { timeout: 90_000 }).catch(() => {});
    await page.waitForTimeout(2500);

    finalUrl = page.url();
    title = await page.title().catch(() => null);

    const bodyText = await page.evaluate(() => (document && document.body ? document.body.innerText : "") || "");
    const lower = bodyText.toLowerCase();
    markers = {
      internal_server_error: lower.includes("internal server error"),
      sign_in: lower.includes("sign in") || lower.includes("login"),
      upgrade: lower.includes("upgrade") || lower.includes("plan"),
      access_denied: lower.includes("access denied") || lower.includes("forbidden") || lower.includes("not authorized"),
      has_403: lower.includes("403"),
    };

    const html = await page.content();
    await fs.writeFile(htmlPath, html, "utf8");
    await page.screenshot({ path: pngPath, fullPage: true }).catch(() => {});

    stages.push({ event: "goto_done", at: new Date().toISOString(), status: gotoStatus, final_url: finalUrl, title, markers });

    await Promise.allSettled(Array.from(responseTasks));

    await fs.writeFile(
      netPath,
      safeStringify({ tab, url, gotoStatus, finalUrl, title, markers, entries: capture.entries }) || "{}",
      "utf8"
    );

        const candidates = capture.entries
      .filter((e) => e && e.body_snippet && typeof e.data_score === "number")
      .map((e) => ({
        url: e.url,
        status: e.status,
        ct: e.response_content_type,
        data_score: e.data_score,
        tab_score: e.tab_score,
        hits: e.tab_hits,
        is_html: Boolean(e.is_html_snippet),
        rsc_hint: Boolean(e.rsc_request_hint),
        snippet: e.body_snippet,
      }))
      .sort((a, b) => (b.data_score || 0) - (a.data_score || 0));

    const bestNonHtml = candidates.find((c) => !c.is_html) || null;
    const best = bestNonHtml || candidates[0] || null;
    const top5 = candidates.slice(0, 5);
    stages.push({ event: "best_candidate", at: new Date().toISOString(), best, top5 });

    await fs.writeFile(logPath, safeStringify({ tab, url, stages }, 2) || "{}", "utf8");

    process.stdout.write(`\n[tab:${tab}] status=${gotoStatus} title=${title || ""}\n`);
        if (best) {
      process.stdout.write(`[tab:${tab}] Best candidate: data_score=${best.data_score} tab_score=${best.tab_score ?? 0} status=${best.status} ct=${best.ct || ""} rsc_hint=${best.rsc_hint} is_html=${best.is_html}\n`);
      process.stdout.write(`[tab:${tab}] URL: ${best.url}\n`);
      process.stdout.write(`[tab:${tab}] hits: ${JSON.stringify(best.hits)}\n`);
      process.stdout.write(`[tab:${tab}] snippet: ${String(best.snippet).replace(/\s+/g, " ").slice(0, 200)}\n`);

      const top = (top5 || []).slice(0, 3);
      if (top.length) {
        process.stdout.write(`[tab:${tab}] Top candidates:\n`);
        for (const c of top) {
          process.stdout.write(`  - data_score=${c.data_score} tab_score=${c.tab_score ?? 0} status=${c.status} ct=${c.ct || ""} rsc_hint=${c.rsc_hint} is_html=${c.is_html} url=${c.url}\n`);
        }
      }
    } else {
      process.stdout.write(`[tab:${tab}] No obvious data response found (only shells/tracking). See ${path.relative(process.cwd(), netPath)}\n`);
    }

    process.stdout.write(`Artifacts: ${path.relative(process.cwd(), logPath)} | ${path.relative(process.cwd(), htmlPath)} | ${path.relative(process.cwd(), pngPath)} | ${path.relative(process.cwd(), netPath)}\n`);
  } catch (err) {
    const msg = String(err && err.stack ? err.stack : err);
    stages.push({ event: "error", at: new Date().toISOString(), message: msg });
    await Promise.allSettled(Array.from(responseTasks));
    await fs.writeFile(netPath, safeStringify({ tab, url, gotoStatus, finalUrl, title, markers, entries: capture.entries, error: msg }) || "{}", "utf8");
    await fs.writeFile(logPath, safeStringify({ tab, url, stages }, 2) || "{}", "utf8");
    process.stdout.write(`\n[tab:${tab}] ERROR: ${msg}\n`);
    process.stdout.write(`Artifacts: ${path.relative(process.cwd(), logPath)} | ${path.relative(process.cwd(), netPath)}\n`);
  } finally {
    await page.close().catch(() => {});
  }
}

async function main() {
  const argv = minimist(process.argv.slice(2));

  const headless = Boolean(argv.headless);
  const urlArg = argv.url ? String(argv.url) : null;

  const appId = argv.app_id != null ? String(argv.app_id) : null;
  const store = argv.store != null ? String(argv.store) : "apple";
  const country = argv.country != null ? Number(argv.country) : 999;

  let from = argv.from != null ? String(argv.from) : null;
  let to = argv.to != null ? String(argv.to) : null;
  if (!from || !to) {
    const month = argv.month != null ? String(argv.month) : "2026-01";
    const r = monthToRange(month);
    from = from || r.from;
    to = to || r.to;
  }

  const tabsArg = argv.tabs != null ? String(argv.tabs) : null;
  const tabSingle = argv.tab != null ? String(argv.tab) : null;

  let targets = [];
  if (urlArg) {
    const inferredTab = (() => {
      const u = urlArg.toLowerCase();
      if (u.includes("/overview/")) return "overview";
      if (u.includes("/usage-and-engagement/")) return "usage_sessions";
      if (u.includes("/reviews/")) return "reviews";
      if (u.includes("/revenue/")) return "revenue";
      if (u.includes("/audience-analysis/")) return "audience";
      if (u.includes("/technographics/")) return "technographics";
      return "custom";
    })();
    targets = [{ tab: inferredTab, url: urlArg }];
  } else {
    if (!appId) throw new Error("Provide either --url or --app_id");

    let tabs = [];
    if (tabsArg) tabs = tabsArg.split(",").map((x) => x.trim()).filter(Boolean);
    else if (tabSingle) tabs = tabSingle.split(",").map((x) => x.trim()).filter(Boolean);
    else tabs = ["overview"]; // default

    if (tabs.includes("all")) {
      tabs = ["overview", "usage_sessions", "reviews", "revenue", "audience", "technographics"];
    }

    for (const tab of tabs) {
      const url = buildTabUrl({ tab, store, id: appId, country, from, to });
      targets.push({ tab, url });
    }
  }

  const storageStatePath = path.join(__dirname, "storageState.json");
  const logsDir = path.join(process.cwd(), "logs");
  await ensureDir(logsDir);

  const stamp = nowStampUtc();

  const browser = await chromium.launch({ headless });
  try {
    const context = await browser.newContext({ storageState: storageStatePath });

    process.stdout.write(`Headless=${headless} | country=${country} | from=${from} | to=${to}\n`);
    process.stdout.write(`Targets: ${targets.map((t) => t.tab).join(", ")}\n\n`);

    for (const t of targets) {
      await runOneTab({ context, tab: t.tab, url: t.url, logsDir, stamp, headless });
      await new Promise((r) => setTimeout(r, 1200));
    }

    await context.close().catch(() => {});
  } finally {
    await browser.close().catch(() => {});
  }
}

main().catch((err) => {
  process.stderr.write(String(err && err.stack ? err.stack : err) + "\n");
  process.exit(1);
});




