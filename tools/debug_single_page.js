import fs from "node:fs/promises";
import path from "node:path";
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

function safeStringify(v, maxChars = null) {
  try {
    const s = JSON.stringify(v);
    if (maxChars != null && s.length > maxChars) return s.slice(0, maxChars);
    return s;
  } catch {
    return null;
  }
}

function isPlainObject(x) {
  return x != null && typeof x === "object" && !Array.isArray(x);
}

function* walkJson(obj, { maxNodes = 30000 } = {}) {
  const stack = [{ value: obj, path: "$" }];
  let nodes = 0;
  while (stack.length) {
    const { value, path: p } = stack.pop();
    nodes += 1;
    if (nodes > maxNodes) return;

    yield { value, path: p };

    if (Array.isArray(value)) {
      for (let i = value.length - 1; i >= 0; i--) {
        stack.push({ value: value[i], path: `${p}[${i}]` });
      }
      continue;
    }
    if (!isPlainObject(value)) continue;

    const entries = Object.entries(value);
    for (let i = entries.length - 1; i >= 0; i--) {
      const [k, v] = entries[i];
      const nextPath = `${p}.${k}`;
      stack.push({ value: v, path: nextPath });
    }
  }
}

function findKeyPaths(obj, wantedKeysLower) {
  const out = [];
  for (const { value, path: p } of walkJson(obj)) {
    if (!isPlainObject(value)) continue;
    for (const [k, v] of Object.entries(value)) {
      const lk = String(k).toLowerCase();
      if (!wantedKeysLower.has(lk)) continue;
      out.push({ key: lk, path: `${p}.${k}`, value: v });
    }
  }
  return out;
}

function normalizeNumberText(s) {
  if (s == null) return null;
  if (typeof s === "number" && Number.isFinite(s)) return s;
  const raw = String(s).trim();
  if (!raw || raw === "-" || /^n\/a$/i.test(raw)) return null;
  const cleaned = raw.replace(/,/g, "").replace(/\s+/g, " ").trim();
  const m = cleaned.match(/^(-?\d+(?:\.\d+)?)([KMB])?$/i);
  if (m) {
    const n = Number(m[1]);
    if (!Number.isFinite(n)) return null;
    const suf = (m[2] || "").toUpperCase();
    const mult = suf === "K" ? 1e3 : suf === "M" ? 1e6 : suf === "B" ? 1e9 : 1;
    return n * mult;
  }
  const n2 = Number(cleaned);
  return Number.isFinite(n2) ? n2 : null;
}

function normalizeCurrencyUsd(s) {
  if (s == null) return { usd: null, text: null };
  if (typeof s === "number" && Number.isFinite(s)) return { usd: s, text: String(s) };
  const text = String(s).trim();
  if (!text || text === "-" || /^n\/a$/i.test(text)) return { usd: null, text: text || null };
  const cleaned = text.replace(/,/g, "").replace(/\$/g, "").trim();
  const m = cleaned.match(/^(-?\d+(?:\.\d+)?)([KMB])?$/i);
  if (m) {
    const base = Number(m[1]);
    if (!Number.isFinite(base)) return { usd: null, text };
    const suf = (m[2] || "").toUpperCase();
    const mult = suf === "K" ? 1e3 : suf === "M" ? 1e6 : suf === "B" ? 1e9 : 1;
    return { usd: base * mult, text };
  }
  const n = Number(cleaned);
  return { usd: Number.isFinite(n) ? n : null, text };
}

function pickMetricFromHits(hits, { kind, min = null, max = null } = {}) {
  for (const h of hits) {
    const v = h.value;
    let n = null;
    if (kind === "currency") {
      if (typeof v === "number") n = v;
      else if (typeof v === "string") n = normalizeCurrencyUsd(v).usd;
    } else {
      n = normalizeNumberText(v);
    }
    if (!Number.isFinite(n)) continue;
    if (min != null && n < min) continue;
    if (max != null && n > max) continue;
    return { path: h.path, value: n };
  }
  return { path: null, value: null };
}

function analyzePayloads(payloads) {
  const wantedKeys = new Set(
    [
      "mau",
      "monthlyactiveusers",
      "downloads",
      "storedownloads",
      "store_downloads",
      "totaldownloads",
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
    ].map((k) => k.toLowerCase())
  );

  const scored = [];
  for (const p of payloads) {
    if (!p.body || typeof p.body !== "object") continue;
    const hits = findKeyPaths(p.body, wantedKeys);
    if (!hits.length) continue;

    const byKey = new Map();
    for (const h of hits) {
      if (!byKey.has(h.key)) byKey.set(h.key, []);
      byKey.get(h.key).push(h);
    }

    const mau = pickMetricFromHits([...(byKey.get("mau") || []), ...(byKey.get("monthlyactiveusers") || [])]);
    const downloads = pickMetricFromHits([
      ...(byKey.get("downloads") || []),
      ...(byKey.get("store_downloads") || []),
      ...(byKey.get("storedownloads") || []),
      ...(byKey.get("totaldownloads") || []),
    ]);
    const revenue = pickMetricFromHits(
      [...(byKey.get("totalrevenue") || []), ...(byKey.get("revenueusd") || []), ...(byKey.get("revenue") || [])],
      { kind: "currency" }
    );
    const ratingAvg = pickMetricFromHits([...(byKey.get("ratingavg") || []), ...(byKey.get("rating") || [])], { min: 0, max: 5 });
    const ratingsCount = pickMetricFromHits([...(byKey.get("ratingscount") || []), ...(byKey.get("ratings_count") || [])]);
    const rankNum = pickMetricFromHits([...(byKey.get("rank") || []), ...(byKey.get("ranking") || [])]);

    let rankText = null;
    const rankTextHit = (byKey.get("ranktext") || []).find((h) => typeof h.value === "string" && h.value.trim());
    if (rankTextHit) rankText = { path: rankTextHit.path, value: rankTextHit.value };
    const rankingText = rankText?.value || (rankNum.value != null ? `#${rankNum.value}` : null);

    const score = [mau.value, downloads.value, revenue.value, ratingAvg.value, ratingsCount.value, rankingText].filter((x) => x != null).length;
    scored.push({
      url: p.url,
      score,
      metrics: {
        mau,
        revenue_usd: revenue,
        store_downloads: downloads,
        rating_avg: ratingAvg,
        ratings_count: ratingsCount,
        ranking_text: rankText?.path ? { path: rankText.path, value: rankText.value } : { path: rankNum.path, value: rankingText },
      },
    });
  }

  scored.sort((a, b) => b.score - a.score);
  return scored[0] || null;
}

const METRIC_HINTS = [
  "totalDownloads",
  "storeDownloads",
  "totalRevenue",
  "revenueUsd",
  "revenueUSD",
  "mau",
  "monthlyActiveUsers",
  "ratingAvg",
  "ratingsCount",
  "ranking",
  "rankText",
  "Store Downloads",
  "Total Revenue",
  "MAU",
];

function textLooksLikeMetrics(t) {
  if (!t) return false;
  const s = String(t);
  return METRIC_HINTS.some((h) => s.includes(h));
}

function extractFromTextHeuristics(t) {
  const text = String(t || "");
  const out = { mau: null, revenue_usd: null, store_downloads: null, rating_avg: null, ratings_count: null, ranking_text: null };

  const mDownloads = text.match(/"(?:totalDownloads|storeDownloads|store_downloads|total_downloads)"\s*:\s*\"?(-?\d+(?:\.\d+)?)\"?/);
  if (mDownloads) out.store_downloads = Number(mDownloads[1]);

  const mMau = text.match(/"(?:mau|monthlyActiveUsers)"\s*:\s*\"?(-?\d+(?:\.\d+)?)\"?/);
  if (mMau) out.mau = Number(mMau[1]);

  const mRev = text.match(/"(?:totalRevenue|revenueUsd|revenueUSD|revenue_usd)"\s*:\s*\"?(-?\d+(?:\.\d+)?)\"?/);
  if (mRev) out.revenue_usd = Number(mRev[1]);

  const mRankText = text.match(/"(?:rankText|rankingText)"\s*:\s*"([^"]+)"/);
  if (mRankText) out.ranking_text = mRankText[1];

  const mRankNum = text.match(/"(?:rank|ranking)"\s*:\s*\"?(\d+)\"?/);
  if (!out.ranking_text && mRankNum) out.ranking_text = `#${mRankNum[1]}`;

  const mRating = text.match(/"(?:ratingAvg|rating)"\s*:\s*\"?([0-5](?:\.\d+)?)\"?/);
  if (mRating) out.rating_avg = Number(mRating[1]);

  const mRatingsCount = text.match(/"(?:ratingsCount|ratings_count)"\s*:\s*\"?(\d+(?:\.\d+)?)\"?/);
  if (mRatingsCount) out.ratings_count = Number(mRatingsCount[1]);

  return out;
}
async function main() {
  const argv = minimist(process.argv.slice(2), {
    boolean: ["headless"],
    string: ["url", "storage"],
    default: {
      headless: false,
      url: "https://apps.similarweb.com/app-analysis/overview/apple/835599320?country=999&from=2026-01-01&to=2026-01-31&window=false",
      storage: path.join(__dirname, "storageState.json"),
    },
  });

  const targetUrl = String(argv.url);
  const storageStatePath = String(argv.storage);

  const logsDir = path.join(__dirname, "..", "logs");
  await fs.mkdir(logsDir, { recursive: true });
  const stamp = nowStampUtc();

  const inspectPath = path.join(logsDir, `inspect_payloads_${stamp}.json`);
  const inspectNonJsonPath = path.join(logsDir, `inspect_nonjson_${stamp}.json`);
  const nextDataPath = path.join(logsDir, `next_data_${stamp}.json`);
  const embeddedJsonPath = path.join(logsDir, embedded_json_.json);
  const pageHtmlPath = path.join(logsDir, page_html_.html);
  const xhrFetchPath = path.join(logsDir, xhr_fetch_.json);

  const interestingNeedles = [
    "graphql",
    "api",
    "app-analysis",
    "overview",
    "metrics",
    "download",
    "revenue",
    "mau",
  ];

  const browser = await chromium.launch({ headless: Boolean(argv.headless) });
  const captured = [];
  const capturedNonJson = [];

  const seenInteresting = [];
  const allXhrFetch = [];


  let mainDocStatus = null;
  let mainDocCt = null;

  let pageHtml = null;
  let docExtracted = null;
  try {
    const context = await browser.newContext({ storageState: storageStatePath });
    context.setDefaultTimeout(60_000);
    const page = await context.newPage();

    page.on("request", (req) => {
      try {
        const url = req.url();
        const rt = req.resourceType();
        const lower = url.toLowerCase();
        if (rt === "xhr" || rt === "fetch") {
          if (allXhrFetch.length < 800) allXhrFetch.push({ url, resource_type: rt, method: req.method() });
        }
        if (interestingNeedles.some((n) => lower.includes(n))) {
          seenInteresting.push({ url, resource_type: rt, method: req.method() });
          if (seenInteresting.length <= 80) {
            process.stdout.write(`[req] ${rt} ${req.method()} ${url}\n`);
          }
        }
      } catch {
        // ignore
      }
    });

    const responseTasks = new Set();
    page.on("response", (resp) => {
      const task = (async () => {
        const url = resp.url();
        const status = resp.status();
        const headers = resp.headers();
        const ct = String(headers["content-type"] || headers["Content-Type"] || "");
        const lowerUrl = url.toLowerCase();

        if (lowerUrl === targetUrl.toLowerCase()) {
          mainDocStatus = status;
          mainDocCt = ct;
          try {
            pageHtml = await resp.text();
          } catch {
            pageHtml = null;
          }
        }

        const isJson = ct.toLowerCase().includes("application/json") || lowerUrl.includes("graphql");
        const isRsc = ct.toLowerCase().includes("text/x-component") || lowerUrl.includes("_rsc=");
        const isFetchLike = resp.request().resourceType() === "xhr" || resp.request().resourceType() === "fetch";

        if (!isJson && !isRsc && !isFetchLike) return;

        let body = null;
        let text = null;
        try {
          body = await resp.json();
        } catch {
          try {
            text = await resp.text();
            const trimmed = String(text || "").trim();
            if (trimmed.startsWith("{") || trimmed.startsWith("[")) body = JSON.parse(trimmed);
          } catch {
            body = null;
          }
        }

        const bodyStr = body != null ? safeStringify(body) : text;
        const snippet = bodyStr ? String(bodyStr).slice(0, 2000) : null;

        captured.push({
          url,
          status,
          headers,
          body_snippet: snippet,
          body_full_if_small: bodyStr && bodyStr.length <= 200_000 ? bodyStr : null,
          body: body && safeStringify(body).length <= 200_000 ? body : null,
        });

        if (captured.length <= 20) {
          process.stdout.write(`[json] ${status} ${url}\n`);
        }
      })();

      responseTasks.add(task);
      void task.finally(() => responseTasks.delete(task)).catch(() => {});
    });

    process.stdout.write(`\nOpening (Jan 2026 only): ${targetUrl}\n\n`);
    await page.goto(targetUrl, { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle", { timeout: 30_000 }).catch(() => {});
    await page.waitForTimeout(8000);
    await Promise.allSettled(Array.from(responseTasks));

    // Dump all xhr/fetch requests (even if they don't match interesting needles)
    const uniq = new Map();
    for (const r of allXhrFetch) {
      if (!uniq.has(r.url)) uniq.set(r.url, r);
    }
    await fs.writeFile(xhrFetchPath, JSON.stringify(Array.from(uniq.values()), null, 2) + "\n", "utf8");

    if (pageHtml) {
      await fs.writeFile(pageHtmlPath, String(pageHtml), "utf8");
      const heur = extractFromTextHeuristics(pageHtml);
      const score = [heur.mau, heur.revenue_usd, heur.store_downloads, heur.ranking_text, heur.rating_avg, heur.ratings_count].filter((x) => x != null)
        .length;
      docExtracted = { score, extracted: heur, has_metric_hints: textLooksLikeMetrics(pageHtml) };
    }

    // __NEXT_DATA__
    let nextData = null;
    try {
      nextData = await page.evaluate(() => {
        // @ts-ignore
        if (typeof window !== "undefined" && window.__NEXT_DATA__) return window.__NEXT_DATA__;
        const el = document.querySelector("script#__NEXT_DATA__");
        if (el && el.textContent) {
          try {
            return JSON.parse(el.textContent);
          } catch {
            return null;
          }
        }
        return null;
      });
    } catch {
      nextData = null;
    }
    if (nextData) await fs.writeFile(nextDataPath, JSON.stringify(nextData, null, 2) + "\n", "utf8");

    // Embedded JSON scripts
    let embedded = [];
    try {
      embedded = await page.evaluate(() => {
        const els = Array.from(document.querySelectorAll('script[type="application/json"]'));
        return els.slice(0, 50).map((el) => {
          const txt = el.textContent || "";
          const id = el.getAttribute("id");
          const len = txt.length;
          let parsed = null;
          if (txt && len <= 300_000) {
            try {
              parsed = JSON.parse(txt);
            } catch {
              parsed = null;
            }
          }
          return { id, len, text_snippet: txt.slice(0, 2000), parsed };
        });
      });
    } catch {
      embedded = [];
    }
    if (embedded && embedded.length) await fs.writeFile(embeddedJsonPath, JSON.stringify(embedded, null, 2) + "\n", "utf8");

    // Save captured payloads
    const out = captured.map((p) => ({
      url: p.url,
      status: p.status,
      headers: p.headers,
      body_snippet: p.body_snippet,
      body_full_if_small: p.body_full_if_small,
    }));
    await fs.writeFile(inspectPath, JSON.stringify(out, null, 2) + "\n", "utf8");

    // Analyze
    const payloadObjs = [];
    for (const p of captured) {
      if (p.body != null) payloadObjs.push({ url: p.url, body: p.body });
      else if (p.body_full_if_small) {
        try {
          const parsed = JSON.parse(p.body_full_if_small);
          payloadObjs.push({ url: p.url, body: parsed });
        } catch {
          // ignore
        }
      }
    }
    if (nextData) payloadObjs.push({ url: "__NEXT_DATA__", body: nextData });
    for (const e of embedded || []) {
      if (e.parsed) payloadObjs.push({ url: `__EMBEDDED_JSON__:${e.id || "(no-id)"}`, body: e.parsed });
    }

    const best = analyzePayloads(payloadObjs);
    const bestTextCandidate = capturedNonJson
      .filter((p) => p && p.extracted)
      .map((p) => {
        const m = p.extracted;
        const score = [m.mau, m.revenue_usd, m.store_downloads, m.ranking_text, m.rating_avg, m.ratings_count].filter((x) => x != null).length;
        return { url: p.url, kind: p.kind, score, extracted: m };
      })
      .sort((a, b) => b.score - a.score)[0] || null;

    const rendered = await page.evaluate(() => {
      const t = (document && document.body ? document.body.innerText : "") || "";
      const hasLabel = /store\s+downloads/i.test(t) || /total\s+revenue/i.test(t) || /\bMAU\b/.test(t);
      const hasNumber = /\$\s*\d|#\s*\d|\b\d[\d,]*(?:\.\d+)?\s*[KMB]?\b/.test(t);
      return { hasLabel, hasNumber, text_sample: t.slice(0, 600) };
    });

    const blockedSignals = {
      main_doc_status: mainDocStatus,
      main_doc_content_type: mainDocCt,
      has_login_text: rendered.text_sample.toLowerCase().includes("sign in") || rendered.text_sample.toLowerCase().includes("login"),
      has_captcha_text: rendered.text_sample.toLowerCase().includes("captcha") || rendered.text_sample.toLowerCase().includes("access denied"),
    };

    const topUrls = Array.from(
      new Map(seenInteresting.map((x) => [x.url, x])).values()
    ).slice(0, 20);

    if (best) {
      process.stdout.write("\nSummary\n");
      process.stdout.write(`- Best candidate endpoint: ${best.url}\n`);
      process.stdout.write(`- JSON path for mau: ${best.metrics.mau.path || "(not found)"}\n`);
      process.stdout.write(`- JSON path for revenue: ${best.metrics.revenue_usd.path || "(not found)"}\n`);
      process.stdout.write(`- JSON path for downloads: ${best.metrics.store_downloads.path || "(not found)"}\n`);
      process.stdout.write(`- JSON path for ranking: ${best.metrics.ranking_text.path || "(not found)"}\n`);
      process.stdout.write(`- JSON path for rating_avg: ${best.metrics.rating_avg.path || "(not found)"}\n`);
      process.stdout.write(`- JSON path for ratings_count: ${best.metrics.ratings_count.path || "(not found)"}\n`);
    } else if (bestTextCandidate && bestTextCandidate.score > 0) {
      process.stdout.write("\nSummary (from RSC/text response)\n");
      process.stdout.write(`- Best candidate endpoint: ${bestTextCandidate.url} (${bestTextCandidate.kind})\n`);
      process.stdout.write(`- mau (regex): ${bestTextCandidate.extracted.mau ?? "(not found)"}\n`);
      process.stdout.write(`- revenue_usd (regex): ${bestTextCandidate.extracted.revenue_usd ?? "(not found)"}\n`);
      process.stdout.write(`- downloads (regex): ${bestTextCandidate.extracted.store_downloads ?? "(not found)"}\n`);
      process.stdout.write(`- ranking_text (regex): ${bestTextCandidate.extracted.ranking_text ?? "(not found)"}\n`);
      process.stdout.write(`- rating_avg (regex): ${bestTextCandidate.extracted.rating_avg ?? "(not found)"}\n`);
      process.stdout.write(`- ratings_count (regex): ${bestTextCandidate.extracted.ratings_count ?? "(not found)"}\n`);
    } else {
      process.stdout.write("\nMetrics not found in JSON/Next/embedded.\n");
      process.stdout.write(`- Rendered visually? labels=${rendered.hasLabel} numbers=${rendered.hasNumber}\n`);
      process.stdout.write(`- Blocked signals: ${JSON.stringify(blockedSignals)}\n`);
      process.stdout.write("- Top 20 interesting URLs:\n");
      for (const u of topUrls) process.stdout.write(`  - ${u.resource_type} ${u.method} ${u.url}\n`);

      const uniqX = Array.from(new Map(allXhrFetch.map((x) => [x.url, x])).values()).slice(0, 20);
      process.stdout.write("- Top 20 xhr/fetch URLs:\n");
      for (const u of uniqX) process.stdout.write(`  - ${u.resource_type} ${u.method} ${u.url}\n`);

      if (docExtracted) {
        process.stdout.write(`- Document HTML hints: has_metric_hints=${docExtracted.has_metric_hints} score=${docExtracted.score}\n`);
        process.stdout.write(`- Document HTML extracted (regex): ${JSON.stringify(docExtracted.extracted)}\n`);
      }
    }

    process.stdout.write("\nArtifacts\n");
    process.stdout.write(`- ${path.relative(process.cwd(), inspectPath)}\n`);
    process.stdout.write(`- ${path.relative(process.cwd(), inspectNonJsonPath)}\n`);
    process.stdout.write(`- ${path.relative(process.cwd(), xhrFetchPath)}\n`);
    if (pageHtml) process.stdout.write(`- ${path.relative(process.cwd(), pageHtmlPath)}\n`);
    if (nextData) process.stdout.write(`- ${path.relative(process.cwd(), nextDataPath)}\n`);
    if (embedded && embedded.length) process.stdout.write(`- ${path.relative(process.cwd(), embeddedJsonPath)}\n`);

    // keep browser open briefly in headful mode so user can see UI
    if (!argv.headless) {
      process.stdout.write("\nClose the browser window to end.\n");
      await page.waitForEvent("close", { timeout: 0 }).catch(() => {});
    }
  } finally {
    await browser.close().catch(() => {});
  }
}

main().catch((err) => {
  process.stderr.write(String(err && err.stack ? err.stack : err) + "\n");
  process.exit(1);
});






