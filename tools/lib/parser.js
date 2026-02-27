function stripQuotes(s) {
  if (s == null) return null;
  const t = String(s).trim();
  if (t.startsWith('"') && t.endsWith('"')) return t.slice(1, -1);
  return t;
}

export function normalizeNumberText(s) {
  if (s == null) return null;
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

export function normalizeCurrencyUsd(s) {
  if (s == null) return { usd: null, text: null };
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

export function normalizePercent(s) {
  if (s == null) return null;
  const text = String(s).trim();
  if (!text || text === "-" || /^n\/a$/i.test(text)) return null;
  const cleaned = text.replace(/%/g, "").replace(/,/g, "").trim();
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

export function parseTimeToSeconds(text) {
  if (!text) return null;
  const t = String(text).trim();
  const m = t.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return null;
  const a = Number(m[1]);
  const b = Number(m[2]);
  const c = m[3] == null ? null : Number(m[3]);
  if (![a, b, c == null ? 0 : c].every((x) => Number.isFinite(x))) return null;
  if (c == null) return a * 60 + b;
  return a * 3600 + b * 60 + c;
}

function parseFirstJsonNumberByKeys(rscText, keys) {
  for (const key of keys) {
    const re = new RegExp(`"${key}"\\s*:\\s*("?)(-?\\d+(?:\\.\\d+)?)\\1`);
    const m = rscText.match(re);
    if (m) {
      const n = Number(m[2]);
      if (Number.isFinite(n)) return n;
    }
  }
  return null;
}

function parseFirstJsonStringByKeys(rscText, keys) {
  for (const key of keys) {
    const re = new RegExp(`"${key}"\\s*:\\s*"([^"]+)"`);
    const m = rscText.match(re);
    if (m) return m[1];
  }
  return null;
}

function parseValueNearLabel(rscText, label) {
  const idx = rscText.toLowerCase().indexOf(String(label).toLowerCase());
  if (idx < 0) return null;
  const window = rscText.slice(idx, Math.min(rscText.length, idx + 800));

  const m1 = window.match(/"value"\s*:\s*"([^"]+)"/);
  if (m1) return m1[1];
  const m2 = window.match(/"valueText"\s*:\s*"([^"]+)"/);
  if (m2) return m2[1];
  const m3 = window.match(/"value"\s*:\s*(-?\d+(?:\.\d+)?)/);
  if (m3) return m3[1];

  const m4 = window.match(/(\$?-?\d+(?:\.\d+)?\s*[KMB]?%?)/);
  if (m4) return m4[1];

  return null;
}

export function extractGooglePackageFromRsc(rscText) {
  if (!rscText) return null;
  const m = String(rscText).match(/\/app-analysis\/overview\/google\/([^?"'\s]+)/i);
  if (!m) return null;
  const pkg = m[1].trim();
  if (!pkg || pkg.length > 250) return null;
  return pkg;
}

export function parseOverviewPerformance(rscText) {
  const text = String(rscText || "");

  const storeDownloadsRaw =
    parseValueNearLabel(text, "Store Downloads") ||
    parseValueNearLabel(text, "Store downloads") ||
    parseFirstJsonNumberByKeys(text, ["storeDownloads", "store_downloads", "totalDownloads", "total_downloads"]);

  const rankingText =
    parseValueNearLabel(text, "Ranking") ||
    parseValueNearLabel(text, "Rank") ||
    parseFirstJsonStringByKeys(text, ["ranking", "rank", "rankingText", "ranking_text"]);

  const ratingAvgRaw =
    parseValueNearLabel(text, "Rating") ||
    parseFirstJsonNumberByKeys(text, ["ratingAvg", "rating_avg", "rating", "avgRating"]);

  const ratingsCountRaw =
    parseValueNearLabel(text, "Ratings") ||
    parseFirstJsonNumberByKeys(text, ["ratingsCount", "ratings_count", "ratingCount", "userRatingCount"]);

  const analyzedTotalRaw = parseValueNearLabel(text, "Analyzed Reviews") || parseFirstJsonNumberByKeys(text, ["analyzedReviewsTotal"]);
  const analyzedNegRaw = parseValueNearLabel(text, "Negative") || parseFirstJsonNumberByKeys(text, ["analyzedReviewsNegative"]);
  const analyzedMixedRaw = parseValueNearLabel(text, "Mixed") || parseFirstJsonNumberByKeys(text, ["analyzedReviewsMixed"]);
  const analyzedPosRaw = parseValueNearLabel(text, "Positive") || parseFirstJsonNumberByKeys(text, ["analyzedReviewsPositive"]);

  const mauRaw =
    parseValueNearLabel(text, "MAU") ||
    parseValueNearLabel(text, "Monthly Active Users") ||
    parseFirstJsonNumberByKeys(text, ["mau", "monthlyActiveUsers"]);

  const stickinessRaw =
    parseValueNearLabel(text, "Daily Stickiness") ||
    parseValueNearLabel(text, "Stickiness") ||
    parseFirstJsonNumberByKeys(text, ["dailyStickiness", "daily_stickiness_pct"]);

  const revenueText = parseValueNearLabel(text, "Revenue") || parseFirstJsonStringByKeys(text, ["revenueText", "revenue_text"]);
  const revenueNumeric =
    parseFirstJsonNumberByKeys(text, ["totalRevenue", "revenueUsd", "revenueUSD", "revenue_usd"]) ||
    normalizeCurrencyUsd(revenueText).usd;

  return {
    store_downloads: normalizeNumberText(storeDownloadsRaw),
    ranking_text: rankingText ? stripQuotes(rankingText) : null,
    rating_avg: typeof ratingAvgRaw === "number" ? ratingAvgRaw : normalizeNumberText(ratingAvgRaw),
    ratings_count: typeof ratingsCountRaw === "number" ? ratingsCountRaw : normalizeNumberText(ratingsCountRaw),
    analyzed_reviews_total: normalizeNumberText(analyzedTotalRaw),
    analyzed_reviews_negative: normalizeNumberText(analyzedNegRaw),
    analyzed_reviews_mixed: normalizeNumberText(analyzedMixedRaw),
    analyzed_reviews_positive: normalizeNumberText(analyzedPosRaw),
    mau: normalizeNumberText(mauRaw),
    daily_stickiness_pct: typeof stickinessRaw === "number" ? stickinessRaw : normalizePercent(stickinessRaw),
    revenue_usd: revenueNumeric,
    revenue_text: revenueText ? String(revenueText) : null,
  };
}

export function parseReviewsReplyRate(rscText) {
  const text = String(rscText || "");
  const raw =
    parseValueNearLabel(text, "Reply rate") ||
    parseValueNearLabel(text, "Reply Rate") ||
    parseFirstJsonNumberByKeys(text, ["replyRate", "reply_rate", "replyRatePct", "reply_rate_pct"]);
  return { reply_rate_pct: typeof raw === "number" ? raw : normalizePercent(raw) };
}

export function parseUsageSessions(rscText) {
  const text = String(rscText || "");

  const mauRaw = parseValueNearLabel(text, "MAU") || parseFirstJsonNumberByKeys(text, ["mau"]);
  const wauRaw = parseValueNearLabel(text, "WAU") || parseFirstJsonNumberByKeys(text, ["wau"]);
  const dauRaw = parseValueNearLabel(text, "DAU") || parseFirstJsonNumberByKeys(text, ["dau"]);
  const stickRaw = parseValueNearLabel(text, "Daily Stickiness") || parseFirstJsonNumberByKeys(text, ["dailyStickiness"]);

  const sessionsRaw =
    parseValueNearLabel(text, "Avg Sessions") ||
    parseValueNearLabel(text, "Sessions per user") ||
    parseFirstJsonNumberByKeys(text, ["avgSessionsPerUser", "avg_sessions_per_user"]);

  const avgSessionTimeText =
    parseValueNearLabel(text, "Avg Session Time") ||
    parseValueNearLabel(text, "Average Session Time") ||
    parseFirstJsonStringByKeys(text, ["avgSessionTime", "avg_session_time_text"]);

  const avgTotalTimeText =
    parseValueNearLabel(text, "Avg Total Time") ||
    parseValueNearLabel(text, "Average Total Time") ||
    parseFirstJsonStringByKeys(text, ["avgTotalTime", "avg_total_time_text"]);

  return {
    mau: normalizeNumberText(mauRaw),
    wau: normalizeNumberText(wauRaw),
    dau: normalizeNumberText(dauRaw),
    daily_stickiness_pct: typeof stickRaw === "number" ? stickRaw : normalizePercent(stickRaw),
    avg_sessions_per_user: typeof sessionsRaw === "number" ? sessionsRaw : normalizeNumberText(sessionsRaw),
    avg_session_time_sec: parseTimeToSeconds(avgSessionTimeText),
    avg_session_time_text: avgSessionTimeText ? String(avgSessionTimeText) : null,
    avg_total_time_sec: parseTimeToSeconds(avgTotalTimeText),
    avg_total_time_text: avgTotalTimeText ? String(avgTotalTimeText) : null,
  };
}

export function parseTechnographicsOverview(rscText) {
  const text = String(rscText || "");
  const raw =
    parseValueNearLabel(text, "SDKs") ||
    parseValueNearLabel(text, "Total SDKs") ||
    parseFirstJsonNumberByKeys(text, ["sdksTotal", "sdks_total", "sdksCount"]);
  return { sdks_total: normalizeNumberText(raw) };
}

export function parseTechnographicsSdks(rscText) {
  const text = String(rscText || "");

  const rows = [];
  const re = /\{[^{}]*"sdkName"\s*:\s*"([^"]+)"[^{}]*\}/g;
  let m;
  while ((m = re.exec(text))) {
    const chunkStart = Math.max(0, m.index - 200);
    const chunk = text.slice(chunkStart, Math.min(text.length, m.index + 400));
    const name = m[1];
    const category = (chunk.match(/"sdkCategory"\s*:\s*"([^"]+)"/) || [])[1] || null;
    const installedStr = (chunk.match(/"installed"\s*:\s*(true|false)/) || [])[1] || null;
    const installed = installedStr == null ? null : installedStr === "true";
    const installedDate = (chunk.match(/"installedDate"\s*:\s*"(\d{4}-\d{2}-\d{2})"/) || [])[1] || null;

    rows.push({
      sdk_name: name,
      sdk_category: category,
      installed: installed == null ? false : installed,
      installed_date: installedDate,
    });
  }

  if (rows.length) return rows;

  const re2 = /"sdk"\s*:\s*"([^"]+)"\s*,\s*"category"\s*:\s*"([^"]+)"/g;
  while ((m = re2.exec(text))) {
    rows.push({ sdk_name: m[1], sdk_category: m[2], installed: false, installed_date: null });
  }

  return rows;
}

export function parseRevenueTotal(rscText) {
  const text = String(rscText || "");
  const raw =
    parseValueNearLabel(text, "Total Revenue") ||
    parseValueNearLabel(text, "Total revenue") ||
    parseFirstJsonNumberByKeys(text, ["totalRevenue", "total_revenue_usd", "totalRevenueUsd"]);

  const asNumber = typeof raw === "number" ? raw : null;
  if (asNumber != null) return { total_revenue_usd: asNumber };

  const { usd } = normalizeCurrencyUsd(raw);
  return { total_revenue_usd: usd };
}

export function parseAudience(rscText) {
  const text = String(rscText || "");

  const maleRaw = parseValueNearLabel(text, "Male") || parseFirstJsonNumberByKeys(text, ["male", "malePct", "genderMalePct"]);
  const femaleRaw =
    parseValueNearLabel(text, "Female") || parseFirstJsonNumberByKeys(text, ["female", "femalePct", "genderFemalePct"]);

  const buckets = {
    age_18_24_pct: normalizePercent(parseValueNearLabel(text, "18-24") || parseValueNearLabel(text, "18 - 24")),
    age_25_34_pct: normalizePercent(parseValueNearLabel(text, "25-34") || parseValueNearLabel(text, "25 - 34")),
    age_35_44_pct: normalizePercent(parseValueNearLabel(text, "35-44") || parseValueNearLabel(text, "35 - 44")),
    age_45_54_pct: normalizePercent(parseValueNearLabel(text, "45-54") || parseValueNearLabel(text, "45 - 54")),
    age_55_plus_pct: normalizePercent(parseValueNearLabel(text, "55+") || parseValueNearLabel(text, "55 +")),
  };

  return {
    gender_male_pct: typeof maleRaw === "number" ? maleRaw : normalizePercent(maleRaw),
    gender_female_pct: typeof femaleRaw === "number" ? femaleRaw : normalizePercent(femaleRaw),
    ...buckets,
  };
}
