function parseFirstNumberAfterKey(text, key) {
  if (!text) return null;
  const re = new RegExp(`"${key}"\\s*:\\s*("?)(-?\\d+(?:\\.\\d+)?)\\1`);
  const m = text.match(re);
  if (!m) return null;
  const n = Number(m[2]);
  return Number.isFinite(n) ? n : null;
}

export function parseDownloadsMetrics(rscText) {
  return {
    total_downloads: parseFirstNumberAfterKey(rscText, "totalDownloads"),
  };
}

export function parseRevenueMetrics(rscText) {
  return {
    total_revenue: parseFirstNumberAfterKey(rscText, "totalRevenue"),
  };
}

export function parseOverviewMetrics(rscText) {
  return {
    total_downloads: parseFirstNumberAfterKey(rscText, "totalDownloads"),
    total_revenue: parseFirstNumberAfterKey(rscText, "totalRevenue"),
  };
}

export function extractGooglePackageFromText(rscText) {
  if (!rscText) return null;
  const m = rscText.match(/\/app-analysis\/overview\/google\/([^?"'\s]+)/i);
  if (!m) return null;
  const pkg = m[1].trim();
  if (!pkg || pkg.length > 250) return null;
  return pkg;
}
