function pad2(n) {
  return String(n).padStart(2, "0");
}

export function formatDateUTC(date) {
  const y = date.getUTCFullYear();
  const m = pad2(date.getUTCMonth() + 1);
  const d = pad2(date.getUTCDate());
  return `${y}-${m}-${d}`;
}

export function monthFromYyyyMm(yyyyMm) {
  const m = String(yyyyMm || "").match(/^(\\d{4})-(\\d{2})$/);
  if (!m) throw new Error(`Invalid --month; expected YYYY-MM, got: ${yyyyMm}`);
  const year = Number(m[1]);
  const monthIndex = Number(m[2]) - 1;
  return new Date(Date.UTC(year, monthIndex, 1, 0, 0, 0));
}

export function previousFullMonthUtc() {
  const now = new Date();
  const firstOfThisMonth = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  const lastOfPrevMonth = new Date(firstOfThisMonth.getTime() - 24 * 60 * 60 * 1000);
  return new Date(Date.UTC(lastOfPrevMonth.getUTCFullYear(), lastOfPrevMonth.getUTCMonth(), 1));
}

export function monthRangeUtc(monthDateUtc) {
  const from = new Date(Date.UTC(monthDateUtc.getUTCFullYear(), monthDateUtc.getUTCMonth(), 1));
  const firstOfNext = new Date(Date.UTC(monthDateUtc.getUTCFullYear(), monthDateUtc.getUTCMonth() + 1, 1));
  const to = new Date(firstOfNext.getTime() - 24 * 60 * 60 * 1000);
  return { from, to };
}

export function formatMonthYyyyMm(monthDateUtc) {
  const y = monthDateUtc.getUTCFullYear();
  const m = pad2(monthDateUtc.getUTCMonth() + 1);
  return `${y}-${m}`;
}
