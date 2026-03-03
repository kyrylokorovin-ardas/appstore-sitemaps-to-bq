import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

import { loginAndSaveState } from "../similarweb_login.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function looksLikeLoginUrl(url) {
  const u = String(url || "").toLowerCase();
  return u.includes("/login") || u.includes("signin") || u.includes("sign-in") || u.includes("/auth");
}

async function fileExists(p) {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

async function checkSession({ storageStatePath, userDataDir, urlToCheck }) {
  let browser = null;
  let context = null;
  browser = userDataDir ? null : await chromium.launch({ headless: true });
  try {
    context = userDataDir ? await chromium.launchPersistentContext(userDataDir, { headless: true }) : await browser.newContext({ storageState: storageStatePath });
    context.setDefaultTimeout(45_000);

    const page = await context.newPage();
    const resp = await page.goto(urlToCheck, { waitUntil: "domcontentloaded" });
    await page.waitForLoadState("networkidle", { timeout: 20_000 }).catch(() => {});
    await page.waitForTimeout(2500);

    const finalUrl = page.url();
    const status = resp ? resp.status() : null;

    if (looksLikeLoginUrl(finalUrl)) {
      return { ok: false, reason: `redirect_to_login (${finalUrl})`, status, finalUrl };
    }

    if (status === 401 || status === 403) {
      return { ok: false, reason: `http_${status}`, status, finalUrl };
    }

    const perfVisible = await page
      .locator("text=Performance Overview")
      .first()
      .isVisible({ timeout: 15_000 })
      .catch(() => false);

    if (!perfVisible) {
      return { ok: false, reason: "performance_overview_not_visible", status, finalUrl };
    }

    return { ok: true, reason: "ok", status, finalUrl };
  } finally {
    if (context) await context.close().catch(() => {});
    if (browser) await browser.close().catch(() => {});
  }
}

export async function ensureSimilarwebAuth({
  urlToCheck,
  headfulOnRelogin = true,
  userDataDir = null,
  storageStatePath: storageStatePathOverride = null,
  cookiesPath: cookiesPathOverride = null,
} = {}) {
  if (!urlToCheck) throw new Error("ensureSimilarwebAuth: missing urlToCheck");

  const storageStatePath = storageStatePathOverride || path.join(__dirname, "..", "storageState.json");
  const cookiesPath = cookiesPathOverride || path.join(__dirname, "..", "cookies.json");

  if (!userDataDir && !(await fileExists(storageStatePath))) {
    process.stdout.write("Similarweb auth: missing storageState.json; opening login...\n");
    await loginAndSaveState({ headful: headfulOnRelogin, userDataDir, storageStatePath, cookiesPath });
    return;
  }

  const res = await checkSession({ storageStatePath, userDataDir, urlToCheck });
  if (res.ok) return;

  process.stdout.write(`Similarweb auth: session invalid (${res.reason}); opening login...\n`);
  await loginAndSaveState({ headful: headfulOnRelogin, url: urlToCheck, userDataDir, storageStatePath, cookiesPath });

  const res2 = await checkSession({ storageStatePath, userDataDir, urlToCheck });
  if (!res2.ok) {
    throw new Error(`Similarweb auth still invalid after relogin: ${res2.reason}`);
  }
}
