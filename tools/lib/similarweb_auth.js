import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

import { loginAndSaveState } from "../similarweb_login.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

let reloginInFlight = null;

async function reloginOnce(loginArgs) {
  if (!reloginInFlight) {
    reloginInFlight = (async () => {
      await loginAndSaveState(loginArgs);
    })().finally(() => {
      reloginInFlight = null;
    });
  }
  await reloginInFlight;
}

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

async function checkSession({ storageStatePath, userDataDir, urlToCheck, headful = false }) {
  let browser = null;
  let context = null;
  browser = userDataDir ? null : await chromium.launch({ headless: !headful, channel: "chrome" }).catch(() => chromium.launch({ headless: !headful }));
  try {
    context = userDataDir ? await chromium.launchPersistentContext(userDataDir, { headless: !headful, channel: "chrome" }).catch(() => chromium.launchPersistentContext(userDataDir, { headless: !headful })) : await browser.newContext({ storageState: storageStatePath });
    context.setDefaultTimeout(45_000);

    const page = await context.newPage();
    let resp = null;
    try {
      resp = await page.goto(urlToCheck, { waitUntil: "domcontentloaded" });
      await page.waitForLoadState("networkidle", { timeout: 20_000 }).catch(() => {});
      await page.waitForTimeout(2500).catch(() => {});
    } catch (err) {
      const msg = String(err?.message || err || "");
      if (/target page, context or browser has been closed|target closed/i.test(msg)) {
        return { ok: false, reason: "target_closed", status: null, finalUrl: null };
      }
      throw err;
    }

    const finalUrl = page.url();
    const status = resp ? resp.status() : null;

    if (looksLikeLoginUrl(finalUrl)) {
      return { ok: false, reason: `redirect_to_login (${finalUrl})`, status, finalUrl };
    }

    if (status === 401 || status === 403) {
      return { ok: false, reason: `http_${status}`, status, finalUrl };
    }

    const cookieStr = await page.evaluate(() => document.cookie || "").catch(() => "");
    const urlOk = finalUrl.includes("/app-analysis/");
    const cookieOk = String(cookieStr).toLowerCase().includes("auth");

    if (urlOk || cookieOk) {
      return { ok: true, reason: "ok", status, finalUrl };
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
  checkHeadful = false,
  userDataDir = null,
  storageStatePath: storageStatePathOverride = null,
  cookiesPath: cookiesPathOverride = null,
} = {}) {
  if (!urlToCheck) throw new Error("ensureSimilarwebAuth: missing urlToCheck");

  const storageStatePath = storageStatePathOverride || path.join(__dirname, "..", "storageState.json");
  const cookiesPath = cookiesPathOverride || path.join(__dirname, "..", "cookies.json");
  if (!userDataDir && !(await fileExists(storageStatePath))) {
    process.stdout.write("Similarweb auth: missing storageState.json; opening login...");
    await reloginOnce({ headful: headfulOnRelogin, url: urlToCheck, userDataDir, storageStatePath, cookiesPath });
    const res0 = await checkSession({ storageStatePath, userDataDir, urlToCheck, headful: checkHeadful });
    if (!res0.ok) throw new Error("Similarweb auth still invalid after login: " + res0.reason);
    return;
  }

  const res = await checkSession({ storageStatePath, userDataDir, urlToCheck, headful: checkHeadful });
  if (res.ok) return;

  process.stdout.write(`Similarweb auth: session invalid (${res.reason}); opening login...`);
  if (!reloginInFlight) {    reloginInFlight = (async () => {      await loginAndSaveState({ headful: headfulOnRelogin, url: urlToCheck, userDataDir, storageStatePath, cookiesPath });    })().finally(() => {      reloginInFlight = null;    });  }  await reloginInFlight;

  const res2 = await checkSession({ storageStatePath, userDataDir, urlToCheck, headful: checkHeadful });
  if (!res2.ok) {
    throw new Error(`Similarweb auth still invalid after relogin: ${res2.reason}`);
  }
}
