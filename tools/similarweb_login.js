import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import minimist from "minimist";
import { chromium } from "playwright";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function looksLikeLoginUrl(url) {
  const u = String(url || "").toLowerCase();
  return u.includes("/login") || u.includes("signin") || u.includes("sign-in") || u.includes("/auth");
}

export async function loginAndSaveState({ headful = true, timeoutMinutes = 20, url = null, userDataDir = null, storageStatePath = null, cookiesPath = null, verifyHttp = false } = {}) {
  const timeoutMs = Number(timeoutMinutes) * 60 * 1000;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new Error(`Invalid timeoutMinutes: ${timeoutMinutes}`);

  let browser = null;
  let context = null;
  try {
    browser = userDataDir ? null : await chromium.launch({ headless: !headful, channel: "chrome" }).catch(() => chromium.launch({ headless: !headful }));
    context = userDataDir ? await chromium.launchPersistentContext(String(userDataDir), { headless: !headful, channel: "chrome" }).catch(() => chromium.launchPersistentContext(String(userDataDir), { headless: !headful })) : await browser.newContext();
    const page = await context.newPage();

    process.stdout.write(
      [
        "Similarweb login bootstrap",
        "- Browser will open to https://apps.similarweb.com/",
        "- Log in manually (CAPTCHA/MFA supported)",
        "- Navigate to any Similarweb app page (e.g. /app-analysis/overview/...)",
        "- This script will auto-export cookies once it detects you're logged in",
        "",
      ].join("\n") + "\n"
    );

    const startUrl = url ? String(url) : "https://apps.similarweb.com/";
    await page.goto(startUrl, { waitUntil: "domcontentloaded" });

    const loginDetectDeadline = Date.now() + 90_000;
    const authCheckUrl = "https://apps.similarweb.com/app-analysis/overview/apple/835599320";
    let authed = false;
    let lastNavAt = 0;
    while (Date.now() < loginDetectDeadline) {
      const cur = page.url() || "";

      // If the user already reached an app-analysis page, consider login detected.
      if (cur.includes("/app-analysis/") && !looksLikeLoginUrl(cur)) {
        authed = true;
        break;
      }

      const low = cur.toLowerCase();

      // Do not interrupt the user while they are in login / MFA / password reset flows.
      const mfaInputVisible = await page
        .locator("input[autocomplete=one-time-code], input[type=tel], input[name*=code], input[id*=code]")
        .first()
        .isVisible({ timeout: 500 })
        .catch(() => false);

      const activeIsInput = await page
        .evaluate(() => {
          const el = document && document.activeElement ? document.activeElement : null;
          const tag = el && el.tagName ? String(el.tagName).toUpperCase() : "";
          return tag === "INPUT" || tag === "TEXTAREA";
        })
        .catch(() => false);

      const inAuthFlow =
        looksLikeLoginUrl(cur) ||
        low.includes("password") ||
        low.includes("reset") ||
        low.includes("new-password") ||
        low.includes("change-password") ||
        low.includes("mfa") ||
        low.includes("2fa") ||
        low.includes("twofactor") ||
        low.includes("two-factor") ||
        low.includes("otp") ||
        low.includes("verification") ||
        low.includes("verify") ||
        low.includes("challenge") ||
        mfaInputVisible ||
        activeIsInput;

      if (inAuthFlow) {
        await page.waitForTimeout(5000);
        continue;
      }

      // Avoid hammering Similarweb (can cause 403). Only navigate to the check URL every ~15s.
      if (Date.now() - lastNavAt < 15_000) {
        await page.waitForTimeout(3000);
        continue;
      }
      lastNavAt = Date.now();

      try {
        const resp = await page.goto(authCheckUrl, { waitUntil: "domcontentloaded" });
        await page.waitForLoadState("networkidle", { timeout: 45_000 }).catch(() => {});
        await page.waitForTimeout(1000);

        const finalUrl = page.url() || "";
        const status = resp ? resp.status() : null;
        const urlOk = finalUrl.includes("/app-analysis/");
        const looksLikeLogin = looksLikeLoginUrl(finalUrl);

        if ((status == null || status < 400) && urlOk && !looksLikeLogin) {
          authed = true;
          break;
        }
      } catch {
        // ignore and retry
      }

      await page.waitForTimeout(3000);
    }

if (!authed) {
      process.stdout.write("LOGIN NOT DETECTED - open any app-analysis page\n");
      throw new Error("Login not detected");
    }

        // Visit secure.similarweb.com once so its cookies are captured too (if used by the session).
    try {
      await page.goto("https://secure.similarweb.com/", { waitUntil: "domcontentloaded" });
      await page.waitForLoadState("networkidle", { timeout: 20_000 }).catch(() => {});
      await page.waitForTimeout(500);
    } catch {
      // ignore
    }
const outStorage = storageStatePath ? String(storageStatePath) : path.join(__dirname, "storageState.json");
    const outCookies = cookiesPath ? String(cookiesPath) : path.join(__dirname, "cookies.json");

    await context.storageState({ path: outStorage });
    const cookies = await context.cookies();
    await fs.writeFile(outCookies, JSON.stringify(cookies, null, 2) + "\n", "utf8");

    process.stdout.write(`Login OK. Cookies exported.\n- storageState: ${outStorage}\n- cookies: ${outCookies}\n- cookies_count: ${cookies.length}\n`);

    if (verifyHttp) {
      const { SimilarwebHttpClient } = await import("./lib/httpClient.js");
      const http = new SimilarwebHttpClient({ cookiesPath: outCookies });
      const verifyUrl = "https://apps.similarweb.com/app-analysis/overview/apple/835599320?country=999&from=2026-01-01&to=2026-01-31&window=false";
      await http.fetchRscText(verifyUrl, { maxAttempts: 1 });
      process.stdout.write("HTTP OK\n");
    }
  } finally {
    if (context) await context.close().catch(() => {});
    if (browser) await browser.close().catch(() => {});
  }
}

async function main() {
  const argv = minimist(process.argv.slice(2), {
    boolean: ["headless", "verify_http"],
    string: ["url", "profile_dir"],
    default: { timeout_minutes: 20, headless: false },
  });

  const profileDirArg = argv.profile_dir != null ? String(argv.profile_dir) : null;
  const baseSessionDir = profileDirArg ? path.resolve(__dirname, "..", profileDirArg) : __dirname;
  if (profileDirArg) await fs.mkdir(baseSessionDir, { recursive: true });

  await loginAndSaveState({
    headful: !Boolean(argv.headless),
    timeoutMinutes: Number(argv.timeout_minutes ?? 20),
    url: argv.url ? String(argv.url) : null,
    userDataDir: profileDirArg ? baseSessionDir : null,
    storageStatePath: path.join(baseSessionDir, "storageState.json"),
    cookiesPath: path.join(baseSessionDir, "cookies.json"),
    verifyHttp: Boolean(argv.verify_http),
  });
}

const argv1 = process.argv[1];
if (argv1 && fileURLToPath(import.meta.url) === path.resolve(argv1)) {
  main().catch((err) => {
    process.stderr.write(`${err?.stack || err}\n`);
    process.exitCode = 1;
  });
}
