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

export async function loginAndSaveState({ headful = true, timeoutMinutes = 20, url = null, userDataDir = null, storageStatePath = null, cookiesPath = null } = {}) {
  const timeoutMs = Number(timeoutMinutes) * 60 * 1000;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new Error(`Invalid timeoutMinutes: ${timeoutMinutes}`);

  let browser = null;
  let context = null;
  try {
    browser = userDataDir ? null : await chromium.launch({ headless: !headful });
    context = userDataDir ? await chromium.launchPersistentContext(String(userDataDir), { headless: !headful }) : await browser.newContext();
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
    while (Date.now() < loginDetectDeadline) {
      const cur = page.url() || "";
      if (looksLikeLoginUrl(cur)) {
        await page.waitForTimeout(3000);
        continue;
      }
      try {
        const resp = await page.goto(authCheckUrl, { waitUntil: "domcontentloaded" });
        await page.waitForLoadState("networkidle", { timeout: 45_000 }).catch(() => {});
        await page.waitForTimeout(1000);

        const finalUrl = page.url() || "";
        const status = resp ? resp.status() : null;
        const cookieStr = await page.evaluate(() => document.cookie || "").catch(() => "");

        const urlOk = finalUrl.includes("/app-analysis/");
        const cookieOk = String(cookieStr).toLowerCase().includes("auth");

        if ((status == null || status < 400) && (urlOk || cookieOk)) {
          authed = true;
          break;
        }
      } catch {
        // ignore and retry
      }

      await page.waitForTimeout(3000);
    }

    if (!authed) {
      process.stdout.write("LOGIN NOT DETECTED – open any app-analysis page\n");
      throw new Error("Login not detected");
    }

    const outStorage = storageStatePath ? String(storageStatePath) : path.join(__dirname, "storageState.json");
    const outCookies = cookiesPath ? String(cookiesPath) : path.join(__dirname, "cookies.json");

    await context.storageState({ path: outStorage });
    const cookies = await context.cookies();
    await fs.writeFile(outCookies, JSON.stringify(cookies, null, 2) + "\n", "utf8");

    process.stdout.write("Login OK. Cookies exported.\n");
  } finally {
    if (context) await context.close().catch(() => {});
    if (browser) await browser.close().catch(() => {});
  }
}

async function main() {
  const argv = minimist(process.argv.slice(2), {
    boolean: ["headless"],
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
  });
}

const argv1 = process.argv[1];
if (argv1 && fileURLToPath(import.meta.url) === path.resolve(argv1)) {
  main().catch((err) => {
    process.stderr.write(`${err?.stack || err}\n`);
    process.exitCode = 1;
  });
}
