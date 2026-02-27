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

export async function loginAndSaveState({ headful = true, timeoutMinutes = 20, url = null } = {}) {
  const timeoutMs = Number(timeoutMinutes) * 60 * 1000;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new Error(`Invalid timeoutMinutes: ${timeoutMinutes}`);

  const browser = await chromium.launch({ headless: !headful });
  try {
    const context = await browser.newContext();
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

    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const cur = page.url() || "";
      const looksLikeSimilarweb = cur.startsWith("https://apps.similarweb.com/");
      const looksLikeLogin = looksLikeLoginUrl(cur);

      if (looksLikeSimilarweb && !looksLikeLogin) {
        const cookies = await context.cookies();
        if (cookies.length > 0) break;
      }

      await page.waitForTimeout(1000);
    }

    await context.storageState({ path: path.join(__dirname, "storageState.json") });
    const cookies = await context.cookies();
    await fs.writeFile(path.join(__dirname, "cookies.json"), JSON.stringify(cookies, null, 2) + "\n", "utf8");

    process.stdout.write("Login OK. Cookies exported.\n");
  } finally {
    await browser.close().catch(() => {});
  }
}

async function main() {
  const argv = minimist(process.argv.slice(2), {
    boolean: ["headless"],
    string: ["url"],
    default: { timeout_minutes: 20, headless: false },
  });

  await loginAndSaveState({
    headful: !Boolean(argv.headless),
    timeoutMinutes: Number(argv.timeout_minutes ?? 20),
    url: argv.url ? String(argv.url) : null,
  });
}

if (fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  main().catch((err) => {
    process.stderr.write(`${err?.stack || err}\n`);
    process.exitCode = 1;
  });
}
