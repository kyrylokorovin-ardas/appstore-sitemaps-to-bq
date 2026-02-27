import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import minimist from "minimist";
import { chromium } from "playwright";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function main() {
  const argv = minimist(process.argv.slice(2), {
    string: ["url"],
    default: { timeout_minutes: 20 },
  });

  const timeoutMinutes = Number(argv.timeout_minutes ?? 20);
  if (!Number.isFinite(timeoutMinutes) || timeoutMinutes <= 0) {
    throw new Error(`Invalid --timeout_minutes: ${argv.timeout_minutes}`);
  }

  const browser = await chromium.launch({ headless: false });
  const context = await browser.newContext();
  const page = await context.newPage();

  process.stdout.write(
    [
      "Similarweb login bootstrap",
      "- Browser will open to https://apps.similarweb.com/",
      "- Log in manually",
      "- After login, navigate to any Similarweb app page (e.g. /app-analysis/overview/...)",
      "- This script will auto-export cookies once it detects you're logged in",
      "",
    ].join("\n") + "\n"
  );

  const startUrl = argv.url ? String(argv.url) : "https://apps.similarweb.com/";
  await page.goto(startUrl, { waitUntil: "domcontentloaded" });

  const deadlineMs = Date.now() + timeoutMinutes * 60 * 1000;
  while (Date.now() < deadlineMs) {
    const url = page.url() || "";
    const looksLikeSimilarweb = url.startsWith("https://apps.similarweb.com/");
    const looksLikeLogin = /\/login\b|signin|sign-in|auth/i.test(url);

    if (looksLikeSimilarweb && !looksLikeLogin) {
      const cookies = await context.cookies();
      if (cookies.length > 0) break;
    }

    await page.waitForTimeout(1000);
  }

  await context.storageState({ path: path.join(__dirname, "storageState.json") });
  const cookies = await context.cookies();
  await fs.writeFile(path.join(__dirname, "cookies.json"), JSON.stringify(cookies, null, 2) + "\n", "utf8");

  await browser.close();
  process.stdout.write("Login OK. Cookies exported.\n");
}

main().catch((err) => {
  process.stderr.write(`${err?.stack || err}\n`);
  process.exitCode = 1;
});
