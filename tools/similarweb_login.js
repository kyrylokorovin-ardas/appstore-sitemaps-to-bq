import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function main() {
  const browser = await chromium.launch({ headless: false });
  const context = await browser.newContext();
  const page = await context.newPage();

  process.stdout.write(
    [
      "Similarweb login helper",
      "- A browser window will open.",
      "- Log in manually at https://apps.similarweb.com/",
      "- After you see the app dashboard, return here and press Enter.",
      "",
    ].join("\n") + "\n"
  );

  await page.goto("https://apps.similarweb.com/", { waitUntil: "domcontentloaded" });

  await new Promise((resolve) => {
    process.stdin.resume();
    process.stdin.setEncoding("utf8");
    process.stdin.once("data", () => resolve());
  });

  await context.storageState({ path: path.join(__dirname, "storageState.json") });
  const cookies = await context.cookies();
  await fs.writeFile(path.join(__dirname, "cookies.json"), JSON.stringify(cookies, null, 2) + "\n", "utf8");

  await browser.close();

  process.stdout.write(
    [
      "Saved:",
      "- tools/storageState.json",
      "- tools/cookies.json",
      "",
      "You can now run:",
      "- node tools/similarweb_scrape.js --mode=backfill",
      "",
    ].join("\n") + "\n"
  );
}

main().catch((err) => {
  process.stderr.write(`Login failed: ${err?.stack || err}\n`);
  process.exitCode = 1;
});
