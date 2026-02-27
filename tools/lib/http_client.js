import fs from "node:fs/promises";
import crypto from "node:crypto";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randomRscToken() {
  return crypto.randomBytes(6).toString("hex");
}

function cookiesToHeader(cookies) {
  if (!Array.isArray(cookies)) throw new Error("cookies.json must be an array (Playwright cookies format)");
  return cookies
    .filter((c) => c && typeof c.name === "string" && typeof c.value === "string")
    .map((c) => `${c.name}=${c.value}`)
    .join("; ");
}

function classifyFatalAuthIssue({ status, location, text }) {
  const lower = (text || "").toLowerCase();
  if (status >= 300 && status < 400 && location && /login|signin|sign-in|auth/i.test(location)) return "login_expired";
  if (status === 401) return "login_expired";
  if (status === 403 && /captcha|access denied|forbidden|bot|cloudflare/.test(lower)) return "blocked";
  if (/sign in|log in|login/.test(lower) && /password|email|continue/.test(lower)) return "login_expired";
  if (/captcha|access denied|forbidden|unusual traffic|cloudflare/.test(lower)) return "blocked";
  return null;
}

export class SimilarwebHttpClient {
  constructor({ cookiesPath }) {
    this.cookiesPath = cookiesPath;
    this.cookieHeader = null;
  }

  async loadCookies() {
    const raw = await fs.readFile(this.cookiesPath, "utf8");
    const cookies = JSON.parse(raw);
    this.cookieHeader = cookiesToHeader(cookies);
  }

  async fetchRscText(url, { maxAttempts = 5 } = {}) {
    if (!this.cookieHeader) await this.loadCookies();

    let attempt = 0;
    let lastErr = null;

    while (attempt < maxAttempts) {
      attempt += 1;
      const rscToken = randomRscToken();
      const fullUrl = url.includes("&_rsc=") || url.includes("?_rsc=")
        ? url
        : `${url}${url.includes("?") ? "&" : "?"}_rsc=${rscToken}`;

      try {
        const res = await fetch(fullUrl, {
          method: "GET",
          redirect: "manual",
          headers: {
            accept: "text/x-component",
            rsc: "1",
            "user-agent":
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            cookie: this.cookieHeader,
          },
        });

        const location = res.headers.get("location") || "";
        const text = await res.text();

        const fatal = classifyFatalAuthIssue({ status: res.status, location, text });
        if (fatal === "login_expired") {
          const err = new Error(
            "Similarweb session appears expired. Re-run `node tools/similarweb_login.js` to export fresh cookies."
          );
          err.code = "SW_LOGIN_EXPIRED";
          err.httpStatus = res.status;
          throw err;
        }
        if (fatal === "blocked") {
          const err = new Error("Similarweb access blocked (captcha / access denied). Stop and try again later.");
          err.code = "SW_BLOCKED";
          err.httpStatus = res.status;
          throw err;
        }

        if (res.status >= 500 || res.status === 429) {
          const backoffMs = Math.min(30_000, 500 * 2 ** (attempt - 1)) + Math.floor(Math.random() * 250);
          await sleep(backoffMs);
          continue;
        }

        if (!res.ok) {
          const err = new Error(`HTTP ${res.status} from Similarweb`);
          err.code = "SW_HTTP_ERROR";
          err.httpStatus = res.status;
          err.bodySnippet = text.slice(0, 400);
          throw err;
        }

        return { url: fullUrl, status: res.status, text };
      } catch (err) {
        lastErr = err;
        if (err?.code === "SW_LOGIN_EXPIRED" || err?.code === "SW_BLOCKED") throw err;
        const backoffMs = Math.min(30_000, 500 * 2 ** (attempt - 1)) + Math.floor(Math.random() * 250);
        await sleep(backoffMs);
      }
    }

    throw lastErr || new Error("Failed to fetch Similarweb RSC response");
  }
}
