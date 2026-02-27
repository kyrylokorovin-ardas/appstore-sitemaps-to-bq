import fs from "node:fs/promises";
import crypto from "node:crypto";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function jitter(minMs, maxMs) {
  return minMs + Math.floor(Math.random() * (maxMs - minMs + 1));
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
  if (/captcha|access denied|forbidden|unusual traffic|cloudflare/.test(lower)) return "blocked";

  if (/sign in|log in|login/.test(lower) && /password|email|continue/.test(lower)) return "login_expired";

  return null;
}

export class SimilarwebHttpClient {
  constructor({ cookiesPath, alertsSink }) {
    this.cookiesPath = cookiesPath;
    this.cookieHeader = null;
    this.alertsSink = alertsSink || null;
    this.consecutive429 = 0;
    this.consecutive403 = 0;
  }

  async loadCookies() {
    const raw = await fs.readFile(this.cookiesPath, "utf8");
    const cookies = JSON.parse(raw);
    this.cookieHeader = cookiesToHeader(cookies);
  }

  async fetchRscText(url, { maxAttempts = 3 } = {}) {
    if (!this.cookieHeader) await this.loadCookies();

    let attempt = 0;
    let lastErr = null;
    const backoffs = [2000, 5000, 10000];

    while (attempt < maxAttempts) {
      attempt += 1;

      await sleep(jitter(800, 1500));

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

        if (res.status === 429) this.consecutive429 += 1;
        else this.consecutive429 = 0;

        if (res.status === 403) this.consecutive403 += 1;
        else this.consecutive403 = 0;

        if (this.consecutive429 >= 6) {
          const err = new Error("Too many 429 responses in a row; stopping to avoid a ban.");
          err.code = "SW_TOO_MANY_429";
          err.httpStatus = 429;
          throw err;
        }
        if (this.consecutive403 >= 2) {
          const err = new Error("Repeated 403 responses; stopping (captcha/access denied likely). ");
          err.code = "SW_TOO_MANY_403";
          err.httpStatus = 403;
          throw err;
        }

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

        if (res.status === 429 || res.status >= 500) {
          const backoffMs = backoffs[Math.min(attempt - 1, backoffs.length - 1)];
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

        this.consecutive429 = 0;
        this.consecutive403 = 0;
        return { url: fullUrl, status: res.status, text };
      } catch (err) {
        lastErr = err;
        if (
          err?.code === "SW_LOGIN_EXPIRED" ||
          err?.code === "SW_BLOCKED" ||
          err?.code === "SW_TOO_MANY_429" ||
          err?.code === "SW_TOO_MANY_403"
        ) {
          throw err;
        }
        const backoffMs = backoffs[Math.min(attempt - 1, backoffs.length - 1)];
        await sleep(backoffMs);
      }
    }

    throw lastErr || new Error("Failed to fetch Similarweb RSC response");
  }
}
