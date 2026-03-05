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

async function appendJsonLine(logPath, obj) {
  if (!logPath) return;
  try {
    await fs.appendFile(logPath, JSON.stringify(obj) + "\n", "utf8");
  } catch {
    // ignore
  }
}

function mustCookiesArray(value) {
  if (!Array.isArray(value)) throw new Error("cookies.json must be an array (Playwright cookies format)");
  return value;
}

function normalizeCookieDomain(domain) {
  const d = String(domain || "").trim().toLowerCase();
  if (!d) return "";
  return d.startsWith(".") ? d.slice(1) : d;
}

function domainMatches(cookieDomain, host) {
  const cd = normalizeCookieDomain(cookieDomain);
  const h = String(host || "").toLowerCase();
  if (!cd || !h) return false;
  return h === cd || h.endsWith("." + cd);
}

function pathMatches(cookiePath, reqPath) {
  const cp = String(cookiePath || "/");
  const rp = String(reqPath || "/");
  if (!cp.startsWith("/")) return rp.startsWith("/" + cp);
  return rp.startsWith(cp);
}

function cookieNotExpired(cookie, nowMs) {
  const exp = cookie && cookie.expires;
  if (typeof exp !== "number") return true; // session cookie
  if (exp <= 0) return true; // session cookie
  return nowMs < exp * 1000;
}

function cookiesToHeaderForUrl(cookies, url) {
  const u = new URL(url);
  const nowMs = Date.now();

  const selected = [];
  for (const c of cookies) {
    if (!c || typeof c.name !== "string" || typeof c.value !== "string") continue;
    if (!cookieNotExpired(c, nowMs)) continue;
    if (c.secure && u.protocol !== "https:") continue;
    if (!domainMatches(c.domain, u.hostname)) continue;
    if (!pathMatches(c.path, u.pathname)) continue;
    selected.push(c);
  }

  const header = selected.map((c) => `${c.name}=${c.value}`).join("; ");
  return { header, cookieCount: selected.length };
}

function classifyFatalAuthIssue({ status, location, text }) {
  const lower = (text || "").toLowerCase();

  if (status >= 300 && status < 400 && location && /login|signin|sign-in|auth/i.test(location)) return "login_expired";
  if (status === 401) return "login_expired";

  const blockRe = /captcha|access denied|unusual traffic|cloudflare|verify you are human|bot detection|blocked/i;
  if ((status === 403 || status === 429) && blockRe.test(lower)) return "blocked";
  if (status >= 400 && blockRe.test(lower)) return "blocked";

  if (/sign in|log in|login/.test(lower) && /password|email|continue/.test(lower)) return "login_expired";

  return null;
}

export class SimilarwebHttpClient {
  constructor({ cookiesPath, runLogPath = null, alertsSink = null } = {}) {
    this.cookiesPath = cookiesPath;
    this.runLogPath = runLogPath;
    this.cookies = null;
    this.alertsSink = alertsSink;
    this.consecutive429 = 0;
    this.consecutive403 = 0;
  }

  async loadCookies() {
    if (!this.cookiesPath) {
      const err = new Error("Missing cookiesPath for SimilarwebHttpClient");
      err.code = "SW_COOKIES_MISSING";
      throw err;
    }

    let raw;
    try {
      raw = await fs.readFile(this.cookiesPath, "utf8");
    } catch (e) {
      const err = new Error(`Missing cookies.json at ${this.cookiesPath}. Run node tools/similarweb_login.js first.`);
      err.code = "SW_COOKIES_MISSING";
      err.cause = e;
      throw err;
    }

    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (e) {
      const err = new Error(`Invalid JSON in ${this.cookiesPath}`);
      err.code = "SW_COOKIES_INVALID";
      err.cause = e;
      throw err;
    }

    const cookies = mustCookiesArray(parsed);
    if (!cookies.length) {
      const err = new Error(`cookies.json is empty at ${this.cookiesPath}. Re-run node tools/similarweb_login.js`);
      err.code = "SW_COOKIES_EMPTY";
      throw err;
    }

    this.cookies = cookies;
  }

  async fetchRscText(url, { maxAttempts = 3 } = {}) {
    if (!this.cookies) await this.loadCookies();

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

      const { header: cookieHeader, cookieCount } = cookiesToHeaderForUrl(this.cookies, fullUrl);
      if (!cookieHeader) {
        const err = new Error(
          `No applicable cookies for ${new URL(fullUrl).hostname}. Re-run node tools/similarweb_login.js to export fresh cookies.`
        );
        err.code = "SW_COOKIES_NO_MATCH";
        throw err;
      }

      await appendJsonLine(this.runLogPath, {
        event: "http_rsc_attempt",
        at: new Date().toISOString(),
        attempt,
        url: fullUrl,
        cookie_len: cookieHeader.length,
        cookie_count: cookieCount,
      });

      try {
        const res = await fetch(fullUrl, {
          method: "GET",
          redirect: "manual",
          headers: {
            accept: "text/x-component",
            rsc: "1",
            "user-agent":
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            cookie: cookieHeader,
          },
        });

        const location = res.headers.get("location") || "";
        const text = await res.text();

        await appendJsonLine(this.runLogPath, {
          event: "http_rsc_response",
          at: new Date().toISOString(),
          attempt,
          url: fullUrl,
          status: res.status,
          location: location || null,
          body_snippet: text ? text.slice(0, 600) : null,
        });

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
          err.location = location;
          err.bodySnippet = text ? text.slice(0, 800) : null;
          throw err;
        }
        if (fatal === "blocked") {
          const err = new Error("Similarweb access blocked (captcha / access denied). Stop and try again later.");
          err.code = "SW_BLOCKED";
          err.httpStatus = res.status;
          err.location = location;
          err.bodySnippet = text ? text.slice(0, 800) : null;
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
          err.location = location;
          err.bodySnippet = text ? text.slice(0, 600) : null;
          throw err;
        }

        this.consecutive429 = 0;
        this.consecutive403 = 0;
        return { url: fullUrl, status: res.status, text };
      } catch (err) {
        lastErr = err;

        await appendJsonLine(this.runLogPath, {
          event: "http_rsc_error",
          at: new Date().toISOString(),
          attempt,
          url: fullUrl,
          code: err?.code || null,
          httpStatus: err?.httpStatus || null,
          message: String(err?.message || err),
        });

        if (
          err?.code === "SW_LOGIN_EXPIRED" ||
          err?.code === "SW_BLOCKED" ||
          err?.code === "SW_TOO_MANY_429" ||
          err?.code === "SW_TOO_MANY_403" ||
          err?.code === "SW_COOKIES_MISSING" ||
          err?.code === "SW_COOKIES_EMPTY" ||
          err?.code === "SW_COOKIES_INVALID" ||
          err?.code === "SW_COOKIES_NO_MATCH"
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