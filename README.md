## Similarweb scraper (no API): Playwright login once + HTTP RSC bulk

This repo contains a Similarweb scraper that:

- Uses **Playwright only once** to log in and export a session (`tools/cookies.json`, `tools/storageState.json`).
- Scrapes Similarweb internal pages via **HTTP** using React Server Components (RSC):
  - `accept: text/x-component`
  - `rsc: 1`
  - query `&_rsc=<random>`
  - cookies from the exported session

### Install

```bash
npm install
npx playwright install chromium
```

### Login (bootstrap session)

```bash
node tools/similarweb_login.js
```

Outputs (gitignored):

- `tools/cookies.json`
- `tools/storageState.json`

If scraping fails with `SW_LOGIN_EXPIRED`, run login again.
Auth expiry behavior:

- The scraper (`tools/similarweb_scrape.js`) automatically validates your session at startup.
- If the session expires (startup or mid-run), it opens a **headful** Playwright login window, waits for you to log in (CAPTCHA/MFA supported), refreshes `tools/storageState.json` + `tools/cookies.json`, then retries the current app once and continues.

Manual login (any time):

```bash
node tools/similarweb_login.js
```

### Run (backfill)

Defaults:

- `--mode=backfill`
- `--limit=150`
- `--country=999`
- `--month`: previous full month (UTC) (e.g. on 2026-02-27 it defaults to `2026-01`)

```bash
node tools/similarweb_scrape.js --mode=backfill --limit=150 --country=999 --month=2026-01
```

### Run (weekly)

Weekly mode targets new app_ids from `app_urls_raw` that are not yet in `similarweb_app_map`.

```bash
node tools/similarweb_scrape.js --mode=weekly --limit=150 --country=999
```

Task Scheduler wrapper:

```bat
tools\run_similarweb_weekly.bat
```

### Dry run (selection only)

```bash
node tools/similarweb_scrape.js --dry_run --mode=backfill --limit=150 --country=999 --month=2026-01
```

### App selection logic

Primary selection source: `esoteric-parsec-147012.appstore_eu.app_metadata_by_country`.

Filters:

- `current_version_release_date` must be non-null
- exclude apps where `current_version_release_date` is older than **365 days** from *today (UTC)*

Priority tiers:

- Tier A: release date within last **30 days** (inclusive)
- Tier B: release date within **31–365 days**

Ordering within each tier:

- `MAX(user_rating_count)` desc per `app_id`

### Tables (BigQuery)

Dataset: `esoteric-parsec-147012.appstore_eu`

- Mapping:
  - `similarweb_app_map` (app_id -> google_package)
- Alerts:
  - `similarweb_alerts`

Apple tables:

- `similarweb_appstore_overview`
- `similarweb_appstore_reviews`
- `similarweb_appstore_usage_sessions`
- `similarweb_appstore_technographics_overview`
- `similarweb_appstore_technographics_sdks`
- `similarweb_appstore_revenue`
- `similarweb_appstore_audience`

Google Play tables:

- `similarweb_googleplay_overview`
- `similarweb_googleplay_reviews`
- `similarweb_googleplay_usage_sessions`
- `similarweb_googleplay_technographics_overview`
- `similarweb_googleplay_technographics_sdks`
- `similarweb_googleplay_revenue`
- `similarweb_googleplay_audience`

### Logs + resume

Local logs:

- `logs/similarweb_run_<YYYY-MM-DD>.log`
- `logs/similarweb_alerts_<YYYY-MM-DD>.log`

Resume state:

- `tools/state_similarweb.json`

### Windows Task Scheduler (weekly)

1. Open **Task Scheduler** → **Create Task...**
2. **Triggers** → **New...** → set Weekly day/time
3. **Actions** → **New...**
   - Program/script: `cmd.exe`
   - Add arguments: `/c tools\run_similarweb_weekly.bat`
   - Start in: repo root

### Verification queries

Replace the month as needed:

```sql
-- Overview rows
SELECT month, COUNT(*) AS rows, COUNT(DISTINCT app_id) AS apps
FROM `esoteric-parsec-147012.appstore_eu.similarweb_appstore_overview`
WHERE month = DATE('2026-01-01') AND country = 999
GROUP BY month;

-- Reviews sample
SELECT month, app_id, reply_rate_pct, pulled_at
FROM `esoteric-parsec-147012.appstore_eu.similarweb_appstore_reviews`
WHERE month = DATE('2026-01-01') AND country = 999
ORDER BY pulled_at DESC
LIMIT 20;

-- Usage sample
SELECT month, app_id, mau, dau, daily_stickiness_pct, avg_sessions_per_user, pulled_at
FROM `esoteric-parsec-147012.appstore_eu.similarweb_appstore_usage_sessions`
WHERE month = DATE('2026-01-01') AND country = 999
ORDER BY pulled_at DESC
LIMIT 20;

-- Technographics SDKs sample
SELECT month, app_id, sdk_name, sdk_category, installed, installed_date, pulled_at
FROM `esoteric-parsec-147012.appstore_eu.similarweb_appstore_technographics_sdks`
WHERE month = DATE('2026-01-01') AND country = 999
ORDER BY pulled_at DESC
LIMIT 50;

-- Revenue sample
SELECT month, app_id, total_revenue_usd, pulled_at
FROM `esoteric-parsec-147012.appstore_eu.similarweb_appstore_revenue`
WHERE month = DATE('2026-01-01') AND country = 999
ORDER BY pulled_at DESC
LIMIT 20;

-- Audience sample
SELECT month, app_id, gender_male_pct, gender_female_pct, age_25_34_pct, pulled_at
FROM `esoteric-parsec-147012.appstore_eu.similarweb_appstore_audience`
WHERE month = DATE('2026-01-01') AND country = 999
ORDER BY pulled_at DESC
LIMIT 20;

-- Alerts last 7 days
SELECT *
FROM `esoteric-parsec-147012.appstore_eu.similarweb_alerts`
WHERE pulled_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY pulled_at DESC
LIMIT 200;
```

