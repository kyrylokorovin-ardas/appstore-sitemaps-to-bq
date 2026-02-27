## Similarweb scraper (hybrid: Playwright login once + HTTP RSC bulk)

This repo includes a Similarweb scraper that avoids opening a browser per app:

1. Use Playwright **once** to log in and export cookies + storage state.
2. Bulk scrape Similarweb internal routes via HTTP with **RSC** responses:
   - Header `accept: text/x-component`
   - Header `rsc: 1`
   - Query param `&_rsc=<random>`
   - Cookies from the logged-in session

### Prerequisites

- Node.js 18+
- `gcloud auth application-default login` (ADC) or another BigQuery credential source

### Install deps

```bash
npm install
npx playwright install chromium
```

### Login once (exports cookies)

```bash
node tools/similarweb_login.js
```

This writes:

- `tools/storageState.json` (Playwright storage state)
- `tools/cookies.json` (cookies used for HTTP scraping)

### Run backfill (default month: previous full month)

If `--month` is not provided, the scraper uses the **previous full month** (e.g. on 2026-02-27 it uses `2026-01`).

```bash
node tools/similarweb_scrape.js --mode=backfill --limit=150 --country=999 --month=2026-01
```

### Run weekly (new app_ids only)

Weekly mode selects app_ids that are not yet present in `similarweb_app_map` and not yet scraped for the given month.

```bash
node tools/similarweb_scrape.js --mode=weekly --limit=150
```

Windows Task Scheduler friendly wrapper:

```bat
tools\run_similarweb_weekly.bat
```

### Windows Task Scheduler (weekly)

1. Open **Task Scheduler** → **Create Task...**
2. **General**: choose a name like `similarweb-weekly`
3. **Triggers**: **New...** → set `Weekly` and pick day/time
4. **Actions**: **New...**
   - **Program/script**: `cmd.exe`
   - **Add arguments**: `/c tools\run_similarweb_weekly.bat`
   - **Start in**: (repo root folder)
5. **Conditions/Settings**: configure as desired (e.g. retry on failure)

### BigQuery destination tables

Dataset: `esoteric-parsec-147012.appstore_eu`

- Apple:
  - `similarweb_appstore_overview`
  - `similarweb_appstore_store_downloads`
  - `similarweb_appstore_revenue`
- Google Play:
  - `similarweb_googleplay_overview`
  - `similarweb_googleplay_store_downloads`
  - `similarweb_googleplay_revenue`
- Mapping: `similarweb_app_map`
- Alerts: `similarweb_alerts`

### Verification queries

Replace the month as needed:

```sql
-- Apple overview rows for a month
SELECT month, COUNT(*) AS rows, COUNT(DISTINCT app_id) AS apps
FROM `esoteric-parsec-147012.appstore_eu.similarweb_appstore_overview`
WHERE month = DATE('2026-01-01') AND country = 999
GROUP BY month;

-- Google overview sample
SELECT month, app_id, google_package, total_downloads, total_revenue, pulled_at
FROM `esoteric-parsec-147012.appstore_eu.similarweb_googleplay_overview`
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
