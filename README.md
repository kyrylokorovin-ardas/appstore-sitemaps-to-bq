## Similarweb scraper (no API): Playwright login once + HTTP RSC bulk

This repo contains a Similarweb scraper that:

- Uses Playwright only to bootstrap a logged-in session (manual login).
- Scrapes Similarweb internal pages via HTTP using React Server Components (RSC):
  - accept: text/x-component
  - rsc: 1
  - query param: &_rsc=<random>
  - cookies from the exported session

### Install

`ash
npm install
npx playwright install chromium
`

### Login (bootstrap session)

`ash
node tools/similarweb_login.js
`

Outputs (gitignored):

- tools/cookies.json
- tools/storageState.json

Auth expiry behavior:

- tools/similarweb_scrape.js validates your session at startup.
- If the session expires (startup or mid-run), it opens a headful Playwright login window, waits for you to log in (CAPTCHA/MFA supported), refreshes session files, retries the current app once, then continues.

Manual login (any time):

`ash
node tools/similarweb_login.js
`

### Run (backfill)

Tabs (controls which destination table(s) are written this run):

- --tab=overview|reviews|usage_sessions|technographics|revenue|audience|all
- Default: --tab=overview
- --tab=all runs tabs sequentially per app (no parallelism).

Single worker examples:

`ash
node tools/similarweb_scrape.js --mode=backfill --tab=overview --limit=150 --country=999 --month=2026-01
node tools/similarweb_scrape.js --mode=backfill --tab=revenue  --limit=150 --country=999 --month=2026-01
node tools/similarweb_scrape.js --mode=backfill --tab=all      --limit=150 --country=999 --month=2026-01
`

Parallel sharding (two scrapers, no overlap, separate sessions):

`ash
node tools/similarweb_scrape.js --mode=backfill --tab=overview --limit=5000 --country=999 --month=2026-01 --workers=2 --worker=0 --profile_dir=profiles/acc0
node tools/similarweb_scrape.js --mode=backfill --tab=overview --limit=5000 --country=999 --month=2026-01 --workers=2 --worker=1 --profile_dir=profiles/acc1
`

Defaults:

- --mode=backfill
- --tab=overview
- --limit=150
- --country=999
- --month: previous full month (UTC)

### Run (weekly)

`ash
node tools/similarweb_scrape.js --mode=weekly --tab=overview --limit=150 --country=999
`

Task Scheduler wrapper:

`at
tools\run_similarweb_weekly.bat
`

### Dry run (selection only)

`ash
node tools/similarweb_scrape.js --dry_run --mode=backfill --tab=overview --limit=150 --country=999 --month=2026-01
`

### App selection and skip-already-processed

Universe:

- app_ids come from esoteric-parsec-147012.appstore_eu.app_urls_raw (distinct app_id)

Prioritization metadata:

- esoteric-parsec-147012.appstore_eu.app_metadata_by_country
  - priority 1: apps that have any row with country = 'us'
  - priority 2: MAX(user_rating_count) DESC per app_id
  - tie-breaker: app_id ASC

Skip already processed (idempotent backfill):

- Selection anti-joins against the destination table(s) for the selected --tab for (month, country) on the Apple tables (google_package IS NULL).
- Within the per-app loop, each store/tab also checks existence in BigQuery and records SKIPPED_ALREADY_EXISTS.
- This makes restarts safe: if the script crashes, the next run automatically continues with remaining missing work.

### Auditability (reconcile attempted vs inserted)

The scraper prints a run_id at startup and writes one audit row per (app_id, store, tab) attempt:

- esoteric-parsec-147012.appstore_eu.similarweb_app_audit

Use this to reconcile progress:

`sql
-- Audit by status for a specific run
SELECT status, COUNT(*) AS rows
FROM esoteric-parsec-147012.appstore_eu.similarweb_app_audit
WHERE run_id = 'PASTE_RUN_ID_HERE'
GROUP BY status
ORDER BY rows DESC;
`

### Tables (BigQuery)

Dataset: esoteric-parsec-147012.appstore_eu

- Mapping:
  - similarweb_app_map (app_id -> google_package)
- Alerts:
  - similarweb_alerts
- Audit:
  - similarweb_app_audit

Apple tables:

- similarweb_appstore_overview
- similarweb_appstore_reviews
- similarweb_appstore_usage_sessions
- similarweb_appstore_technographics_overview
- similarweb_appstore_technographics_sdks
- similarweb_appstore_revenue
- similarweb_appstore_audience

Google Play tables:

- similarweb_googleplay_overview
- similarweb_googleplay_reviews
- similarweb_googleplay_usage_sessions
- similarweb_googleplay_technographics_overview
- similarweb_googleplay_technographics_sdks
- similarweb_googleplay_revenue
- similarweb_googleplay_audience

### Logs

Local logs:

- logs/similarweb_run_<YYYY-MM-DD>.log
- logs/similarweb_alerts_<YYYY-MM-DD>.log

### Windows Task Scheduler (weekly)

1. Open Task Scheduler -> Create Task...
2. Triggers -> New... -> set Weekly day/time
3. Actions -> New...
   - Program/script: cmd.exe
   - Add arguments: /c tools\run_similarweb_weekly.bat
   - Start in: repo root

### Verification queries

Replace the month as needed:

`sql
-- Overview rows
SELECT month, COUNT(*) AS rows, COUNT(DISTINCT app_id) AS apps
FROM esoteric-parsec-147012.appstore_eu.similarweb_appstore_overview
WHERE month = DATE('2026-01-01') AND country = 999
GROUP BY month;

-- Audit (last 7 days)
SELECT status, COUNT(*) AS rows
FROM esoteric-parsec-147012.appstore_eu.similarweb_app_audit
WHERE pulled_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY status
ORDER BY rows DESC;

-- Alerts last 7 days
SELECT *
FROM esoteric-parsec-147012.appstore_eu.similarweb_alerts
WHERE pulled_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY pulled_at DESC
LIMIT 200;
`