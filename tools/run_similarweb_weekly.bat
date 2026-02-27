@echo off
setlocal
cd /d %~dp0\..
node tools\similarweb_scrape.js --mode=weekly --limit=150
endlocal
