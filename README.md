# Semiconductor job bot

**Repository:** [github.com/Shivanesh13/Semiconductor_Jobs_Bot](https://github.com/Shivanesh13/Semiconductor_Jobs_Bot)

Python CLI that aggregates **hardware / silicon design, verification, and performance-modeling** style roles from public career sources (Workday, Greenhouse, Lever, JobSpy/Indeed mirrors, Ashby, etc.), scores them against configurable keywords, stores state in **SQLite**, and writes **Markdown** + **Excel** outputs.

## Features

- **Multi-ATS fetch** via `companies.yaml` (board URLs, tenants, JobSpy queries).
- **Keyword scoring** with `keywords.yaml` (design / verification / performance tracks, US-only option, title excludes).
- **SQLite** (`jobs.db`) for dedupe, applied/not-applied, and relevance metadata.
- **Daily Markdown logs** under `daily_logs/` for newly seen jobs.
- **`jobs_tracker.xlsx`** — two sheets (co-op/intern vs full time), sorted by recency and applied state.
- **Optional macOS notifications** when new jobs appear (`--notify`).
- **Daemon mode** for periodic scans (`daemon --interval …`).

## Quick start

**Python 3.10+** required.

```bash
./install.sh
source .venv/bin/activate
python bot.py scan
```

Full command reference, notifications, and troubleshooting: **[HOW_TO_RUN.md](HOW_TO_RUN.md)**.

```bash
python bot.py export-excel   # refresh jobs_tracker.xlsx from jobs.db
```

## Configuration

| File | Purpose |
|------|---------|
| `companies.yaml` | Employers and ATS endpoints |
| `keywords.yaml` | Match phrases, `min_score`, filters |

## Legal / fair use

This tool is intended for **personal job search** and learning. Career sites and aggregators have **terms of use** and rate limits. You are responsible for compliant, respectful use (reasonable scan intervals, no credential sharing, etc.). The authors are not responsible for misuse.

## License

[MIT](LICENSE)
# Semiconductor_Jobs_Bot
# Semiconductor_Jobs_Bot
