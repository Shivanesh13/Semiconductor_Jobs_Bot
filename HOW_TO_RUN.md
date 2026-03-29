# How to run the semiconductor job bot

## Prerequisites

- **Python 3.10 or newer** (required for `python-jobspy` / JobSpy).
- macOS is assumed for desktop notifications (`--notify`); scans work on other OSes without that flag.

## One-time setup

From the project root (`semiconductor-job-bot/`):

```bash
./install.sh
```

This picks a suitable `python3.10+`, creates or refreshes `.venv`, and installs everything from `requirements.txt`.

If `install.sh` fails with `env: bash\r: No such file or directory`, fix Windows line endings:

```bash
sed -i '' $'s/\r$//' install.sh
```

Manual setup (equivalent):

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Every session

```bash
cd /path/to/semiconductor-job-bot
source .venv/bin/activate
```

When the prompt shows `(.venv)`, you are in the virtual environment.

## Main commands

| Command | What it does |
|--------|----------------|
| `python bot.py` | Same as `scan` — one pass over all companies in `companies.yaml`. |
| `python bot.py scan` | Fetch jobs, score with `keywords.yaml`, update `jobs.db`, print matches. |
| `python bot.py scan --notify` | On macOS, show a notification if **new** rows appear (not every open role). |
| `python bot.py daemon` | Repeat `scan` forever (default **360** minutes between runs). Stop with **Ctrl+C**. |
| `python bot.py daemon --interval 60 --notify` | Example: scan every **60** minutes + notifications for new jobs. |
| `python bot.py list-unapplied` | Print unapplied jobs already stored in the database. |
| `python bot.py mark-applied --url "<job URL>"` | Mark one job applied (URL must match what the bot printed). |
| `python bot.py export-excel` | Regenerate `jobs_tracker.xlsx` from `jobs.db` (also runs after `scan` / `mark-applied` when possible). |

Global flags before the subcommand are treated as `scan` options, e.g. `python bot.py --notify`.

## Notifications (`--notify`)

- **Only macOS** — the bot uses AppleScript (`osascript`) to show a banner. On Linux/Windows this flag does nothing.
- **Only when something is new** — a notification fires if the scan finds at least one job that was **not** already in `jobs.db` for this run. It does **not** alert for every “OPEN” line you see in the report, only **NEW** first-seen postings.
- **How to use**
  - One-off scan with notify:  
    `python bot.py scan --notify`
  - Shorthand (same as `scan --notify`):  
    `python bot.py --notify`
  - Daemon with notify on each cycle that discovers new jobs:  
    `python bot.py daemon --notify`  
    (add `--interval` as below)
- **If you see no banner** — check **System Settings → Notifications** for Terminal, Cursor, or the app running `python`. Focus / Do Not Disturb can hide alerts. You can sanity-check with:  
  `osascript -e 'display notification "Test" with title "Job bot"'`

## Daemon interval (`--interval`)

- **`daemon` only** — controls how many **minutes** the bot waits after finishing one full scan before starting the next.
- **Default:** `360` (6 hours). Example: scan every hour → `--interval 60`; every 2 hours → `--interval 120`.
- **Examples**
  ```bash
  python bot.py daemon --interval 60
  python bot.py daemon --interval 60 --notify
  python bot.py daemon --interval 720
  ```
- **Practical note** — each run hits many career sites. Very short intervals (e.g. every few minutes) increase load and the chance of rate limits; **30–120 minutes** is a common balance if you want fresher results than the default.

## Configuration

- **`companies.yaml`** — which employers and how to reach them (Workday, Greenhouse, JobSpy, etc.).
- **`keywords.yaml`** — keyword tracks, `min_score`, US-only filter, title exclude regexes.
- **`jobs.db`** — SQLite; created/updated automatically.
- **`jobs_tracker.xlsx`** — Excel export (co-op/intern vs full time); requires `openpyxl` from `requirements.txt`.
- **`daily_logs/`** — markdown logs of **new** jobs per day (not the full terminal report).

## Help

```bash
python bot.py scan -h
python bot.py daemon -h
python bot.py mark-applied -h
```

## Leaving the virtual environment

```bash
deactivate
```
