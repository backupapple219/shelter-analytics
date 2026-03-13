# 🐾 Shelter Analytics Pipeline

An end-to-end data engineering and analytics project analyzing animal shelter intake and outcome data — built to demonstrate a full pipeline from API ingestion through SQL storage to visualization.

## Overview

| Step | Script | Description |
|---|---|---|
| 1 | `01_ingest.py` | Pulls data from Socrata public API |
| 2 | `02_clean.py` | Cleans, normalizes, and engineers features |
| 3 | `03_load_db.py` | Loads into structured SQLite database |
| 4 | `04_analyze.py` | Runs analytical SQL queries, exports CSVs |
| 5 | `05_visualize.py` | Generates charts for dashboard |

## Dataset

Uses the **Austin Animal Center** public dataset (~170k+ records, 2013–present) via the Socrata Open Data API — the same API format used by Indianapolis Animal Care Services (IACS) and hundreds of other municipal shelter portals.

**Source:** https://data.austintexas.gov  
**Endpoints:** Austin Animal Center Intakes & Outcomes

## Project Structure

```
shelter-analytics/
├── scripts/
│   ├── 01_ingest.py
│   ├── 02_clean.py
│   ├── 03_load_db.py
│   ├── 04_analyze.py
│   └── 05_visualize.py
├── sql/
│   ├── schema.sql
│   └── queries.sql
├── data/              # gitignored — generated locally by running pipeline
├── outputs/           # gitignored — generated locally by running pipeline
├── requirements.txt
└── README.md
```

## Setup & Usage

```bash
pip install -r requirements.txt

python scripts/01_ingest.py    # ~170k records, takes 2-3 min
python scripts/02_clean.py
python scripts/03_load_db.py
python scripts/04_analyze.py
python scripts/05_visualize.py
```

Charts saved to `outputs/` as PNGs. CSVs exported to `outputs/` for Tableau / Power BI.

## Key Insights

- **COVID impact** — intake volume dropped sharply in early 2020 and has not fully recovered to pre-pandemic levels
- **No-kill status** — Austin Animal Center has maintained a live outcome rate above 90% every year since 2015
- **Breed adoption gap** — Blue Lacy and Redbone Hound have the highest adoption rates (70%+) while high-volume breeds like Pit Bull have significantly lower rates
- **Length of stay** — Abandoned and owner-surrendered animals stay ~27 days on average vs ~18 days for strays
- **Euthanasia request intakes** — dogs arrive and leave within 3 days on average, the shortest stay of any intake type

## Analytical Queries

Eight SQL queries covering:
- Monthly intake trends with rolling averages
- Outcome distribution by species
- Live outcome rate by year (no-kill threshold tracking)
- Top breeds by intake volume
- Breed-level adoption rates
- Seasonal intake patterns
- Length of stay by intake type
- Named vs unnamed animal outcomes

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.13 |
| Data wrangling | pandas, numpy |
| HTTP / API | requests |
| Database | SQLite |
| Visualization | matplotlib, seaborn |
| Dashboard | Tableau Public / Power BI |
