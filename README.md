# AI For Accountability Hackathon — Recipient Risk Intelligence Tool

Built for the **AI For Accountability Hackathon** (April 29, 2026) · Challenge 1: Zombie Recipients.

---

## What this is

A searchable **Recipient Risk Intelligence Tool** that helps a reviewer identify organizations that received large amounts of public funding and may have ceased operations, stopped filing, or become inactive shortly afterward.

Every displayed fact is traceable to source data. The tool does not accuse organizations of fraud or misconduct — it surfaces structured signals and lets a reviewer decide.

**Live tool:** `http://localhost:3801` after following the Quick Start below.

---

## Quick start (under 5 minutes)

### Prerequisites

| Requirement | Notes |
|---|---|
| **Node.js 18+** | `node --version` to confirm |
| **Google Cloud SDK** | `gcloud auth login` to authenticate |
| **BigQuery access** | Team project `agency2026ot-bco-0429` |

### 1 — Clone and install

```bash
git clone <repo-url>
cd agency-26-hackathon-main/general
npm install
```

### 2 — Configure environment

Create `general/.env` with your BigQuery credentials:

```env
DATA_BACKEND=bigquery
BIGQUERY_ENABLED=1
BIGQUERY_PROJECT_ID=agency2026ot-bco-0429
BIGQUERY_DATA_PROJECT_ID=agency2026ot-data-1776775157
BIGQUERY_LOCATION=northamerica-northeast1
BIGQUERY_GENERAL_DATASET=general
BIGQUERY_CRA_DATASET=cra
BIGQUERY_FED_DATASET=fed
BIGQUERY_AB_DATASET=ab

# Optional — enables the AI explanation tab
GEMINI_API_KEY=your-gemini-api-key
```

See [`general/.env.example`](general/.env.example) for the full list of options including Postgres fallback and LLM providers.

### 3 — Authenticate to Google Cloud

```bash
gcloud auth login
```

The server calls `gcloud auth print-access-token` automatically on each request. Alternatively, set `BIGQUERY_ACCESS_TOKEN` directly in `.env` if you can't run gcloud locally.

### 4 — Start the server

```bash
cd general
npm run entities:dossier
```

Opens at **[http://localhost:3801](http://localhost:3801)**.

---

## Features

### Summary page
- **Zombie Quadrant** — scatter plot of all scored organizations. X-axis: public-funding dependency ratio. Y-axis: months from last major funding event to last observed activity. The top-right zone (high dependency, no later activity) is the "zombie zone." Click any dot to open the case.
- **Triage Agent** — click "Run triage" to have the agent automatically rank the highest-risk cases with plain-language rationales.
- **Review feed** — top 16 highest-exposure organizations sorted by risk score.

### Research + Compare page
- **Search** by organization name or Business Number (BN). Results are drawn from ~851K canonical entities resolved across CRA, federal grants, and Alberta data.
- **Overview tab** — key metrics (total funding, largest single event, max dependency ratio, last CRA filing, top funder) plus a score breakdown bar showing how many points each risk category contributed out of its maximum.
- **Timeline tab** — funding events bar chart by fiscal year, government dependency ratio trend with 70%/80% reference lines, and a filing continuity strip showing which years had CRA filings vs. gaps.
- **Flags tab** — each risk flag with its evidence, source table citation, and a steelman alternative interpretation (what an innocent explanation would look like).
- **AI Analysis tab** — click "Generate AI explanation" for a Gemini-powered summary that reads only the structured profile data. Requires `GEMINI_API_KEY`.
- **Review tab** — investigation agent trace (deterministic 6-step evidence walk), disposition recording, Section 34 attestation, and a tamper-evident audit log.
- **Peer comparison** — add up to 3 organizations to compare funding, dependency, score, and flags side by side.

### Oversight page
Filterable table of all flagged organizations that have not yet been actioned. Filter by risk level, data source, flag code, reviewer, and days since raised.

### Committee Binder
Generate a printable briefing package for any flagged organization — issue brief, risk summary, interpretations, recommended responses, and prior dispositions. Exports cleanly to PDF via browser print.

---

## Risk scoring

Scores are fully deterministic. No AI is involved in the base calculation. Five categories contribute up to 100 points total:

| Category | Max pts | Key signals |
|---|---|---|
| Post-funding inactivity + filing continuity | 30 | No CRA filing within 12 months after major funding; filing gaps |
| Public funding dependency | 25 | Gov't revenue share ≥ 70% (10 pts) or ≥ 80% (15 pts) |
| Funding scale + concentration | 20 | Single event ≥ $500K; top funder > 80% share of total |
| Identity continuity + source coverage | 15 | Entity-resolution confidence; related entity count |
| Data-quality cautions | 10 | Amendment handling; source defects affecting interpretation |

**Score bands:** High Review Priority ≥ 60 · Medium ≥ 30 · Low < 30

Every flag includes source table citations and a limitations array. The AI explanation reads only these structured outputs and cannot fabricate evidence.

---

## Data sources

| Schema | Source | Records |
|---|---|---|
| `cra` | CRA T3010 charity filings | ~8.76M rows across 49 tables |
| `fed` | Federal Grants & Contributions (Treasury Board Secretariat) | ~1.28M rows |
| `ab` | Alberta open data (grants, contracts, sole-source, non-profit registry) | ~2.61M rows |
| `general` | Cross-dataset entity resolution backbone | ~10.5M rows |

All data is open-government data redistributed under the original publishers' licences. See [`ATTRIBUTIONS.md`](ATTRIBUTIONS.md).

The `general` schema is the **canonical identity layer** — ~851K organizations resolved from ~1M source records using deterministic name matching, Splink probabilistic matching, and LLM-assisted deduplication. Every source record links back to a canonical entity via `entity_source_links`.

---

## Architecture

```
general/
├── visualizations/
│   ├── server.js                  ← Express API server (port 3801)
│   ├── dashboard.html             ← Main risk intelligence UI (Vue 3)
│   ├── dossier.html               ← Deep per-entity explorer (7 tabs)
│   ├── bigquery-risk-service.js   ← All BigQuery queries + risk logic
│   ├── bigquery-client.js         ← Lightweight BigQuery REST client (no SDK needed)
│   ├── risk-service.js            ← Postgres risk service (fallback)
│   └── workflow-store.js          ← Audit log + flag state (local JSON files)
├── lib/
│   ├── db.js                      ← Postgres connection pool + env loading
│   ├── entity-resolver.js         ← 6-layer entity matching engine
│   ├── fuzzy-match.js             ← Name normalization utilities
│   └── llm-review.js              ← 100-concurrent LLM verdict workers
└── scripts/                       ← 8-stage entity resolution pipeline
```

### How the backend is selected

The server reads `DATA_BACKEND` from your `.env` at startup:

```
DATA_BACKEND=bigquery  →  uses BigQuery (recommended for hackathon)
(unset)                →  falls back to Postgres via DB_CONNECTION_STRING
```

BigQuery auth runs `gcloud auth print-access-token` by default. Set `BIGQUERY_ACCESS_TOKEN` in `.env` to bypass gcloud entirely.

### API endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/health` | Server status and backend info |
| GET | `/api/search?q=` | Entity search by name or BN |
| GET | `/api/review-queue?limit=` | Top-scored entities for review |
| GET | `/api/discover/zombie-quadrant?limit=` | Scatter plot data |
| GET | `/api/entity/:id/risk-profile` | Full risk profile with flags and score |
| POST | `/api/entity/:id/risk-explanation` | AI summary (Gemini, requires key) |
| POST | `/api/agent/investigate/:id` | Deterministic 6-step investigation trace |
| POST | `/api/agent/triage` | Rank top cases with rationales |
| POST | `/api/agent/verify-flag/:id/:flag` | Re-verify a specific flag against source data |
| GET | `/api/compare?ids=id1,id2,id3` | Side-by-side profile comparison |
| GET | `/api/oversight/flagged-not-actioned` | Oversight report |
| GET | `/api/entity/:id/audit-trail` | Tamper-evident audit log |
| POST | `/api/entity/:id/disposition` | Record a review action |
| POST | `/api/entity/:id/attestation` | Section 34 attestation |
| GET | `/api/entity/:id/binder` | Committee briefing package |

---

## Environment variables

All variables go in `general/.env`. The full template with comments is at [`general/.env.example`](general/.env.example).

| Variable | Required | Description |
|---|---|---|
| `DATA_BACKEND` | BigQuery | Set to `bigquery` |
| `BIGQUERY_PROJECT_ID` | BigQuery | Your team's GCP billing project |
| `BIGQUERY_DATA_PROJECT_ID` | BigQuery | Project holding the pre-loaded datasets |
| `BIGQUERY_LOCATION` | BigQuery | e.g. `northamerica-northeast1` |
| `BIGQUERY_GENERAL_DATASET` | BigQuery | Dataset name for the general schema |
| `BIGQUERY_CRA_DATASET` | BigQuery | Dataset name for the CRA schema |
| `BIGQUERY_FED_DATASET` | BigQuery | Dataset name for the FED schema |
| `BIGQUERY_AB_DATASET` | BigQuery | Dataset name for the AB schema |
| `DB_CONNECTION_STRING` | Postgres | Full Postgres connection string |
| `GEMINI_API_KEY` | Optional | Enables AI explanation tab |
| `GOOGLE_API_KEY` | Optional | Alternative to `GEMINI_API_KEY` |
| `ANTHROPIC_API_KEY` | Pipeline only | LLM entity resolution (not needed for the tool) |
| `PORT` | Optional | Server port (default: `3801`) |

---

## Running with Postgres instead of BigQuery

If you have access to the shared Postgres database or a local copy via `.local-db/import.js`:

```env
# general/.env
DB_CONNECTION_STRING=postgresql://user:pass@host:5432/database?sslmode=require
# Leave DATA_BACKEND unset
```

Then start the server the same way:

```bash
npm run entities:dossier
```

The Postgres backend supports all read endpoints. Agentic investigation, triage, and flag verification also work against Postgres.

---

## Per-module data pipeline

The data platform has four modules beyond the risk tool itself. Each has its own `npm install`, `.env.example`, and README.

```bash
cd CRA && npm install   # CRA T3010 charity pipeline
cd FED && npm install   # Federal grants pipeline
cd AB  && npm install   # Alberta open data pipeline
```

Hackathon participants receive pre-populated `.env.public` files in the event-day info pack. Drop each file into the matching module directory. See [`SECURITY.md`](SECURITY.md) for the full credential convention.

---

## Accuracy and anti-hallucination design

1. **Deterministic first** — flags and scores are computed from SQL against source tables. Every flag carries explicit `source_table` and `fields` citations.
2. **Structured profile second** — the risk profile is a fixed JSON schema. The AI layer receives only this object, not raw database rows.
3. **AI last** — Gemini summarizes the structured profile with a hard system prompt: *"Do not infer fraud, abuse, corruption, misconduct, bankruptcy, or dissolution unless explicitly supported by source fields."*
4. **Limitations always visible** — every flag has a `limitations` array shown in the Flags tab. Filing gaps are labeled review signals, not proof of inactivity.
5. **No scores without evidence** — a flag is only raised when its threshold condition is met by real data. Zero-evidence flags do not appear.

---

## Known data issues

See [`KNOWN-DATA-ISSUES.md`](KNOWN-DATA-ISSUES.md) for the full catalogue (F-series = FED, C-series = CRA, A-series = AB). Each entry includes a reproducing SQL query, a row count, and mitigation status.

Key issues affecting the risk tool:

| ID | Issue | Mitigation in tool |
|---|---|---|
| F-1 | `ref_number` collisions in FED (~41K rows across multiple recipients) | Uses `vw_agreement_current` to deduplicate amendment rows |
| F-AMT | ~4.6K negative `agreement_value` rows (used as reversals) | Current-agreement logic nets these out |
| C-2024 | CRA 2024 data is partial (charities have 6 months to file) | Filing continuity limitation text shown in UI |
| A-FY | Alberta `fiscal_year` field has 118 contaminated rows | `display_fiscal_year` used throughout |

---

## Repository layout

```
agency-26-hackathon-main/
├── general/          ← Risk Intelligence Tool + entity resolution pipeline
├── CRA/              ← CRA T3010 charity data module
├── FED/              ← Federal Grants & Contributions module
├── AB/               ← Alberta open data module
├── .local-db/        ← Local Postgres recreation kit (DDL + import scripts)
├── tests/            ← Cross-module integration tests
├── index.html        ← Interactive schema and documentation browser
├── ATTRIBUTIONS.md   ← Data source and third-party library credits
├── KNOWN-DATA-ISSUES.md ← Documented source-data defects with SQL evidence
├── SECURITY.md       ← Credential conventions and data sensitivity notes
└── README.md         ← This file
```

---

## License

Source code: MIT. Data: original open-government licences — see [`ATTRIBUTIONS.md`](ATTRIBUTIONS.md). The MIT licence covers only the pipeline and tool code, not the underlying datasets.
