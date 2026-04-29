# Recipient Risk Intelligence Tool Plan

## 1. End Product

Build a searchable review assistant for Challenge 1: Zombie Recipients.

The reviewer searches for an organization by name or business number. The tool returns a grounded case profile showing public funding, filing continuity, post-funding activity signals, public-funding dependency, peer comparisons, transparent risk flags, and a safe AI-generated reviewer summary.

The product must feel like a practical public-sector review tool, not a generic dashboard and not an accusation engine.

Primary question:

> Did this organization receive significant public funding, and do structured records suggest it stopped filing, became inactive, dissolved, or became highly dependent on public funding shortly afterward?

## 2. Recommended Data Path

Use the existing PostgreSQL-backed repository as the primary MVP data path.

Reasons:

- The repo already has a mature canonical identity layer in `general`.
- The existing Dossier Explorer already supports search and per-entity profiles.
- `general.entity_golden_records`, `general.entities`, and `general.entity_source_links` already connect CRA, FED, and AB data.
- The same-day build should adapt existing logic instead of rebuilding entity resolution.

Use BigQuery as a serious secondary option:

- Validate heavy aggregate queries if the team confirms table names and permissions.
- Use BigQuery later for cloud hosting, precomputed review queues, or large-scale scans.
- Do not make BigQuery the MVP blocker unless it already contains equivalent `general` tables.

## 3. Core Product Screens

### A. Review Queue

The first screen should show a ranked list of organizations that deserve review.

Columns:

- Organization name
- BN root, if available
- Entity type
- Dataset coverage
- Total external public funding
- Largest single funding event
- Last funding date
- Last CRA filing year, if available
- Public-funding dependency ratio, if available
- Top risk flag
- Review priority badge

Purpose:

This makes the tool feel proactive and agentic. Judges can immediately see that the system surfaces cases, not just charts.

### B. Organization Search

Search by:

- canonical name
- alias
- business number

Reuse the existing search logic in `general/visualizations/server.js`.

Search results should show:

- canonical name
- BN root
- source coverage
- alias count
- source link count
- quick review priority badge

### C. Case Profile

After selecting an organization, show tabs:

1. Risk Review
2. After Funding Timeline
3. Funding
4. Filing & Activity
5. Dependency
6. Peers
7. Evidence

## 4. Signature Standout Features

### Feature 1: After Funding Timeline

This should be the main visual.

Show one horizontal timeline with:

- federal funding events
- Alberta grant events
- Alberta contract events
- Alberta sole-source events
- CRA filing years
- AB non-profit status, if available
- last known funding date
- last known filing date
- 12-month review window after the largest or latest major funding event
- missing filing years

Why it matters:

The official challenge asks for entities that received funding and then stopped operating, dissolved, went bankrupt, or stopped filing within 12 months. A timeline makes that pattern instantly visible.

### Feature 2: Evidence-Backed Review Priority Badge

Use review priority language, not accusation language.

Bands:

- Low Review Priority
- Medium Review Priority
- High Review Priority

Under the badge, show the exact evidence:

- Received `$X` in public funding
- Largest funding event was `$Y` on `date`
- Government funding was `Z%` of reported revenue
- Last CRA filing found was `year`
- No later CRA filing found in available data
- AB registry status is `status`, if available
- Similar organizations had different filing/activity patterns

### Feature 3: Public Funding Dependency Meter

Show a visual threshold meter for government funding share of revenue.

Thresholds:

- `< 50%`: lower dependency
- `50% to 69.9%`: watch
- `70% to 79.9%`: challenge threshold exceeded
- `>= 80%`: high dependency threshold exceeded

Metric:

Use `cra.govt_funding_by_charity.govt_share_of_rev`.

Display:

- government funding amount
- total revenue
- ratio
- fiscal year
- source table

### Feature 4: Funding Source Breakdown

Show a stacked chart or compact source cards:

- Federal grants and contributions
- Alberta grants
- Alberta contracts
- Alberta sole-source contracts
- CRA-reported government revenue, when available

Important:

Do not combine CRA total revenue with external grant totals as if they are additive. CRA revenue can already include government funding. Keep external funding and CRA self-reported revenue conceptually separate.

### Feature 5: Peer Comparison

Compare each organization to similar peers.

Peer grouping logic:

- same `entity_type`, if available
- same CRA designation/category, if CRA-linked
- same province, if available
- similar funding scale band
- same dataset coverage where possible

Peer metrics:

- funding percentile
- government-dependency percentile
- filing-continuity percentile
- largest-single-payment percentile
- funder-concentration percentile

Plain-English examples:

- “This organization is in the 94th percentile for government-funding dependency among similar organizations.”
- “Comparable organizations filed in 4 of 5 available years; this organization filed in 2 of 5.”

### Feature 6: Evidence Drawer

Every flag should have a “show evidence” drawer.

Example:

Flag: `NO_FILING_AFTER_FUNDING_12M`

Evidence:

- Last major funding event: `$750,000`
- Funding source: `Federal grants and contributions`
- Funding date: `2022-06-14`
- Last CRA filing found: `2022`
- Later CRA filing found: `No`
- Data checked: `cra.cra_financial_details`, `cra.cra_identification`

### Feature 7: Grounded AI Reviewer Summary

The AI should not calculate risk or create new facts.

The deterministic pipeline creates a structured `risk_profile` JSON first. The AI only summarizes that JSON.

AI output:

- short summary
- top evidence points
- reviewer questions
- limitations

Forbidden AI output:

- fraud claims
- misconduct claims
- bankruptcy/dissolution claims unless directly present in source data
- facts not in the structured profile

### Feature 8: Reviewer Questions

Generate practical next-step questions from structured evidence.

Examples:

- “Was the organization still operating after the final public funding event?”
- “Was the funding reported as government revenue in the CRA filing?”
- “Was there a registry status change after the final payment?”
- “Was funding concentrated in one department or program?”
- “Does the entity have aliases or related entities that should be reviewed together?”

### Feature 9: Data Quality Warnings

Show a data-quality panel to build trust.

Warnings:

- Missing or malformed business number
- Federal amendment rows require current-agreement logic
- Entity match depends on alias/entity resolution
- CRA filing has arithmetic impossibility or plausibility flag
- AB status is present but status-date semantics are limited
- CRA dependency ratio unavailable because organization is not CRA-linked

### Feature 10: Case File Export

Export a case summary as JSON or Markdown.

Include:

- identity
- aliases
- source coverage
- funding summary
- dependency summary
- timeline events
- flags
- score
- AI summary
- reviewer questions
- limitations
- source trace

## 5. Exact Metrics And Flags

### Funding Metrics

- `fed_current_commitment_total`
- `fed_original_commitment_total`
- `ab_grants_total`
- `ab_contracts_total`
- `ab_sole_source_total`
- `total_external_public_funding`
- `largest_public_funding_event_amount`
- `largest_public_funding_event_date`
- `last_public_funding_date`
- `funding_event_count`
- `funder_count`
- `top_funder_share`

Important:

For FED totals, use `fed.vw_agreement_current` or equivalent current-agreement logic. Do not naively sum all rows in `fed.grants_contributions`.

### Filing And Activity Metrics

- `cra_filing_years`
- `cra_filing_count`
- `cra_first_filing_year`
- `cra_last_filing_year`
- `cra_missing_years_after_first_filing`
- `months_between_last_major_funding_and_next_filing`
- `has_filing_within_12_months_after_major_funding`
- `ab_non_profit_status`
- `ab_non_profit_registration_date`

### Dependency Metrics

- `cra_total_revenue`
- `cra_total_government_funding`
- `cra_federal_government_funding`
- `cra_provincial_government_funding`
- `cra_municipal_government_funding`
- `public_funding_dependency_ratio`
- `max_dependency_ratio`
- `dependency_years_over_70`
- `dependency_years_over_80`

### Peer Metrics

- `peer_group_size`
- `funding_percentile`
- `dependency_percentile`
- `filing_continuity_percentile`
- `largest_event_percentile`
- `top_funder_concentration_percentile`

### Risk Flags

- `PUBLIC_DEPENDENCY_70`
- `PUBLIC_DEPENDENCY_80`
- `NO_FILING_AFTER_FUNDING_12M`
- `FILING_GAP_AFTER_FUNDING`
- `AB_DISSOLVED_OR_STRUCK_AFTER_FUNDING`
- `POST_FUNDING_NO_ACTIVITY`
- `LARGE_RECENT_FUNDING`
- `FEW_GRANTS_HIGH_VALUE`
- `SINGLE_FUNDER_CONCENTRATION`
- `FED_CRA_REPORTED_GAP`
- `MISSING_OR_MALFORMED_BN`
- `ENTITY_MATCH_REVIEW_NEEDED`
- `SOURCE_DATA_QUALITY_CAUTION`

## 6. Transparent Score

Total score: 100 points.

Breakdown:

- 30 points: post-funding inactivity and filing continuity
- 25 points: public-funding dependency
- 20 points: funding scale and concentration
- 15 points: identity continuity and source coverage
- 10 points: data-quality cautions

Bands:

- `0-29`: Low Review Priority
- `30-59`: Medium Review Priority
- `60-100`: High Review Priority

Every point must be traceable to a displayed metric or flag.

## 7. Backend Implementation Plan

Adapt `general/visualizations/server.js`.

Add endpoints:

- `GET /api/review-queue`
- `GET /api/entity/:id/risk-profile`
- `POST /api/entity/:id/risk-explanation`
- `GET /api/entity/:id/case-file`

Core functions:

- `getEntityIdentity(entityId)`
- `getFundingEvents(entityId)`
- `getCraContinuity(bnRoot)`
- `getDependencyMetrics(bnRoot)`
- `getAbLifecycle(entityId)`
- `getPeerComparison(entityId, metrics)`
- `computeRiskFlags(profile)`
- `computeRiskScore(flags, metrics)`
- `buildSourceTrace(profile)`
- `buildRiskProfile(entityId)`

The `risk-profile` endpoint should be deterministic and work without AI.

## 8. Frontend Implementation Plan

Adapt `general/visualizations/dossier.html`.

Add:

- Review Queue first view
- Review Priority badge
- After Funding Timeline
- Dependency Meter
- Funding Source Breakdown
- Peer Comparison panel
- Evidence drawers for every flag
- Data Quality Warnings panel
- AI Reviewer Summary panel
- Case File export button

Keep the interface dense, practical, and reviewer-oriented.

Avoid a landing page. The first screen should be the usable review queue/search interface.

## 9. AI Safety Design

The AI prompt receives only structured JSON.

Prompt rules:

- Use only the provided structured evidence.
- If evidence is missing, say it is missing.
- Do not infer fraud, misconduct, corruption, bankruptcy, or dissolution unless explicitly supported by source fields.
- Use “review priority” language.
- Distinguish facts, calculations, and limitations.

Expected response schema:

```json
{
  "summary": "...",
  "key_evidence": ["..."],
  "review_questions": ["..."],
  "limitations": ["..."]
}
```

## 10. Build Order

### Phase 1: Deterministic Profile

- Implement `risk-profile`.
- Compute funding totals.
- Compute filing continuity.
- Compute dependency ratios.
- Compute risk flags and score.

### Phase 2: Compelling UI

- Add Review Queue.
- Add After Funding Timeline.
- Add Dependency Meter.
- Add evidence drawers.

### Phase 3: Peer Context

- Add peer grouping.
- Add percentiles.
- Add comparison text.

### Phase 4: Grounded AI

- Add AI reviewer summary.
- Add reviewer questions.
- Add limitations.

### Phase 5: Demo Polish

- Pick 3-5 defensible demo entities.
- Validate each with raw SQL.
- Add case file export.
- Tighten wording.

## 11. What To Cut If Time Gets Tight

Keep:

- Review Queue
- Search
- Risk profile
- Funding timeline
- Dependency meter
- Evidence-backed flags
- AI summary

Cut first:

- BigQuery adapter
- advanced peer grouping
- Cloud deployment
- complex chart interactions
- markdown export polish
- broad leaderboard filters

## 12. Winning MVP

The winning MVP is:

> A searchable Recipient Risk Intelligence Tool that ranks organizations for review, shows an after-funding timeline, calculates public-funding dependency against the 70% and 80% challenge thresholds, flags filing/activity gaps, compares the organization to peers, and produces a grounded AI reviewer summary based only on structured evidence.

This is the thinnest version that still feels like a real public-sector review system.
