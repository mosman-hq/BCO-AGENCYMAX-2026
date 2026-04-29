# CODEX KICKOFF NOTE

Please inspect this repository before proposing architecture or code.

## Read these first
1. The root README.
2. KNOWN-DATA-ISSUES.md.
3. index.html or any schema browser or schema documentation files.
4. Any files related to the `general` schema or entity-resolution logic.
5. Any files related to the `cra`, `fed`, and `ab` schemas.
6. Any dossier, explorer, entity search, or per-organization profile logic already present in the repo.
7. Any setup docs showing how the shared PostgreSQL database is structured or queried.

## Important context
We have already chosen Challenge 1: Zombie Recipients.

We want to build a Recipient Risk Intelligence Tool, not a generic dashboard.

We want the tool to do the following:
- Search for an organization.
- Show funding history and total public funding.
- Show funding dependency relative to revenue.
- Show filing continuity and post-funding inactivity signals.
- Show peer comparisons.
- Show discrepancy flags.
- Show a grounded AI explanation that reads structured evidence only.

## Google Cloud context
We also have a dedicated Google Cloud project and BigQuery access.

Important details:
- The hackathon datasets are already pre-loaded into BigQuery in a separate project called `Agency2026Ottawa Data`.
- We can access that data from our team project named `Agency2026 - [Team Name]`.
- We have access to BigQuery, Gemini Cloud Assist, Gemini Enterprise Agent Platform, Gemini CLI, and a Gemini API.

Please evaluate whether BigQuery should be the primary data access path instead of the shared PostgreSQL database.

## What to optimize for
Please optimize for:
- accuracy
- speed
- traceability
- useful demo flow
- strong public-sector reviewer utility
- realistic same-day implementation

## What to avoid
Please avoid:
- vague AI concepts
- black-box risk scoring
- unsupported claims
- fraud accusations
- unnecessary infrastructure
- overbuilding

## Important constraint
Please do not start from scratch if the repo already contains useful entity or explorer logic that can be adapted.
We want the fastest path to a strong, grounded build.

## Deliverable expectation
Before suggesting code, please first produce a complete implementation plan that explains exactly what to build, what data to use, what metrics to compute, what architecture to use, and what MVP to prioritize.
