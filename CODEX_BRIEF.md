# CODEX BRIEF

## Project
Agency 2026 Ottawa Hackathon

## Chosen Challenge
Challenge 1: Zombie Recipients

## Objective
We want to build a winning solution for the Zombie Recipients challenge.

The solution should be an interactive Recipient Risk Intelligence Tool that helps a reviewer search for an organization and quickly assess whether that organization appears to be a high-risk recipient of public funding based on measurable signals such as public-funding dependency, filing continuity, post-funding inactivity, and comparison to peer organizations.

The solution must go beyond a generic dashboard or static report. It should feel like a practical review tool that a public-sector auditor, policy analyst, journalist, or accountability reviewer could actually use.

## Official Challenge Framing
The challenge asks participants to identify companies and nonprofits that received large amounts of public funding and then ceased operations shortly after. It specifically asks teams to identify entities that went bankrupt, dissolved, or stopped filing within 12 months of receiving funding. It also explicitly asks teams to flag entities where public funding makes up more than 70 to 80 percent of total revenue.

Our solution must stay tightly aligned with this wording.

## Non-Negotiable Product Principles
The build must follow these rules:

1. Every displayed fact must be traceable to source data or deterministic calculations.
2. The tool must not hallucinate or invent facts.
3. The tool must not accuse organizations of fraud, abuse, corruption, or misconduct unless the source data directly proves it, which we should not assume.
4. The AI layer must only summarize structured facts produced by our pipeline.
5. The solution must prioritize speed, clarity, and defensibility over complexity.
6. The final demo must feel interactive, useful, and grounded.
7. The final system should feel agentic in a controlled and practical way, not in a vague or flashy way.

## What We Want to Build
We want to build a searchable Recipient Risk Intelligence Tool with the following capabilities:

- A reviewer can search for an organization by name or identifier.
- The tool returns a grounded profile for that organization.
- The profile shows total public funding, funding history, funding by source, dependency ratios, filing continuity, and post-funding survival signals.
- The tool compares the organization against peer organizations.
- The tool surfaces discrepancy flags that help a reviewer identify why the organization may deserve closer attention.
- The tool includes an AI-assisted explanation layer that summarizes the evidence and explains why the organization appears low, medium, or high risk.
- The AI explanation must be based only on structured facts and must not make unsupported judgments.

## Available Data and Repo Context
We have the GovAlta `agency-26-hackathon` repository and want Codex to inspect it carefully.

Please inspect, prioritize, and learn from the following:

- The root README.
- KNOWN-DATA-ISSUES.md.
- index.html or any schema browser or schema documentation files.
- All files and folders related to general, cra, fed, and ab.
- Any per-entity explorer, dossier, or search-related logic already present in the repo.
- Any SQL, scripts, documentation, package files, or utilities that show how the data is structured or queried.

The repo structure matters because the data has already been modeled around four key areas:
- general, which is the canonical entity-resolution layer.
- fed, which contains federal grants and contributions.
- ab, which contains Alberta grants, contracts, sole-source contracts, and related public records.
- cra, which contains charity filings and financial information.

The general layer should be treated as the canonical identity backbone unless there is a very strong reason not to.

## Google Cloud Environment
We also have a dedicated Google Cloud project provisioned for the hackathon.

Important details:
- Our team has its own Google Cloud project named `Agency2026 - [Team Name]`.
- This project lives under the `CNU-Ottawa-Hackathon-Cloud-Sandbox-925634288385` organization.
- The hackathon datasets are already pre-loaded into BigQuery in a separate project called `Agency2026Ottawa Data`.
- We can access that pre-loaded data from our team project.
- We also have access to Gemini Cloud Assist, BigQuery, Gemini Enterprise Agent Platform, and Gemini CLI through the Google Cloud environment.
- We also have access to a Gemini API.

This means Codex should treat BigQuery as a serious primary data option, not as an afterthought.

## Resource Options We May Use
Please plan with the following available resources in mind:

- The hackathon GitHub repository.
- The pre-loaded BigQuery datasets in the `Agency2026Ottawa Data` project.
- Our team Google Cloud project for development and app hosting.
- Gemini Cloud Assist.
- Gemini Enterprise Agent Platform.
- Gemini CLI in Cloud Shell or local terminal.
- The Gemini API.
- The shared hosted PostgreSQL database if useful.

## What We Need From You
We want a complete implementation plan that tells us exactly what to build and how to build it.

Please do not give us a generic brainstorm.
Please do not give us vague options without choosing.
Please do not assume we want to rebuild the entire data platform from scratch.

We want a plan that:
- Uses the actual repo and actual hackathon resources.
- Decides whether BigQuery or shared PostgreSQL should be the primary data access layer.
- Explains the recommended architecture.
- Defines the exact variables, ratios, flags, and scoring logic we should use.
- Describes the agentic layer in a grounded way.
- Gives us the exact UI structure.
- Gives us the exact build order.
- Tells us what to cut if time gets tight.
- Tells us what the MVP must include to still feel like a winning project.

## Required Product Features
At minimum, the product should include:

1. Organization search and selection.
2. Organization overview panel.
3. Funding history and timeline.
4. Filing continuity and post-funding activity view.
5. Public-funding dependency ratio and related metrics.
6. Risk flags and transparent scoring logic.
7. Peer comparison against similar organizations.
8. AI-assisted explanation layer that is grounded in structured evidence.
9. A clean, demo-friendly interface that can be explained quickly.

## Desired Product Positioning
This should not be framed as a generic dashboard.

This should be framed as:
- A review assistant.
- A funding-risk intelligence tool.
- A discrepancy detection and comparison system.
- A faster way for a reviewer to identify whether public funding may have gone to an organization that became inactive shortly afterward.

## Accuracy Requirements
The final plan must protect against hallucination and overclaiming.

Please design the system so that:
- deterministic calculations happen first,
- structured organization profiles are produced next,
- the AI layer only reads those structured profiles,
- and the final interface clearly separates facts, calculated metrics, and AI-generated explanations.

Please include validation steps to make sure any demo case is defensible.

## Build Constraints
This is a hackathon build with limited time before code freeze.

The architecture must be realistic for a same-day implementation.
We do not want a large or fragile system.
We want the thinnest, strongest version that still looks impressive and useful.

## Preferred Output From Codex
Please return a complete technical battle plan with clear sections covering:

1. Final product definition.
2. Recommended data source strategy.
3. Exact variables, ratios, metrics, and flags.
4. Accuracy and anti-hallucination design.
5. Recommended tech stack.
6. End-to-end system architecture.
7. UI and product screen design.
8. Agentic layer design.
9. Query and data work plan.
10. Team execution plan.
11. Demo strategy.
12. Risks and mitigation.
13. Final MVP recommendation and stretch features.

## Final Instruction
We want a decisive, execution-ready plan for a solution that can realistically win the Zombie Recipients challenge while staying accurate, grounded, and aligned with the official materials.
