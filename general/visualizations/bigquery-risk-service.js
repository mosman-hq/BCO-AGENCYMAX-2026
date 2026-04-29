const {
  computeRiskFlags,
  computeRiskScore,
} = require('./risk-service');
const { BigQueryClient, tableRef } = require('./bigquery-client');

const DEFAULT_LIMIT = 25;

function num(value) {
  if (value === null || value === undefined || value === '') return 0;
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function maybeNum(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function fiscalYearLabelFromDate(dateValue) {
  if (!dateValue) return null;
  const d = new Date(dateValue);
  if (Number.isNaN(d.getTime())) return null;
  const y = d.getUTCFullYear();
  const m = d.getUTCMonth() + 1;
  return m >= 4 ? `${y}-${y + 1}` : `${y - 1}-${y}`;
}

function addMonths(date, months) {
  const d = new Date(date);
  if (Number.isNaN(d.getTime())) return null;
  d.setUTCMonth(d.getUTCMonth() + months);
  return d;
}

function currentDate() {
  return process.env.RISK_PROFILE_CURRENT_DATE ? new Date(process.env.RISK_PROFILE_CURRENT_DATE) : new Date();
}

function monthsBetween(start, end) {
  const a = new Date(start);
  const b = new Date(end);
  if (Number.isNaN(a.getTime()) || Number.isNaN(b.getTime())) return null;
  return Math.max(0, (b.getUTCFullYear() - a.getUTCFullYear()) * 12 + (b.getUTCMonth() - a.getUTCMonth()));
}

function lastObservedActivityDate(profile) {
  const candidates = [];
  if (profile.filing_continuity?.cra_last_filing_year) candidates.push(`${profile.filing_continuity.cra_last_filing_year}-12-31`);
  if (profile.lifecycle?.ab_non_profit_registration_date) candidates.push(profile.lifecycle.ab_non_profit_registration_date);
  return candidates.sort().at(-1) || null;
}

function bqConfig() {
  const project = process.env.BIGQUERY_DATA_PROJECT_ID || process.env.BIGQUERY_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT || 'agency2026ot-data-1776775157';
  const general = process.env.BIGQUERY_GENERAL_DATASET || 'general';
  const cra = process.env.BIGQUERY_CRA_DATASET || 'cra';
  const fed = process.env.BIGQUERY_FED_DATASET || 'fed';
  const ab = process.env.BIGQUERY_AB_DATASET || 'ab';
  return {
    entities: tableRef('BIGQUERY_TABLE_GENERAL_ENTITIES', `${project}.${general}.entities`),
    golden: tableRef('BIGQUERY_TABLE_GENERAL_GOLDEN_RECORDS', `${project}.${general}.entity_golden_records`),
    links: tableRef('BIGQUERY_TABLE_GENERAL_SOURCE_LINKS', `${project}.${general}.entity_source_links`),
    fedGrants: tableRef('BIGQUERY_TABLE_FED_GRANTS', `${project}.${fed}.grants_contributions`),
    craIdentification: tableRef('BIGQUERY_TABLE_CRA_IDENTIFICATION', `${project}.${cra}.cra_identification`),
    craFinancialDetails: tableRef('BIGQUERY_TABLE_CRA_FINANCIAL_DETAILS', `${project}.${cra}.cra_financial_details`),
    craGovtFunding: tableRef('BIGQUERY_TABLE_CRA_GOVT_FUNDING', `${project}.${cra}.govt_funding_by_charity`),
    abGrants: tableRef('BIGQUERY_TABLE_AB_GRANTS', `${project}.${ab}.ab_grants`),
    abContracts: tableRef('BIGQUERY_TABLE_AB_CONTRACTS', `${project}.${ab}.ab_contracts`),
    abSoleSource: tableRef('BIGQUERY_TABLE_AB_SOLE_SOURCE', `${project}.${ab}.ab_sole_source`),
    abNonProfit: tableRef('BIGQUERY_TABLE_AB_NON_PROFIT', `${project}.${ab}.ab_non_profit`),
    abStatusLookup: tableRef('BIGQUERY_TABLE_AB_NON_PROFIT_STATUS_LOOKUP', `${project}.${ab}.ab_non_profit_status_lookup`),
  };
}

function sourcePkExpr(alias, key = 'id') {
  return `JSON_VALUE(${alias}.source_pk, '$.${key}')`;
}

function createBigQueryRiskService(options = {}) {
  const bq = options.client || new BigQueryClient(options);
  const t = options.tables || bqConfig();

  async function searchEntities(q, limit = 30) {
    const digits = String(q || '').replace(/\D/g, '');
    if (digits.length >= 9) {
      return bq.query(`
        SELECT id, canonical_name, CAST(bn_root AS STRING) AS bn_root, dataset_sources,
               ARRAY_LENGTH(alternate_names) AS alias_count,
               COALESCE(source_count, 0) AS link_count
        FROM ${t.entities}
        WHERE CAST(bn_root AS STRING) = SUBSTR(@bn, 1, 9)
          AND merged_into IS NULL
        LIMIT @limit
      `, { bn: digits, limit });
    }
    return bq.query(`
      SELECT id, canonical_name, CAST(bn_root AS STRING) AS bn_root, dataset_sources,
             ARRAY_LENGTH(alternate_names) AS alias_count,
             COALESCE(source_count, 0) AS link_count
      FROM ${t.entities}
      WHERE merged_into IS NULL
        AND (
          UPPER(canonical_name) LIKE CONCAT('%', UPPER(@q), '%')
          OR EXISTS (
            SELECT 1 FROM UNNEST(alternate_names) n
            WHERE UPPER(n) LIKE CONCAT('%', UPPER(@q), '%')
          )
        )
      ORDER BY IF(UPPER(canonical_name) = UPPER(@q), 0, 1), canonical_name
      LIMIT @limit
    `, { q, limit });
  }

  async function getEntityIdentity(entityId) {
    const rows = await bq.query(`
      SELECT e.id, e.canonical_name, CAST(e.bn_root AS STRING) AS bn_root, e.bn_variants, e.alternate_names,
             e.entity_type, e.dataset_sources, e.source_count, e.confidence,
             e.status, e.merged_into,
             gr.aliases, gr.related_entities, gr.merge_history,
             gr.source_link_count, gr.addresses, gr.confidence AS golden_confidence
      FROM ${t.entities} e
      LEFT JOIN ${t.golden} gr ON gr.id = e.id
      WHERE e.id = @entityId
      LIMIT 1
    `, { entityId });
    const row = rows[0];
    if (!row) return null;
    return {
      entity_id: row.id,
      canonical_name: row.canonical_name,
      bn_root: row.bn_root,
      bn_variants: row.bn_variants || [],
      aliases: row.aliases || row.alternate_names || [],
      alternate_names: row.alternate_names || [],
      entity_type: row.entity_type,
      dataset_sources: row.dataset_sources || [],
      source_link_count: row.source_link_count || row.source_count || 0,
      confidence: row.golden_confidence ?? row.confidence,
      status: row.status,
      merged_into: row.merged_into,
      related_entities: row.related_entities || [],
      merge_history: row.merge_history || [],
      addresses: row.addresses || [],
    };
  }

  async function getFundingEvents(entityId) {
    const fedIdExpr = sourcePkExpr('sl', '_id');
    const genericIdExpr = sourcePkExpr('sl', 'id');
    const [fedCurrent, fedOriginal, abGrants, abContracts, abSoleSource] = await Promise.all([
      bq.query(`
        WITH linked AS (
          SELECT gc.*
          FROM ${t.links} sl
          JOIN ${t.fedGrants} gc ON CAST(gc._id AS STRING) = ${fedIdExpr}
          WHERE sl.entity_id = @entityId
            AND sl.source_schema = 'fed'
            AND sl.source_table = 'grants_contributions'
        ),
        ranked AS (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY ref_number, COALESCE(recipient_business_number, recipient_legal_name, CAST(_id AS STRING))
                   ORDER BY SAFE_CAST(REGEXP_REPLACE(amendment_number, r'\\D', '') AS INT64) DESC,
                            amendment_date DESC,
                            _id DESC
                 ) AS rn
          FROM linked
          WHERE ref_number IS NOT NULL
        )
        SELECT 'fed' AS source, 'fed.current_agreement_bigquery_logic' AS source_table,
               TO_JSON_STRING(STRUCT(_id, ref_number)) AS source_pk,
               agreement_value AS amount,
               agreement_start_date AS date,
               recipient_legal_name AS recipient_name,
               owner_org_title AS funder_name,
               prog_name_en AS program_name,
               agreement_title_en AS title
        FROM ranked WHERE rn = 1
        UNION ALL
        SELECT 'fed' AS source, 'fed.current_agreement_bigquery_logic' AS source_table,
               TO_JSON_STRING(STRUCT(_id, ref_number)) AS source_pk,
               agreement_value AS amount,
               agreement_start_date AS date,
               recipient_legal_name AS recipient_name,
               owner_org_title AS funder_name,
               prog_name_en AS program_name,
               agreement_title_en AS title
        FROM linked WHERE ref_number IS NULL
      `, { entityId }),
      bq.query(`
        SELECT 'fed_original' AS source, 'fed.original_agreement_bigquery_logic' AS source_table,
               TO_JSON_STRING(STRUCT(gc._id, gc.ref_number)) AS source_pk,
               gc.agreement_value AS amount,
               gc.agreement_start_date AS date,
               gc.recipient_legal_name AS recipient_name,
               gc.owner_org_title AS funder_name,
               gc.prog_name_en AS program_name,
               gc.agreement_title_en AS title
        FROM ${t.links} sl
        JOIN ${t.fedGrants} gc ON CAST(gc._id AS STRING) = ${fedIdExpr}
        WHERE sl.entity_id = @entityId
          AND sl.source_schema = 'fed'
          AND sl.source_table = 'grants_contributions'
          AND gc.is_amendment = FALSE
      `, { entityId }),
      bq.query(`
        SELECT 'ab_grants' AS source, 'ab.ab_grants' AS source_table,
               TO_JSON_STRING(STRUCT(g.id)) AS source_pk,
               g.amount, g.payment_date AS date, g.recipient AS recipient_name,
               g.ministry AS funder_name, g.program AS program_name,
               g.display_fiscal_year AS fiscal_year_label
        FROM ${t.links} sl
        JOIN ${t.abGrants} g ON CAST(g.id AS STRING) = ${genericIdExpr}
        WHERE sl.entity_id = @entityId AND sl.source_schema = 'ab' AND sl.source_table = 'ab_grants'
      `, { entityId }),
      bq.query(`
        SELECT 'ab_contracts' AS source, 'ab.ab_contracts' AS source_table,
               TO_JSON_STRING(STRUCT(c.id)) AS source_pk,
               c.amount, NULL AS date, c.recipient AS recipient_name,
               c.ministry AS funder_name, NULL AS program_name,
               c.display_fiscal_year AS fiscal_year_label
        FROM ${t.links} sl
        JOIN ${t.abContracts} c ON CAST(c.id AS STRING) = ${genericIdExpr}
        WHERE sl.entity_id = @entityId AND sl.source_schema = 'ab' AND sl.source_table = 'ab_contracts'
      `, { entityId }),
      bq.query(`
        SELECT 'ab_sole_source' AS source, 'ab.ab_sole_source' AS source_table,
               TO_JSON_STRING(STRUCT(ss.id)) AS source_pk,
               ss.amount, ss.start_date AS date, ss.vendor AS recipient_name,
               ss.ministry AS funder_name, ss.contract_services AS program_name,
               ss.display_fiscal_year AS fiscal_year_label
        FROM ${t.links} sl
        JOIN ${t.abSoleSource} ss ON CAST(ss.id AS STRING) = ${genericIdExpr}
        WHERE sl.entity_id = @entityId AND sl.source_schema = 'ab' AND sl.source_table = 'ab_sole_source'
      `, { entityId }),
    ]);

    const normalize = row => ({
      source: row.source,
      source_table: row.source_table,
      source_pk: parseJson(row.source_pk),
      amount: num(row.amount),
      date: row.date ? String(row.date).slice(0, 10) : null,
      year_label: row.fiscal_year_label ? String(row.fiscal_year_label).replace(/\s/g, '') : fiscalYearLabelFromDate(row.date),
      recipient_name: row.recipient_name,
      funder_name: row.funder_name,
      program_name: row.program_name,
      title: row.title,
      event_type: 'funding',
      label: row.source === 'fed' ? 'Federal grant or contribution' :
             row.source === 'ab_grants' ? 'Alberta grant' :
             row.source === 'ab_contracts' ? 'Alberta contract' :
             row.source === 'ab_sole_source' ? 'Alberta sole-source contract' : 'Public funding',
    });
    return {
      current_events: [...fedCurrent, ...abGrants, ...abContracts, ...abSoleSource].map(normalize),
      fed_original_events: fedOriginal.map(normalize),
    };
  }

  function getFundingSummary(events, fedOriginalEvents = []) {
    const bySource = { fed_current: 0, fed_original: 0, ab_grants: 0, ab_contracts: 0, ab_sole_source: 0 };
    const positive = events.filter(e => e.amount > 0);
    for (const e of events) {
      if (e.source === 'fed') bySource.fed_current += e.amount;
      if (e.source === 'ab_grants') bySource.ab_grants += e.amount;
      if (e.source === 'ab_contracts') bySource.ab_contracts += e.amount;
      if (e.source === 'ab_sole_source') bySource.ab_sole_source += e.amount;
    }
    bySource.fed_original = fedOriginalEvents.reduce((sum, e) => sum + e.amount, 0);
    const total = bySource.fed_current + bySource.ab_grants + bySource.ab_contracts + bySource.ab_sole_source;
    const funders = {};
    for (const e of positive) funders[e.funder_name || 'Unknown funder'] = (funders[e.funder_name || 'Unknown funder'] || 0) + e.amount;
    const top = Object.entries(funders).sort((a, b) => b[1] - a[1])[0] || [null, 0];
    const largest = positive.slice().sort((a, b) => b.amount - a.amount)[0] || null;
    const latest = positive.filter(e => e.date).sort((a, b) => String(b.date).localeCompare(String(a.date)))[0] || null;
    return {
      fed_current_commitment_total: bySource.fed_current,
      fed_original_commitment_total: bySource.fed_original,
      ab_grants_total: bySource.ab_grants,
      ab_contracts_total: bySource.ab_contracts,
      ab_sole_source_total: bySource.ab_sole_source,
      total_external_public_funding: total,
      funding_event_count: positive.length,
      largest_public_funding_event_amount: largest?.amount || 0,
      largest_public_funding_event_date: largest?.date || null,
      last_public_funding_date: latest?.date || null,
      top_funder_name: top[0],
      top_funder_amount: top[1],
      top_funder_share: total > 0 ? Number((top[1] / total * 100).toFixed(1)) : 0,
      events,
    };
  }

  async function getCraFilingContinuity(bnRoot, fundingSummary) {
    if (!bnRoot) return { available: false, filing_continuity_limitations: ['CRA filing continuity requires a usable business number root.'] };
    const rows = await bq.query(`
      SELECT DISTINCT fiscal_year FROM (
        SELECT CAST(fiscal_year AS INT64) AS fiscal_year FROM ${t.craIdentification} WHERE SUBSTR(bn, 1, 9) = @bnRoot
        UNION DISTINCT
        SELECT EXTRACT(YEAR FROM fpe) AS fiscal_year FROM ${t.craFinancialDetails} WHERE SUBSTR(bn, 1, 9) = @bnRoot
      )
      ORDER BY fiscal_year
    `, { bnRoot });
    const years = rows.map(r => Number(r.fiscal_year)).filter(Boolean).sort((a, b) => a - b);
    if (!years.length) return { available: false, cra_filing_years: [], filing_continuity_limitations: ['No CRA filings found for this BN root in available BigQuery data.'] };
    const first = years[0];
    const last = years[years.length - 1];
    const full = Array.from({ length: last - first + 1 }, (_, i) => first + i);
    const missing = full.filter(y => !years.includes(y));
    const now = currentDate();
    const majorDate = fundingSummary.largest_public_funding_event_date || fundingSummary.last_public_funding_date;
    const majorWindowEnd = majorDate ? addMonths(majorDate, 12) : null;
    const majorWindowMature = majorWindowEnd ? majorWindowEnd <= now : false;
    const majorYear = majorDate ? new Date(majorDate).getUTCFullYear() : null;
    const lastFundingWindowEnd = fundingSummary.last_public_funding_date ? addMonths(fundingSummary.last_public_funding_date, 12) : null;
    const lastFundingWindowMature = lastFundingWindowEnd ? lastFundingWindowEnd <= now : false;
    return {
      available: true,
      cra_filing_years: years,
      cra_filing_count: years.length,
      cra_first_filing_year: first,
      cra_last_filing_year: last,
      available_cra_year_window: { first_year: first, last_year: last },
      missing_years_after_first_filing: missing,
      last_filing_after_last_major_funding: majorYear ? last >= majorYear : null,
      has_filing_within_12_months_after_major_funding: majorYear && majorWindowMature ? years.some(y => y === majorYear || y === majorYear + 1) : null,
      post_funding_observation_window_mature: majorWindowMature,
      has_later_activity_signal: !fundingSummary.last_public_funding_date || !lastFundingWindowMature
        ? null
        : last >= new Date(fundingSummary.last_public_funding_date).getUTCFullYear(),
      filing_continuity_limitations: ['Missing CRA filings are review signals, not proof that an organization stopped operating.'],
    };
  }

  async function getDependencyMetrics(bnRoot) {
    if (!bnRoot) return { available: false, rows: [], dependency_limitations: ['Public-funding dependency requires CRA revenue data and a usable BN root.'] };
    const rows = await bq.query(`
      SELECT fiscal_year, federal, provincial, municipal, combined_sectiond,
             total_govt, revenue, govt_share_of_rev
      FROM ${t.craGovtFunding}
      WHERE SUBSTR(bn, 1, 9) = @bnRoot
      ORDER BY fiscal_year
    `, { bnRoot });
    const normalized = rows.map(r => ({
      fiscal_year: r.fiscal_year,
      federal: num(r.federal),
      provincial: num(r.provincial),
      municipal: num(r.municipal),
      combined_sectiond: num(r.combined_sectiond),
      total_govt: num(r.total_govt),
      revenue: num(r.revenue),
      govt_share_of_rev: maybeNum(r.govt_share_of_rev),
    }));
    const ratios = normalized.map(r => r.govt_share_of_rev).filter(r => r !== null);
    return {
      available: normalized.length > 0,
      rows: normalized,
      max_dependency_ratio: ratios.length ? Math.max(...ratios) : null,
      latest_dependency_ratio: normalized.length ? normalized[normalized.length - 1].govt_share_of_rev : null,
      dependency_years_over_70: normalized.filter(r => num(r.govt_share_of_rev) >= 70).map(r => r.fiscal_year),
      dependency_years_over_80: normalized.filter(r => num(r.govt_share_of_rev) >= 80).map(r => r.fiscal_year),
      dependency_limitations: normalized.length ? [] : ['No CRA government-funding dependency rows found for this BN root.'],
    };
  }

  async function getAbLifecycleSignals(entityId) {
    const idExpr = sourcePkExpr('esl', 'id');
    const rows = await bq.query(`
      SELECT np.status, sl.description AS status_description, np.registration_date,
             np.legal_name, np.type, np.city, np.postal_code
      FROM ${t.links} esl
      JOIN ${t.abNonProfit} np ON CAST(np.id AS STRING) = ${idExpr}
      LEFT JOIN ${t.abStatusLookup} sl ON sl.status = np.status
      WHERE esl.entity_id = @entityId
        AND esl.source_schema = 'ab'
        AND esl.source_table = 'ab_non_profit'
      ORDER BY np.registration_date DESC
      LIMIT 5
    `, { entityId });
    const primary = rows[0];
    const statusText = `${primary?.status || ''} ${primary?.status_description || ''}`.toLowerCase();
    return {
      available: rows.length > 0,
      ab_non_profit_status: primary?.status || null,
      ab_non_profit_status_description: primary?.status_description || null,
      ab_non_profit_registration_date: primary?.registration_date ? String(primary.registration_date).slice(0, 10) : null,
      status_source: rows.length ? 'ab.ab_non_profit' : null,
      is_inactive_status: /\b(dissolved|struck|inactive|cancelled|canceled|revoked)\b/.test(statusText),
      records: rows,
      lifecycle_limitations: rows.length ? [] : ['No linked Alberta non-profit registry record found for this entity.'],
    };
  }

  function dataQuality(identity, dependency) {
    const warnings = [];
    if (!identity.bn_root) warnings.push({ code: 'NO_BN_ROOT', label: 'No usable BN root is available; some CRA checks may be unavailable.', source_trace: [{ source_table: 'general.entities', fields: ['bn_root'] }] });
    if (!dependency.available) warnings.push({ code: 'NO_CRA_DEPENDENCY_DATA', label: 'CRA public-funding dependency ratio is unavailable for this entity.', source_trace: [{ source_table: 'cra.govt_funding_by_charity' }] });
    warnings.push({ code: 'FED_AMENDMENT_HANDLING', label: 'Federal funding totals use current-agreement logic to avoid double-counting amendment rows.', source_trace: [{ source_table: 'fed.grants_contributions' }] });
    return warnings;
  }

  async function getPeerComparison(profile) {
    const total = profile.funding.total_external_public_funding || 0;
    const lower = total > 0 ? total * 0.25 : 0;
    const upper = total > 0 ? total * 4 : 1000000;
    const rows = await bq.query(`
      SELECT id AS entity_id, canonical_name, entity_type, dataset_sources,
             (
               COALESCE(SAFE_CAST(JSON_VALUE(fed_profile, '$.total_grants') AS FLOAT64), 0) +
               COALESCE(SAFE_CAST(JSON_VALUE(ab_profile, '$.total_grants') AS FLOAT64), 0) +
               COALESCE(SAFE_CAST(JSON_VALUE(ab_profile, '$.total_contracts') AS FLOAT64), 0) +
               COALESCE(SAFE_CAST(JSON_VALUE(ab_profile, '$.total_sole_source') AS FLOAT64), 0)
             ) AS total_all_funding,
             SAFE_CAST(JSON_VALUE(cra_profile, '$.filing_count') AS INT64) AS cra_filing_count
      FROM ${t.golden}
      WHERE id != @entityId
        AND (@entityType IS NULL OR entity_type = @entityType)
        AND (
          COALESCE(SAFE_CAST(JSON_VALUE(fed_profile, '$.total_grants') AS FLOAT64), 0) +
          COALESCE(SAFE_CAST(JSON_VALUE(ab_profile, '$.total_grants') AS FLOAT64), 0) +
          COALESCE(SAFE_CAST(JSON_VALUE(ab_profile, '$.total_contracts') AS FLOAT64), 0) +
          COALESCE(SAFE_CAST(JSON_VALUE(ab_profile, '$.total_sole_source') AS FLOAT64), 0)
        ) BETWEEN @lower AND @upper
      ORDER BY total_all_funding
      LIMIT 500
    `, { entityId: profile.identity.entity_id, entityType: profile.identity.entity_type || null, lower, upper });
    if (rows.length < 10) return { available: false, reason: 'Peer group too small for defensible comparison', peer_group_size: rows.length };
    const fundingValues = rows.map(r => num(r.total_all_funding)).sort((a, b) => a - b);
    const filingValues = rows.map(r => num(r.cra_filing_count)).sort((a, b) => a - b);
    const pct = (values, value) => Number((values.filter(v => v <= value).length / values.length * 100).toFixed(1));
    return {
      available: true,
      peer_group_size: rows.length,
      peer_basis: { entity_type: profile.identity.entity_type || null, funding_scale_min: lower, funding_scale_max: upper },
      funding_percentile: pct(fundingValues, total),
      dependency_percentile: null,
      largest_event_percentile: null,
      filing_continuity_percentile: profile.filing_continuity.cra_filing_count ? pct(filingValues, profile.filing_continuity.cra_filing_count) : null,
      top_funder_concentration_percentile: null,
      limitations: ['Peer comparison uses BigQuery golden-record funding profile fields for candidate grouping.'],
    };
  }

  function buildTimeline(profile) {
    const events = [];
    for (const e of profile.funding.events || []) {
      events.push({
        date: e.date,
        year_label: e.year_label,
        event_type: 'funding',
        source: e.source,
        label: e.label,
        amount: e.amount,
        description: e.program_name || e.title || e.funder_name,
        source_table: e.source_table,
        source_pk: e.source_pk,
      });
    }
    for (const y of profile.filing_continuity.cra_filing_years || []) {
      events.push({ date: `${y}-12-31`, year_label: String(y), event_type: 'cra_filing', source: 'cra', label: 'CRA filing found', amount: null, description: `CRA filing year ${y}`, source_table: 'cra.cra_identification / cra.cra_financial_details', source_pk: { bn_root: profile.identity.bn_root, fiscal_year: y } });
    }
    for (const row of profile.dependency.rows || []) {
      if (num(row.govt_share_of_rev) >= 70) {
        events.push({ date: `${row.fiscal_year}-12-31`, year_label: String(row.fiscal_year), event_type: 'dependency_threshold', source: 'cra', label: num(row.govt_share_of_rev) >= 80 ? '80% public-funding dependency threshold exceeded' : '70% public-funding dependency threshold exceeded', amount: row.total_govt, description: `Government funding was ${row.govt_share_of_rev}% of reported revenue.`, source_table: 'cra.govt_funding_by_charity', source_pk: { bn_root: profile.identity.bn_root, fiscal_year: row.fiscal_year } });
      }
    }
    return events.sort((a, b) => String(a.date || '').localeCompare(String(b.date || '')));
  }

  function sourceTrace() {
    return [
      'general.entities',
      'general.entity_golden_records',
      'general.entity_source_links',
      'fed.grants_contributions',
      'cra.cra_identification',
      'cra.cra_financial_details',
      'cra.govt_funding_by_charity',
      'ab.ab_grants',
      'ab.ab_contracts',
      'ab.ab_sole_source',
      'ab.ab_non_profit',
    ].map(source_table => ({ source_table }));
  }

  function limitations(profile) {
    return [
      'This profile identifies review signals, not fraud, misconduct, or proof of ceased operations.',
      'Missing filings mean no filing was found in available data; they are not proof of inactivity outside these datasets.',
      'Federal totals use current-agreement logic to avoid amendment double-counting.',
      ...(profile.filing_continuity.filing_continuity_limitations || []),
      ...(profile.dependency.dependency_limitations || []),
      ...(profile.lifecycle.lifecycle_limitations || []),
      ...(profile.peer_comparison.limitations || []),
    ];
  }

  async function buildRiskProfile(entityId) {
    const identity = await getEntityIdentity(entityId);
    if (!identity) return null;
    const fundingEvents = await getFundingEvents(entityId);
    const funding = getFundingSummary(fundingEvents.current_events, fundingEvents.fed_original_events);
    const [filingContinuity, dependency, lifecycle] = await Promise.all([
      getCraFilingContinuity(identity.bn_root, funding),
      getDependencyMetrics(identity.bn_root),
      getAbLifecycleSignals(entityId),
    ]);
    const profile = {
      entity: { id: identity.entity_id, canonical_name: identity.canonical_name, bn_root: identity.bn_root, entity_type: identity.entity_type, dataset_sources: identity.dataset_sources },
      identity,
      funding,
      timeline: [],
      filing_continuity: filingContinuity,
      dependency,
      lifecycle,
      peer_comparison: {},
      flags: [],
      score: {},
      data_quality: dataQuality(identity, dependency),
      source_trace: sourceTrace(),
      limitations: [],
      backend: 'bigquery',
    };
    profile.peer_comparison = await getPeerComparison(profile);
    profile.timeline = buildTimeline(profile);
    profile.flags = computeRiskFlags(profile);
    profile.score = computeRiskScore(profile.flags, {
      total_external_public_funding: funding.total_external_public_funding,
      max_dependency_ratio: dependency.max_dependency_ratio,
    });
    profile.limitations = limitations(profile);
    return profile;
  }

  async function buildReviewQueue(limit = DEFAULT_LIMIT) {
    const fedIdExpr = sourcePkExpr('sl', '_id');
    const genericIdExpr = sourcePkExpr('sl', 'id');
    const rows = await bq.query(`
      WITH fed_linked AS (
        SELECT sl.entity_id, gc.*
        FROM ${t.links} sl
        JOIN ${t.fedGrants} gc ON CAST(gc._id AS STRING) = ${fedIdExpr}
        WHERE sl.source_schema = 'fed'
          AND sl.source_table = 'grants_contributions'
      ),
      fed_current AS (
        SELECT *
        FROM (
          SELECT *,
                 ROW_NUMBER() OVER (
                   PARTITION BY entity_id, ref_number, COALESCE(recipient_business_number, recipient_legal_name, CAST(_id AS STRING))
                   ORDER BY SAFE_CAST(REGEXP_REPLACE(amendment_number, r'\\D', '') AS INT64) DESC,
                            amendment_date DESC,
                            _id DESC
                 ) AS rn
          FROM fed_linked
          WHERE ref_number IS NOT NULL
        )
        WHERE rn = 1
        UNION ALL
        SELECT *, 1 AS rn FROM fed_linked WHERE ref_number IS NULL
      ),
      fed AS (
        SELECT entity_id,
               SUM(COALESCE(agreement_value, 0)) AS fed_total,
               COUNTIF(COALESCE(agreement_value, 0) > 0) AS fed_count,
               MAX(COALESCE(agreement_value, 0)) AS fed_largest,
               MAX(agreement_start_date) AS fed_last_date
        FROM fed_current
        GROUP BY entity_id
      ),
      abg AS (
        SELECT sl.entity_id,
               SUM(COALESCE(g.amount, 0)) AS ab_grants_total,
               COUNTIF(COALESCE(g.amount, 0) > 0) AS ab_grants_count,
               MAX(COALESCE(g.amount, 0)) AS ab_grants_largest,
               MAX(g.payment_date) AS ab_grants_last_date
        FROM ${t.links} sl
        JOIN ${t.abGrants} g ON CAST(g.id AS STRING) = ${genericIdExpr}
        WHERE sl.source_schema = 'ab' AND sl.source_table = 'ab_grants'
        GROUP BY sl.entity_id
      ),
      abc AS (
        SELECT sl.entity_id,
               SUM(COALESCE(c.amount, 0)) AS ab_contracts_total,
               COUNTIF(COALESCE(c.amount, 0) > 0) AS ab_contracts_count,
               MAX(COALESCE(c.amount, 0)) AS ab_contracts_largest
        FROM ${t.links} sl
        JOIN ${t.abContracts} c ON CAST(c.id AS STRING) = ${genericIdExpr}
        WHERE sl.source_schema = 'ab' AND sl.source_table = 'ab_contracts'
        GROUP BY sl.entity_id
      ),
      abs AS (
        SELECT sl.entity_id,
               SUM(COALESCE(ss.amount, 0)) AS ab_sole_source_total,
               COUNTIF(COALESCE(ss.amount, 0) > 0) AS ab_sole_source_count,
               MAX(COALESCE(ss.amount, 0)) AS ab_sole_source_largest,
               MAX(ss.start_date) AS ab_sole_source_last_date
        FROM ${t.links} sl
        JOIN ${t.abSoleSource} ss ON CAST(ss.id AS STRING) = ${genericIdExpr}
        WHERE sl.source_schema = 'ab' AND sl.source_table = 'ab_sole_source'
        GROUP BY sl.entity_id
      ),
      funding_ids AS (
        SELECT entity_id FROM fed
        UNION DISTINCT SELECT entity_id FROM abg
        UNION DISTINCT SELECT entity_id FROM abc
        UNION DISTINCT SELECT entity_id FROM abs
      ),
      funding AS (
        SELECT ids.entity_id,
               COALESCE(fed.fed_total, 0) + COALESCE(abg.ab_grants_total, 0) + COALESCE(abc.ab_contracts_total, 0) + COALESCE(abs.ab_sole_source_total, 0) AS total_external_public_funding,
               COALESCE(fed.fed_count, 0) + COALESCE(abg.ab_grants_count, 0) + COALESCE(abc.ab_contracts_count, 0) + COALESCE(abs.ab_sole_source_count, 0) AS funding_event_count,
               GREATEST(COALESCE(fed.fed_largest, 0), COALESCE(abg.ab_grants_largest, 0), COALESCE(abc.ab_contracts_largest, 0), COALESCE(abs.ab_sole_source_largest, 0)) AS largest_funding_event_amount,
               GREATEST(
                 COALESCE(CAST(fed.fed_last_date AS TIMESTAMP), TIMESTAMP '0001-01-01'),
                 COALESCE(CAST(abg.ab_grants_last_date AS TIMESTAMP), TIMESTAMP '0001-01-01'),
                 COALESCE(CAST(abs.ab_sole_source_last_date AS TIMESTAMP), TIMESTAMP '0001-01-01')
               ) AS last_public_funding_date
        FROM funding_ids ids
        LEFT JOIN fed ON fed.entity_id = ids.entity_id
        LEFT JOIN abg ON abg.entity_id = ids.entity_id
        LEFT JOIN abc ON abc.entity_id = ids.entity_id
        LEFT JOIN abs ON abs.entity_id = ids.entity_id
      ),
      dep AS (
        SELECT SUBSTR(bn, 1, 9) AS bn_root,
               MAX(govt_share_of_rev) AS max_public_funding_dependency_ratio,
               MAX(fiscal_year) AS last_cra_filing_year
        FROM ${t.craGovtFunding}
        GROUP BY 1
      )
      SELECT c.entity_id, c.canonical_name, c.bn_root, c.entity_type,
             c.dataset_sources, c.source_link_count,
             f.total_external_public_funding,
             f.largest_funding_event_amount,
             NULL AS largest_funding_event_date,
             f.last_public_funding_date,
             d.last_cra_filing_year,
             d.max_public_funding_dependency_ratio,
             CASE
               WHEN d.max_public_funding_dependency_ratio >= 80 THEN 'PUBLIC_DEPENDENCY_80'
               WHEN d.max_public_funding_dependency_ratio >= 70 THEN 'PUBLIC_DEPENDENCY_70'
               WHEN f.total_external_public_funding >= 1000000 THEN 'LARGE_PUBLIC_FUNDING_EVENT'
               ELSE 'SOURCE_DATA_QUALITY_CAUTION'
             END AS top_flag,
             LEAST(100,
               IF(d.max_public_funding_dependency_ratio >= 80, 25, IF(d.max_public_funding_dependency_ratio >= 70, 18, 0)) +
               IF(f.total_external_public_funding >= 1000000, 10, IF(f.total_external_public_funding >= 500000, 6, 0)) +
               4
             ) AS score
      FROM (
        SELECT id AS entity_id, canonical_name, CAST(bn_root AS STRING) AS bn_root,
               entity_type, dataset_sources, source_link_count
        FROM ${t.golden}
      ) c
      JOIN funding f ON f.entity_id = c.entity_id
      LEFT JOIN dep d ON d.bn_root = c.bn_root
      WHERE f.total_external_public_funding > 0
        AND LOWER(COALESCE(c.entity_type, '')) NOT IN ('government', 'individual', 'person')
        AND NOT REGEXP_CONTAINS(LOWER(c.canonical_name), r'^(government of|province of|city of|town of|municipality of|municipal district|county of)')
      ORDER BY score DESC, f.total_external_public_funding DESC
      LIMIT @limit
    `, { limit });
    return rows.map(r => ({
      entity_id: r.entity_id,
      canonical_name: r.canonical_name,
      bn_root: r.bn_root,
      entity_type: r.entity_type,
      dataset_sources: parseJson(r.dataset_sources) || r.dataset_sources || [],
      source_link_count: r.source_link_count || 0,
      total_external_public_funding: num(r.total_external_public_funding),
      largest_funding_event_amount: maybeNum(r.largest_funding_event_amount),
      largest_funding_event_date: r.largest_funding_event_date || null,
      last_public_funding_date: r.last_public_funding_date ? String(r.last_public_funding_date).slice(0, 10) : null,
      last_cra_filing_year: r.last_cra_filing_year || null,
      max_public_funding_dependency_ratio: maybeNum(r.max_public_funding_dependency_ratio),
      top_flag: r.top_flag,
      review_priority: Number(r.score) >= 60 ? 'High Review Priority' : Number(r.score) >= 30 ? 'Medium Review Priority' : 'Low Review Priority',
      score: Number(r.score),
    }));
  }

  async function buildZombieQuadrant(limit = 500) {
    const queue = await buildReviewQueue(Math.min(Math.max(limit, 10), 1000));
    const points = queue.map(row => {
      const activityDate = row.last_cra_filing_year ? `${row.last_cra_filing_year}-12-31` : null;
      const months = row.last_public_funding_date && activityDate ? monthsBetween(row.last_public_funding_date, activityDate) : null;
      const dependency = row.max_public_funding_dependency_ratio;
      const dependencyOver70 = dependency !== null && dependency >= 70;
      const dependencyOver80 = dependency !== null && dependency >= 80;
      const shortActivityWindow = months !== null && months <= 12;
      const activityAfterFundingFound = Boolean(row.last_public_funding_date && activityDate && new Date(activityDate) >= new Date(row.last_public_funding_date));
      const observationWindowMature = row.last_public_funding_date ? addMonths(row.last_public_funding_date, 12) <= currentDate() : false;
      const status = dependencyOver80 && shortActivityWindow && observationWindowMature ? 'high' :
                     dependencyOver70 && shortActivityWindow ? 'medium' :
                     dependencyOver70 || shortActivityWindow ? 'medium' : 'low';
      const matrixPriority = status === 'high' ? 'High Matrix Priority' : status === 'medium' ? 'Medium Matrix Priority' : 'Low Matrix Priority';
      return {
        entity_id: row.entity_id,
        canonical_name: row.canonical_name,
        entity_type: row.entity_type,
        x_dependency_ratio: dependency,
        y_months_funding_to_last_activity: months,
        total_public_funding: row.total_external_public_funding,
        dot_size_value: row.total_external_public_funding,
        review_status: status,
        review_priority: matrixPriority,
        score_band: row.review_priority,
        score: row.score,
        activity_after_funding_found: activityAfterFundingFound,
        observation_window_mature: observationWindowMature,
        reference_lines: { dependency_70: 70, dependency_80: 80, months_12: 12 },
        last_public_funding_date: row.last_public_funding_date,
        last_observed_activity_date: activityDate,
        top_flag: row.top_flag,
      };
    });
    return {
      generated_at: new Date().toISOString(),
      chart: 'zombie_quadrant',
      axes: {
        x: 'Public-funding dependency ratio (%)',
        y: 'Months from last major funding event to last observed activity',
        size: 'Total public funding received',
        color: 'Review status',
      },
      reference_lines: { dependency_70: 70, dependency_80: 80, months_12: 12 },
      points,
      limitations: [
        'Matrix priority is a chart-level triage status; the numeric score band remains available as score_band.',
        'A y value of 0 can mean the latest observed activity predates the latest funding event, so no later activity was found in the available signals.',
        'Last observed activity uses available CRA filing and lifecycle signals; absence of later activity is not proof of ceased operations.',
        'Use an organization risk profile for evidence before drawing conclusions about a specific recipient.',
      ],
    };
  }

  async function investigateEntity(entityId) {
    const trace = [];
    const profile = await buildRiskProfile(entityId);
    if (!profile) return null;
    trace.push({ step: 1, tool: 'get_organization_profile', input: { entity_id: entityId }, observation: profile.entity });
    trace.push({ step: 2, tool: 'get_funding_timeline', input: { entity_id: entityId }, observation: {
      total_external_public_funding: profile.funding.total_external_public_funding,
      funding_event_count: profile.funding.funding_event_count,
      largest_public_funding_event_amount: profile.funding.largest_public_funding_event_amount,
      last_public_funding_date: profile.funding.last_public_funding_date,
    }});
    trace.push({ step: 3, tool: 'check_filing_continuity', input: { entity_id: entityId, window: '12mo' }, observation: {
      cra_filing_years: profile.filing_continuity.cra_filing_years || [],
      cra_last_filing_year: profile.filing_continuity.cra_last_filing_year || null,
      has_filing_within_12_months_after_major_funding: profile.filing_continuity.has_filing_within_12_months_after_major_funding,
      observation_window_mature: profile.filing_continuity.post_funding_observation_window_mature,
    }});
    trace.push({ step: 4, tool: 'compute_dependency_ratio', input: { entity_id: entityId }, observation: {
      max_dependency_ratio: profile.dependency.max_dependency_ratio,
      years_over_70: profile.dependency.dependency_years_over_70 || [],
      years_over_80: profile.dependency.dependency_years_over_80 || [],
    }});
    trace.push({ step: 5, tool: 'find_related_entities', input: { entity_id: entityId, hops: 2 }, observation: {
      related_entity_count: profile.identity.related_entities?.length || 0,
      related_entities: (profile.identity.related_entities || []).slice(0, 10),
    }});
    trace.push({ step: 6, tool: 'draft_findings', input: { profile_id: entityId }, observation: {
      review_priority: profile.score.band,
      score: profile.score.total,
      flags: profile.flags.map(f => f.code),
      limitations: profile.limitations,
    }});
    return {
      generated_at: new Date().toISOString(),
      agent: 'Investigation Agent',
      entity: profile.entity,
      trace,
      findings: {
        review_priority: profile.score.band,
        score: profile.score.total,
        flags: profile.flags,
        recommended_next_actions: [
          'Review source evidence for each active flag.',
          'Confirm whether later filings or registry activity exist outside the available dataset.',
          'Review related entities before making any recipient-level conclusion.',
        ],
      },
      risk_profile: profile,
    };
  }

  async function triageCases(limit = 20) {
    const queue = await buildReviewQueue(Math.min(Math.max(limit, 1), 50));
    return {
      generated_at: new Date().toISOString(),
      agent: 'Triage Agent',
      request: `Surface the ${limit} most concerning cases from available data.`,
      trace: [
        { step: 1, tool: 'get_filtered_set', input: { exclude_government: true, has_public_funding: true }, observation: { candidate_count_returned: queue.length } },
        { step: 2, tool: 'rank_cases', input: { sort: ['score', 'total_external_public_funding'] }, observation: { top_score: queue[0]?.score || null } },
        { step: 3, tool: 'draft_case_rationales', input: { cases: queue.map(q => q.entity_id) }, observation: { rationale_count: queue.length } },
      ],
      cases: queue.map(row => ({
        ...row,
        rationale: `${row.canonical_name} is prioritized because ${row.top_flag || 'source-data review signals'} appears with $${Math.round(row.total_external_public_funding).toLocaleString()} in public funding${row.max_public_funding_dependency_ratio !== null ? ` and a maximum public-funding dependency ratio of ${row.max_public_funding_dependency_ratio}%.` : '.'}`,
      })),
    };
  }

  async function verifyFlag(entityId, flagCode) {
    const profile = await buildRiskProfile(entityId);
    if (!profile) return null;
    const flag = profile.flags.find(f => f.code === flagCode);
    const confirmed = Boolean(flag);
    return {
      generated_at: new Date().toISOString(),
      agent: 'Verification Agent',
      entity: profile.entity,
      flag_code: flagCode,
      verdict: confirmed ? 'confirmed' : 'cannot_verify',
      trace: [
        { step: 1, tool: 'get_org_profile', input: { entity_id: entityId }, observation: profile.entity },
        { step: 2, tool: 'recompute_risk_profile', input: { entity_id: entityId }, observation: { flags: profile.flags.map(f => f.code), score: profile.score.total } },
        { step: 3, tool: 'verify_flag', input: { flag_code: flagCode }, observation: flag || null },
      ],
      evidence: flag?.evidence || [],
      source_trace: flag?.source_trace || [],
      limitations: flag?.limitations || profile.limitations,
    };
  }

  async function buildCaseFile(entityId) {
    const riskProfile = await buildRiskProfile(entityId);
    if (!riskProfile) return null;
    return { generated_at: new Date().toISOString(), format: 'recipient-risk-case-file/v1', risk_profile: riskProfile };
  }

  async function generateRiskExplanation() {
    return {
      available: false,
      summary: 'AI explanation is not configured in the BigQuery backend yet. Deterministic risk profile remains authoritative.',
      key_evidence: [],
      review_questions: [
        'Was the organization still active after the final public funding event?',
        'Was public funding reported consistently in available CRA filings?',
        'Should related entities or aliases be reviewed together?',
      ],
      limitations: ['AI summaries are optional and do not affect deterministic flags or scores.'],
    };
  }

  return {
    searchEntities,
    buildRiskProfile,
    buildReviewQueue,
    buildZombieQuadrant,
    investigateEntity,
    triageCases,
    verifyFlag,
    buildCaseFile,
    generateRiskExplanation,
  };
}

function parseJson(value) {
  if (!value || typeof value !== 'string') return value || {};
  try { return JSON.parse(value); } catch (_) { return {}; }
}

module.exports = { createBigQueryRiskService };
