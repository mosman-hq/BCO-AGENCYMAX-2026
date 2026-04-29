const DEFAULT_REVIEW_QUEUE_LIMIT = 25;

function toNumber(value) {
  if (value === null || value === undefined || value === '') return 0;
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function toNullableNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function unique(values) {
  return [...new Set((values || []).filter(v => v !== null && v !== undefined && v !== ''))];
}

function fiscalYearLabelFromDate(dateValue) {
  if (!dateValue) return null;
  const d = new Date(dateValue);
  if (Number.isNaN(d.getTime())) return null;
  const year = d.getUTCFullYear();
  const month = d.getUTCMonth() + 1;
  return month >= 4 ? `${year}-${year + 1}` : `${year - 1}-${year}`;
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

function severityForPoints(points) {
  if (points >= 12) return 'high';
  if (points >= 6) return 'medium';
  return 'low';
}

function makeFlag(code, label, points, evidence = [], sourceTrace = [], limitations = []) {
  return {
    code,
    label,
    severity: severityForPoints(points),
    points,
    evidence,
    source_trace: sourceTrace,
    limitations,
  };
}

function bandForScore(total) {
  if (total >= 60) return 'High Review Priority';
  if (total >= 30) return 'Medium Review Priority';
  return 'Low Review Priority';
}

function computeRiskFlags(profile) {
  const flags = [];
  const funding = profile.funding || {};
  const dependency = profile.dependency || {};
  const continuity = profile.filing_continuity || {};
  const lifecycle = profile.lifecycle || {};
  const identity = profile.identity || {};
  const dataQuality = profile.data_quality || [];

  if ((dependency.max_dependency_ratio || 0) >= 70) {
    const row = dependency.rows?.find(r => toNumber(r.govt_share_of_rev) >= 70);
    flags.push(makeFlag(
      'PUBLIC_DEPENDENCY_70',
      'Public funding dependency exceeded 70%',
      10,
      [{ fiscal_year: row?.fiscal_year, govt_share_of_rev: row?.govt_share_of_rev, total_govt: row?.total_govt, revenue: row?.revenue }],
      [{ source_table: 'cra.govt_funding_by_charity', fields: ['govt_share_of_rev', 'total_govt', 'revenue'] }]
    ));
  }

  if ((dependency.max_dependency_ratio || 0) >= 80) {
    const row = dependency.rows?.find(r => toNumber(r.govt_share_of_rev) >= 80);
    flags.push(makeFlag(
      'PUBLIC_DEPENDENCY_80',
      'Public funding dependency exceeded 80%',
      15,
      [{ fiscal_year: row?.fiscal_year, govt_share_of_rev: row?.govt_share_of_rev, total_govt: row?.total_govt, revenue: row?.revenue }],
      [{ source_table: 'cra.govt_funding_by_charity', fields: ['govt_share_of_rev', 'total_govt', 'revenue'] }]
    ));
  }

  if (continuity.has_filing_within_12_months_after_major_funding === false) {
    flags.push(makeFlag(
      'NO_FILING_AFTER_FUNDING_12M',
      'No CRA filing found within 12 months after major public funding',
      15,
      [{
        largest_public_funding_event_date: funding.largest_public_funding_event_date,
        largest_public_funding_event_amount: funding.largest_public_funding_event_amount,
        cra_last_filing_year: continuity.cra_last_filing_year,
      }],
      [{ source_table: 'cra.cra_financial_details', fields: ['bn', 'fpe'] }],
      ['Missing filings are a review signal, not proof that an organization ceased operations.']
    ));
  }

  if ((continuity.missing_years_after_first_filing || []).length > 0 && funding.total_external_public_funding > 0) {
    flags.push(makeFlag(
      'FILING_GAP_AFTER_FUNDING',
      'CRA filing gap found after first available filing and public funding',
      8,
      [{ missing_years_after_first_filing: continuity.missing_years_after_first_filing }],
      [{ source_table: 'cra.cra_identification', fields: ['bn', 'fiscal_year'] }],
      ['CRA data covers only the available years in this repository.']
    ));
  }

  if (lifecycle.is_inactive_status) {
    flags.push(makeFlag(
      'AB_DISSOLVED_OR_STRUCK_AFTER_FUNDING',
      'Alberta non-profit registry status indicates an inactive, dissolved, or struck status',
      12,
      [{ status: lifecycle.ab_non_profit_status, status_description: lifecycle.ab_non_profit_status_description, registration_date: lifecycle.ab_non_profit_registration_date }],
      [{ source_table: lifecycle.status_source || 'ab.ab_non_profit', fields: ['status', 'registration_date'] }]
    ));
  }

  if (funding.total_external_public_funding > 0 && continuity.has_later_activity_signal === false) {
    flags.push(makeFlag(
      'POST_FUNDING_NO_ACTIVITY',
      'No later filing or lifecycle activity signal found after public funding in available data',
      10,
      [{ last_public_funding_date: funding.last_public_funding_date, cra_last_filing_year: continuity.cra_last_filing_year }],
      [{ source_table: 'general.entity_source_links', fields: ['source_schema', 'source_table', 'source_pk'] }],
      ['This does not prove inactivity outside the datasets.']
    ));
  }

  if ((funding.largest_public_funding_event_amount || 0) >= 500000) {
    flags.push(makeFlag(
      'LARGE_PUBLIC_FUNDING_EVENT',
      'Large public funding event found',
      funding.largest_public_funding_event_amount >= 1000000 ? 10 : 6,
      [{ amount: funding.largest_public_funding_event_amount, date: funding.largest_public_funding_event_date }],
      [{ source_table: 'funding event source', fields: ['amount', 'date'] }]
    ));
  }

  if ((funding.funding_event_count || 0) <= 2 && (funding.total_external_public_funding || 0) >= 1000000) {
    flags.push(makeFlag(
      'FEW_EVENTS_HIGH_VALUE',
      'Few public funding events account for high total value',
      8,
      [{ funding_event_count: funding.funding_event_count, total_external_public_funding: funding.total_external_public_funding }],
      [{ source_table: 'derived from funding events', fields: ['amount'] }]
    ));
  }

  if ((funding.top_funder_share || 0) >= 80 && (funding.total_external_public_funding || 0) >= 500000) {
    flags.push(makeFlag(
      'SINGLE_FUNDER_CONCENTRATION',
      'Public funding is concentrated with one funder',
      8,
      [{ top_funder_name: funding.top_funder_name, top_funder_share: funding.top_funder_share, top_funder_amount: funding.top_funder_amount }],
      [{ source_table: 'derived from funding events', fields: ['department_or_ministry', 'amount'] }]
    ));
  }

  if (!identity.bn_root) {
    flags.push(makeFlag(
      'MISSING_OR_MALFORMED_BN',
      'No usable business number root is available for this entity',
      6,
      [{ bn_root: identity.bn_root, bn_variants: identity.bn_variants || [] }],
      [{ source_table: 'general.entities', fields: ['bn_root', 'bn_variants'] }]
    ));
  }

  if ((identity.confidence !== null && identity.confidence !== undefined && Number(identity.confidence) < 0.75) || (identity.related_entities || []).length > 0) {
    flags.push(makeFlag(
      'ENTITY_MATCH_REVIEW_NEEDED',
      'Entity-resolution context should be reviewed before relying on conclusions',
      5,
      [{ confidence: identity.confidence, related_entity_count: (identity.related_entities || []).length }],
      [{ source_table: 'general.entity_golden_records', fields: ['confidence', 'related_entities'] }]
    ));
  }

  if (dataQuality.length > 0) {
    flags.push(makeFlag(
      'SOURCE_DATA_QUALITY_CAUTION',
      'One or more source data quality cautions apply',
      Math.min(10, 3 + dataQuality.length),
      dataQuality.slice(0, 5),
      dataQuality.flatMap(q => q.source_trace || [])
    ));
  }

  return flags;
}

function cap(value, max) {
  return Math.min(max, Math.max(0, value));
}

function computeRiskScore(flags, metrics = {}) {
  const byCode = Object.fromEntries(flags.map(f => [f.code, f]));
  const evidenceFor = (...codes) => codes.filter(c => byCode[c]).map(c => byCode[c]);

  const inactivity = cap(
    (byCode.NO_FILING_AFTER_FUNDING_12M ? 15 : 0) +
    (byCode.FILING_GAP_AFTER_FUNDING ? 6 : 0) +
    (byCode.POST_FUNDING_NO_ACTIVITY ? 6 : 0) +
    (byCode.AB_DISSOLVED_OR_STRUCK_AFTER_FUNDING ? 12 : 0),
    30
  );
  const dependency = cap(
    (byCode.PUBLIC_DEPENDENCY_80 ? 25 : byCode.PUBLIC_DEPENDENCY_70 ? 18 : 0),
    25
  );
  const funding = cap(
    (byCode.LARGE_PUBLIC_FUNDING_EVENT ? byCode.LARGE_PUBLIC_FUNDING_EVENT.points : 0) +
    (byCode.FEW_EVENTS_HIGH_VALUE ? 6 : 0) +
    (byCode.SINGLE_FUNDER_CONCENTRATION ? 6 : 0),
    20
  );
  const identity = cap(
    (byCode.MISSING_OR_MALFORMED_BN ? 8 : 0) +
    (byCode.ENTITY_MATCH_REVIEW_NEEDED ? 7 : 0),
    15
  );
  const quality = cap(byCode.SOURCE_DATA_QUALITY_CAUTION ? byCode.SOURCE_DATA_QUALITY_CAUTION.points : 0, 10);

  const components = [
    { name: 'Post-funding inactivity and filing continuity', points: inactivity, max_points: 30, evidence: evidenceFor('NO_FILING_AFTER_FUNDING_12M', 'FILING_GAP_AFTER_FUNDING', 'POST_FUNDING_NO_ACTIVITY', 'AB_DISSOLVED_OR_STRUCK_AFTER_FUNDING') },
    { name: 'Public funding dependency', points: dependency, max_points: 25, evidence: evidenceFor('PUBLIC_DEPENDENCY_70', 'PUBLIC_DEPENDENCY_80') },
    { name: 'Funding scale and concentration', points: funding, max_points: 20, evidence: evidenceFor('LARGE_PUBLIC_FUNDING_EVENT', 'FEW_EVENTS_HIGH_VALUE', 'SINGLE_FUNDER_CONCENTRATION') },
    { name: 'Identity continuity and source coverage', points: identity, max_points: 15, evidence: evidenceFor('MISSING_OR_MALFORMED_BN', 'ENTITY_MATCH_REVIEW_NEEDED') },
    { name: 'Data-quality cautions', points: quality, max_points: 10, evidence: evidenceFor('SOURCE_DATA_QUALITY_CAUTION') },
  ];

  const total = components.reduce((sum, c) => sum + c.points, 0);
  return {
    total,
    band: bandForScore(total),
    components,
    metrics,
  };
}

function createRiskService(pool) {
  async function safeQuery(sql, params = [], fallbackRows = []) {
    try {
      const result = await pool.query(sql, params);
      return result.rows;
    } catch (error) {
      return fallbackRows;
    }
  }

  async function getEntityIdentity(entityId) {
    const rows = await pool.query(`
      SELECT e.id, e.canonical_name, e.bn_root, e.bn_variants, e.alternate_names,
             e.entity_type, e.dataset_sources, e.source_count, e.confidence,
             e.status, e.merged_into,
             gr.aliases, gr.related_entities, gr.merge_history,
             gr.source_link_count, gr.addresses, gr.confidence AS golden_confidence
      FROM general.entities e
      LEFT JOIN general.entity_golden_records gr ON gr.id = e.id
      WHERE e.id = $1
    `, [entityId]);
    const row = rows.rows[0];
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
    const [fedCurrent, fedOriginal, abGrants, abContracts, abSoleSource] = await Promise.all([
      safeQuery(`
        WITH linked AS (
          SELECT gc.*
          FROM general.entity_source_links sl
          JOIN fed.grants_contributions gc ON gc._id = (sl.source_pk->>'_id')::int
          WHERE sl.entity_id = $1
            AND sl.source_schema = 'fed'
            AND sl.source_table = 'grants_contributions'
        ),
        current_with_ref AS (
          SELECT DISTINCT ON (
            ref_number,
            COALESCE(recipient_business_number, recipient_legal_name, _id::text)
          )
            *
          FROM linked
          WHERE ref_number IS NOT NULL
          ORDER BY ref_number,
            COALESCE(recipient_business_number, recipient_legal_name, _id::text),
            NULLIF(regexp_replace(amendment_number, '\\D', '', 'g'), '')::int DESC NULLS LAST,
            amendment_date DESC NULLS LAST,
            _id DESC
        ),
        current_without_ref AS (
          SELECT * FROM linked WHERE ref_number IS NULL
        )
        SELECT 'fed' AS source, 'fed.vw_agreement_current_logic' AS source_table,
               jsonb_build_object('_id', _id, 'ref_number', ref_number) AS source_pk,
               agreement_value AS amount,
               agreement_start_date AS date,
               recipient_legal_name AS recipient_name,
               owner_org_title AS funder_name,
               prog_name_en AS program_name,
               agreement_title_en AS title,
               recipient_business_number AS source_bn,
               is_amendment
        FROM current_with_ref
        UNION ALL
        SELECT 'fed' AS source, 'fed.vw_agreement_current_logic' AS source_table,
               jsonb_build_object('_id', _id, 'ref_number', ref_number) AS source_pk,
               agreement_value AS amount,
               agreement_start_date AS date,
               recipient_legal_name AS recipient_name,
               owner_org_title AS funder_name,
               prog_name_en AS program_name,
               agreement_title_en AS title,
               recipient_business_number AS source_bn,
               is_amendment
        FROM current_without_ref
      `, [entityId]),
      safeQuery(`
        SELECT 'fed_original' AS source, 'fed.vw_agreement_originals_logic' AS source_table,
               jsonb_build_object('_id', gc._id, 'ref_number', gc.ref_number) AS source_pk,
               gc.agreement_value AS amount,
               gc.agreement_start_date AS date,
               gc.recipient_legal_name AS recipient_name,
               gc.owner_org_title AS funder_name,
               gc.prog_name_en AS program_name,
               gc.agreement_title_en AS title
        FROM general.entity_source_links sl
        JOIN fed.grants_contributions gc ON gc._id = (sl.source_pk->>'_id')::int
        WHERE sl.entity_id = $1
          AND sl.source_schema = 'fed'
          AND sl.source_table = 'grants_contributions'
          AND gc.is_amendment = false
      `, [entityId]),
      safeQuery(`
        SELECT 'ab_grants' AS source, 'ab.ab_grants' AS source_table,
               jsonb_build_object('id', g.id) AS source_pk,
               g.amount, g.payment_date AS date, g.recipient AS recipient_name,
               g.ministry AS funder_name, g.program AS program_name,
               g.display_fiscal_year AS fiscal_year_label
        FROM general.entity_source_links sl
        JOIN ab.ab_grants g ON g.id = (sl.source_pk->>'id')::int
        WHERE sl.entity_id = $1 AND sl.source_schema = 'ab' AND sl.source_table = 'ab_grants'
      `, [entityId]),
      safeQuery(`
        SELECT 'ab_contracts' AS source, 'ab.ab_contracts' AS source_table,
               jsonb_build_object('id', c.id) AS source_pk,
               c.amount, NULL::date AS date, c.recipient AS recipient_name,
               c.ministry AS funder_name, NULL::text AS program_name,
               c.display_fiscal_year AS fiscal_year_label
        FROM general.entity_source_links sl
        JOIN ab.ab_contracts c ON c.id = (sl.source_pk->>'id')::uuid
        WHERE sl.entity_id = $1 AND sl.source_schema = 'ab' AND sl.source_table = 'ab_contracts'
      `, [entityId]),
      safeQuery(`
        SELECT 'ab_sole_source' AS source, 'ab.ab_sole_source' AS source_table,
               jsonb_build_object('id', ss.id) AS source_pk,
               ss.amount, ss.start_date AS date, ss.vendor AS recipient_name,
               ss.ministry AS funder_name, ss.contract_services AS program_name,
               ss.display_fiscal_year AS fiscal_year_label
        FROM general.entity_source_links sl
        JOIN ab.ab_sole_source ss ON ss.id = (sl.source_pk->>'id')::uuid
        WHERE sl.entity_id = $1 AND sl.source_schema = 'ab' AND sl.source_table = 'ab_sole_source'
      `, [entityId]),
    ]);

    const normalize = (row) => ({
      source: row.source,
      source_table: row.source_table,
      source_pk: row.source_pk || {},
      amount: toNumber(row.amount),
      date: row.date ? new Date(row.date).toISOString().slice(0, 10) : null,
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
    const positiveEvents = events.filter(e => e.amount > 0);
    for (const e of events) {
      if (e.source === 'fed') bySource.fed_current += e.amount;
      if (e.source === 'ab_grants') bySource.ab_grants += e.amount;
      if (e.source === 'ab_contracts') bySource.ab_contracts += e.amount;
      if (e.source === 'ab_sole_source') bySource.ab_sole_source += e.amount;
    }
    bySource.fed_original = fedOriginalEvents.reduce((sum, e) => sum + e.amount, 0);

    const funderTotals = {};
    for (const e of positiveEvents) {
      const name = e.funder_name || 'Unknown funder';
      funderTotals[name] = (funderTotals[name] || 0) + e.amount;
    }
    const topFunder = Object.entries(funderTotals).sort((a, b) => b[1] - a[1])[0] || [null, 0];
    const largest = positiveEvents.slice().sort((a, b) => b.amount - a.amount)[0] || null;
    const latest = positiveEvents.filter(e => e.date).sort((a, b) => String(b.date).localeCompare(String(a.date)))[0] || null;
    const totalExternal = bySource.fed_current + bySource.ab_grants + bySource.ab_contracts + bySource.ab_sole_source;

    return {
      ...bySource,
      fed_current_commitment_total: bySource.fed_current,
      fed_original_commitment_total: bySource.fed_original,
      ab_grants_total: bySource.ab_grants,
      ab_contracts_total: bySource.ab_contracts,
      ab_sole_source_total: bySource.ab_sole_source,
      total_external_public_funding: totalExternal,
      funding_event_count: positiveEvents.length,
      largest_public_funding_event_amount: largest?.amount || 0,
      largest_public_funding_event_date: largest?.date || null,
      last_public_funding_date: latest?.date || null,
      top_funder_name: topFunder[0],
      top_funder_amount: topFunder[1],
      top_funder_share: totalExternal > 0 ? Number((topFunder[1] / totalExternal * 100).toFixed(1)) : 0,
      events,
    };
  }

  async function getCraFilingContinuity(bnRoot, fundingSummary) {
    if (!bnRoot) {
      return {
        available: false,
        filing_continuity_limitations: ['CRA filing continuity requires a usable business number root.'],
      };
    }
    const rows = await safeQuery(`
      SELECT DISTINCT fiscal_year::int AS fiscal_year
      FROM cra.cra_identification
      WHERE LEFT(bn, 9) = $1
      UNION
      SELECT DISTINCT EXTRACT(YEAR FROM fpe)::int AS fiscal_year
      FROM cra.cra_financial_details
      WHERE LEFT(bn, 9) = $1
      ORDER BY fiscal_year
    `, [bnRoot]);
    const years = rows.map(r => Number(r.fiscal_year)).filter(Boolean).sort((a, b) => a - b);
    if (!years.length) {
      return {
        available: false,
        cra_filing_years: [],
        filing_continuity_limitations: ['No CRA filings found for this business number root in the available data.'],
      };
    }
    const first = years[0];
    const last = years[years.length - 1];
    const fullRange = Array.from({ length: last - first + 1 }, (_, i) => first + i);
    const missing = fullRange.filter(y => !years.includes(y));
    const now = currentDate();
    const majorDate = fundingSummary.largest_public_funding_event_date || fundingSummary.last_public_funding_date;
    const majorWindowEnd = majorDate ? addMonths(majorDate, 12) : null;
    const majorWindowMature = majorWindowEnd ? majorWindowEnd <= now : false;
    const majorYear = majorDate ? new Date(majorDate).getUTCFullYear() : null;
    const nextFiling = majorYear ? years.find(y => y >= majorYear) : null;
    const hasWithin12 = majorYear && majorWindowMature ? years.some(y => y === majorYear || y === majorYear + 1) : null;
    const lastFundingWindowEnd = fundingSummary.last_public_funding_date ? addMonths(fundingSummary.last_public_funding_date, 12) : null;
    const lastFundingWindowMature = lastFundingWindowEnd ? lastFundingWindowEnd <= now : false;
    const hasLaterActivitySignal = !fundingSummary.last_public_funding_date || !lastFundingWindowMature
      ? null
      : last >= new Date(fundingSummary.last_public_funding_date).getUTCFullYear();

    return {
      available: true,
      cra_filing_years: years,
      cra_filing_count: years.length,
      cra_first_filing_year: first,
      cra_last_filing_year: last,
      available_cra_year_window: { first_year: first, last_year: last },
      missing_years_after_first_filing: missing,
      last_filing_after_last_major_funding: majorYear ? last >= majorYear : null,
      has_filing_within_12_months_after_major_funding: hasWithin12,
      post_funding_observation_window_mature: majorWindowMature,
      next_filing_year_after_major_funding: nextFiling || null,
      has_later_activity_signal: hasLaterActivitySignal,
      filing_continuity_limitations: ['Missing CRA filings are review signals, not proof that an organization stopped operating.'],
    };
  }

  async function getDependencyMetrics(bnRoot) {
    if (!bnRoot) {
      return { available: false, rows: [], dependency_limitations: ['Public-funding dependency requires CRA revenue data and a usable BN root.'] };
    }
    const rows = await safeQuery(`
      SELECT fiscal_year, federal, provincial, municipal, combined_sectiond,
             total_govt, revenue, govt_share_of_rev
      FROM cra.govt_funding_by_charity
      WHERE LEFT(bn, 9) = $1
      ORDER BY fiscal_year
    `, [bnRoot]);
    const normalized = rows.map(r => ({
      fiscal_year: r.fiscal_year,
      federal: toNumber(r.federal),
      provincial: toNumber(r.provincial),
      municipal: toNumber(r.municipal),
      combined_sectiond: toNumber(r.combined_sectiond),
      total_govt: toNumber(r.total_govt),
      revenue: toNumber(r.revenue),
      govt_share_of_rev: toNullableNumber(r.govt_share_of_rev),
    }));
    const ratios = normalized.map(r => r.govt_share_of_rev).filter(r => r !== null);
    return {
      available: normalized.length > 0,
      rows: normalized,
      max_dependency_ratio: ratios.length ? Math.max(...ratios) : null,
      latest_dependency_ratio: normalized.length ? normalized[normalized.length - 1].govt_share_of_rev : null,
      dependency_years_over_70: normalized.filter(r => toNumber(r.govt_share_of_rev) >= 70).map(r => r.fiscal_year),
      dependency_years_over_80: normalized.filter(r => toNumber(r.govt_share_of_rev) >= 80).map(r => r.fiscal_year),
      dependency_limitations: normalized.length ? [] : ['No CRA government-funding dependency rows found for this BN root.'],
    };
  }

  async function getAbLifecycleSignals(entityId) {
    const rows = await safeQuery(`
      SELECT np.status, sl.description AS status_description, np.registration_date,
             np.legal_name, np.type, np.city, np.postal_code
      FROM general.entity_source_links esl
      JOIN ab.ab_non_profit np ON np.id = (esl.source_pk->>'id')::uuid
      LEFT JOIN ab.ab_non_profit_status_lookup sl ON sl.status = np.status
      WHERE esl.entity_id = $1
        AND esl.source_schema = 'ab'
        AND esl.source_table = 'ab_non_profit'
      ORDER BY np.registration_date DESC NULLS LAST
      LIMIT 5
    `, [entityId]);
    const primary = rows[0];
    const statusText = `${primary?.status || ''} ${primary?.status_description || ''}`.toLowerCase();
    const inactive = /\b(dissolved|struck|inactive|cancelled|canceled|revoked)\b/.test(statusText);
    return {
      available: rows.length > 0,
      ab_non_profit_status: primary?.status || null,
      ab_non_profit_status_description: primary?.status_description || null,
      ab_non_profit_registration_date: primary?.registration_date ? new Date(primary.registration_date).toISOString().slice(0, 10) : null,
      status_source: rows.length ? 'ab.ab_non_profit' : null,
      is_inactive_status: inactive,
      records: rows,
      lifecycle_limitations: rows.length ? [] : ['No linked Alberta non-profit registry record found for this entity.'],
    };
  }

  async function getDataQuality(identity, dependency) {
    const warnings = [];
    if (!identity.bn_root) {
      warnings.push({
        code: 'NO_BN_ROOT',
        label: 'No usable BN root is available; some CRA continuity and dependency checks may be unavailable.',
        source_trace: [{ source_table: 'general.entities', fields: ['bn_root'] }],
      });
    }
    if (!dependency.available) {
      warnings.push({
        code: 'NO_CRA_DEPENDENCY_DATA',
        label: 'CRA public-funding dependency ratio is unavailable for this entity.',
        source_trace: [{ source_table: 'cra.govt_funding_by_charity' }],
      });
    }
    if ((identity.confidence !== null && identity.confidence !== undefined && Number(identity.confidence) < 0.75)) {
      warnings.push({
        code: 'LOW_ENTITY_CONFIDENCE',
        label: 'Entity-resolution confidence is below 0.75 and should be reviewed.',
        source_trace: [{ source_table: 'general.entity_golden_records', fields: ['confidence'] }],
      });
    }
    warnings.push({
      code: 'FED_AMENDMENT_HANDLING',
      label: 'Federal funding totals use current-agreement logic to avoid double-counting amendment rows.',
      source_trace: [{ source_table: 'fed.grants_contributions', fields: ['ref_number', 'amendment_number', 'agreement_value'] }],
    });
    return warnings;
  }

  async function getPeerComparison(profile) {
    const identity = profile.identity;
    const total = profile.funding.total_external_public_funding || 0;
    const lower = total > 0 ? total * 0.25 : 0;
    const upper = total > 0 ? total * 4 : 1000000;
    const rows = await safeQuery(`
      SELECT entity_id, canonical_name, entity_type, dataset_sources,
             total_all_funding::float AS total_all_funding,
             cra_filing_count
      FROM general.vw_entity_funding
      WHERE entity_id != $1
        AND ($2::text IS NULL OR entity_type = $2)
        AND total_all_funding BETWEEN $3 AND $4
      ORDER BY total_all_funding
      LIMIT 500
    `, [identity.entity_id, identity.entity_type || null, lower, upper]);
    if (rows.length < 10) {
      return { available: false, reason: 'Peer group too small for defensible comparison', peer_group_size: rows.length };
    }
    const fundingValues = rows.map(r => toNumber(r.total_all_funding)).sort((a, b) => a - b);
    const filingValues = rows.map(r => toNumber(r.cra_filing_count)).sort((a, b) => a - b);
    const pct = (values, value) => Number((values.filter(v => v <= value).length / values.length * 100).toFixed(1));
    return {
      available: true,
      peer_group_size: rows.length,
      peer_basis: {
        entity_type: identity.entity_type || null,
        funding_scale_min: lower,
        funding_scale_max: upper,
      },
      funding_percentile: pct(fundingValues, total),
      dependency_percentile: null,
      largest_event_percentile: null,
      filing_continuity_percentile: profile.filing_continuity.cra_filing_count ? pct(filingValues, profile.filing_continuity.cra_filing_count) : null,
      top_funder_concentration_percentile: null,
      limitations: ['Peer comparison uses the existing aggregate entity-funding view for candidate grouping; exact profile funding uses current-agreement federal logic.'],
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
    for (const year of profile.filing_continuity.cra_filing_years || []) {
      events.push({
        date: `${year}-12-31`,
        year_label: String(year),
        event_type: 'cra_filing',
        source: 'cra',
        label: 'CRA filing found',
        amount: null,
        description: `CRA filing year ${year}`,
        source_table: 'cra.cra_identification / cra.cra_financial_details',
        source_pk: { bn_root: profile.identity.bn_root, fiscal_year: year },
      });
    }
    for (const row of profile.dependency.rows || []) {
      if (toNumber(row.govt_share_of_rev) >= 70) {
        events.push({
          date: `${row.fiscal_year}-12-31`,
          year_label: String(row.fiscal_year),
          event_type: 'dependency_threshold',
          source: 'cra',
          label: toNumber(row.govt_share_of_rev) >= 80 ? '80% public-funding dependency threshold exceeded' : '70% public-funding dependency threshold exceeded',
          amount: row.total_govt,
          description: `Government funding was ${row.govt_share_of_rev}% of reported revenue.`,
          source_table: 'cra.govt_funding_by_charity',
          source_pk: { bn_root: profile.identity.bn_root, fiscal_year: row.fiscal_year },
        });
      }
    }
    if (profile.lifecycle.available) {
      events.push({
        date: profile.lifecycle.ab_non_profit_registration_date,
        year_label: profile.lifecycle.ab_non_profit_registration_date?.slice(0, 4) || null,
        event_type: 'lifecycle_status',
        source: 'ab',
        label: 'AB non-profit registry status',
        amount: null,
        description: `${profile.lifecycle.ab_non_profit_status || ''} ${profile.lifecycle.ab_non_profit_status_description || ''}`.trim(),
        source_table: 'ab.ab_non_profit',
        source_pk: { entity_id: profile.identity.entity_id },
      });
    }
    const majorDate = profile.funding.largest_public_funding_event_date;
    if (majorDate) {
      const d = new Date(majorDate);
      const end = new Date(d);
      end.setUTCFullYear(end.getUTCFullYear() + 1);
      events.push({
        date: d.toISOString().slice(0, 10),
        year_label: String(d.getUTCFullYear()),
        event_type: 'review_window_start',
        source: 'derived',
        label: '12-month review window starts',
        amount: null,
        description: 'Window begins at largest public funding event.',
        source_table: 'derived',
        source_pk: {},
      });
      events.push({
        date: end.toISOString().slice(0, 10),
        year_label: String(end.getUTCFullYear()),
        event_type: 'review_window_end',
        source: 'derived',
        label: '12-month review window ends',
        amount: null,
        description: 'Challenge-aligned post-funding review window.',
        source_table: 'derived',
        source_pk: {},
      });
    }
    return events.sort((a, b) => String(a.date || '').localeCompare(String(b.date || '')));
  }

  function buildSourceTrace(profile) {
    return unique([
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
    ]).map(source_table => ({ source_table }));
  }

  function buildLimitations(profile) {
    return unique([
      'This profile identifies review signals, not fraud, misconduct, or proof of ceased operations.',
      'Missing filings mean no filing was found in available data; they are not proof of inactivity outside these datasets.',
      'Federal totals use current-agreement logic to avoid amendment double-counting.',
      ...(profile.filing_continuity.filing_continuity_limitations || []),
      ...(profile.dependency.dependency_limitations || []),
      ...(profile.lifecycle.lifecycle_limitations || []),
      ...(profile.peer_comparison.limitations || []),
    ]);
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
    const dataQuality = await getDataQuality(identity, dependency);
    const baseProfile = {
      entity: {
        id: identity.entity_id,
        canonical_name: identity.canonical_name,
        bn_root: identity.bn_root,
        entity_type: identity.entity_type,
        dataset_sources: identity.dataset_sources,
      },
      identity,
      funding,
      filing_continuity: filingContinuity,
      dependency,
      lifecycle,
      peer_comparison: {},
      flags: [],
      score: {},
      data_quality: dataQuality,
      source_trace: [],
      limitations: [],
    };
    baseProfile.peer_comparison = await getPeerComparison(baseProfile);
    baseProfile.timeline = buildTimeline(baseProfile);
    baseProfile.flags = computeRiskFlags(baseProfile);
    baseProfile.score = computeRiskScore(baseProfile.flags, {
      total_external_public_funding: funding.total_external_public_funding,
      max_dependency_ratio: dependency.max_dependency_ratio,
    });
    baseProfile.source_trace = buildSourceTrace(baseProfile);
    baseProfile.limitations = buildLimitations(baseProfile);
    return baseProfile;
  }

  async function buildReviewQueue(limit = DEFAULT_REVIEW_QUEUE_LIMIT) {
    const candidates = await safeQuery(`
      SELECT entity_id
      FROM general.vw_entity_funding
      WHERE total_all_funding > 0
      ORDER BY total_all_funding DESC NULLS LAST
      LIMIT $1
    `, [Math.max(limit * 3, 50)]);
    const profiles = [];
    for (const row of candidates) {
      const profile = await buildRiskProfile(row.entity_id);
      if (profile) profiles.push(profile);
    }
    return profiles
      .sort((a, b) => b.score.total - a.score.total || b.funding.total_external_public_funding - a.funding.total_external_public_funding)
      .slice(0, limit)
      .map(p => ({
        entity_id: p.entity.id,
        canonical_name: p.entity.canonical_name,
        bn_root: p.entity.bn_root,
        entity_type: p.entity.entity_type,
        dataset_sources: p.entity.dataset_sources,
        source_link_count: p.identity.source_link_count,
        total_external_public_funding: p.funding.total_external_public_funding,
        largest_funding_event_amount: p.funding.largest_public_funding_event_amount,
        largest_funding_event_date: p.funding.largest_public_funding_event_date,
        last_public_funding_date: p.funding.last_public_funding_date,
        last_cra_filing_year: p.filing_continuity.cra_last_filing_year || null,
        max_public_funding_dependency_ratio: p.dependency.max_dependency_ratio,
        top_flag: p.flags[0]?.code || null,
        review_priority: p.score.band,
        score: p.score.total,
      }));
  }

  async function buildCaseFile(entityId) {
    const riskProfile = await buildRiskProfile(entityId);
    if (!riskProfile) return null;
    return {
      generated_at: new Date().toISOString(),
      format: 'recipient-risk-case-file/v1',
      risk_profile: riskProfile,
    };
  }

  async function generateRiskExplanation(profile) {
    const apiKey = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;
    if (!apiKey) {
      return {
        available: false,
        summary: 'AI explanation unavailable because no Gemini API key is configured.',
        key_evidence: [],
        review_questions: [
          'Was the organization still active after the final public funding event?',
          'Was public funding reported consistently in available CRA filings?',
          'Should related entities or aliases be reviewed together?',
        ],
        limitations: ['Deterministic risk profile is still available and authoritative.'],
      };
    }

    const model = process.env.GEMINI_MODEL || 'gemini-1.5-flash';
    const prompt = [
      'You are summarizing a structured public-funding recipient risk profile.',
      'Use only the provided JSON evidence.',
      'Do not infer fraud, abuse, corruption, misconduct, bankruptcy, or dissolution unless explicitly supported by source fields.',
      'Use review-priority language. If evidence is missing, say it is missing.',
      'Return strict JSON with keys: summary, key_evidence, review_questions, limitations.',
      JSON.stringify(profile),
    ].join('\n\n');

    const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] }),
    });
    if (!response.ok) throw new Error(`Gemini API error ${response.status}: ${await response.text()}`);
    const data = await response.json();
    const text = data.candidates?.[0]?.content?.parts?.map(p => p.text).join('\n') || '{}';
    const jsonText = text.replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```$/i, '').trim();
    const parsed = JSON.parse(jsonText);
    return {
      available: true,
      summary: parsed.summary || '',
      key_evidence: Array.isArray(parsed.key_evidence) ? parsed.key_evidence : [],
      review_questions: Array.isArray(parsed.review_questions) ? parsed.review_questions : [],
      limitations: Array.isArray(parsed.limitations) ? parsed.limitations : [],
    };
  }

  return {
    buildRiskProfile,
    buildReviewQueue,
    buildCaseFile,
    generateRiskExplanation,
  };
}

module.exports = {
  createRiskService,
  computeRiskFlags,
  computeRiskScore,
  makeFlag,
  bandForScore,
};
