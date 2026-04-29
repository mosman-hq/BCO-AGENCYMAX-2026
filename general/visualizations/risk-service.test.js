const test = require('node:test');
const assert = require('node:assert/strict');
const { computeRiskFlags, computeRiskScore } = require('./risk-service');

function baseProfile(overrides = {}) {
  return {
    identity: {
      entity_id: 1,
      canonical_name: 'Example Organization',
      bn_root: '123456789',
      bn_variants: ['123456789RR0001'],
      confidence: 0.95,
      related_entities: [],
    },
    funding: {
      total_external_public_funding: 0,
      funding_event_count: 0,
      largest_public_funding_event_amount: 0,
      largest_public_funding_event_date: null,
      last_public_funding_date: null,
      top_funder_share: 0,
    },
    filing_continuity: {
      cra_filing_years: [2020, 2021, 2022, 2023, 2024],
      cra_last_filing_year: 2024,
      missing_years_after_first_filing: [],
      has_filing_within_12_months_after_major_funding: null,
      has_later_activity_signal: true,
    },
    dependency: {
      available: true,
      rows: [],
      max_dependency_ratio: null,
      dependency_years_over_70: [],
      dependency_years_over_80: [],
    },
    lifecycle: {
      available: false,
      is_inactive_status: false,
    },
    data_quality: [],
    ...overrides,
  };
}

function codes(flags) {
  return flags.map(f => f.code);
}

test('dependency over 70 creates 70 threshold flag', () => {
  const flags = computeRiskFlags(baseProfile({
    dependency: {
      available: true,
      rows: [{ fiscal_year: 2023, govt_share_of_rev: 72, total_govt: 720, revenue: 1000 }],
      max_dependency_ratio: 72,
    },
  }));
  assert.deepEqual(codes(flags), ['PUBLIC_DEPENDENCY_70']);
});

test('dependency over 80 creates both challenge threshold flags', () => {
  const flags = computeRiskFlags(baseProfile({
    dependency: {
      available: true,
      rows: [{ fiscal_year: 2023, govt_share_of_rev: 84, total_govt: 840, revenue: 1000 }],
      max_dependency_ratio: 84,
    },
  }));
  assert.equal(codes(flags).includes('PUBLIC_DEPENDENCY_80'), true);
  assert.equal(codes(flags).includes('PUBLIC_DEPENDENCY_70'), true);
});

test('large funding with no later filing creates filing and activity review flags', () => {
  const flags = computeRiskFlags(baseProfile({
    funding: {
      total_external_public_funding: 1200000,
      funding_event_count: 1,
      largest_public_funding_event_amount: 1200000,
      largest_public_funding_event_date: '2023-06-01',
      last_public_funding_date: '2023-06-01',
      top_funder_share: 100,
      top_funder_name: 'Example Department',
      top_funder_amount: 1200000,
    },
    filing_continuity: {
      cra_filing_years: [2020, 2021, 2022],
      cra_last_filing_year: 2022,
      missing_years_after_first_filing: [],
      has_filing_within_12_months_after_major_funding: false,
      has_later_activity_signal: false,
    },
  }));
  assert.equal(codes(flags).includes('NO_FILING_AFTER_FUNDING_12M'), true);
  assert.equal(codes(flags).includes('POST_FUNDING_NO_ACTIVITY'), true);
  assert.equal(codes(flags).includes('LARGE_PUBLIC_FUNDING_EVENT'), true);
});

test('missing BN creates identity warning without creating false CRA filing claim', () => {
  const flags = computeRiskFlags(baseProfile({
    identity: {
      entity_id: 1,
      canonical_name: 'No BN Organization',
      bn_root: null,
      bn_variants: [],
      confidence: 0.95,
      related_entities: [],
    },
    filing_continuity: {
      available: false,
      filing_continuity_limitations: ['CRA filing continuity requires a usable business number root.'],
    },
    dependency: {
      available: false,
      rows: [],
      max_dependency_ratio: null,
    },
  }));
  assert.equal(codes(flags).includes('MISSING_OR_MALFORMED_BN'), true);
  assert.equal(codes(flags).includes('NO_FILING_AFTER_FUNDING_12M'), false);
});

test('inactive Alberta lifecycle status creates lifecycle flag only when source supports it', () => {
  const activeFlags = computeRiskFlags(baseProfile({
    lifecycle: { available: true, is_inactive_status: false, ab_non_profit_status: 'A' },
  }));
  assert.equal(codes(activeFlags).includes('AB_DISSOLVED_OR_STRUCK_AFTER_FUNDING'), false);

  const inactiveFlags = computeRiskFlags(baseProfile({
    lifecycle: {
      available: true,
      is_inactive_status: true,
      ab_non_profit_status: 'D',
      ab_non_profit_status_description: 'Dissolved',
      ab_non_profit_registration_date: '2018-01-01',
      status_source: 'ab.ab_non_profit',
    },
  }));
  assert.equal(codes(inactiveFlags).includes('AB_DISSOLVED_OR_STRUCK_AFTER_FUNDING'), true);
});

test('score bands calculate from component flags', () => {
  const flags = computeRiskFlags(baseProfile({
    funding: {
      total_external_public_funding: 1200000,
      funding_event_count: 1,
      largest_public_funding_event_amount: 1200000,
      largest_public_funding_event_date: '2023-06-01',
      last_public_funding_date: '2023-06-01',
      top_funder_share: 100,
      top_funder_name: 'Example Department',
      top_funder_amount: 1200000,
    },
    filing_continuity: {
      cra_filing_years: [2020, 2021, 2022],
      cra_last_filing_year: 2022,
      missing_years_after_first_filing: [2021],
      has_filing_within_12_months_after_major_funding: false,
      has_later_activity_signal: false,
    },
    dependency: {
      available: true,
      rows: [{ fiscal_year: 2022, govt_share_of_rev: 85, total_govt: 850000, revenue: 1000000 }],
      max_dependency_ratio: 85,
    },
  }));
  const score = computeRiskScore(flags);
  assert.equal(score.total >= 60, true);
  assert.equal(score.band, 'High Review Priority');
  assert.equal(score.components.length, 5);
});
