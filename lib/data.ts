export interface FundingEvent {
  source: string
  program: string
  amount: number
  date: string
  yearLabel: string
}

export interface FilingYear {
  year: number
  filed: boolean
}

export interface Flag {
  code: string
  label: string
  severity: 'high' | 'medium' | 'low'
  points: number
  explanation: string
  evidence: string
  sourceTable: string
  sourceField: string
}

export interface ScoreComponent {
  name: string
  points: number
  maxPoints: number
  explanation: string
}

export interface Organization {
  id: string
  name: string
  bnRoot: string
  sector: string
  province: string
  totalFunding: number
  totalRevenue: number
  dependencyRatio: number
  lastFilingDate: string
  lastFilingYear: number
  firstFilingYear: number
  filingYears: number[]
  daysSinceActivity: number
  monthsPostFundingActivity: number
  score: number
  reviewStatus: 'HIGH REVIEW PRIORITY' | 'MEDIUM REVIEW PRIORITY' | 'LOW REVIEW PRIORITY'
  criterionA: boolean
  criterionB: boolean
  bothCriteria: boolean
  employees: number | null
  hasAddress: boolean
  revenueFromTransfers: number
  fundingEvents: FundingEvent[]
  flags: Flag[]
  scoreComponents: ScoreComponent[]
  dataSources: string[]
  relatedEntities: string[]
}

export const ORGANIZATIONS: Organization[] = [
  {
    id: 'ent-001',
    name: 'Northgate Solutions Inc.',
    bnRoot: '119234567',
    sector: 'Social Services',
    province: 'Ontario',
    totalFunding: 2400000,
    totalRevenue: 2553191,
    dependencyRatio: 94,
    lastFilingDate: '2023-11-12',
    lastFilingYear: 2023,
    firstFilingYear: 2016,
    filingYears: [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023],
    daysSinceActivity: 534,
    monthsPostFundingActivity: 4,
    score: 91,
    reviewStatus: 'HIGH REVIEW PRIORITY',
    criterionA: true,
    criterionB: true,
    bothCriteria: true,
    employees: 0,
    hasAddress: false,
    revenueFromTransfers: 2400000,
    fundingEvents: [
      { source: 'Employment and Social Development Canada', program: 'Community Employment Fund', amount: 1200000, date: '2022-04-15', yearLabel: '2022' },
      { source: 'Employment and Social Development Canada', program: 'Skills & Partnership Fund', amount: 800000, date: '2023-01-10', yearLabel: '2023' },
      { source: 'Ontario Ministry of Community', program: 'Community Opportunity Program', amount: 400000, date: '2023-06-20', yearLabel: '2023' },
    ],
    flags: [
      { code: 'ZOMBIE_INACTIVITY', label: 'Post-Funding Inactivity Detected', severity: 'high', points: 30, explanation: 'Organization ceased CRA filings within 4 months of receiving $2.4M in public funding. No subsequent filings observed.', evidence: 'Last CRA filing: 2023. Last funding event: 2023-06-20. Corporate registry dissolved 2024-01-15.', sourceTable: 'cra_t3010', sourceField: 'fiscal_period_end' },
      { code: 'HIGH_DEPENDENCY', label: 'Extreme Public Funding Dependency', severity: 'high', points: 15, explanation: '94% of total revenue came from government sources. The organization had virtually no independent revenue.', evidence: 'Total revenue: $2.55M. Government transfers: $2.4M. Own-source revenue: $153K.', sourceTable: 'cra_t3010', sourceField: 'total_revenue, gov_funding' },
      { code: 'NO_EMPLOYEES', label: 'No Employees on Record', severity: 'medium', points: 8, explanation: 'Organization reported zero employees across all available filings despite receiving $2.4M in funding.', evidence: 'Employee count: 0 across 2016-2023 filings.', sourceTable: 'cra_t3010', sourceField: 'num_employees' },
      { code: 'NO_ADDRESS', label: 'No Verifiable Physical Address', severity: 'medium', points: 7, explanation: 'No physical address could be verified through corporate registry or CRA filings.', evidence: 'Address field: null in corporate registry. CRA address: PO Box only.', sourceTable: 'corporate_registry', sourceField: 'registered_address' },
    ],
    scoreComponents: [
      { name: 'Post-Funding Inactivity', points: 30, maxPoints: 30, explanation: 'Ceased filings within 12 months of last major funding event' },
      { name: 'Public Funding Dependency', points: 15, maxPoints: 25, explanation: '94% dependency exceeds the 80% threshold' },
      { name: 'Funding Size & Recency', points: 18, maxPoints: 20, explanation: '$2.4M total, most recent funding in 2023' },
      { name: 'Organizational Capacity', points: 15, maxPoints: 15, explanation: 'No employees, no physical address, no own-source revenue' },
      { name: 'Relationship & Patterns', points: 3, maxPoints: 10, explanation: 'One director appears on one other flagged organization' },
    ],
    dataSources: ['cra', 'fed', 'corporate_registry'],
    relatedEntities: ['ent-009'],
  },
  {
    id: 'ent-002',
    name: 'Prairie Community Trust',
    bnRoot: '123456789',
    sector: 'Community Development',
    province: 'Saskatchewan',
    totalFunding: 890000,
    totalRevenue: 1022988,
    dependencyRatio: 87,
    lastFilingDate: '2024-01-05',
    lastFilingYear: 2024,
    firstFilingYear: 2018,
    filingYears: [2018, 2019, 2020, 2022, 2024],
    daysSinceActivity: 480,
    monthsPostFundingActivity: 10,
    score: 78,
    reviewStatus: 'HIGH REVIEW PRIORITY',
    criterionA: true,
    criterionB: true,
    bothCriteria: true,
    employees: 2,
    hasAddress: true,
    revenueFromTransfers: 890000,
    fundingEvents: [
      { source: 'Western Economic Diversification', program: 'Community Futures', amount: 450000, date: '2022-09-01', yearLabel: '2022' },
      { source: 'Saskatchewan Ministry of Social Services', program: 'Community-Based Organization Fund', amount: 440000, date: '2023-03-15', yearLabel: '2023' },
    ],
    flags: [
      { code: 'ZOMBIE_INACTIVITY', label: 'Post-Funding Inactivity Detected', severity: 'high', points: 25, explanation: 'Filing gap detected: no CRA filing for 2021, 2023. Activity ceased within 10 months after major funding.', evidence: 'Filing years: 2018-2020, 2022, 2024. Gap years: 2021, 2023.', sourceTable: 'cra_t3010', sourceField: 'fiscal_period_end' },
      { code: 'HIGH_DEPENDENCY', label: 'High Public Funding Dependency', severity: 'high', points: 15, explanation: '87% of total revenue came from government sources.', evidence: 'Total revenue: $1.02M. Government transfers: $890K.', sourceTable: 'cra_t3010', sourceField: 'total_revenue, gov_funding' },
      { code: 'FILING_GAP', label: 'CRA Filing Gaps', severity: 'medium', points: 10, explanation: 'Two filing gaps detected (2021, 2023) suggesting intermittent organizational activity.', evidence: 'Expected filings: 7. Actual filings: 5. Gap years: 2021, 2023.', sourceTable: 'cra_t3010', sourceField: 'fiscal_period_end' },
    ],
    scoreComponents: [
      { name: 'Post-Funding Inactivity', points: 25, maxPoints: 30, explanation: 'Filing gaps within 12 months of funding but not complete cessation' },
      { name: 'Public Funding Dependency', points: 15, maxPoints: 25, explanation: '87% dependency exceeds the 80% threshold' },
      { name: 'Funding Size & Recency', points: 14, maxPoints: 20, explanation: '$890K total, moderately recent' },
      { name: 'Organizational Capacity', points: 4, maxPoints: 15, explanation: '2 employees reported, address on file' },
      { name: 'Relationship & Patterns', points: 0, maxPoints: 10, explanation: 'No linked flagged entities detected' },
    ],
    dataSources: ['cra', 'fed'],
    relatedEntities: [],
  },
  {
    id: 'ent-003',
    name: 'Clearwater Innovations Ltd.',
    bnRoot: '234567890',
    sector: 'Technology',
    province: 'British Columbia',
    totalFunding: 1100000,
    totalRevenue: 1208791,
    dependencyRatio: 91,
    lastFilingDate: '2023-12-20',
    lastFilingYear: 2023,
    firstFilingYear: 2019,
    filingYears: [2019, 2020, 2021, 2022, 2023],
    daysSinceActivity: 496,
    monthsPostFundingActivity: 7,
    score: 88,
    reviewStatus: 'HIGH REVIEW PRIORITY',
    criterionA: true,
    criterionB: true,
    bothCriteria: true,
    employees: 0,
    hasAddress: true,
    revenueFromTransfers: 1100000,
    fundingEvents: [
      { source: 'Innovation, Science and Economic Development', program: 'Strategic Innovation Fund', amount: 700000, date: '2022-06-01', yearLabel: '2022' },
      { source: 'BC Ministry of Jobs', program: 'BC Tech Fund', amount: 400000, date: '2023-05-15', yearLabel: '2023' },
    ],
    flags: [
      { code: 'ZOMBIE_INACTIVITY', label: 'Post-Funding Inactivity Detected', severity: 'high', points: 28, explanation: 'Organization missed T2 corporate filing deadline. No CRA activity since December 2023 despite funding in May 2023.', evidence: 'Last CRA filing: 2023-12-20. Last funding: 2023-05-15. No subsequent activity.', sourceTable: 'cra_t3010', sourceField: 'fiscal_period_end' },
      { code: 'HIGH_DEPENDENCY', label: 'Extreme Public Funding Dependency', severity: 'high', points: 15, explanation: '91% of total revenue came from government sources.', evidence: 'Total revenue: $1.21M. Government transfers: $1.1M.', sourceTable: 'cra_t3010', sourceField: 'total_revenue, gov_funding' },
      { code: 'NO_EMPLOYEES', label: 'No Employees on Record', severity: 'medium', points: 8, explanation: 'Zero employees reported despite receiving $1.1M in innovation funding.', evidence: 'Employee count: 0 across all filings.', sourceTable: 'cra_t3010', sourceField: 'num_employees' },
    ],
    scoreComponents: [
      { name: 'Post-Funding Inactivity', points: 28, maxPoints: 30, explanation: 'Ceased within 7 months of last funding' },
      { name: 'Public Funding Dependency', points: 15, maxPoints: 25, explanation: '91% dependency exceeds the 80% threshold' },
      { name: 'Funding Size & Recency', points: 16, maxPoints: 20, explanation: '$1.1M total, recent funding in 2023' },
      { name: 'Organizational Capacity', points: 8, maxPoints: 15, explanation: 'No employees, but has address on file' },
      { name: 'Relationship & Patterns', points: 1, maxPoints: 10, explanation: 'Minor pattern overlap detected' },
    ],
    dataSources: ['cra', 'fed', 'corporate_registry'],
    relatedEntities: [],
  },
  {
    id: 'ent-004',
    name: 'Capital Bridge Group',
    bnRoot: '345678901',
    sector: 'Financial Services',
    province: 'Alberta',
    totalFunding: 3200000,
    totalRevenue: 4102564,
    dependencyRatio: 78,
    lastFilingDate: '2024-02-01',
    lastFilingYear: 2024,
    firstFilingYear: 2015,
    filingYears: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
    daysSinceActivity: 452,
    monthsPostFundingActivity: 15,
    score: 62,
    reviewStatus: 'MEDIUM REVIEW PRIORITY',
    criterionA: false,
    criterionB: true,
    bothCriteria: false,
    employees: 5,
    hasAddress: true,
    revenueFromTransfers: 3200000,
    fundingEvents: [
      { source: 'Alberta Innovates', program: 'Technology Commercialization', amount: 1500000, date: '2021-08-01', yearLabel: '2021' },
      { source: 'Western Economic Diversification', program: 'Regional Innovation Ecosystem', amount: 1000000, date: '2022-03-01', yearLabel: '2022' },
      { source: 'Alberta Ministry of Finance', program: 'CAIP Fund', amount: 700000, date: '2023-01-15', yearLabel: '2023' },
    ],
    flags: [
      { code: 'HIGH_DEPENDENCY', label: 'High Public Funding Dependency', severity: 'high', points: 10, explanation: '78% of total revenue came from government sources. Exceeds 70% threshold but below 80%.', evidence: 'Total revenue: $4.1M. Government transfers: $3.2M.', sourceTable: 'cra_t3010', sourceField: 'total_revenue, gov_funding' },
      { code: 'LARGE_CONCENTRATION', label: 'Concentrated Funding Source', severity: 'medium', points: 8, explanation: 'Single funder (Alberta Innovates) provided 47% of all public funding received.', evidence: 'Top funder: Alberta Innovates — $1.5M of $3.2M total.', sourceTable: 'fed_grants', sourceField: 'funder_name, amount' },
    ],
    scoreComponents: [
      { name: 'Post-Funding Inactivity', points: 8, maxPoints: 30, explanation: '15 months of activity observed after last funding — above 12-month threshold' },
      { name: 'Public Funding Dependency', points: 10, maxPoints: 25, explanation: '78% dependency exceeds 70% but not 80% threshold' },
      { name: 'Funding Size & Recency', points: 18, maxPoints: 20, explanation: '$3.2M total, significant funding across 3 years' },
      { name: 'Organizational Capacity', points: 4, maxPoints: 15, explanation: '5 employees, address on file, some own-source revenue' },
      { name: 'Relationship & Patterns', points: 2, maxPoints: 10, explanation: 'Minor overlap with one other recipient' },
    ],
    dataSources: ['cra', 'fed', 'ab'],
    relatedEntities: ['ent-007'],
  },
  {
    id: 'ent-005',
    name: 'Northern Light Society',
    bnRoot: '456789012',
    sector: 'Arts & Culture',
    province: 'Manitoba',
    totalFunding: 560000,
    totalRevenue: 674698,
    dependencyRatio: 83,
    lastFilingDate: '2023-10-30',
    lastFilingYear: 2023,
    firstFilingYear: 2017,
    filingYears: [2017, 2018, 2019, 2020, 2023],
    daysSinceActivity: 547,
    monthsPostFundingActivity: 3,
    score: 85,
    reviewStatus: 'HIGH REVIEW PRIORITY',
    criterionA: true,
    criterionB: true,
    bothCriteria: true,
    employees: null,
    hasAddress: false,
    revenueFromTransfers: 560000,
    fundingEvents: [
      { source: 'Canadian Heritage', program: 'Building Communities Through Arts', amount: 320000, date: '2022-07-01', yearLabel: '2022' },
      { source: 'Manitoba Arts Council', program: 'Operating Grants', amount: 240000, date: '2023-07-20', yearLabel: '2023' },
    ],
    flags: [
      { code: 'ZOMBIE_INACTIVITY', label: 'Post-Funding Inactivity Detected', severity: 'high', points: 30, explanation: 'Only 3 months of activity observed after last funding. Filing gaps: 2021, 2022.', evidence: 'Last funding: 2023-07-20. Last filing: 2023-10-30. Gap years: 2021, 2022.', sourceTable: 'cra_t3010', sourceField: 'fiscal_period_end' },
      { code: 'HIGH_DEPENDENCY', label: 'High Public Funding Dependency', severity: 'high', points: 15, explanation: '83% of revenue came from government sources.', evidence: 'Total revenue: $675K. Government transfers: $560K.', sourceTable: 'cra_t3010', sourceField: 'total_revenue, gov_funding' },
      { code: 'NO_ADDRESS', label: 'No Verifiable Physical Address', severity: 'medium', points: 7, explanation: 'No physical address in corporate registry or CRA records.', evidence: 'Address field: null.', sourceTable: 'corporate_registry', sourceField: 'registered_address' },
      { code: 'FILING_GAP', label: 'CRA Filing Gaps', severity: 'medium', points: 8, explanation: 'Two consecutive gap years (2021-2022) in filing history.', evidence: 'Expected: 7 years. Filed: 5 years.', sourceTable: 'cra_t3010', sourceField: 'fiscal_period_end' },
    ],
    scoreComponents: [
      { name: 'Post-Funding Inactivity', points: 30, maxPoints: 30, explanation: 'Only 3 months of post-funding activity with prior gaps' },
      { name: 'Public Funding Dependency', points: 15, maxPoints: 25, explanation: '83% dependency exceeds 80% threshold' },
      { name: 'Funding Size & Recency', points: 10, maxPoints: 20, explanation: '$560K total, recent funding in 2023' },
      { name: 'Organizational Capacity', points: 12, maxPoints: 15, explanation: 'No employee data, no physical address' },
      { name: 'Relationship & Patterns', points: 0, maxPoints: 10, explanation: 'No linked flagged entities detected' },
    ],
    dataSources: ['cra', 'fed'],
    relatedEntities: [],
  },
  {
    id: 'ent-006',
    name: 'Westfield Charitable Foundation',
    bnRoot: '567890123',
    sector: 'Social Services',
    province: 'Ontario',
    totalFunding: 1800000,
    totalRevenue: 2022471,
    dependencyRatio: 89,
    lastFilingDate: '2023-09-15',
    lastFilingYear: 2023,
    firstFilingYear: 2014,
    filingYears: [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023],
    daysSinceActivity: 592,
    monthsPostFundingActivity: 2,
    score: 86,
    reviewStatus: 'HIGH REVIEW PRIORITY',
    criterionA: true,
    criterionB: true,
    bothCriteria: true,
    employees: 1,
    hasAddress: true,
    revenueFromTransfers: 1800000,
    fundingEvents: [
      { source: 'Public Health Agency of Canada', program: 'Community Action Program for Children', amount: 900000, date: '2022-01-15', yearLabel: '2022' },
      { source: 'Employment and Social Development Canada', program: 'Social Development Partnerships', amount: 600000, date: '2023-04-01', yearLabel: '2023' },
      { source: 'Ontario Trillium Foundation', program: 'Grow Grant', amount: 300000, date: '2023-07-01', yearLabel: '2023' },
    ],
    flags: [
      { code: 'ZOMBIE_INACTIVITY', label: 'Post-Funding Inactivity Detected', severity: 'high', points: 30, explanation: 'Ceased activity within 2 months of last funding despite 10-year filing history.', evidence: 'Last funding: 2023-07-01. Last filing: 2023-09-15. No subsequent activity.', sourceTable: 'cra_t3010', sourceField: 'fiscal_period_end' },
      { code: 'HIGH_DEPENDENCY', label: 'Extreme Public Funding Dependency', severity: 'high', points: 15, explanation: '89% of revenue from government sources.', evidence: 'Total revenue: $2.02M. Government transfers: $1.8M.', sourceTable: 'cra_t3010', sourceField: 'total_revenue, gov_funding' },
    ],
    scoreComponents: [
      { name: 'Post-Funding Inactivity', points: 30, maxPoints: 30, explanation: 'Only 2 months of activity after last funding' },
      { name: 'Public Funding Dependency', points: 15, maxPoints: 25, explanation: '89% dependency exceeds 80% threshold' },
      { name: 'Funding Size & Recency', points: 17, maxPoints: 20, explanation: '$1.8M total, very recent funding' },
      { name: 'Organizational Capacity', points: 2, maxPoints: 15, explanation: '1 employee reported, address on file' },
      { name: 'Relationship & Patterns', points: 2, maxPoints: 10, explanation: 'Director shared with one other flagged org' },
    ],
    dataSources: ['cra', 'fed'],
    relatedEntities: ['ent-009'],
  },
  {
    id: 'ent-007',
    name: 'Pine Valley Industries',
    bnRoot: '678901234',
    sector: 'Agriculture',
    province: 'Alberta',
    totalFunding: 420000,
    totalRevenue: 591549,
    dependencyRatio: 71,
    lastFilingDate: '2024-03-01',
    lastFilingYear: 2024,
    firstFilingYear: 2019,
    filingYears: [2019, 2020, 2021, 2022, 2023, 2024],
    daysSinceActivity: 424,
    monthsPostFundingActivity: 18,
    score: 42,
    reviewStatus: 'LOW REVIEW PRIORITY',
    criterionA: false,
    criterionB: true,
    bothCriteria: false,
    employees: 8,
    hasAddress: true,
    revenueFromTransfers: 420000,
    fundingEvents: [
      { source: 'Agriculture and Agri-Food Canada', program: 'AgriInnovate Program', amount: 250000, date: '2022-04-01', yearLabel: '2022' },
      { source: 'Alberta Ministry of Agriculture', program: 'Farm Technology Program', amount: 170000, date: '2022-10-15', yearLabel: '2022' },
    ],
    flags: [
      { code: 'HIGH_DEPENDENCY', label: 'Elevated Public Funding Dependency', severity: 'medium', points: 10, explanation: '71% of revenue from government sources. Exceeds 70% threshold.', evidence: 'Total revenue: $592K. Government transfers: $420K.', sourceTable: 'cra_t3010', sourceField: 'total_revenue, gov_funding' },
    ],
    scoreComponents: [
      { name: 'Post-Funding Inactivity', points: 0, maxPoints: 30, explanation: '18 months of activity after last funding — no inactivity signal' },
      { name: 'Public Funding Dependency', points: 10, maxPoints: 25, explanation: '71% dependency exceeds 70% but not 80%' },
      { name: 'Funding Size & Recency', points: 8, maxPoints: 20, explanation: '$420K total, moderate amount' },
      { name: 'Organizational Capacity', points: 0, maxPoints: 15, explanation: '8 employees, address on file, own-source revenue' },
      { name: 'Relationship & Patterns', points: 2, maxPoints: 10, explanation: 'Minor overlap with Capital Bridge Group' },
    ],
    dataSources: ['cra', 'fed', 'ab'],
    relatedEntities: ['ent-004'],
  },
  {
    id: 'ent-008',
    name: 'Summit Research Corp',
    bnRoot: '789012345',
    sector: 'Technology',
    province: 'Quebec',
    totalFunding: 2100000,
    totalRevenue: 2282608,
    dependencyRatio: 92,
    lastFilingDate: '2023-08-22',
    lastFilingYear: 2023,
    firstFilingYear: 2020,
    filingYears: [2020, 2021, 2022, 2023],
    daysSinceActivity: 615,
    monthsPostFundingActivity: 5,
    score: 84,
    reviewStatus: 'HIGH REVIEW PRIORITY',
    criterionA: true,
    criterionB: true,
    bothCriteria: true,
    employees: 0,
    hasAddress: true,
    revenueFromTransfers: 2100000,
    fundingEvents: [
      { source: 'Natural Sciences and Engineering Research Council', program: 'Alliance Grant', amount: 800000, date: '2022-01-01', yearLabel: '2022' },
      { source: 'Quebec Economic Development', program: 'Innovation PME', amount: 750000, date: '2022-09-01', yearLabel: '2022' },
      { source: 'National Research Council', program: 'IRAP', amount: 550000, date: '2023-03-15', yearLabel: '2023' },
    ],
    flags: [
      { code: 'ZOMBIE_INACTIVITY', label: 'Post-Funding Inactivity Detected', severity: 'high', points: 28, explanation: 'Ceased within 5 months of last funding. Short organizational history (4 years).', evidence: 'Last funding: 2023-03-15. Last filing: 2023-08-22.', sourceTable: 'cra_t3010', sourceField: 'fiscal_period_end' },
      { code: 'HIGH_DEPENDENCY', label: 'Extreme Public Funding Dependency', severity: 'high', points: 15, explanation: '92% of revenue from government sources.', evidence: 'Total revenue: $2.28M. Government transfers: $2.1M.', sourceTable: 'cra_t3010', sourceField: 'total_revenue, gov_funding' },
      { code: 'NO_EMPLOYEES', label: 'No Employees on Record', severity: 'medium', points: 8, explanation: 'Zero employees despite $2.1M in research funding.', evidence: 'Employee count: 0 across all filings.', sourceTable: 'cra_t3010', sourceField: 'num_employees' },
    ],
    scoreComponents: [
      { name: 'Post-Funding Inactivity', points: 28, maxPoints: 30, explanation: '5 months activity post-funding, short org history' },
      { name: 'Public Funding Dependency', points: 15, maxPoints: 25, explanation: '92% dependency exceeds 80% threshold' },
      { name: 'Funding Size & Recency', points: 17, maxPoints: 20, explanation: '$2.1M total across 3 sources' },
      { name: 'Organizational Capacity', points: 8, maxPoints: 15, explanation: 'No employees, but has address' },
      { name: 'Relationship & Patterns', points: 0, maxPoints: 10, explanation: 'No linked flagged entities' },
    ],
    dataSources: ['cra', 'fed'],
    relatedEntities: [],
  },
  {
    id: 'ent-009',
    name: 'Atlantic Health Network',
    bnRoot: '890123456',
    sector: 'Healthcare',
    province: 'Nova Scotia',
    totalFunding: 1200000,
    totalRevenue: 3428571,
    dependencyRatio: 35,
    lastFilingDate: '2024-03-15',
    lastFilingYear: 2024,
    firstFilingYear: 2010,
    filingYears: [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
    daysSinceActivity: 410,
    monthsPostFundingActivity: 42,
    score: 18,
    reviewStatus: 'LOW REVIEW PRIORITY',
    criterionA: false,
    criterionB: false,
    bothCriteria: false,
    employees: 45,
    hasAddress: true,
    revenueFromTransfers: 1200000,
    fundingEvents: [
      { source: 'Health Canada', program: 'Health Promotion Fund', amount: 600000, date: '2021-01-01', yearLabel: '2021' },
      { source: 'Nova Scotia Health Authority', program: 'Community Health Innovation', amount: 600000, date: '2021-06-01', yearLabel: '2021' },
    ],
    flags: [],
    scoreComponents: [
      { name: 'Post-Funding Inactivity', points: 0, maxPoints: 30, explanation: '42 months of continuous activity post-funding' },
      { name: 'Public Funding Dependency', points: 0, maxPoints: 25, explanation: '35% dependency — well below thresholds' },
      { name: 'Funding Size & Recency', points: 8, maxPoints: 20, explanation: '$1.2M total but not recent (2021)' },
      { name: 'Organizational Capacity', points: 0, maxPoints: 15, explanation: '45 employees, address on file, diversified revenue' },
      { name: 'Relationship & Patterns', points: 0, maxPoints: 10, explanation: 'Shares director with Northgate Solutions — flagged but Atlantic Health itself clean' },
    ],
    dataSources: ['cra', 'fed'],
    relatedEntities: ['ent-001'],
  },
  {
    id: 'ent-010',
    name: 'Ontario Green Energy Fund',
    bnRoot: '901234567',
    sector: 'Environment',
    province: 'Ontario',
    totalFunding: 2800000,
    totalRevenue: 15555555,
    dependencyRatio: 18,
    lastFilingDate: '2024-03-20',
    lastFilingYear: 2024,
    firstFilingYear: 2008,
    filingYears: [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
    daysSinceActivity: 405,
    monthsPostFundingActivity: 52,
    score: 12,
    reviewStatus: 'LOW REVIEW PRIORITY',
    criterionA: false,
    criterionB: false,
    bothCriteria: false,
    employees: 120,
    hasAddress: true,
    revenueFromTransfers: 2800000,
    fundingEvents: [
      { source: 'Natural Resources Canada', program: 'Clean Energy Fund', amount: 1800000, date: '2020-04-01', yearLabel: '2020' },
      { source: 'Ontario Ministry of Energy', program: 'Green Ontario Fund', amount: 1000000, date: '2020-09-01', yearLabel: '2020' },
    ],
    flags: [],
    scoreComponents: [
      { name: 'Post-Funding Inactivity', points: 0, maxPoints: 30, explanation: '52 months of continuous activity post-funding' },
      { name: 'Public Funding Dependency', points: 0, maxPoints: 25, explanation: '18% dependency — low' },
      { name: 'Funding Size & Recency', points: 6, maxPoints: 20, explanation: '$2.8M total but from 2020' },
      { name: 'Organizational Capacity', points: 0, maxPoints: 15, explanation: '120 employees, established since 2008' },
      { name: 'Relationship & Patterns', points: 0, maxPoints: 10, explanation: 'No flagged connections' },
    ],
    dataSources: ['cra', 'fed'],
    relatedEntities: [],
  },
]

export function formatMoney(value: number): string {
  if (Math.abs(value) >= 1e9) return `$${(value / 1e9).toFixed(1)}B`
  if (Math.abs(value) >= 1e6) return `$${(value / 1e6).toFixed(1)}M`
  if (Math.abs(value) >= 1e3) return `$${(value / 1e3).toFixed(0)}K`
  return `$${value.toLocaleString()}`
}

export function getFilteredOrgs(filter: 'all' | 'criterionA' | 'criterionB' | 'both'): Organization[] {
  switch (filter) {
    case 'criterionA': return ORGANIZATIONS.filter((o) => o.criterionA)
    case 'criterionB': return ORGANIZATIONS.filter((o) => o.criterionB)
    case 'both': return ORGANIZATIONS.filter((o) => o.bothCriteria)
    default: return ORGANIZATIONS
  }
}

export function getSummaryStats(orgs: Organization[]) {
  const flagged = orgs.filter((o) => o.criterionA || o.criterionB)
  return {
    totalOrgs: orgs.length,
    bothCriteria: orgs.filter((o) => o.bothCriteria).length,
    totalFundingAtRisk: flagged.reduce((s, o) => s + o.totalFunding, 0),
    highPriority: orgs.filter((o) => o.reviewStatus === 'HIGH REVIEW PRIORITY').length,
    criterionAOnly: orgs.filter((o) => o.criterionA && !o.criterionB).length,
    criterionBOnly: orgs.filter((o) => o.criterionB && !o.criterionA).length,
    avgScore: orgs.length > 0 ? Math.round(orgs.reduce((s, o) => s + o.score, 0) / orgs.length) : 0,
    totalFlagsRaised: orgs.reduce((s, o) => s + o.flags.length, 0),
    avgDependency: orgs.length > 0 ? Math.round(orgs.reduce((s, o) => s + o.dependencyRatio, 0) / orgs.length) : 0,
    mediumPriority: orgs.filter((o) => o.reviewStatus === 'MEDIUM REVIEW PRIORITY').length,
    lowPriority: orgs.filter((o) => o.reviewStatus === 'LOW REVIEW PRIORITY').length,
  }
}

export interface AggregateRow {
  label: string
  count: number
  totalFunding: number
  avgScore: number
  avgDependency: number
  highPriority: number
}

export function getAggregateByIndustry(orgs: Organization[]): AggregateRow[] {
  const groups: Record<string, Organization[]> = {}
  for (const org of orgs) {
    if (!groups[org.sector]) groups[org.sector] = []
    groups[org.sector].push(org)
  }
  return Object.entries(groups)
    .map(([label, items]) => ({
      label,
      count: items.length,
      totalFunding: items.reduce((s, o) => s + o.totalFunding, 0),
      avgScore: Math.round(items.reduce((s, o) => s + o.score, 0) / items.length),
      avgDependency: Math.round(items.reduce((s, o) => s + o.dependencyRatio, 0) / items.length),
      highPriority: items.filter((o) => o.reviewStatus === 'HIGH REVIEW PRIORITY').length,
    }))
    .sort((a, b) => b.avgScore - a.avgScore)
}

export function getAggregateBySizeBand(orgs: Organization[]): AggregateRow[] {
  const bands = [
    { label: 'Under $500K', min: 0, max: 500000 },
    { label: '$500K – $1M', min: 500000, max: 1000000 },
    { label: '$1M – $2M', min: 1000000, max: 2000000 },
    { label: '$2M – $5M', min: 2000000, max: 5000000 },
    { label: 'Over $5M', min: 5000000, max: Infinity },
  ]
  return bands.map((band) => {
    const items = orgs.filter((o) => o.totalFunding >= band.min && o.totalFunding < band.max)
    return {
      label: band.label,
      count: items.length,
      totalFunding: items.reduce((s, o) => s + o.totalFunding, 0),
      avgScore: items.length > 0 ? Math.round(items.reduce((s, o) => s + o.score, 0) / items.length) : 0,
      avgDependency: items.length > 0 ? Math.round(items.reduce((s, o) => s + o.dependencyRatio, 0) / items.length) : 0,
      highPriority: items.filter((o) => o.reviewStatus === 'HIGH REVIEW PRIORITY').length,
    }
  }).filter((b) => b.count > 0)
}

export function getAggregateBySource(orgs: Organization[]): AggregateRow[] {
  const sourceMap: Record<string, { orgs: Set<string>; totalFunding: number; scores: number[]; deps: number[]; highCount: number }> = {}
  for (const org of orgs) {
    for (const ev of org.fundingEvents) {
      if (!sourceMap[ev.source]) {
        sourceMap[ev.source] = { orgs: new Set(), totalFunding: 0, scores: [], deps: [], highCount: 0 }
      }
      const entry = sourceMap[ev.source]
      if (!entry.orgs.has(org.id)) {
        entry.orgs.add(org.id)
        entry.scores.push(org.score)
        entry.deps.push(org.dependencyRatio)
        if (org.reviewStatus === 'HIGH REVIEW PRIORITY') entry.highCount++
      }
      entry.totalFunding += ev.amount
    }
  }
  return Object.entries(sourceMap)
    .map(([label, data]) => ({
      label,
      count: data.orgs.size,
      totalFunding: data.totalFunding,
      avgScore: Math.round(data.scores.reduce((s, v) => s + v, 0) / data.scores.length),
      avgDependency: Math.round(data.deps.reduce((s, v) => s + v, 0) / data.deps.length),
      highPriority: data.highCount,
    }))
    .sort((a, b) => b.totalFunding - a.totalFunding)
}

export function getAggregateByDependencyBand(orgs: Organization[]): AggregateRow[] {
  const bands = [
    { label: '0–30%', min: 0, max: 30 },
    { label: '30–50%', min: 30, max: 50 },
    { label: '50–70%', min: 50, max: 70 },
    { label: '70–80%', min: 70, max: 80 },
    { label: '80–100%', min: 80, max: 101 },
  ]
  return bands.map((band) => {
    const items = orgs.filter((o) => o.dependencyRatio >= band.min && o.dependencyRatio < band.max)
    return {
      label: band.label,
      count: items.length,
      totalFunding: items.reduce((s, o) => s + o.totalFunding, 0),
      avgScore: items.length > 0 ? Math.round(items.reduce((s, o) => s + o.score, 0) / items.length) : 0,
      avgDependency: items.length > 0 ? Math.round(items.reduce((s, o) => s + o.dependencyRatio, 0) / items.length) : 0,
      highPriority: items.filter((o) => o.reviewStatus === 'HIGH REVIEW PRIORITY').length,
    }
  }).filter((b) => b.count > 0)
}

export function getScoreDistribution(orgs: Organization[]): Array<{ range: string; count: number }> {
  const bands = [
    { range: '0–20', min: 0, max: 20 },
    { range: '20–40', min: 20, max: 40 },
    { range: '40–60', min: 40, max: 60 },
    { range: '60–80', min: 60, max: 80 },
    { range: '80–100', min: 80, max: 101 },
  ]
  return bands.map((band) => ({
    range: band.range,
    count: orgs.filter((o) => o.score >= band.min && o.score < band.max).length,
  }))
}

export function getProvinceBreakdown(orgs: Organization[]): Array<{ province: string; count: number; totalFunding: number }> {
  const groups: Record<string, Organization[]> = {}
  for (const org of orgs) {
    if (!groups[org.province]) groups[org.province] = []
    groups[org.province].push(org)
  }
  return Object.entries(groups)
    .map(([province, items]) => ({
      province,
      count: items.length,
      totalFunding: items.reduce((s, o) => s + o.totalFunding, 0),
    }))
    .sort((a, b) => b.count - a.count)
}
