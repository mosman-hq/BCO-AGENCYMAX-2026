import {
  ORGANIZATIONS,
  getFilteredOrgs,
  getSummaryStats,
  formatMoney,
  getAggregateByIndustry,
  getAggregateBySizeBand,
  getAggregateBySource,
  getAggregateByDependencyBand,
  getScoreDistribution,
  getProvinceBreakdown,
  type Organization,
} from './data'

const API_BASE = 'http://localhost:3801/api'

async function tryFetch<T>(path: string, fallback: () => T): Promise<T> {
  try {
    const response = await fetch(`${API_BASE}${path}`, {
      signal: AbortSignal.timeout(3000),
    })
    if (!response.ok) throw new Error(`HTTP ${response.status}`)
    return await response.json() as T
  } catch {
    return fallback()
  }
}

export async function fetchOrganizations(filter: string = 'all'): Promise<Organization[]> {
  return tryFetch(`/organizations?filter=${filter}`, () =>
    getFilteredOrgs(filter as 'all' | 'criterionA' | 'criterionB' | 'both')
  )
}

export async function fetchOrganization(id: string): Promise<Organization | null> {
  return tryFetch(`/organizations/${id}`, () =>
    ORGANIZATIONS.find((o) => o.id === id) ?? null
  )
}

export async function fetchOrgFunding(id: string) {
  return tryFetch(`/organizations/${id}/funding`, () => {
    const org = ORGANIZATIONS.find((o) => o.id === id)
    return org?.fundingEvents ?? []
  })
}

export async function fetchOrgFilings(id: string) {
  return tryFetch(`/organizations/${id}/filings`, () => {
    const org = ORGANIZATIONS.find((o) => o.id === id)
    if (!org) return []
    const years = []
    for (let y = org.firstFilingYear; y <= org.lastFilingYear; y++) {
      years.push({ year: y, filed: org.filingYears.includes(y) })
    }
    return years
  })
}

export async function fetchOrgFlags(id: string) {
  return tryFetch(`/organizations/${id}/flags`, () => {
    const org = ORGANIZATIONS.find((o) => o.id === id)
    return org?.flags ?? []
  })
}

export async function fetchOrgScore(id: string) {
  return tryFetch(`/organizations/${id}/score`, () => {
    const org = ORGANIZATIONS.find((o) => o.id === id)
    return org
      ? { score: org.score, components: org.scoreComponents }
      : null
  })
}

export async function fetchOrgRelated(id: string) {
  return tryFetch(`/organizations/${id}/related`, () => {
    const org = ORGANIZATIONS.find((o) => o.id === id)
    if (!org) return []
    return org.relatedEntities.map((rid) => {
      const related = ORGANIZATIONS.find((o) => o.id === rid)
      return related
        ? { id: related.id, name: related.name, score: related.score, reviewStatus: related.reviewStatus }
        : { id: rid, name: 'Unknown', score: 0, reviewStatus: 'LOW REVIEW PRIORITY' as const }
    })
  })
}

export async function fetchOrgAIAnalysis(id: string) {
  return tryFetch(`/organizations/${id}/ai-analysis`, () => {
    const org = ORGANIZATIONS.find((o) => o.id === id)
    if (!org) return null
    return {
      explanation: `${org.name} (BN ${org.bnRoot}) received ${formatMoney(org.totalFunding)} in public funding with ${org.dependencyRatio}% of total revenue coming from government sources. ${org.criterionA ? `The organization showed signs of inactivity within ${org.monthsPostFundingActivity} months of its most recent funding event.` : 'Filing continuity does not indicate post-funding inactivity.'} ${org.flags.length > 0 ? `The system identified ${org.flags.length} flag(s) through deterministic analysis.` : 'No flags were raised.'}`,
      evidence: [
        `Total public funding: ${formatMoney(org.totalFunding)} across ${org.fundingEvents.length} events`,
        `Dependency ratio: ${org.dependencyRatio}% (threshold: 70-80%)`,
        `Filing continuity: ${org.filingYears.length} filings from ${org.firstFilingYear} to ${org.lastFilingYear}`,
        org.employees !== null ? `Employees on record: ${org.employees}` : 'Employee data unavailable',
        `Score: ${org.score}/100 (${org.reviewStatus})`,
      ],
      nextSteps: [
        'Verify funding amounts against original contribution agreements',
        `Cross-reference CRA filing data for fiscal years ${org.lastFilingYear} and ${org.lastFilingYear + 1}`,
        org.relatedEntities.length > 0
          ? `Review related entities: ${org.relatedEntities.join(', ')}`
          : 'Check corporate registry for director cross-references',
        'Request updated filing status from CRA if gaps exist',
      ],
    }
  })
}

export async function fetchOrgAINextSteps(id: string) {
  return tryFetch(`/organizations/${id}/ai-nextsteps`, () => {
    const org = ORGANIZATIONS.find((o) => o.id === id)
    if (!org) return []
    const steps: Array<{ priority: 'high' | 'medium' | 'low'; action: string; rationale: string }> = []
    if (org.criterionA) {
      steps.push({
        priority: 'high',
        action: `Confirm cessation: request CRA filing status for ${org.lastFilingYear + 1}`,
        rationale: `Last filing was ${org.lastFilingDate}. ${org.daysSinceActivity} days without activity.`,
      })
    }
    if (org.criterionB) {
      steps.push({
        priority: 'high',
        action: 'Verify contribution agreements match reported funding amounts',
        rationale: `${org.dependencyRatio}% dependency on ${formatMoney(org.totalFunding)} in public funding.`,
      })
    }
    if (org.relatedEntities.length > 0) {
      steps.push({
        priority: 'medium',
        action: `Review related entities: ${org.relatedEntities.join(', ')}`,
        rationale: 'Director or address overlap detected with other flagged organizations.',
      })
    }
    if (org.employees === 0 || org.employees === null) {
      steps.push({
        priority: 'medium',
        action: 'Cross-reference employee records with T4 filings and corporate registry',
        rationale: org.employees === 0 ? 'Zero employees reported despite receiving funding.' : 'Employee data unavailable.',
      })
    }
    steps.push({
      priority: 'low',
      action: 'Check corporate registry for dissolution, name change, or amalgamation',
      rationale: 'Standard due diligence for all flagged organizations.',
    })
    return steps
  })
}

export interface ReviewDisposition {
  orgId: string
  status: 'pending' | 'confirmed-zombie' | 'cleared' | 'needs-info'
  notes: string
  attestedBy: string
  attestedAt: string
  auditTrail: Array<{ action: string; timestamp: string; user: string }>
}

export async function fetchOrgReview(id: string): Promise<ReviewDisposition | null> {
  return tryFetch(`/organizations/${id}/review`, () => null)
}

export async function saveOrgReview(id: string, review: Partial<ReviewDisposition>): Promise<ReviewDisposition> {
  const now = new Date().toISOString()
  const disposition: ReviewDisposition = {
    orgId: id,
    status: review.status ?? 'pending',
    notes: review.notes ?? '',
    attestedBy: review.attestedBy ?? 'Current User',
    attestedAt: now,
    auditTrail: [
      ...(review.auditTrail ?? []),
      { action: `Status set to ${review.status ?? 'pending'}`, timestamp: now, user: review.attestedBy ?? 'Current User' },
    ],
  }
  try {
    const response = await fetch(`${API_BASE}/organizations/${id}/review`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(disposition),
      signal: AbortSignal.timeout(3000),
    })
    if (response.ok) return await response.json() as ReviewDisposition
  } catch { /* fall through */ }
  return disposition
}

export async function fetchTriageResults(filter: string = 'all', limit: number = 10): Promise<Organization[]> {
  return tryFetch(`/triage?filter=${filter}&limit=${limit}`, () => {
    const orgs = getFilteredOrgs(filter as 'all' | 'criterionA' | 'criterionB' | 'both')
    return [...orgs].sort((a, b) => b.score - a.score).slice(0, limit)
  })
}

export async function fetchStats(filter: string = 'all') {
  return tryFetch(`/stats?filter=${filter}`, () => {
    const orgs = getFilteredOrgs(filter as 'all' | 'criterionA' | 'criterionB' | 'both')
    return getSummaryStats(orgs)
  })
}

export async function fetchStatsByIndustry(filter: string = 'all') {
  return tryFetch(`/stats/by-sector?filter=${filter}`, () => {
    const orgs = getFilteredOrgs(filter as 'all' | 'criterionA' | 'criterionB' | 'both')
    return getAggregateByIndustry(orgs)
  })
}

export async function fetchStatsBySource(filter: string = 'all') {
  return tryFetch(`/stats/by-source?filter=${filter}`, () => {
    const orgs = getFilteredOrgs(filter as 'all' | 'criterionA' | 'criterionB' | 'both')
    return getAggregateBySource(orgs)
  })
}

export async function fetchStatsBySizeBand(filter: string = 'all') {
  return tryFetch(`/stats/by-size?filter=${filter}`, () => {
    const orgs = getFilteredOrgs(filter as 'all' | 'criterionA' | 'criterionB' | 'both')
    return getAggregateBySizeBand(orgs)
  })
}

export async function fetchStatsByDependency(filter: string = 'all') {
  return tryFetch(`/stats/by-dependency?filter=${filter}`, () => {
    const orgs = getFilteredOrgs(filter as 'all' | 'criterionA' | 'criterionB' | 'both')
    return getAggregateByDependencyBand(orgs)
  })
}

export async function fetchCompareData(ids: string[]) {
  const query = ids.map((id) => `id=${id}`).join('&')
  return tryFetch(`/compare?${query}`, () =>
    ids.map((id) => ORGANIZATIONS.find((o) => o.id === id)).filter(Boolean) as Organization[]
  )
}

export {
  getScoreDistribution,
  getProvinceBreakdown,
}
