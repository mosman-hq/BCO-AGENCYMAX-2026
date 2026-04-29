'use client'

import { useMemo, useState, useCallback } from 'react'
import { motion } from 'framer-motion'
import { ArrowLeft, Printer, FileText, ChevronDown, ChevronUp } from 'lucide-react'
import Link from 'next/link'
import { ORGANIZATIONS, formatMoney, getSummaryStats, getAggregateByIndustry, type Organization } from '@/lib/data'

const STATUS_STYLES: Record<string, string> = {
  'HIGH REVIEW PRIORITY': 'bg-red-primary text-white',
  'MEDIUM REVIEW PRIORITY': 'bg-amber-warn text-white',
  'LOW REVIEW PRIORITY': 'bg-[#6B7280] text-white',
}

export default function BinderPage() {
  const [expandedOrg, setExpandedOrg] = useState<string | null>(null)

  const highPriorityOrgs = useMemo(() =>
    [...ORGANIZATIONS]
      .filter((o) => o.reviewStatus === 'HIGH REVIEW PRIORITY')
      .sort((a, b) => b.score - a.score),
  [])

  const allFlaggedOrgs = useMemo(() =>
    [...ORGANIZATIONS]
      .filter((o) => o.criterionA || o.criterionB)
      .sort((a, b) => b.score - a.score),
  [])

  const stats = useMemo(() => getSummaryStats(ORGANIZATIONS), [])
  const industryAgg = useMemo(() => getAggregateByIndustry(ORGANIZATIONS), [])
  const totalFlagged = allFlaggedOrgs.length
  const totalFunding = allFlaggedOrgs.reduce((s, o) => s + o.totalFunding, 0)

  const handlePrint = useCallback(() => {
    window.print()
  }, [])

  return (
    <div className="min-h-screen bg-white">
      {/* Header */}
      <nav className="sticky top-0 z-50 bg-white border-b border-red-dim print:hidden">
        <div className="max-w-7xl mx-auto px-6 flex items-center h-14 justify-between">
          <div className="flex items-center gap-4">
            <Link href="/" className="p-2 hover:bg-red-dim/30 transition">
              <ArrowLeft className="w-4 h-4 text-red-primary" />
            </Link>
            <div className="font-syne font-700 text-red-primary text-lg tracking-tight">RRI</div>
            <span className="font-ibm text-xs text-muted-foreground uppercase tracking-wider">Committee Binder</span>
          </div>
          <button
            onClick={handlePrint}
            className="flex items-center gap-2 px-4 py-2 bg-red-primary text-white font-ibm text-xs uppercase tracking-wider hover:bg-red-primary/90 transition"
          >
            <Printer className="w-3.5 h-3.5" /> Print / PDF
          </button>
        </div>
      </nav>

      <div className="max-w-4xl mx-auto px-6 py-10 space-y-10 print:px-0 print:max-w-none">
        {/* Cover page */}
        <div className="text-center py-16 border-b border-red-dim">
          <div className="font-syne text-3xl font-700 text-red-primary mb-4">Recipient Risk Intelligence</div>
          <div className="font-syne text-xl font-600 text-charcoal mb-2">Committee Briefing Package</div>
          <div className="font-ibm text-sm text-muted-foreground">
            AI for Accountability Hackathon 2026 · BCO AgencyMax
          </div>
          <div className="font-ibm text-xs text-muted-foreground mt-4">
            Generated: {new Date().toLocaleDateString('en-CA', { year: 'numeric', month: 'long', day: 'numeric' })}
          </div>
          <div className="font-ibm text-xs text-muted-foreground mt-1">
            Scope: {ORGANIZATIONS.length} organizations · {totalFlagged} flagged · {formatMoney(totalFunding)} in funding at risk
          </div>
        </div>

        {/* Executive Summary */}
        <section>
          <SectionHeader number="1" title="Executive Summary" />
          <div className="space-y-4 mt-4">
            <p className="font-ibm text-sm text-charcoal leading-relaxed">
              This briefing package summarizes findings from the Recipient Risk Intelligence system&apos;s analysis of {ORGANIZATIONS.length} organizations
              that received public funding. The system uses a deterministic 100-point scoring model based on five weighted dimensions:
              post-funding inactivity (30 pts), public funding dependency (25 pts), funding size and recency (20 pts),
              organizational capacity (15 pts), and relationship patterns (10 pts).
            </p>
            <p className="font-ibm text-sm text-charcoal leading-relaxed">
              Of {ORGANIZATIONS.length} organizations analyzed, {stats.highPriority} are classified as high review priority,
              {' '}{stats.bothCriteria} meet both zombie criteria (post-funding inactivity + high dependency),
              and {formatMoney(stats.totalFundingAtRisk)} in public funding is associated with flagged cases.
              The average risk score across the entire dataset is {stats.avgScore}/100.
            </p>
          </div>

          {/* Key findings */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-6">
            {[
              { label: 'Organizations Analyzed', value: String(stats.totalOrgs) },
              { label: 'High Priority Cases', value: String(stats.highPriority) },
              { label: 'Both Criteria Met', value: String(stats.bothCriteria) },
              { label: 'Funding at Risk', value: formatMoney(stats.totalFundingAtRisk) },
            ].map((card) => (
              <div key={card.label} className="border border-red-dim p-4 border-l-4 border-l-red-primary">
                <div className="font-ibm text-xl font-700 text-charcoal tabular-nums">{card.value}</div>
                <div className="font-syne text-xs font-600 text-charcoal mt-1">{card.label}</div>
              </div>
            ))}
          </div>
        </section>

        {/* Methodology */}
        <section>
          <SectionHeader number="2" title="Methodology" />
          <div className="space-y-4 mt-4">
            <p className="font-ibm text-sm text-charcoal leading-relaxed">
              The RRI system applies two binary criteria to identify potential zombie recipients:
            </p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="border border-red-dim p-4">
                <h4 className="font-syne text-sm font-600 text-red-primary mb-2">Criterion A: Post-Funding Inactivity</h4>
                <p className="font-ibm text-xs text-charcoal leading-relaxed">
                  The organization ceased CRA filings or observable activity within 12 months of its most recent major
                  funding event. Measured by comparing the last filing date against the last funding date.
                </p>
              </div>
              <div className="border border-red-dim p-4">
                <h4 className="font-syne text-sm font-600 text-red-primary mb-2">Criterion B: High Public Funding Dependency</h4>
                <p className="font-ibm text-xs text-charcoal leading-relaxed">
                  Government transfers comprise 70% or more of total reported revenue. Organizations above 80% are
                  considered extreme dependency; 70-80% is elevated.
                </p>
              </div>
            </div>
            <p className="font-ibm text-sm text-charcoal leading-relaxed">
              Organizations meeting both criteria are classified as &quot;zombie profiles&quot; — entities that received public
              funding and then ceased meaningful operations. The 100-point score provides nuance within this binary classification.
            </p>
          </div>
        </section>

        {/* Industry breakdown */}
        <section>
          <SectionHeader number="3" title="Sector Analysis" />
          <div className="mt-4 border border-red-dim overflow-hidden">
            <table className="w-full">
              <thead>
                <tr className="bg-charcoal">
                  {['Sector', 'Orgs', 'Total Funding', 'Avg Score', 'Avg Dep %', 'High Priority'].map((h) => (
                    <th key={h} className="text-left px-4 py-2.5 font-ibm text-[10px] uppercase tracking-wider text-white font-500">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {industryAgg.map((row) => (
                  <tr key={row.label} className="border-b border-red-dim">
                    <td className="px-4 py-2.5 font-syne text-sm text-charcoal">{row.label}</td>
                    <td className="px-4 py-2.5 font-ibm text-sm tabular-nums text-charcoal">{row.count}</td>
                    <td className="px-4 py-2.5 font-ibm text-sm tabular-nums text-charcoal">{formatMoney(row.totalFunding)}</td>
                    <td className="px-4 py-2.5 font-ibm text-sm tabular-nums text-charcoal">{row.avgScore}</td>
                    <td className="px-4 py-2.5 font-ibm text-sm tabular-nums text-charcoal">{row.avgDependency}%</td>
                    <td className="px-4 py-2.5 font-ibm text-sm tabular-nums text-red-primary font-600">{row.highPriority}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        {/* High priority profiles */}
        <section>
          <SectionHeader number="4" title={`High Priority Profiles (${highPriorityOrgs.length})`} />
          <div className="space-y-4 mt-4">
            {highPriorityOrgs.map((org) => (
              <OrgBrief
                key={org.id}
                org={org}
                expanded={expandedOrg === org.id}
                onToggle={() => setExpandedOrg(expandedOrg === org.id ? null : org.id)}
              />
            ))}
          </div>
        </section>

        {/* All flagged organizations */}
        <section>
          <SectionHeader number="5" title={`All Flagged Organizations (${allFlaggedOrgs.length})`} />
          <div className="mt-4 border border-red-dim overflow-hidden">
            <table className="w-full">
              <thead>
                <tr className="bg-charcoal">
                  {['Name', 'Score', 'Dep %', 'Funding', 'A', 'B', 'Flags', 'Status'].map((h) => (
                    <th key={h} className="text-left px-3 py-2.5 font-ibm text-[10px] uppercase tracking-wider text-white font-500">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {allFlaggedOrgs.map((org) => (
                  <tr key={org.id} className="border-b border-red-dim">
                    <td className="px-3 py-2 font-syne text-xs font-600 text-charcoal">{org.name}</td>
                    <td className="px-3 py-2 font-ibm text-xs tabular-nums font-600 text-red-primary">{org.score}</td>
                    <td className="px-3 py-2 font-ibm text-xs tabular-nums text-charcoal">{org.dependencyRatio}%</td>
                    <td className="px-3 py-2 font-ibm text-xs tabular-nums text-charcoal">{formatMoney(org.totalFunding)}</td>
                    <td className="px-3 py-2 font-ibm text-xs">{org.criterionA ? '✓' : '—'}</td>
                    <td className="px-3 py-2 font-ibm text-xs">{org.criterionB ? '✓' : '—'}</td>
                    <td className="px-3 py-2 font-ibm text-xs tabular-nums text-charcoal">{org.flags.length}</td>
                    <td className="px-3 py-2">
                      <span className={`font-ibm text-[9px] uppercase tracking-wider px-2 py-0.5 ${STATUS_STYLES[org.reviewStatus]}`}>
                        {org.reviewStatus.replace(' REVIEW PRIORITY', '')}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        {/* Disclaimer */}
        <section className="border-t border-red-dim pt-8">
          <SectionHeader number="6" title="Disclaimer & Notes" />
          <div className="mt-4 border-2 border-dashed border-amber-warn bg-amber-warn/5 p-5">
            <p className="font-ibm text-sm text-charcoal leading-relaxed">
              This analysis is generated by a deterministic scoring system and does not constitute an accusation of fraud or
              misuse. All AI-generated content is clearly marked and should be verified against source data. The system
              identifies patterns that warrant human review — it does not make final determinations. Scores and flags
              are based on publicly available data from CRA T3010 filings, federal grants databases, and corporate
              registries. Data may be incomplete or delayed.
            </p>
          </div>
        </section>

        {/* Footer */}
        <footer className="border-t border-red-dim pt-8 pb-16 text-center">
          <span className="font-syne text-sm font-700 text-red-primary tracking-tight">Recipient Risk Intelligence</span>
          <p className="font-ibm text-xs text-muted-foreground mt-1">AI for Accountability Hackathon 2026 · BCO AgencyMax</p>
        </footer>
      </div>
    </div>
  )
}

function SectionHeader({ number, title }: { number: string; title: string }) {
  return (
    <div className="flex items-center gap-3">
      <div className="w-8 h-8 bg-red-primary flex items-center justify-center font-ibm text-sm font-700 text-white">{number}</div>
      <h2 className="font-syne text-xl font-700 text-charcoal">{title}</h2>
    </div>
  )
}

function OrgBrief({ org, expanded, onToggle }: { org: Organization; expanded: boolean; onToggle: () => void }) {
  return (
    <div className="border border-red-dim bg-card-bg">
      <button onClick={onToggle} className="w-full p-4 flex items-center justify-between text-left hover:bg-red-dim/20 transition">
        <div className="flex items-center gap-4">
          <span className="font-ibm text-xl font-700 text-red-primary tabular-nums">{org.score}</span>
          <div>
            <span className="font-syne text-sm font-600 text-charcoal">{org.name}</span>
            <div className="font-ibm text-[10px] text-muted-foreground mt-0.5">{org.sector} · {org.province} · BN {org.bnRoot}</div>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <span className={`font-ibm text-[9px] uppercase tracking-wider px-2 py-0.5 ${STATUS_STYLES[org.reviewStatus]}`}>
            {org.reviewStatus.replace(' REVIEW PRIORITY', '')}
          </span>
          {expanded ? <ChevronUp className="w-4 h-4 text-muted-foreground" /> : <ChevronDown className="w-4 h-4 text-muted-foreground" />}
        </div>
      </button>

      {expanded && (
        <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="border-t border-red-dim p-4 space-y-4">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <MiniStat label="Total Funding" value={formatMoney(org.totalFunding)} />
            <MiniStat label="Dependency" value={`${org.dependencyRatio}%`} />
            <MiniStat label="Post-Funding Activity" value={`${org.monthsPostFundingActivity} months`} />
            <MiniStat label="Days Inactive" value={`${org.daysSinceActivity}`} />
          </div>

          <div>
            <h4 className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground mb-2">Flags ({org.flags.length})</h4>
            <div className="space-y-2">
              {org.flags.map((flag) => (
                <div key={flag.code} className="flex items-start gap-2">
                  <span className={`font-ibm text-[9px] uppercase tracking-wider px-1.5 py-0.5 mt-0.5 ${flag.severity === 'high' ? 'bg-red-primary text-white' : 'bg-amber-warn text-white'}`}>
                    {flag.severity}
                  </span>
                  <div>
                    <span className="font-ibm text-xs font-600 text-charcoal">{flag.code}</span>
                    <p className="font-ibm text-xs text-muted-foreground">{flag.explanation}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div>
            <h4 className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground mb-2">Score Breakdown</h4>
            <div className="space-y-1.5">
              {org.scoreComponents.map((comp) => (
                <div key={comp.name} className="flex items-center justify-between">
                  <span className="font-ibm text-xs text-charcoal">{comp.name}</span>
                  <span className="font-ibm text-xs tabular-nums text-muted-foreground">{comp.points}/{comp.maxPoints}</span>
                </div>
              ))}
            </div>
          </div>

          <div>
            <h4 className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground mb-1">Data Sources</h4>
            <div className="flex gap-1.5">
              {org.dataSources.map((s) => (
                <span key={s} className="font-ibm text-[9px] uppercase tracking-wider px-1.5 py-0.5 border border-red-dim text-muted-foreground">{s}</span>
              ))}
            </div>
          </div>
        </motion.div>
      )}
    </div>
  )
}

function MiniStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="border border-red-dim p-3">
      <div className="font-ibm text-sm font-700 text-charcoal tabular-nums">{value}</div>
      <div className="font-ibm text-[10px] text-muted-foreground mt-0.5">{label}</div>
    </div>
  )
}
