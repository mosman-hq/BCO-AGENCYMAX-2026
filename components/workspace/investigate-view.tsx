'use client'

import { useMemo, useState, useCallback } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts'
import {
  ArrowLeft, Plus, MessageSquare, Play, Loader2, CheckCircle2,
  Shield, ShieldAlert, AlertTriangle, FileText, Eye, X,
  ChevronRight, ClipboardCopy, Download,
} from 'lucide-react'
import { useWorkspace } from '@/lib/workspace-context'
import { ORGANIZATIONS, formatMoney, type Organization, type Flag } from '@/lib/data'

const PROFILE_TABS = ['Overview', 'Funding', 'Continuity', 'Flags', 'AI Analysis', 'Review'] as const
type ProfileTab = typeof PROFILE_TABS[number]

const STATUS_BG: Record<string, string> = {
  'HIGH REVIEW PRIORITY': 'bg-red-primary text-white',
  'MEDIUM REVIEW PRIORITY': 'bg-amber-warn text-white',
  'LOW REVIEW PRIORITY': 'bg-[#6B7280] text-white',
}

const DISPOSITION_OPTIONS = [
  { value: 'pending', label: 'Pending', color: 'bg-[#6B7280]' },
  { value: 'confirmed-zombie', label: 'Confirmed Zombie', color: 'bg-red-primary' },
  { value: 'cleared', label: 'Cleared', color: 'bg-green-600' },
  { value: 'needs-info', label: 'Needs More Info', color: 'bg-amber-warn' },
] as const

export default function InvestigateView() {
  const { selectedOrgId, setActiveView, addToCompare, openAssistantForOrg } = useWorkspace()
  const [activeTab, setActiveTab] = useState<ProfileTab>('Overview')
  const [drawerFlag, setDrawerFlag] = useState<Flag | null>(null)
  const org = useMemo(() => ORGANIZATIONS.find((o) => o.id === selectedOrgId), [selectedOrgId])

  if (!org) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-center">
          <p className="font-syne text-lg text-muted-foreground">No organization selected</p>
          <button onClick={() => setActiveView('discover')} className="mt-4 font-ibm text-sm text-red-primary underline">
            Return to Discover
          </button>
        </div>
      </div>
    )
  }

  const summaryLine = org.bothCriteria
    ? `This organization meets both zombie criteria: post-funding inactivity detected within ${org.monthsPostFundingActivity} months, and ${org.dependencyRatio}% public funding dependency.`
    : org.criterionA
    ? `This organization shows post-funding inactivity: activity ceased within ${org.monthsPostFundingActivity} months of receiving ${formatMoney(org.totalFunding)}.`
    : org.criterionB
    ? `This organization has ${org.dependencyRatio}% public funding dependency, exceeding the 70% review threshold.`
    : `This organization does not currently meet zombie criteria. Low priority for review.`

  return (
    <div className="space-y-6">
      {/* Back + header */}
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-start gap-4">
          <button onClick={() => setActiveView('discover')} className="mt-1 p-2 border border-red-dim hover:bg-red-dim/30 transition">
            <ArrowLeft className="w-4 h-4 text-red-primary" />
          </button>
          <div>
            <div className="flex items-center gap-3">
              <span className={`font-ibm text-xs uppercase tracking-wider px-3 py-1 ${STATUS_BG[org.reviewStatus]}`}>
                {org.reviewStatus}
              </span>
              <span className="font-ibm text-2xl font-700 text-red-primary tabular-nums">{org.score}</span>
            </div>
            <h2 className="font-syne text-2xl font-700 text-charcoal mt-2">{org.name}</h2>
            <p className="font-ibm text-sm text-muted-foreground mt-1">{summaryLine}</p>
            <div className="flex gap-2 mt-2">
              {org.dataSources.map((s) => (
                <span key={s} className="font-ibm text-[10px] uppercase tracking-wider px-2 py-0.5 border border-red-dim text-muted-foreground">{s}</span>
              ))}
            </div>
          </div>
        </div>
        <div className="flex gap-2 flex-shrink-0">
          <button onClick={() => addToCompare(org.id)} className="flex items-center gap-2 px-3 py-2 border border-red-dim font-ibm text-xs uppercase tracking-wider hover:bg-red-dim/30 transition">
            <Plus className="w-3.5 h-3.5 text-red-primary" /> Compare
          </button>
          <button onClick={() => openAssistantForOrg(org.id)} className="flex items-center gap-2 px-3 py-2 border border-red-dim font-ibm text-xs uppercase tracking-wider hover:bg-red-dim/30 transition">
            <MessageSquare className="w-3.5 h-3.5 text-red-primary" /> Ask
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-0 border border-red-dim overflow-x-auto">
        {PROFILE_TABS.map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={`px-5 py-3 font-syne text-sm border-r border-red-dim last:border-r-0 transition-colors whitespace-nowrap ${
              activeTab === tab ? 'bg-charcoal text-white font-600' : 'bg-white text-charcoal hover:bg-card-bg'
            }`}
          >
            {tab}
            {tab === 'Flags' && ` (${org.flags.length})`}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {activeTab === 'Overview' && <OverviewTab org={org} />}
      {activeTab === 'Funding' && <FundingTab org={org} />}
      {activeTab === 'Continuity' && <ContinuityTab org={org} />}
      {activeTab === 'Flags' && <FlagsTab org={org} onOpenEvidence={setDrawerFlag} />}
      {activeTab === 'AI Analysis' && <AIAnalysisTab org={org} />}
      {activeTab === 'Review' && <ReviewTab org={org} />}

      {/* Evidence drawer */}
      <AnimatePresence>
        {drawerFlag && (
          <EvidenceDrawer flag={drawerFlag} org={org} onClose={() => setDrawerFlag(null)} />
        )}
      </AnimatePresence>
    </div>
  )
}

function OverviewTab({ org }: { org: Organization }) {
  const [investigationState, setInvestigationState] = useState<'idle' | 'running' | 'done'>('idle')
  const [investigationTrace, setInvestigationTrace] = useState<Array<{ step: number; title: string; detail: string; evidence: string }>>([])
  const [expandedStep, setExpandedStep] = useState<number | null>(null)

  const peers = useMemo(() => {
    return ORGANIZATIONS
      .filter((o) => o.id !== org.id && o.sector === org.sector)
      .sort((a, b) => b.score - a.score)
      .slice(0, 3)
  }, [org])

  const relatedOrgs = useMemo(() => {
    return org.relatedEntities
      .map((rid) => ORGANIZATIONS.find((o) => o.id === rid))
      .filter(Boolean) as Organization[]
  }, [org])

  const runInvestigation = useCallback(() => {
    setInvestigationState('running')
    const steps = [
      { step: 1, title: 'Retrieve canonical identity and linked source records', detail: `Entity ${org.id} — ${org.name} (BN ${org.bnRoot}). Sources: ${org.dataSources.join(', ')}.`, evidence: `Linked entity IDs: ${org.relatedEntities.length > 0 ? org.relatedEntities.join(', ') : 'none'}` },
      { step: 2, title: 'Retrieve all funding events and compute funding summary', detail: `${org.fundingEvents.length} funding events totaling ${formatMoney(org.totalFunding)}. Largest event: ${formatMoney(Math.max(...org.fundingEvents.map((e) => e.amount)))}. Most recent: ${org.fundingEvents[org.fundingEvents.length - 1]?.date || 'N/A'}.`, evidence: org.fundingEvents.map((e) => `${e.source}: ${formatMoney(e.amount)} (${e.date})`).join('; ') },
      { step: 3, title: 'Retrieve CRA filing continuity and time-to-inactivity', detail: `Filing range: ${org.firstFilingYear}–${org.lastFilingYear}. Filed years: ${org.filingYears.join(', ')}. Months of post-funding activity: ${org.monthsPostFundingActivity}.`, evidence: `Expected filings: ${org.lastFilingYear - org.firstFilingYear + 1}. Actual: ${org.filingYears.length}. Gap years: ${getGapYears(org).join(', ') || 'none'}` },
      { step: 4, title: 'Compute dependency ratio and evaluate threshold flags', detail: `Total revenue: ${formatMoney(org.totalRevenue)}. Government transfers: ${formatMoney(org.revenueFromTransfers)}. Dependency ratio: ${org.dependencyRatio}%. Criterion B met: ${org.criterionB ? 'YES' : 'NO'}.`, evidence: `Threshold check: ${org.dependencyRatio}% ${org.dependencyRatio >= 80 ? '≥ 80% (HIGH)' : org.dependencyRatio >= 70 ? '≥ 70% (ELEVATED)' : '< 70% (BELOW THRESHOLD)'}` },
      { step: 5, title: 'Evaluate all risk flags and compile final score', detail: `${org.flags.length} flags raised. Score components: ${org.scoreComponents.map((c) => `${c.name}: ${c.points}/${c.maxPoints}`).join(', ')}. Final score: ${org.score}/100.`, evidence: `Flags: ${org.flags.map((f) => f.code).join(', ') || 'none'}` },
      { step: 6, title: 'Produce structured summary with evidence chain', detail: `Review status: ${org.reviewStatus}. Criterion A (inactivity): ${org.criterionA ? 'MET' : 'NOT MET'}. Criterion B (dependency): ${org.criterionB ? 'MET' : 'NOT MET'}. Both criteria: ${org.bothCriteria ? 'YES' : 'NO'}.`, evidence: `Data sources consulted: ${org.dataSources.join(', ')}. Evidence chain complete.` },
    ]

    let i = 0
    const interval = setInterval(() => {
      if (i < steps.length) {
        setInvestigationTrace((prev) => [...prev, steps[i]])
        i++
      } else {
        clearInterval(interval)
        setInvestigationState('done')
      }
    }, 500)
  }, [org])

  return (
    <div className="space-y-6">
      {/* Key metrics */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        <MetricCard label="Total Public Funding" desc="All government grants and contributions received on record" value={formatMoney(org.totalFunding)} />
        <MetricCard label="Total Revenue" desc="All revenue sources across available CRA filings" value={formatMoney(org.totalRevenue)} />
        <MetricCard label="Public Funding Dependency" desc={`${org.dependencyRatio}% of revenue came from government sources`} value={`${org.dependencyRatio}%`} highlight={org.dependencyRatio >= 70} />
        <MetricCard label="Filing Continuity" desc={`Last CRA filing observed on ${org.lastFilingDate}`} value={org.lastFilingDate} />
        <MetricCard label="Days Since Activity" desc="Calendar days since last observed organizational activity" value={`${org.daysSinceActivity} days`} highlight={org.daysSinceActivity > 365} />
        <MetricCard label="Employees on Record" desc="Number of employees reported across available filings" value={org.employees !== null ? String(org.employees) : 'Unknown'} highlight={org.employees === 0} />
      </div>

      {/* Criteria badges */}
      <div className="flex gap-3">
        <div className={`flex items-center gap-2 px-4 py-2 border ${org.criterionA ? 'border-red-primary bg-red-primary/5' : 'border-red-dim'}`}>
          {org.criterionA ? <ShieldAlert className="w-4 h-4 text-red-primary" /> : <Shield className="w-4 h-4 text-muted-foreground" />}
          <span className={`font-ibm text-xs uppercase tracking-wider ${org.criterionA ? 'text-red-primary font-600' : 'text-muted-foreground'}`}>
            Criterion A: {org.criterionA ? 'Met' : 'Not Met'}
          </span>
        </div>
        <div className={`flex items-center gap-2 px-4 py-2 border ${org.criterionB ? 'border-red-primary bg-red-primary/5' : 'border-red-dim'}`}>
          {org.criterionB ? <ShieldAlert className="w-4 h-4 text-red-primary" /> : <Shield className="w-4 h-4 text-muted-foreground" />}
          <span className={`font-ibm text-xs uppercase tracking-wider ${org.criterionB ? 'text-red-primary font-600' : 'text-muted-foreground'}`}>
            Criterion B: {org.criterionB ? 'Met' : 'Not Met'}
          </span>
        </div>
      </div>

      {/* Score breakdown */}
      <div className="border border-red-dim bg-card-bg p-5">
        <h3 className="font-syne text-base font-600 text-charcoal mb-4">Score Breakdown — {org.score}/100</h3>
        <div className="space-y-3">
          {org.scoreComponents.map((comp) => (
            <div key={comp.name}>
              <div className="flex justify-between items-baseline mb-1">
                <span className="font-syne text-sm text-charcoal">{comp.name}</span>
                <span className="font-ibm text-xs tabular-nums text-muted-foreground">{comp.points}/{comp.maxPoints}</span>
              </div>
              <div className="h-1.5 bg-red-dim">
                <div
                  className={`h-full transition-all duration-500 ${comp.points / comp.maxPoints >= 0.75 ? 'bg-red-primary' : comp.points / comp.maxPoints >= 0.4 ? 'bg-amber-warn' : 'bg-[#6B7280]'}`}
                  style={{ width: `${comp.maxPoints > 0 ? (comp.points / comp.maxPoints) * 100 : 0}%` }}
                />
              </div>
              <p className="font-ibm text-[11px] text-muted-foreground mt-1">{comp.explanation}</p>
            </div>
          ))}
        </div>
      </div>

      {/* AI Next Steps */}
      <AINextStepsWidget org={org} />

      {/* Peer comparison */}
      {peers.length > 0 && (
        <div className="border border-red-dim bg-card-bg p-5">
          <h3 className="font-syne text-base font-600 text-charcoal mb-3">Sector Peers — {org.sector}</h3>
          <p className="font-ibm text-[11px] text-muted-foreground mb-4">Other organizations in the same sector for context</p>
          <div className="space-y-2">
            {peers.map((peer) => (
              <div key={peer.id} className="flex items-center justify-between py-2 border-b border-red-dim last:border-b-0">
                <div>
                  <span className="font-syne text-sm font-600 text-charcoal">{peer.name}</span>
                  <span className="font-ibm text-[11px] text-muted-foreground ml-2">{peer.province}</span>
                </div>
                <div className="flex items-center gap-3">
                  <span className="font-ibm text-sm tabular-nums text-charcoal">{peer.dependencyRatio}%</span>
                  <span className="font-ibm text-sm tabular-nums font-600 text-red-primary">{peer.score}</span>
                  <span className={`font-ibm text-[10px] uppercase tracking-wider px-2 py-0.5 ${STATUS_BG[peer.reviewStatus]}`}>
                    {peer.reviewStatus.replace(' REVIEW PRIORITY', '')}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Relationship summary */}
      {relatedOrgs.length > 0 && (
        <div className="border border-red-dim bg-card-bg p-5">
          <h3 className="font-syne text-base font-600 text-charcoal mb-3">Related Entities</h3>
          <p className="font-ibm text-[11px] text-muted-foreground mb-4">Organizations sharing directors, addresses, or other linkages</p>
          <div className="space-y-2">
            {relatedOrgs.map((rel) => (
              <div key={rel.id} className="flex items-center justify-between py-2 border-b border-red-dim last:border-b-0">
                <div>
                  <span className="font-syne text-sm font-600 text-charcoal">{rel.name}</span>
                  <span className="font-ibm text-[11px] text-muted-foreground ml-2">{rel.sector} · {rel.province}</span>
                </div>
                <div className="flex items-center gap-3">
                  <span className="font-ibm text-sm tabular-nums font-600 text-red-primary">{rel.score}</span>
                  <span className={`font-ibm text-[10px] uppercase tracking-wider px-2 py-0.5 ${STATUS_BG[rel.reviewStatus]}`}>
                    {rel.reviewStatus.replace(' REVIEW PRIORITY', '')}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Investigation agent */}
      <div className="border border-red-dim bg-card-bg">
        <div className="p-5 border-b border-red-dim flex items-center justify-between">
          <div>
            <h3 className="font-syne text-base font-600 text-charcoal">Investigation Agent</h3>
            <p className="font-ibm text-[11px] text-muted-foreground mt-1">6-step deterministic investigation trace — no LLM</p>
          </div>
          <button
            onClick={runInvestigation}
            disabled={investigationState === 'running'}
            className="flex items-center gap-2 px-4 py-2 bg-red-primary text-white font-ibm text-xs uppercase tracking-wider hover:bg-red-primary/90 transition disabled:opacity-50"
          >
            {investigationState === 'idle' && <><Play className="w-3.5 h-3.5" /> Investigate</>}
            {investigationState === 'running' && <><Loader2 className="w-3.5 h-3.5 animate-spin" /> Running...</>}
            {investigationState === 'done' && <><CheckCircle2 className="w-3.5 h-3.5" /> Complete</>}
          </button>
        </div>

        <AnimatePresence>
          {investigationTrace.length > 0 && (
            <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="p-5 space-y-3">
              {investigationState === 'done' && investigationTrace.length === 6 && (
                <div className="p-4 border border-red-primary bg-red-primary/5 mb-4">
                  <h4 className="font-syne text-sm font-600 text-red-primary mb-2">Investigation Summary</h4>
                  <p className="font-ibm text-sm text-charcoal">{investigationTrace[5].detail}</p>
                </div>
              )}
              {investigationTrace.map((step) => (
                <motion.div
                  key={step.step}
                  initial={{ opacity: 0, x: -10 }}
                  animate={{ opacity: 1, x: 0 }}
                  className="border border-red-dim"
                >
                  <button
                    onClick={() => setExpandedStep(expandedStep === step.step ? null : step.step)}
                    className="w-full flex items-center gap-3 p-3 hover:bg-card-bg transition text-left"
                  >
                    <span className="font-ibm text-xs font-700 text-red-primary tabular-nums w-8">#{step.step}</span>
                    <span className="font-syne text-sm text-charcoal flex-1">{step.title}</span>
                    <CheckCircle2 className="w-4 h-4 text-green-600 flex-shrink-0" />
                  </button>
                  <AnimatePresence>
                    {expandedStep === step.step && (
                      <motion.div initial={{ height: 0 }} animate={{ height: 'auto' }} exit={{ height: 0 }} className="overflow-hidden">
                        <div className="px-3 pb-3 pt-0 ml-11">
                          <p className="font-ibm text-xs text-charcoal">{step.detail}</p>
                          <p className="font-ibm text-[11px] text-muted-foreground mt-2 border-t border-red-dim pt-2">Evidence: {step.evidence}</p>
                        </div>
                      </motion.div>
                    )}
                  </AnimatePresence>
                </motion.div>
              ))}
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  )
}

function AINextStepsWidget({ org }: { org: Organization }) {
  const [generated, setGenerated] = useState(false)
  const [generating, setGenerating] = useState(false)

  const steps = useMemo(() => {
    const result: Array<{ priority: 'high' | 'medium' | 'low'; action: string; rationale: string }> = []
    if (org.criterionA) {
      result.push({
        priority: 'high',
        action: `Confirm cessation: request CRA filing status for ${org.lastFilingYear + 1}`,
        rationale: `Last filing was ${org.lastFilingDate}. ${org.daysSinceActivity} days without activity.`,
      })
    }
    if (org.criterionB) {
      result.push({
        priority: 'high',
        action: 'Verify contribution agreements match reported funding amounts',
        rationale: `${org.dependencyRatio}% dependency on ${formatMoney(org.totalFunding)} in public funding.`,
      })
    }
    if (org.relatedEntities.length > 0) {
      result.push({
        priority: 'medium',
        action: `Review related entities: ${org.relatedEntities.join(', ')}`,
        rationale: 'Director or address overlap detected with other flagged organizations.',
      })
    }
    if (org.employees === 0 || org.employees === null) {
      result.push({
        priority: 'medium',
        action: 'Cross-reference employee records with T4 filings and corporate registry',
        rationale: org.employees === 0 ? 'Zero employees reported despite receiving funding.' : 'Employee data unavailable.',
      })
    }
    result.push({
      priority: 'low',
      action: 'Check corporate registry for dissolution, name change, or amalgamation',
      rationale: 'Standard due diligence for all flagged organizations.',
    })
    return result
  }, [org])

  const generate = useCallback(() => {
    setGenerating(true)
    setTimeout(() => {
      setGenerating(false)
      setGenerated(true)
    }, 1500)
  }, [])

  const priorityColors = { high: 'border-red-primary bg-red-primary/5', medium: 'border-amber-warn bg-amber-warn/5', low: 'border-[#6B7280] bg-[#6B7280]/5' }
  const priorityBadge = { high: 'bg-red-primary text-white', medium: 'bg-amber-warn text-white', low: 'bg-[#6B7280] text-white' }

  return (
    <div className="border border-red-dim bg-card-bg">
      <div className="p-5 border-b border-red-dim flex items-center justify-between">
        <div>
          <h3 className="font-syne text-base font-600 text-charcoal">AI Next Steps</h3>
          <p className="font-ibm text-[11px] text-muted-foreground mt-1">Recommended actions based on structured profile data</p>
        </div>
        <button
          onClick={generate}
          disabled={generating || generated}
          className="flex items-center gap-2 px-4 py-2 bg-red-primary text-white font-ibm text-xs uppercase tracking-wider hover:bg-red-primary/90 transition disabled:opacity-50"
        >
          {generating ? <><Loader2 className="w-3.5 h-3.5 animate-spin" /> Generating...</> : generated ? <><CheckCircle2 className="w-3.5 h-3.5" /> Generated</> : 'Generate Next Steps'}
        </button>
      </div>
      <AnimatePresence>
        {generated && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="p-5 space-y-3">
            <div className="border-2 border-dashed border-amber-warn bg-amber-warn/5 p-3 mb-3">
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 bg-amber-warn" />
                <span className="font-ibm text-[10px] uppercase tracking-wider text-amber-warn font-600">AI-Generated Content</span>
              </div>
            </div>
            {steps.map((step, i) => (
              <motion.div
                key={i}
                initial={{ opacity: 0, x: -10 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: i * 0.1 }}
                className={`border-l-4 p-4 ${priorityColors[step.priority]}`}
              >
                <div className="flex items-center gap-2 mb-2">
                  <span className={`font-ibm text-[10px] uppercase tracking-wider px-2 py-0.5 ${priorityBadge[step.priority]}`}>
                    {step.priority}
                  </span>
                  <ChevronRight className="w-3 h-3 text-muted-foreground" />
                  <span className="font-syne text-sm font-600 text-charcoal">{step.action}</span>
                </div>
                <p className="font-ibm text-xs text-muted-foreground ml-1">{step.rationale}</p>
              </motion.div>
            ))}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

function FundingTab({ org }: { org: Organization }) {
  const chartData = useMemo(() => {
    const byYear: Record<string, number> = {}
    for (const ev of org.fundingEvents) {
      byYear[ev.yearLabel] = (byYear[ev.yearLabel] || 0) + ev.amount
    }
    return Object.entries(byYear).sort().map(([year, amount]) => ({ year, amount }))
  }, [org])

  return (
    <div className="space-y-6">
      <div className="border border-red-dim bg-card-bg p-5">
        <h3 className="font-syne text-base font-600 text-charcoal mb-1">Funding Events by Year</h3>
        <p className="font-ibm text-[11px] text-muted-foreground mb-4">Total: {formatMoney(org.totalFunding)} across {org.fundingEvents.length} events</p>
        <ResponsiveContainer width="100%" height={200}>
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(196,30,58,0.08)" vertical={false} />
            <XAxis dataKey="year" tick={{ fontSize: 11, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
            <YAxis tickFormatter={(v: number) => formatMoney(v)} tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
            <Bar dataKey="amount" fill="#C41E3A" isAnimationActive animationDuration={800} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Steelman for funding concentration */}
      {org.fundingEvents.length > 0 && (
        <div className="border border-red-dim bg-card-bg p-5">
          <h3 className="font-syne text-base font-600 text-charcoal mb-3">Funding Source Analysis</h3>
          <div className="border-2 border-dashed border-amber-warn bg-amber-warn/5 p-4">
            <div className="flex items-center gap-2 mb-2">
              <div className="w-2 h-2 bg-amber-warn" />
              <span className="font-ibm text-[10px] uppercase tracking-wider text-amber-warn font-600">AI-Generated Steelman</span>
            </div>
            <p className="font-ibm text-sm text-charcoal leading-relaxed">
              {org.fundingEvents.length === 1
                ? `A single funding event from ${org.fundingEvents[0].source} may indicate a targeted program rather than repeated dependence. However, this also means no diversification.`
                : `Funding from ${new Set(org.fundingEvents.map((e) => e.source)).size} distinct source(s) across ${new Set(org.fundingEvents.map((e) => e.yearLabel)).size} year(s). ${org.dependencyRatio >= 80 ? 'Despite multiple sources, high dependency persists — suggesting systemic reliance rather than a single-source anomaly.' : 'Multiple sources may indicate legitimate multi-program engagement rather than dependency on a single funder.'}`
              }
            </p>
          </div>
        </div>
      )}

      <div className="border border-red-dim bg-card-bg overflow-hidden">
        <table className="w-full">
          <thead>
            <tr className="bg-charcoal">
              <th className="text-left px-4 py-2.5 font-ibm text-[10px] uppercase tracking-wider text-white">Source</th>
              <th className="text-left px-4 py-2.5 font-ibm text-[10px] uppercase tracking-wider text-white">Program</th>
              <th className="text-right px-4 py-2.5 font-ibm text-[10px] uppercase tracking-wider text-white">Amount</th>
              <th className="text-left px-4 py-2.5 font-ibm text-[10px] uppercase tracking-wider text-white">Date</th>
            </tr>
          </thead>
          <tbody>
            {org.fundingEvents.map((ev, i) => (
              <tr key={i} className="border-b border-red-dim">
                <td className="px-4 py-3 font-ibm text-sm text-charcoal">{ev.source}</td>
                <td className="px-4 py-3 font-ibm text-sm text-muted-foreground">{ev.program}</td>
                <td className="px-4 py-3 font-ibm text-sm tabular-nums text-charcoal text-right">{formatMoney(ev.amount)}</td>
                <td className="px-4 py-3 font-ibm text-sm tabular-nums text-muted-foreground">{ev.date}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function ContinuityTab({ org }: { org: Organization }) {
  const allYears = useMemo(() => {
    const years = []
    for (let y = org.firstFilingYear; y <= org.lastFilingYear; y++) {
      years.push({ year: y, filed: org.filingYears.includes(y) })
    }
    return years
  }, [org])

  const gapYears = getGapYears(org)

  return (
    <div className="space-y-6">
      <MetricCard
        label="Time to Inactivity"
        desc="Months of observable activity after the most recent major funding event"
        value={`${org.monthsPostFundingActivity} months`}
        highlight={org.monthsPostFundingActivity <= 12}
      />

      <div className="border border-red-dim bg-card-bg p-5">
        <h3 className="font-syne text-base font-600 text-charcoal mb-1">CRA Filing History</h3>
        <p className="font-ibm text-[11px] text-muted-foreground mb-4">
          Filing range: {org.firstFilingYear}–{org.lastFilingYear}. Filed: {org.filingYears.length} of {org.lastFilingYear - org.firstFilingYear + 1} expected years.
        </p>
        <div className="flex gap-1.5 flex-wrap">
          {allYears.map((cell) => (
            <div
              key={cell.year}
              className={`w-14 h-14 flex items-center justify-center font-ibm text-xs ${
                cell.filed
                  ? 'bg-green-600 text-white border border-green-700'
                  : 'bg-red-primary/10 text-red-primary border border-red-primary'
              }`}
            >
              {cell.year}
            </div>
          ))}
        </div>
        {gapYears.length > 0 && (
          <p className="font-ibm text-xs text-red-primary mt-3">
            Gap years detected: {gapYears.join(', ')}
          </p>
        )}
      </div>

      {/* Steelman for continuity */}
      {gapYears.length > 0 && (
        <div className="border-2 border-dashed border-amber-warn bg-amber-warn/5 p-4">
          <div className="flex items-center gap-2 mb-2">
            <div className="w-2 h-2 bg-amber-warn" />
            <span className="font-ibm text-[10px] uppercase tracking-wider text-amber-warn font-600">AI-Generated Steelman</span>
          </div>
          <p className="font-ibm text-sm text-charcoal leading-relaxed">
            {gapYears.length === 1
              ? `A single gap year (${gapYears[0]}) could indicate a filing delay rather than organizational cessation. CRA processing backlogs or fiscal year changes sometimes cause one-year gaps that resolve in subsequent filings.`
              : `${gapYears.length} gap years (${gapYears.join(', ')}) are present. While consecutive gaps are a stronger signal of inactivity, some organizations file late or skip years during restructuring. The ${org.filingYears.includes(org.lastFilingYear) ? 'most recent filing being current is a positive indicator' : 'absence of a recent filing strengthens the inactivity signal'}.`
            }
          </p>
        </div>
      )}
    </div>
  )
}

function FlagsTab({ org, onOpenEvidence }: { org: Organization; onOpenEvidence: (flag: Flag) => void }) {
  const [verifiedFlags, setVerifiedFlags] = useState<Set<string>>(new Set())

  const verifyFlag = useCallback((code: string) => {
    setTimeout(() => {
      setVerifiedFlags((prev) => new Set([...prev, code]))
    }, 1200)
  }, [])

  if (!org.flags.length) {
    return <div className="p-8 text-center font-ibm text-sm text-muted-foreground">No flags raised for this organization.</div>
  }

  return (
    <div className="space-y-4">
      {org.flags.map((flag) => (
        <FlagCard
          key={flag.code}
          flag={flag}
          verified={verifiedFlags.has(flag.code)}
          onVerify={() => verifyFlag(flag.code)}
          onOpenEvidence={() => onOpenEvidence(flag)}
        />
      ))}
    </div>
  )
}

function FlagCard({ flag, verified, onVerify, onOpenEvidence }: { flag: Flag; verified: boolean; onVerify: () => void; onOpenEvidence: () => void }) {
  return (
    <div className="border border-red-dim bg-card-bg">
      <div className="p-4 border-b border-red-dim flex items-start justify-between gap-4">
        <div>
          <div className="flex items-center gap-2">
            <AlertTriangle className={`w-4 h-4 ${flag.severity === 'high' ? 'text-red-primary' : 'text-amber-warn'}`} />
            <h4 className="font-syne text-sm font-600 text-charcoal">{flag.label}</h4>
          </div>
          <p className="font-ibm text-xs text-muted-foreground mt-1">{flag.code} · {flag.points} points</p>
        </div>
        <span className={`font-ibm text-[10px] uppercase tracking-wider px-2 py-0.5 ${flag.severity === 'high' ? 'bg-red-primary text-white' : 'bg-amber-warn text-white'}`}>
          {flag.severity}
        </span>
      </div>
      <div className="p-4 space-y-3">
        <p className="font-ibm text-sm text-charcoal">{flag.explanation}</p>
        <div className="border-t border-red-dim pt-3">
          <p className="font-ibm text-[11px] text-muted-foreground mb-1">Evidence:</p>
          <p className="font-ibm text-xs text-charcoal">{flag.evidence}</p>
          <p className="font-ibm text-[10px] text-muted-foreground mt-1">Source: {flag.sourceTable} → {flag.sourceField}</p>
        </div>
        <div className="border-t border-red-dim pt-3 flex items-center gap-3">
          <button
            onClick={onVerify}
            disabled={verified}
            className={`flex items-center gap-1.5 px-3 py-1.5 font-ibm text-xs uppercase tracking-wider transition ${
              verified ? 'bg-green-600 text-white' : 'border border-red-dim text-charcoal hover:bg-red-dim/30'
            }`}
          >
            {verified ? <><CheckCircle2 className="w-3 h-3" /> Verified</> : 'Verify'}
          </button>
          <button
            onClick={onOpenEvidence}
            className="flex items-center gap-1.5 px-3 py-1.5 border border-red-dim font-ibm text-xs uppercase tracking-wider text-charcoal hover:bg-red-dim/30 transition"
          >
            <Eye className="w-3 h-3" /> View Evidence
          </button>
          {verified && <span className="font-ibm text-[11px] text-green-600">Flag evidence confirmed against source data.</span>}
        </div>
      </div>
    </div>
  )
}

function EvidenceDrawer({ flag, org, onClose }: { flag: Flag; org: Organization; onClose: () => void }) {
  return (
    <motion.div
      initial={{ x: '100%' }}
      animate={{ x: 0 }}
      exit={{ x: '100%' }}
      transition={{ type: 'spring', damping: 25, stiffness: 200 }}
      className="fixed top-0 right-0 h-full w-full max-w-lg z-50 bg-white border-l border-red-primary shadow-2xl overflow-y-auto"
    >
      <div className="p-6 space-y-6">
        <div className="flex items-center justify-between">
          <h3 className="font-syne text-lg font-600 text-charcoal">Evidence Detail</h3>
          <button onClick={onClose} className="p-2 hover:bg-red-dim/30 transition"><X className="w-5 h-5 text-charcoal" /></button>
        </div>

        <div className={`p-4 border-l-4 ${flag.severity === 'high' ? 'border-red-primary bg-red-primary/5' : 'border-amber-warn bg-amber-warn/5'}`}>
          <h4 className="font-syne text-sm font-600 text-charcoal">{flag.label}</h4>
          <p className="font-ibm text-xs text-muted-foreground mt-1">{flag.code} · {flag.severity} · {flag.points} points</p>
        </div>

        <div>
          <h4 className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground mb-2">Explanation</h4>
          <p className="font-ibm text-sm text-charcoal leading-relaxed">{flag.explanation}</p>
        </div>

        <div>
          <h4 className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground mb-2">Raw Evidence</h4>
          <div className="bg-charcoal text-white p-4 font-ibm text-xs leading-relaxed whitespace-pre-wrap">{flag.evidence}</div>
        </div>

        <div>
          <h4 className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground mb-2">Source Data</h4>
          <table className="w-full text-sm">
            <tbody>
              <tr className="border-b border-red-dim">
                <td className="py-2 font-ibm text-xs text-muted-foreground">Table</td>
                <td className="py-2 font-ibm text-xs text-charcoal">{flag.sourceTable}</td>
              </tr>
              <tr className="border-b border-red-dim">
                <td className="py-2 font-ibm text-xs text-muted-foreground">Field</td>
                <td className="py-2 font-ibm text-xs text-charcoal">{flag.sourceField}</td>
              </tr>
              <tr className="border-b border-red-dim">
                <td className="py-2 font-ibm text-xs text-muted-foreground">Organization</td>
                <td className="py-2 font-ibm text-xs text-charcoal">{org.name} ({org.id})</td>
              </tr>
              <tr>
                <td className="py-2 font-ibm text-xs text-muted-foreground">BN Root</td>
                <td className="py-2 font-ibm text-xs text-charcoal">{org.bnRoot}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div className="border-2 border-dashed border-amber-warn bg-amber-warn/5 p-4">
          <div className="flex items-center gap-2 mb-2">
            <div className="w-2 h-2 bg-amber-warn" />
            <span className="font-ibm text-[10px] uppercase tracking-wider text-amber-warn font-600">AI-Generated Steelman</span>
          </div>
          <p className="font-ibm text-sm text-charcoal leading-relaxed">
            {flag.code === 'ZOMBIE_INACTIVITY'
              ? `Inactivity could also be explained by a fiscal year-end change, CRA processing delay, or voluntary wind-down. Verify against corporate registry status before concluding cessation.`
              : flag.code === 'HIGH_DEPENDENCY'
              ? `High government funding dependency is common in sectors that serve public mandates (healthcare, social services). Check whether this organization's mandate is primarily public-interest.`
              : flag.code === 'NO_EMPLOYEES'
              ? `Zero reported employees may reflect volunteer-based operations, contracted labor, or seasonal staffing. This does not necessarily indicate a shell organization.`
              : `This flag should be interpreted in context. Consider the organization's sector, size, and history before drawing conclusions.`
            }
          </p>
        </div>
      </div>
    </motion.div>
  )
}

function AIAnalysisTab({ org }: { org: Organization }) {
  const [generated, setGenerated] = useState(false)
  const [generating, setGenerating] = useState(false)

  const generate = useCallback(() => {
    setGenerating(true)
    setTimeout(() => {
      setGenerating(false)
      setGenerated(true)
    }, 2000)
  }, [])

  const explanation = `${org.name} (BN ${org.bnRoot}) received ${formatMoney(org.totalFunding)} in public funding with ${org.dependencyRatio}% of total revenue coming from government sources. ${org.criterionA ? `The organization showed signs of inactivity within ${org.monthsPostFundingActivity} months of its most recent funding event.` : 'Filing continuity does not indicate post-funding inactivity.'} ${org.flags.length > 0 ? `The system identified ${org.flags.length} flag(s) through deterministic analysis.` : 'No flags were raised.'}`

  const evidence = [
    `Total public funding: ${formatMoney(org.totalFunding)} across ${org.fundingEvents.length} events`,
    `Dependency ratio: ${org.dependencyRatio}% (threshold: 70-80%)`,
    `Filing continuity: ${org.filingYears.length} filings from ${org.firstFilingYear} to ${org.lastFilingYear}`,
    org.employees !== null ? `Employees on record: ${org.employees}` : 'Employee data unavailable',
    `Score: ${org.score}/100 (${org.reviewStatus})`,
  ]

  const nextSteps = [
    'Verify funding amounts against original contribution agreements',
    `Cross-reference CRA filing data for fiscal years ${org.lastFilingYear} and ${org.lastFilingYear + 1}`,
    org.relatedEntities.length > 0 ? `Review related entities: ${org.relatedEntities.join(', ')}` : 'Check corporate registry for director cross-references',
    'Request updated filing status from CRA if gaps exist',
  ]

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <button
          onClick={generate}
          disabled={generating}
          className="flex items-center gap-2 px-5 py-2.5 bg-red-primary text-white font-ibm text-xs uppercase tracking-wider hover:bg-red-primary/90 transition disabled:opacity-50"
        >
          {generating ? <><Loader2 className="w-3.5 h-3.5 animate-spin" /> Generating...</> : 'Generate AI Explanation'}
        </button>
        <span className="font-ibm text-[11px] text-muted-foreground">AI reads only structured profile data — no unsupported inferences</span>
      </div>

      {generated && (
        <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="space-y-4">
          <div className="border-2 border-dashed border-amber-warn bg-amber-warn/5 p-5">
            <div className="flex items-center gap-2 mb-3">
              <div className="w-2 h-2 bg-amber-warn" />
              <span className="font-ibm text-[10px] uppercase tracking-wider text-amber-warn font-600">AI-Generated Content</span>
            </div>

            <div className="space-y-4">
              <div>
                <h4 className="font-syne text-sm font-600 text-charcoal mb-2">Explanation</h4>
                <p className="font-ibm text-sm text-charcoal leading-relaxed">{explanation}</p>
              </div>

              <div>
                <h4 className="font-syne text-sm font-600 text-charcoal mb-2">Key Evidence</h4>
                <ul className="space-y-1">
                  {evidence.map((ev, i) => (
                    <li key={i} className="font-ibm text-xs text-charcoal flex items-start gap-2">
                      <span className="text-red-primary mt-0.5">·</span> {ev}
                    </li>
                  ))}
                </ul>
              </div>

              <div>
                <h4 className="font-syne text-sm font-600 text-charcoal mb-2">Suggested Next Steps</h4>
                <ol className="space-y-1.5">
                  {nextSteps.map((step, i) => (
                    <li key={i} className="font-ibm text-xs text-charcoal flex items-start gap-2">
                      <span className="font-ibm text-[10px] text-red-primary font-700 mt-0.5">{i + 1}.</span> {step}
                    </li>
                  ))}
                </ol>
              </div>
            </div>
          </div>

          <div className="border border-red-dim bg-card-bg p-4">
            <h4 className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground mb-2">Evidence Used</h4>
            <p className="font-ibm text-xs text-muted-foreground">
              Sources: {org.dataSources.join(', ')} · Fields: total_revenue, gov_funding, fiscal_period_end, num_employees, registered_address · Score model: v1.0 deterministic
            </p>
          </div>
        </motion.div>
      )}
    </div>
  )
}

function ReviewTab({ org }: { org: Organization }) {
  const [disposition, setDisposition] = useState<'pending' | 'confirmed-zombie' | 'cleared' | 'needs-info'>('pending')
  const [notes, setNotes] = useState('')
  const [attestedBy, setAttestedBy] = useState('')
  const [saved, setSaved] = useState(false)
  const [memoGenerated, setMemoGenerated] = useState(false)
  const [generatingMemo, setGeneratingMemo] = useState(false)

  const auditTrail = useMemo(() => [
    { action: 'Profile created by system', timestamp: '2026-04-15T10:00:00Z', user: 'System' },
    { action: 'Investigation agent run', timestamp: '2026-04-20T14:30:00Z', user: 'System' },
    { action: 'Flagged for review', timestamp: '2026-04-22T09:15:00Z', user: 'Triage Agent' },
  ], [])

  const saveReview = useCallback(() => {
    setSaved(true)
    setTimeout(() => setSaved(false), 3000)
  }, [])

  const generateMemo = useCallback(() => {
    setGeneratingMemo(true)
    setTimeout(() => {
      setGeneratingMemo(false)
      setMemoGenerated(true)
    }, 2000)
  }, [])

  const memoText = useMemo(() => {
    if (!memoGenerated) return ''
    return [
      `REVIEW MEMO — ${org.name}`,
      `BN: ${org.bnRoot} | Score: ${org.score}/100 | Status: ${org.reviewStatus}`,
      ``,
      `DISPOSITION: ${disposition.toUpperCase()}`,
      ``,
      `SUMMARY:`,
      `${org.name} received ${formatMoney(org.totalFunding)} in public funding with ${org.dependencyRatio}% dependency on government sources. ` +
      `${org.criterionA ? `Post-funding inactivity detected within ${org.monthsPostFundingActivity} months. ` : ''}` +
      `${org.criterionB ? `Dependency ratio exceeds 70% threshold. ` : ''}` +
      `${org.bothCriteria ? 'Both zombie criteria are met.' : org.criterionA || org.criterionB ? 'One criterion met.' : 'No criteria met.'}`,
      ``,
      `FLAGS (${org.flags.length}):`,
      ...org.flags.map((f) => `- ${f.code}: ${f.explanation}`),
      ``,
      `SCORE BREAKDOWN:`,
      ...org.scoreComponents.map((c) => `- ${c.name}: ${c.points}/${c.maxPoints} — ${c.explanation}`),
      ``,
      `DATA SOURCES: ${org.dataSources.join(', ')}`,
      notes ? `\nREVIEWER NOTES:\n${notes}` : '',
      attestedBy ? `\nATTESTED BY: ${attestedBy}` : '',
    ].join('\n')
  }, [memoGenerated, org, disposition, notes, attestedBy])

  const copyMemo = useCallback(() => {
    navigator.clipboard.writeText(memoText)
  }, [memoText])

  return (
    <div className="space-y-6">
      {/* Disposition */}
      <div className="border border-red-dim bg-card-bg p-5">
        <h3 className="font-syne text-base font-600 text-charcoal mb-4">Review Disposition</h3>
        <div className="flex gap-2 flex-wrap">
          {DISPOSITION_OPTIONS.map((opt) => (
            <button
              key={opt.value}
              onClick={() => setDisposition(opt.value)}
              className={`flex items-center gap-2 px-4 py-2 font-ibm text-xs uppercase tracking-wider transition border ${
                disposition === opt.value
                  ? `${opt.color} text-white border-transparent`
                  : 'border-red-dim text-charcoal hover:bg-red-dim/30'
              }`}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {/* Attestation */}
      <div className="border border-red-dim bg-card-bg p-5 space-y-4">
        <h3 className="font-syne text-base font-600 text-charcoal">Attestation</h3>
        <div>
          <label className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground">Reviewer Name</label>
          <input
            value={attestedBy}
            onChange={(e) => setAttestedBy(e.target.value)}
            placeholder="Your name"
            className="w-full mt-1 px-3 py-2 border border-red-dim font-ibm text-sm bg-white text-charcoal outline-none"
          />
        </div>
        <div>
          <label className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground">Review Notes</label>
          <textarea
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            placeholder="Add notes about your review findings, additional context, or rationale for disposition..."
            rows={4}
            className="w-full mt-1 px-3 py-2 border border-red-dim font-ibm text-sm bg-white text-charcoal outline-none resize-y"
          />
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={saveReview}
            className="flex items-center gap-2 px-5 py-2 bg-red-primary text-white font-ibm text-xs uppercase tracking-wider hover:bg-red-primary/90 transition"
          >
            {saved ? <><CheckCircle2 className="w-3.5 h-3.5" /> Saved</> : 'Save Review'}
          </button>
          {saved && <span className="font-ibm text-[11px] text-green-600">Review disposition saved successfully.</span>}
        </div>
      </div>

      {/* Audit trail */}
      <div className="border border-red-dim bg-card-bg p-5">
        <h3 className="font-syne text-base font-600 text-charcoal mb-4">Audit Trail</h3>
        <div className="space-y-2">
          {auditTrail.map((entry, i) => (
            <div key={i} className="flex items-center gap-4 py-2 border-b border-red-dim last:border-b-0">
              <div className="w-2 h-2 bg-red-primary rounded-full flex-shrink-0" />
              <div className="flex-1">
                <span className="font-ibm text-xs text-charcoal">{entry.action}</span>
              </div>
              <span className="font-ibm text-[10px] text-muted-foreground tabular-nums">{new Date(entry.timestamp).toLocaleDateString()}</span>
              <span className="font-ibm text-[10px] text-muted-foreground">{entry.user}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Review memo generator */}
      <div className="border border-red-dim bg-card-bg">
        <div className="p-5 border-b border-red-dim flex items-center justify-between">
          <div>
            <h3 className="font-syne text-base font-600 text-charcoal">Review Memo</h3>
            <p className="font-ibm text-[11px] text-muted-foreground mt-1">Generate a printable summary of this review</p>
          </div>
          <button
            onClick={generateMemo}
            disabled={generatingMemo}
            className="flex items-center gap-2 px-4 py-2 bg-red-primary text-white font-ibm text-xs uppercase tracking-wider hover:bg-red-primary/90 transition disabled:opacity-50"
          >
            {generatingMemo ? <><Loader2 className="w-3.5 h-3.5 animate-spin" /> Generating...</> : <><FileText className="w-3.5 h-3.5" /> Generate Memo</>}
          </button>
        </div>

        {memoGenerated && (
          <div className="p-5 space-y-3">
            <div className="border-2 border-dashed border-amber-warn bg-amber-warn/5 p-3">
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 bg-amber-warn" />
                <span className="font-ibm text-[10px] uppercase tracking-wider text-amber-warn font-600">AI-Generated Memo</span>
              </div>
            </div>
            <pre className="bg-charcoal text-white p-5 font-ibm text-xs leading-relaxed whitespace-pre-wrap overflow-x-auto">{memoText}</pre>
            <div className="flex gap-2">
              <button
                onClick={copyMemo}
                className="flex items-center gap-1.5 px-3 py-1.5 border border-red-dim font-ibm text-xs uppercase tracking-wider text-charcoal hover:bg-red-dim/30 transition"
              >
                <ClipboardCopy className="w-3 h-3" /> Copy
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function MetricCard({ label, desc, value, highlight }: { label: string; desc: string; value: string; highlight?: boolean }) {
  return (
    <div className={`bg-card-bg border p-4 ${highlight ? 'border-red-primary border-l-4 border-l-red-primary' : 'border-red-dim'}`}>
      <div className={`font-ibm text-2xl font-700 tabular-nums ${highlight ? 'text-red-primary' : 'text-charcoal'}`}>{value}</div>
      <div className="font-syne text-sm font-600 text-charcoal mt-1">{label}</div>
      <div className="font-ibm text-[11px] text-muted-foreground mt-0.5">{desc}</div>
    </div>
  )
}

function getGapYears(org: Organization): number[] {
  const gaps: number[] = []
  for (let y = org.firstFilingYear; y <= org.lastFilingYear; y++) {
    if (!org.filingYears.includes(y)) gaps.push(y)
  }
  return gaps
}
