'use client'

import { useMemo, useState, useCallback } from 'react'
import { motion } from 'framer-motion'
import { X, Loader2 } from 'lucide-react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell,
} from 'recharts'
import { useWorkspace } from '@/lib/workspace-context'
import { ORGANIZATIONS, formatMoney, type Organization } from '@/lib/data'

const COMPARE_METRICS = [
  { key: 'totalFunding', label: 'Total Public Funding', format: 'money' },
  { key: 'totalRevenue', label: 'Total Revenue', format: 'money' },
  { key: 'dependencyRatio', label: 'Dependency %', format: 'pct' },
  { key: 'monthsPostFundingActivity', label: 'Months Post-Funding Activity', format: 'num' },
  { key: 'daysSinceActivity', label: 'Days Since Activity', format: 'num' },
  { key: 'score', label: 'Risk Score', format: 'num' },
  { key: 'employees', label: 'Employees', format: 'nullable' },
  { key: 'lastFilingYear', label: 'Last Filing Year', format: 'num' },
] as const

const STATUS_BG: Record<string, string> = {
  'HIGH REVIEW PRIORITY': 'bg-red-primary text-white',
  'MEDIUM REVIEW PRIORITY': 'bg-amber-warn text-white',
  'LOW REVIEW PRIORITY': 'bg-[#6B7280] text-white',
}

const ORG_COLORS = ['#C41E3A', '#C8922A', '#6B7280']

export default function CompareView() {
  const { compareSlots, removeFromCompare, clearCompare, setActiveView } = useWorkspace()
  const [aiSummary, setAiSummary] = useState<string | null>(null)
  const [generating, setGenerating] = useState(false)

  const orgs = useMemo(() => {
    return compareSlots.map((id) => ORGANIZATIONS.find((o) => o.id === id)).filter(Boolean) as Organization[]
  }, [compareSlots])

  const discrepancies = useMemo(() => {
    if (orgs.length < 2) return []
    const results: Array<{ metric: string; delta: number; high: string; low: string; description: string }> = []

    const metrics = [
      { key: 'score', label: 'Risk Score', unit: '' },
      { key: 'dependencyRatio', label: 'Dependency Ratio', unit: '%' },
      { key: 'totalFunding', label: 'Total Funding', unit: '$' },
      { key: 'monthsPostFundingActivity', label: 'Post-Funding Activity', unit: 'mo' },
      { key: 'daysSinceActivity', label: 'Days Since Activity', unit: 'd' },
    ]

    for (const m of metrics) {
      const vals = orgs.map((o) => ({ name: o.name, val: (o as unknown as Record<string, unknown>)[m.key] as number })).filter((v) => v.val !== null)
      if (vals.length < 2) continue
      const sorted = [...vals].sort((a, b) => b.val - a.val)
      const delta = sorted[0].val - sorted[sorted.length - 1].val
      if (delta > 0) {
        results.push({
          metric: m.label,
          delta,
          high: sorted[0].name,
          low: sorted[sorted.length - 1].name,
          description: m.unit === '$'
            ? `${sorted[0].name} received ${formatMoney(delta)} more than ${sorted[sorted.length - 1].name}`
            : `${sorted[0].name} is ${delta}${m.unit} higher than ${sorted[sorted.length - 1].name}`,
        })
      }
    }
    return results.sort((a, b) => b.delta - a.delta)
  }, [orgs])

  const sharedRelationships = useMemo(() => {
    if (orgs.length < 2) return []
    const shared: string[] = []
    for (let i = 0; i < orgs.length; i++) {
      for (let j = i + 1; j < orgs.length; j++) {
        if (orgs[i].relatedEntities.includes(orgs[j].id)) {
          shared.push(`${orgs[i].name} ↔ ${orgs[j].name}`)
        }
      }
    }
    return shared
  }, [orgs])

  const sharedSources = useMemo(() => {
    if (orgs.length < 2) return []
    const sourcesPerOrg = orgs.map((o) => new Set(o.fundingEvents.map((e) => e.source)))
    const allSources = new Set(sourcesPerOrg.flatMap((s) => [...s]))
    const shared: Array<{ source: string; orgNames: string[] }> = []
    for (const source of allSources) {
      const matching = orgs.filter((_, i) => sourcesPerOrg[i].has(source))
      if (matching.length >= 2) {
        shared.push({ source, orgNames: matching.map((o) => o.name) })
      }
    }
    return shared
  }, [orgs])

  const barChartData = useMemo(() => {
    return COMPARE_METRICS
      .filter((m) => m.format === 'num' || m.format === 'pct')
      .map((metric) => {
        const row: Record<string, unknown> = { metric: metric.label }
        for (const org of orgs) {
          row[org.name] = (org as unknown as Record<string, unknown>)[metric.key] as number
        }
        return row
      })
  }, [orgs])

  const fundingChartData = useMemo(() => {
    return orgs.map((org) => ({ name: org.name, funding: org.totalFunding }))
  }, [orgs])

  const generateComparison = useCallback(() => {
    if (orgs.length < 2) return
    setGenerating(true)
    setTimeout(() => {
      const sorted = [...orgs].sort((a, b) => b.score - a.score)
      const highest = sorted[0]
      const lowest = sorted[sorted.length - 1]

      setAiSummary(
        `Comparing ${orgs.length} organizations, ${highest.name} presents the strongest zombie profile with a score of ${highest.score}/100, ${highest.dependencyRatio}% public funding dependency, and only ${highest.monthsPostFundingActivity} months of post-funding activity. ` +
        `In contrast, ${lowest.name} scores ${lowest.score}/100 with ${lowest.dependencyRatio}% dependency and ${lowest.monthsPostFundingActivity} months of activity. ` +
        `The most significant discrepancy is in post-funding activity duration: ${highest.name} showed activity for only ${highest.monthsPostFundingActivity} months compared to ${lowest.name}'s ${lowest.monthsPostFundingActivity} months. ` +
        (sharedSources.length > 0
          ? `Notably, these organizations share ${sharedSources.length} common funding source(s): ${sharedSources.map((s) => s.source).join(', ')}. `
          : '') +
        (sharedRelationships.length > 0
          ? `Direct relationships detected: ${sharedRelationships.join('; ')}. `
          : '') +
        `Priority order for review: ${sorted.map((o, i) => `${i + 1}. ${o.name} (${o.score})`).join(', ')}.`
      )
      setGenerating(false)
    }, 2000)
  }, [orgs, sharedSources, sharedRelationships])

  if (orgs.length === 0) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-center">
          <p className="font-syne text-lg text-muted-foreground">No organizations in compare panel</p>
          <p className="font-ibm text-sm text-muted-foreground mt-2">Add organizations from the Discover or Investigate views</p>
          <button onClick={() => setActiveView('discover')} className="mt-4 font-ibm text-sm text-red-primary underline">Go to Discover</button>
        </div>
      </div>
    )
  }

  const formatValue = (key: string, value: unknown, format: string): string => {
    if (value === null || value === undefined) return 'N/A'
    switch (format) {
      case 'money': return formatMoney(value as number)
      case 'pct': return `${value}%`
      case 'nullable': return value !== null ? String(value) : 'Unknown'
      default: return String(value)
    }
  }

  const getDiscrepancyClass = (key: string, values: (number | null)[]): string => {
    const nums = values.filter((v): v is number => v !== null)
    if (nums.length < 2) return ''
    const max = Math.max(...nums)
    const min = Math.min(...nums)
    if (min === 0 && max > 0) return 'bg-red-primary/10'
    if (max / (min || 1) > 1.5) return 'bg-red-primary/5'
    return ''
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="font-syne text-xl font-700 text-charcoal">Side-by-Side Comparison</h2>
          <p className="font-ibm text-sm text-muted-foreground mt-1">Comparing {orgs.length} organizations</p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={generateComparison}
            disabled={generating || orgs.length < 2}
            className="flex items-center gap-2 px-4 py-2 bg-red-primary text-white font-ibm text-xs uppercase tracking-wider hover:bg-red-primary/90 transition disabled:opacity-50"
          >
            {generating ? <><Loader2 className="w-3.5 h-3.5 animate-spin" /> Generating...</> : 'AI Comparison Summary'}
          </button>
          <button onClick={clearCompare} className="px-3 py-2 border border-red-dim font-ibm text-xs uppercase tracking-wider hover:bg-red-dim/30 transition">
            Clear All
          </button>
        </div>
      </div>

      {/* Column headers */}
      <div className="grid gap-4" style={{ gridTemplateColumns: `200px repeat(${orgs.length}, 1fr)` }}>
        <div />
        {orgs.map((org, i) => (
          <motion.div key={org.id} initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="border border-red-dim bg-card-bg p-4 relative">
            <button
              onClick={() => removeFromCompare(org.id)}
              className="absolute top-2 right-2 p-1 hover:bg-red-dim/30 transition"
            >
              <X className="w-3.5 h-3.5 text-muted-foreground" />
            </button>
            <div className="flex items-center gap-2 mb-2">
              <div className="w-3 h-3" style={{ backgroundColor: ORG_COLORS[i] }} />
              <span className={`font-ibm text-[10px] uppercase tracking-wider px-2 py-0.5 ${STATUS_BG[org.reviewStatus]}`}>
                {org.reviewStatus.replace(' REVIEW PRIORITY', '')}
              </span>
            </div>
            <h3 className="font-syne text-base font-600 text-charcoal mt-1">{org.name}</h3>
            <div className="font-ibm text-2xl font-700 text-red-primary tabular-nums mt-1">{org.score}</div>
            <div className="flex gap-1.5 mt-2 flex-wrap">
              {org.dataSources.map((s) => (
                <span key={s} className="font-ibm text-[9px] uppercase tracking-wider px-1.5 py-0.5 border border-red-dim text-muted-foreground">{s}</span>
              ))}
            </div>
          </motion.div>
        ))}
      </div>

      {/* Comparison charts */}
      {orgs.length >= 2 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="border border-red-dim bg-card-bg p-5">
            <h3 className="font-syne text-base font-600 text-charcoal mb-1">Funding Comparison</h3>
            <p className="font-ibm text-[11px] text-muted-foreground mb-4">Total public funding received ($CAD)</p>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={fundingChartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(196,30,58,0.08)" vertical={false} />
                <XAxis dataKey="name" tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
                <YAxis tickFormatter={(v: number) => formatMoney(v)} tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
                <Tooltip content={<CompareTooltip />} />
                <Bar dataKey="funding" isAnimationActive animationDuration={800}>
                  {fundingChartData.map((_, index) => (
                    <Cell key={index} fill={ORG_COLORS[index % ORG_COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="border border-red-dim bg-card-bg p-5">
            <h3 className="font-syne text-base font-600 text-charcoal mb-1">Key Metrics</h3>
            <p className="font-ibm text-[11px] text-muted-foreground mb-4">Score, dependency, activity duration comparison</p>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={[
                { metric: 'Score', ...Object.fromEntries(orgs.map((o) => [o.name, o.score])) },
                { metric: 'Dep %', ...Object.fromEntries(orgs.map((o) => [o.name, o.dependencyRatio])) },
                { metric: 'Months Active', ...Object.fromEntries(orgs.map((o) => [o.name, o.monthsPostFundingActivity])) },
              ]}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(196,30,58,0.08)" vertical={false} />
                <XAxis dataKey="metric" tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
                <YAxis tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
                <Tooltip content={<CompareTooltip />} />
                {orgs.map((org, i) => (
                  <Bar key={org.id} dataKey={org.name} fill={ORG_COLORS[i]} isAnimationActive animationDuration={800} />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Metric rows */}
      <div className="border border-red-dim">
        {COMPARE_METRICS.map((metric) => {
          const values = orgs.map((o) => (o as unknown as Record<string, unknown>)[metric.key] as number | null)
          const discrepancy = getDiscrepancyClass(metric.key, values)

          return (
            <div
              key={metric.key}
              className={`grid border-b border-red-dim last:border-b-0 ${discrepancy}`}
              style={{ gridTemplateColumns: `200px repeat(${orgs.length}, 1fr)` }}
            >
              <div className="px-4 py-3 font-syne text-sm text-charcoal border-r border-red-dim flex items-center">
                {metric.label}
              </div>
              {orgs.map((org) => (
                <div key={org.id} className="px-4 py-3 font-ibm text-sm tabular-nums text-charcoal border-r border-red-dim last:border-r-0 flex items-center">
                  {formatValue(metric.key, (org as unknown as Record<string, unknown>)[metric.key], metric.format)}
                </div>
              ))}
            </div>
          )
        })}

        {/* Flags row */}
        <div
          className="grid border-b border-red-dim last:border-b-0"
          style={{ gridTemplateColumns: `200px repeat(${orgs.length}, 1fr)` }}
        >
          <div className="px-4 py-3 font-syne text-sm text-charcoal border-r border-red-dim">Active Flags</div>
          {orgs.map((org) => (
            <div key={org.id} className="px-4 py-3 border-r border-red-dim last:border-r-0 flex flex-wrap gap-1">
              {org.flags.map((f) => (
                <span key={f.code} className={`font-ibm text-[9px] uppercase tracking-wider px-1.5 py-0.5 ${f.severity === 'high' ? 'bg-red-primary text-white' : 'bg-amber-warn text-white'}`}>
                  {f.code}
                </span>
              ))}
              {org.flags.length === 0 && <span className="font-ibm text-xs text-muted-foreground">None</span>}
            </div>
          ))}
        </div>

        {/* Criteria row */}
        <div
          className="grid"
          style={{ gridTemplateColumns: `200px repeat(${orgs.length}, 1fr)` }}
        >
          <div className="px-4 py-3 font-syne text-sm text-charcoal border-r border-red-dim">Criteria Met</div>
          {orgs.map((org) => (
            <div key={org.id} className="px-4 py-3 border-r border-red-dim last:border-r-0 flex gap-2">
              <span className={`font-ibm text-[10px] uppercase tracking-wider px-2 py-0.5 ${org.criterionA ? 'bg-red-primary text-white' : 'bg-card-bg text-muted-foreground border border-red-dim'}`}>A</span>
              <span className={`font-ibm text-[10px] uppercase tracking-wider px-2 py-0.5 ${org.criterionB ? 'bg-red-primary text-white' : 'bg-card-bg text-muted-foreground border border-red-dim'}`}>B</span>
            </div>
          ))}
        </div>
      </div>

      {/* Discrepancy summary */}
      {discrepancies.length > 0 && (
        <div className="border border-red-dim bg-card-bg p-5">
          <h3 className="font-syne text-base font-600 text-charcoal mb-4">Key Discrepancies</h3>
          <div className="space-y-3">
            {discrepancies.slice(0, 5).map((d, i) => (
              <div key={i} className="flex items-start gap-3 py-2 border-b border-red-dim last:border-b-0">
                <span className="font-ibm text-xs font-700 text-red-primary tabular-nums w-6">#{i + 1}</span>
                <div>
                  <span className="font-syne text-sm font-600 text-charcoal">{d.metric}</span>
                  <p className="font-ibm text-xs text-muted-foreground mt-0.5">{d.description}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Relationship overlap */}
      {(sharedRelationships.length > 0 || sharedSources.length > 0) && (
        <div className="border border-red-dim bg-card-bg p-5">
          <h3 className="font-syne text-base font-600 text-charcoal mb-4">Relationship Overlap</h3>

          {sharedRelationships.length > 0 && (
            <div className="mb-4">
              <h4 className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground mb-2">Direct Entity Relationships</h4>
              {sharedRelationships.map((rel, i) => (
                <div key={i} className="flex items-center gap-2 py-1.5">
                  <div className="w-2 h-2 bg-red-primary" />
                  <span className="font-ibm text-sm text-charcoal">{rel}</span>
                </div>
              ))}
            </div>
          )}

          {sharedSources.length > 0 && (
            <div>
              <h4 className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground mb-2">Shared Funding Sources</h4>
              {sharedSources.map((s, i) => (
                <div key={i} className="flex items-center gap-2 py-1.5 border-b border-red-dim last:border-b-0">
                  <div className="w-2 h-2 bg-amber-warn" />
                  <div>
                    <span className="font-ibm text-sm text-charcoal">{s.source}</span>
                    <span className="font-ibm text-[11px] text-muted-foreground ml-2">({s.orgNames.join(', ')})</span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* AI Summary */}
      {aiSummary && (
        <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}>
          <div className="border-2 border-dashed border-amber-warn bg-amber-warn/5 p-5">
            <div className="flex items-center gap-2 mb-3">
              <div className="w-2 h-2 bg-amber-warn" />
              <span className="font-ibm text-[10px] uppercase tracking-wider text-amber-warn font-600">AI-Generated Comparison</span>
            </div>
            <p className="font-ibm text-sm text-charcoal leading-relaxed">{aiSummary}</p>
          </div>
          <div className="border border-red-dim bg-card-bg p-4 mt-2">
            <h4 className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground mb-1">Evidence Used</h4>
            <p className="font-ibm text-xs text-muted-foreground">
              Compared fields: total_funding, dependency_ratio, months_post_funding, score, flags, criteria_met, related_entities, funding_sources · Model: deterministic scoring v1.0
            </p>
          </div>
        </motion.div>
      )}
    </div>
  )
}

function CompareTooltip({ active, payload, label }: { active?: boolean; payload?: Array<{ dataKey: string; value: number; color: string; name?: string }>; label?: string }) {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-charcoal text-white border border-red-primary p-2.5 shadow-lg">
      <p className="font-ibm text-xs font-600 mb-1 uppercase tracking-wider">{label}</p>
      {payload.map((e) => (
        <div key={e.dataKey} className="flex items-center gap-2 text-xs font-ibm">
          <div className="w-2 h-2" style={{ backgroundColor: e.color }} />
          <span className="text-white/70">{e.name || e.dataKey}:</span>
          <span className="font-500 tabular-nums">{typeof e.value === 'number' && e.value > 1000 ? formatMoney(e.value) : e.value}</span>
        </div>
      ))}
    </div>
  )
}
