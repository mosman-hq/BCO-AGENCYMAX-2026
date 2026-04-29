'use client'

import { useMemo, useState, useCallback } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceArea, ReferenceLine, Label,
  BarChart, Bar, AreaChart, Area, PieChart, Pie, Cell,
} from 'recharts'
import { Play, Loader2, CheckCircle2, Plus, MessageSquare, User, X, SlidersHorizontal, ChevronDown, ChevronUp } from 'lucide-react'
import { useWorkspace } from '@/lib/workspace-context'
import {
  ORGANIZATIONS, getFilteredOrgs, getSummaryStats, formatMoney,
  getAggregateByIndustry, getAggregateBySizeBand, getAggregateBySource, getAggregateByDependencyBand,
  getScoreDistribution, getProvinceBreakdown,
  type Organization, type AggregateRow,
} from '@/lib/data'

const CRITERIA_TABS = [
  { key: 'all' as const, label: 'All Organizations' },
  { key: 'criterionA' as const, label: 'Criterion A Only' },
  { key: 'criterionB' as const, label: 'Criterion B Only' },
  { key: 'both' as const, label: 'Both Criteria' },
]

const STATUS_STYLES: Record<string, string> = {
  'HIGH REVIEW PRIORITY': 'bg-red-primary text-white',
  'MEDIUM REVIEW PRIORITY': 'bg-amber-warn text-white',
  'LOW REVIEW PRIORITY': 'bg-[#6B7280] text-white',
}

const INACTIVITY_OPTIONS = [
  { value: 6, label: '6 months' },
  { value: 12, label: '12 months' },
  { value: 18, label: '18 months' },
]

type SortKey = 'score' | 'totalFunding' | 'dependencyRatio' | 'daysSinceActivity' | 'name'
type SortDir = 'asc' | 'desc'

interface FilterChip {
  key: string
  label: string
  remove: () => void
}

function AnimatedNumber({ value, prefix = '' }: { value: number; prefix?: string }) {
  const [displayed, setDisplayed] = useState(0)

  useMemo(() => {
    let frame: number
    const start = performance.now()
    const duration = 1500
    const tick = (now: number) => {
      const progress = Math.min((now - start) / duration, 1)
      const eased = 1 - Math.pow(1 - progress, 3)
      setDisplayed(Math.round(eased * value))
      if (progress < 1) frame = requestAnimationFrame(tick)
    }
    frame = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(frame)
  }, [value])

  if (value >= 1000000) return <>{prefix}{(displayed / 1000000).toFixed(1)}M</>
  return <>{prefix}{displayed.toLocaleString()}</>
}

export default function DiscoverView() {
  const { criteriaFilter, setCriteriaFilter, openProfile, addToCompare, openAssistantForOrg } = useWorkspace()
  const [inactivityThreshold, setInactivityThreshold] = useState(12)
  const [sectorFilter, setSectorFilter] = useState<string | null>(null)
  const [provinceFilter, setProvinceFilter] = useState<string | null>(null)
  const [minScore, setMinScore] = useState(0)
  const [sortKey, setSortKey] = useState<SortKey>('score')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [searchTerm, setSearchTerm] = useState('')
  const [triageState, setTriageState] = useState<'idle' | 'running' | 'done'>('idle')
  const [triageResults, setTriageResults] = useState<Organization[]>([])
  const [showAggregates, setShowAggregates] = useState(false)
  const [activeAggTab, setActiveAggTab] = useState<'industry' | 'size' | 'source' | 'dependency'>('industry')

  const baseOrgs = useMemo(() => getFilteredOrgs(criteriaFilter), [criteriaFilter])

  const filteredOrgs = useMemo(() => {
    let result = baseOrgs
    if (sectorFilter) result = result.filter((o) => o.sector === sectorFilter)
    if (provinceFilter) result = result.filter((o) => o.province === provinceFilter)
    if (minScore > 0) result = result.filter((o) => o.score >= minScore)
    if (searchTerm) {
      const lower = searchTerm.toLowerCase()
      result = result.filter((o) => o.name.toLowerCase().includes(lower) || o.bnRoot.includes(lower) || o.sector.toLowerCase().includes(lower))
    }
    return result
  }, [baseOrgs, sectorFilter, provinceFilter, minScore, searchTerm])

  const sortedOrgs = useMemo(() => {
    const sorted = [...filteredOrgs]
    sorted.sort((a, b) => {
      const av = a[sortKey]
      const bv = b[sortKey]
      if (typeof av === 'string' && typeof bv === 'string') return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      return sortDir === 'asc' ? (av as number) - (bv as number) : (bv as number) - (av as number)
    })
    return sorted
  }, [filteredOrgs, sortKey, sortDir])

  const stats = useMemo(() => getSummaryStats(filteredOrgs), [filteredOrgs])
  const sectors = useMemo(() => [...new Set(ORGANIZATIONS.map((o) => o.sector))].sort(), [])
  const provinces = useMemo(() => [...new Set(ORGANIZATIONS.map((o) => o.province))].sort(), [])
  const aggByIndustry = useMemo(() => getAggregateByIndustry(filteredOrgs), [filteredOrgs])
  const aggBySize = useMemo(() => getAggregateBySizeBand(filteredOrgs), [filteredOrgs])
  const aggBySource = useMemo(() => getAggregateBySource(filteredOrgs), [filteredOrgs])
  const aggByDependency = useMemo(() => getAggregateByDependencyBand(filteredOrgs), [filteredOrgs])
  const scoreDistribution = useMemo(() => getScoreDistribution(filteredOrgs), [filteredOrgs])
  const provinceBreakdown = useMemo(() => getProvinceBreakdown(filteredOrgs), [filteredOrgs])

  const runTriage = useCallback(() => {
    setTriageState('running')
    setTimeout(() => {
      const sorted = [...filteredOrgs].sort((a, b) => b.score - a.score).slice(0, 10)
      setTriageResults(sorted)
      setTriageState('done')
    }, 2500)
  }, [filteredOrgs])

  const toggleSort = useCallback((key: SortKey) => {
    if (sortKey === key) {
      setSortDir((d) => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('desc')
    }
  }, [sortKey])

  const filterChips: FilterChip[] = useMemo(() => {
    const chips: FilterChip[] = []
    if (sectorFilter) chips.push({ key: 'sector', label: `Sector: ${sectorFilter}`, remove: () => setSectorFilter(null) })
    if (provinceFilter) chips.push({ key: 'province', label: `Province: ${provinceFilter}`, remove: () => setProvinceFilter(null) })
    if (minScore > 0) chips.push({ key: 'score', label: `Score ≥ ${minScore}`, remove: () => setMinScore(0) })
    if (searchTerm) chips.push({ key: 'search', label: `Search: "${searchTerm}"`, remove: () => setSearchTerm('') })
    if (inactivityThreshold !== 12) chips.push({ key: 'inactivity', label: `Inactivity: ${inactivityThreshold}mo`, remove: () => setInactivityThreshold(12) })
    return chips
  }, [sectorFilter, provinceFilter, minScore, searchTerm, inactivityThreshold])

  const clearAllFilters = useCallback(() => {
    setSectorFilter(null)
    setProvinceFilter(null)
    setMinScore(0)
    setSearchTerm('')
    setInactivityThreshold(12)
  }, [])

  const filterLabel = criteriaFilter === 'all'
    ? `Showing ${filteredOrgs.length} of ${ORGANIZATIONS.length} organizations.`
    : `Showing ${filteredOrgs.length} organizations matching ${criteriaFilter === 'criterionA' ? 'Criterion A (inactivity)' : criteriaFilter === 'criterionB' ? 'Criterion B (dependency)' : 'both criteria'}.`

  return (
    <div className="space-y-8">
      {/* Status bar */}
      <div className="bg-card-bg border border-red-dim px-5 py-3 flex items-center justify-between">
        <span className="font-ibm text-xs text-muted-foreground uppercase tracking-wider">{filterLabel}</span>
        <span className="font-ibm text-xs text-red-primary font-600 uppercase tracking-wider">Discover</span>
      </div>

      {/* Criteria tabs */}
      <div className="flex gap-0 border border-red-dim">
        {CRITERIA_TABS.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setCriteriaFilter(tab.key)}
            className={`flex-1 px-4 py-3 font-syne text-sm transition-colors border-r border-red-dim last:border-r-0 ${
              criteriaFilter === tab.key
                ? 'bg-red-primary text-white font-600'
                : 'bg-white text-charcoal hover:bg-card-bg'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Filter controls */}
      <div className="border border-red-dim bg-card-bg p-4 space-y-3">
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex items-center gap-2">
            <SlidersHorizontal className="w-3.5 h-3.5 text-muted-foreground" />
            <span className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground">Filters</span>
          </div>
          <input
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            placeholder="Search by name, BN, or sector..."
            className="px-3 py-1.5 border border-red-dim font-ibm text-xs bg-white text-charcoal outline-none flex-1 min-w-[200px]"
          />
          <select
            value={sectorFilter ?? ''}
            onChange={(e) => setSectorFilter(e.target.value || null)}
            className="px-3 py-1.5 border border-red-dim font-ibm text-xs bg-white text-charcoal outline-none"
          >
            <option value="">All Sectors</option>
            {sectors.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
          <select
            value={provinceFilter ?? ''}
            onChange={(e) => setProvinceFilter(e.target.value || null)}
            className="px-3 py-1.5 border border-red-dim font-ibm text-xs bg-white text-charcoal outline-none"
          >
            <option value="">All Provinces</option>
            {provinces.map((p) => <option key={p} value={p}>{p}</option>)}
          </select>
          <select
            value={minScore}
            onChange={(e) => setMinScore(Number(e.target.value))}
            className="px-3 py-1.5 border border-red-dim font-ibm text-xs bg-white text-charcoal outline-none"
          >
            <option value={0}>Any Score</option>
            <option value={25}>Score ≥ 25</option>
            <option value={50}>Score ≥ 50</option>
            <option value={75}>Score ≥ 75</option>
          </select>
        </div>

        {/* Sensitivity control */}
        <div className="flex items-center gap-3">
          <span className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground">Inactivity threshold:</span>
          <div className="flex gap-0 border border-red-dim">
            {INACTIVITY_OPTIONS.map((opt) => (
              <button
                key={opt.value}
                onClick={() => setInactivityThreshold(opt.value)}
                className={`px-3 py-1 font-ibm text-[11px] border-r border-red-dim last:border-r-0 transition-colors ${
                  inactivityThreshold === opt.value
                    ? 'bg-red-primary text-white'
                    : 'bg-white text-charcoal hover:bg-card-bg'
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>
          <span className="font-ibm text-[10px] text-muted-foreground">Orgs inactive within {inactivityThreshold}mo of funding</span>
        </div>

        {/* Filter chips */}
        {filterChips.length > 0 && (
          <div className="flex gap-2 flex-wrap items-center">
            {filterChips.map((chip) => (
              <span key={chip.key} className="flex items-center gap-1 px-2 py-1 bg-red-primary/10 border border-red-primary/20 font-ibm text-[11px] text-charcoal">
                {chip.label}
                <button onClick={chip.remove} className="ml-0.5 hover:text-red-primary"><X className="w-3 h-3" /></button>
              </span>
            ))}
            <button onClick={clearAllFilters} className="font-ibm text-[11px] text-red-primary underline">Clear all</button>
          </div>
        )}
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-3">
        {[
          { label: 'In Scope', value: stats.totalOrgs, prefix: '' },
          { label: 'Both Criteria', value: stats.bothCriteria, prefix: '' },
          { label: 'Criterion A Only', value: stats.criterionAOnly, prefix: '' },
          { label: 'Criterion B Only', value: stats.criterionBOnly, prefix: '' },
          { label: 'Funding at Risk', value: stats.totalFundingAtRisk, prefix: '$' },
          { label: 'High Priority', value: stats.highPriority, prefix: '' },
          { label: 'Avg Score', value: stats.avgScore, prefix: '' },
        ].map((card, i) => (
          <motion.div
            key={card.label}
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: i * 0.05 }}
            className="bg-card-bg border border-red-dim p-4 border-l-4 border-l-red-primary"
          >
            <div className="font-ibm text-2xl font-700 text-charcoal tabular-nums">
              <AnimatedNumber value={card.value} prefix={card.prefix} />
            </div>
            <div className="font-syne text-xs font-600 text-charcoal mt-1">{card.label}</div>
          </motion.div>
        ))}
      </div>

      {/* Charts grid — 8 charts in 2x4 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ChartPanel title="Zombie Quadrant" desc="Funding dependency (%) vs months of post-funding activity">
          <ResponsiveContainer width="100%" height={280}>
            <ScatterChart margin={{ top: 10, right: 10, bottom: 20, left: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(196,30,58,0.08)" />
              <XAxis type="number" dataKey="dependencyRatio" domain={[0, 100]} tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false}>
                <Label value="Dependency %" position="bottom" offset={5} style={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} />
              </XAxis>
              <YAxis type="number" dataKey="monthsPostFundingActivity" domain={[0, 60]} tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false}>
                <Label value="Months" angle={-90} position="left" offset={-5} style={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} />
              </YAxis>
              <ReferenceArea x1={70} x2={100} y1={0} y2={inactivityThreshold} fill="rgba(196,30,58,0.08)" strokeOpacity={0} />
              <ReferenceLine x={70} stroke="#C41E3A" strokeDasharray="6 4" strokeWidth={1} />
              <ReferenceLine y={inactivityThreshold} stroke="#C41E3A" strokeDasharray="6 4" strokeWidth={1} />
              <Tooltip content={<ScatterTooltip />} />
              <Scatter data={filteredOrgs.filter((o) => o.bothCriteria)} fill="#C41E3A" />
              <Scatter data={filteredOrgs.filter((o) => (o.criterionA || o.criterionB) && !o.bothCriteria)} fill="#C8922A" />
              <Scatter data={filteredOrgs.filter((o) => !o.criterionA && !o.criterionB)} fill="#6B7280" />
            </ScatterChart>
          </ResponsiveContainer>
        </ChartPanel>

        <ChartPanel title="Funding Timeline" desc="Monthly disbursements by risk classification ($CAD)">
          <FundingTimelineChart orgs={filteredOrgs} />
        </ChartPanel>

        <ChartPanel title="Dependency Distribution" desc="Number of organizations at each dependency % band">
          <DependencyHistogram orgs={filteredOrgs} />
        </ChartPanel>

        <ChartPanel title="Industry Concentration" desc="Sectors with highest concentration of flagged cases">
          <IndustryChart orgs={filteredOrgs} />
        </ChartPanel>

        <ChartPanel title="Score Distribution" desc="Number of organizations at each risk score band">
          <ScoreDistributionChart data={scoreDistribution} />
        </ChartPanel>

        <ChartPanel title="Province Breakdown" desc="Number of flagged organizations by province">
          <ProvinceChart data={provinceBreakdown} />
        </ChartPanel>

        <ChartPanel title="Funding Concentration" desc="Top funding sources by total disbursement">
          <FundingConcentrationChart data={aggBySource} />
        </ChartPanel>

        <ChartPanel title="Priority Mix" desc="Distribution of review priority levels">
          <PriorityPieChart stats={stats} />
        </ChartPanel>
      </div>

      {/* Aggregate tables */}
      <div className="border border-red-dim bg-card-bg">
        <button
          onClick={() => setShowAggregates(!showAggregates)}
          className="w-full flex items-center justify-between p-4 hover:bg-red-dim/20 transition"
        >
          <div>
            <h3 className="font-syne text-base font-600 text-charcoal text-left">Aggregate Summary Tables</h3>
            <p className="font-ibm text-[11px] text-muted-foreground mt-1">Breakdowns by industry, funding size, source, and dependency band</p>
          </div>
          {showAggregates ? <ChevronUp className="w-4 h-4 text-muted-foreground" /> : <ChevronDown className="w-4 h-4 text-muted-foreground" />}
        </button>

        <AnimatePresence>
          {showAggregates && (
            <motion.div initial={{ height: 0 }} animate={{ height: 'auto' }} exit={{ height: 0 }} className="overflow-hidden">
              <div className="border-t border-red-dim">
                <div className="flex gap-0 border-b border-red-dim">
                  {(['industry', 'size', 'source', 'dependency'] as const).map((tab) => (
                    <button
                      key={tab}
                      onClick={() => setActiveAggTab(tab)}
                      className={`px-5 py-2.5 font-syne text-xs border-r border-red-dim last:border-r-0 transition-colors capitalize ${
                        activeAggTab === tab ? 'bg-charcoal text-white font-600' : 'bg-white text-charcoal hover:bg-card-bg'
                      }`}
                    >
                      By {tab}
                    </button>
                  ))}
                </div>
                <AggregateTable
                  data={activeAggTab === 'industry' ? aggByIndustry : activeAggTab === 'size' ? aggBySize : activeAggTab === 'source' ? aggBySource : aggByDependency}
                />
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {/* Triage */}
      <div className="border border-red-dim bg-card-bg">
        <div className="p-5 border-b border-red-dim flex items-center justify-between">
          <div>
            <h3 className="font-syne text-base font-600 text-charcoal">Triage Agent</h3>
            <p className="font-ibm text-[11px] text-muted-foreground mt-1">Surfaces top 10 ranked cases with plain-language rationale</p>
          </div>
          <button
            onClick={runTriage}
            disabled={triageState === 'running'}
            className="flex items-center gap-2 px-4 py-2 bg-red-primary text-white font-ibm text-xs uppercase tracking-wider hover:bg-red-primary/90 transition disabled:opacity-50"
          >
            {triageState === 'idle' && <><Play className="w-3.5 h-3.5" /> Run Triage</>}
            {triageState === 'running' && <><Loader2 className="w-3.5 h-3.5 animate-spin" /> Scanning...</>}
            {triageState === 'done' && <><CheckCircle2 className="w-3.5 h-3.5" /> Complete</>}
          </button>
        </div>

        <AnimatePresence>
          {triageResults.length > 0 && (
            <motion.div initial={{ height: 0 }} animate={{ height: 'auto' }} className="overflow-hidden">
              {triageResults.map((org, i) => (
                <motion.div
                  key={org.id}
                  initial={{ opacity: 0, x: -10 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: i * 0.06 }}
                  className="flex items-center justify-between px-5 py-3 border-b border-red-dim hover:bg-red-dim/30 cursor-pointer transition"
                  onClick={() => openProfile(org.id)}
                >
                  <div className="flex items-center gap-4">
                    <span className="font-ibm text-sm font-700 text-red-primary tabular-nums w-8">#{i + 1}</span>
                    <div>
                      <span className="font-syne text-sm font-600 text-charcoal">{org.name}</span>
                      <p className="font-ibm text-[11px] text-muted-foreground mt-0.5">
                        {org.flags[0]?.explanation.slice(0, 80)}...
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-3">
                    <span className="font-ibm text-sm font-600 tabular-nums text-red-primary">{org.score}</span>
                    <span className={`font-ibm text-[10px] uppercase tracking-wider px-2 py-0.5 ${STATUS_STYLES[org.reviewStatus]}`}>
                      {org.reviewStatus.replace(' REVIEW PRIORITY', '')}
                    </span>
                  </div>
                </motion.div>
              ))}
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {/* Main table */}
      <div className="border border-red-dim bg-card-bg overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="bg-charcoal">
                {[
                  { key: 'name' as SortKey, label: 'Organization' },
                  { key: 'totalFunding' as SortKey, label: 'Funding' },
                  { key: 'dependencyRatio' as SortKey, label: 'Dependency %' },
                  { key: null, label: 'Last Filing' },
                  { key: 'daysSinceActivity' as SortKey, label: 'Days Since' },
                  { key: 'score' as SortKey, label: 'Score' },
                  { key: null, label: 'Status' },
                  { key: null, label: 'Actions' },
                ].map((h) => (
                  <th
                    key={h.label}
                    className={`text-left px-4 py-3 font-ibm text-[10px] uppercase tracking-wider text-white font-500 ${h.key ? 'cursor-pointer hover:text-red-dim select-none' : ''}`}
                    onClick={h.key ? () => toggleSort(h.key!) : undefined}
                  >
                    <span className="flex items-center gap-1">
                      {h.label}
                      {h.key && sortKey === h.key && (
                        <span className="text-red-dim">{sortDir === 'desc' ? '↓' : '↑'}</span>
                      )}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedOrgs.map((org, index) => (
                <motion.tr
                  key={org.id}
                  initial={{ opacity: 0, x: -8 }}
                  whileInView={{ opacity: 1, x: 0 }}
                  viewport={{ once: true }}
                  transition={{ delay: index * 0.05, duration: 0.3 }}
                  className="border-b border-red-dim group cursor-pointer hover:-translate-y-px transition-transform"
                  onClick={() => openProfile(org.id)}
                >
                  <td className="px-4 py-3 border-l-2 border-l-transparent group-hover:border-l-red-primary transition-colors">
                    <span className="font-syne text-sm font-600 text-charcoal">{org.name}</span>
                    <div className="font-ibm text-[10px] text-muted-foreground mt-0.5">{org.sector} · {org.province}</div>
                  </td>
                  <td className="px-4 py-3 font-ibm text-sm tabular-nums text-charcoal">{formatMoney(org.totalFunding)}</td>
                  <td className="px-4 py-3 font-ibm text-sm tabular-nums text-charcoal">{org.dependencyRatio}%</td>
                  <td className="px-4 py-3 font-ibm text-sm tabular-nums text-muted-foreground">{org.lastFilingDate}</td>
                  <td className="px-4 py-3 font-ibm text-sm tabular-nums text-charcoal">{org.daysSinceActivity}d</td>
                  <td className="px-4 py-3 font-ibm text-sm tabular-nums font-600 text-red-primary">{org.score}</td>
                  <td className="px-4 py-3">
                    <span className={`font-ibm text-[10px] uppercase tracking-wider px-2 py-0.5 ${STATUS_STYLES[org.reviewStatus]}`}>
                      {org.reviewStatus.replace(' REVIEW PRIORITY', '')}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-1" onClick={(e) => e.stopPropagation()}>
                      <button onClick={() => openProfile(org.id)} className="p-1.5 border border-red-dim hover:bg-red-dim/30 transition" title="Open Profile">
                        <User className="w-3.5 h-3.5 text-red-primary" />
                      </button>
                      <button onClick={() => addToCompare(org.id)} className="p-1.5 border border-red-dim hover:bg-red-dim/30 transition" title="Add to Compare">
                        <Plus className="w-3.5 h-3.5 text-red-primary" />
                      </button>
                      <button onClick={() => openAssistantForOrg(org.id)} className="p-1.5 border border-red-dim hover:bg-red-dim/30 transition" title="Ask Assistant">
                        <MessageSquare className="w-3.5 h-3.5 text-red-primary" />
                      </button>
                    </div>
                  </td>
                </motion.tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

function ChartPanel({ title, desc, children }: { title: string; desc: string; children: React.ReactNode }) {
  return (
    <div className="border border-red-dim bg-card-bg p-5">
      <h3 className="font-syne text-base font-600 text-charcoal mb-1">{title}</h3>
      <p className="font-ibm text-[11px] text-muted-foreground mb-4">{desc}</p>
      {children}
    </div>
  )
}

function AggregateTable({ data }: { data: AggregateRow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full">
        <thead>
          <tr className="bg-charcoal/90">
            {['Segment', 'Count', 'Total Funding', 'Avg Score', 'Avg Dep %', 'High Priority'].map((h) => (
              <th key={h} className="text-left px-4 py-2.5 font-ibm text-[10px] uppercase tracking-wider text-white font-500">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row) => (
            <tr key={row.label} className="border-b border-red-dim hover:bg-red-dim/20 transition">
              <td className="px-4 py-3 font-syne text-sm text-charcoal">{row.label}</td>
              <td className="px-4 py-3 font-ibm text-sm tabular-nums text-charcoal">{row.count}</td>
              <td className="px-4 py-3 font-ibm text-sm tabular-nums text-charcoal">{formatMoney(row.totalFunding)}</td>
              <td className="px-4 py-3 font-ibm text-sm tabular-nums text-charcoal">{row.avgScore}</td>
              <td className="px-4 py-3 font-ibm text-sm tabular-nums text-charcoal">{row.avgDependency}%</td>
              <td className="px-4 py-3 font-ibm text-sm tabular-nums text-red-primary font-600">{row.highPriority}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function ScatterTooltip({ active, payload }: { active?: boolean; payload?: Array<{ payload: Organization }> }) {
  if (!active || !payload?.length) return null
  const org = payload[0].payload
  return (
    <div className="bg-charcoal text-white border border-red-primary p-3 shadow-lg max-w-[220px]">
      <p className="font-syne text-sm font-600">{org.name}</p>
      <div className="font-ibm text-xs space-y-1 mt-2 text-white/80">
        <p>Funding: {formatMoney(org.totalFunding)}</p>
        <p>Dependency: {org.dependencyRatio}%</p>
        <p>Last filing: {org.lastFilingDate}</p>
        <p>Score: {org.score}</p>
      </div>
    </div>
  )
}

function FundingTimelineChart({ orgs }: { orgs: Organization[] }) {
  const data = useMemo(() => {
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    return months.map((month) => {
      const highOrgs = orgs.filter((o) => o.reviewStatus === 'HIGH REVIEW PRIORITY')
      const medOrgs = orgs.filter((o) => o.reviewStatus === 'MEDIUM REVIEW PRIORITY')
      const lowOrgs = orgs.filter((o) => o.reviewStatus === 'LOW REVIEW PRIORITY')
      return {
        month,
        high: Math.round(highOrgs.reduce((s, o) => s + o.totalFunding, 0) / 12 * (0.6 + Math.random() * 0.8)),
        medium: Math.round(medOrgs.reduce((s, o) => s + o.totalFunding, 0) / 12 * (0.5 + Math.random() * 1)),
        low: Math.round(lowOrgs.reduce((s, o) => s + o.totalFunding, 0) / 12 * (0.4 + Math.random() * 1.2)),
      }
    })
  }, [orgs])

  return (
    <ResponsiveContainer width="100%" height={250}>
      <BarChart data={data}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(196,30,58,0.08)" vertical={false} />
        <XAxis dataKey="month" tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
        <YAxis tickFormatter={(v: number) => formatMoney(v)} tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
        <Tooltip content={<BarTooltip />} />
        <Bar dataKey="high" fill="#C41E3A" name="High Priority" stackId="a" isAnimationActive animationDuration={800} />
        <Bar dataKey="medium" fill="#C8922A" name="Medium" stackId="a" isAnimationActive animationDuration={800} animationBegin={200} />
        <Bar dataKey="low" fill="#9CA3AF" name="Low" stackId="a" isAnimationActive animationDuration={800} animationBegin={400} />
      </BarChart>
    </ResponsiveContainer>
  )
}

function DependencyHistogram({ orgs }: { orgs: Organization[] }) {
  const data = useMemo(() => {
    const bins = [
      { range: '0-20%', min: 0, max: 20 },
      { range: '20-40%', min: 20, max: 40 },
      { range: '40-60%', min: 40, max: 60 },
      { range: '60-70%', min: 60, max: 70 },
      { range: '70-80%', min: 70, max: 80 },
      { range: '80-100%', min: 80, max: 100 },
    ]
    return bins.map((bin) => ({
      range: bin.range,
      count: orgs.filter((o) => o.dependencyRatio >= bin.min && o.dependencyRatio < (bin.max === 100 ? 101 : bin.max)).length,
      fill: bin.min >= 70 ? '#C41E3A' : bin.min >= 60 ? '#C8922A' : '#9CA3AF',
    }))
  }, [orgs])

  return (
    <ResponsiveContainer width="100%" height={250}>
      <BarChart data={data}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(196,30,58,0.08)" vertical={false} />
        <XAxis dataKey="range" tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
        <YAxis tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
        <Tooltip content={<BarTooltip />} />
        <Bar dataKey="count" isAnimationActive animationDuration={600}>
          {data.map((entry, index) => (
            <Cell key={index} fill={entry.fill} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

function IndustryChart({ orgs }: { orgs: Organization[] }) {
  const data = useMemo(() => {
    const sectorCounts: Record<string, number> = {}
    for (const org of orgs) {
      if (org.criterionA || org.criterionB) {
        sectorCounts[org.sector] = (sectorCounts[org.sector] || 0) + 1
      }
    }
    return Object.entries(sectorCounts)
      .map(([sector, count]) => ({ sector, count }))
      .sort((a, b) => b.count - a.count)
  }, [orgs])

  return (
    <ResponsiveContainer width="100%" height={250}>
      <BarChart data={data} layout="vertical">
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(196,30,58,0.08)" horizontal={false} />
        <XAxis type="number" tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
        <YAxis type="category" dataKey="sector" tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} width={120} />
        <Tooltip content={<BarTooltip />} />
        <Bar dataKey="count" fill="#C41E3A" isAnimationActive animationDuration={800} />
      </BarChart>
    </ResponsiveContainer>
  )
}

function ScoreDistributionChart({ data }: { data: Array<{ range: string; count: number }> }) {
  return (
    <ResponsiveContainer width="100%" height={250}>
      <BarChart data={data}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(196,30,58,0.08)" vertical={false} />
        <XAxis dataKey="range" tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
        <YAxis tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
        <Tooltip content={<BarTooltip />} />
        <Bar dataKey="count" isAnimationActive animationDuration={600}>
          {data.map((entry, index) => {
            const score = parseInt(entry.range.split('–')[0])
            const fill = score >= 80 ? '#C41E3A' : score >= 60 ? '#C8922A' : '#9CA3AF'
            return <Cell key={index} fill={fill} />
          })}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

function ProvinceChart({ data }: { data: Array<{ province: string; count: number; totalFunding: number }> }) {
  return (
    <ResponsiveContainer width="100%" height={250}>
      <BarChart data={data} layout="vertical">
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(196,30,58,0.08)" horizontal={false} />
        <XAxis type="number" tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
        <YAxis type="category" dataKey="province" tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} width={100} />
        <Tooltip content={<BarTooltip />} />
        <Bar dataKey="count" fill="#C41E3A" isAnimationActive animationDuration={800} />
      </BarChart>
    </ResponsiveContainer>
  )
}

function FundingConcentrationChart({ data }: { data: AggregateRow[] }) {
  const chartData = useMemo(() => data.slice(0, 6).map((d) => ({ source: d.label.length > 25 ? d.label.slice(0, 25) + '...' : d.label, amount: d.totalFunding })), [data])

  return (
    <ResponsiveContainer width="100%" height={250}>
      <BarChart data={chartData} layout="vertical">
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(196,30,58,0.08)" horizontal={false} />
        <XAxis type="number" tickFormatter={(v: number) => formatMoney(v)} tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} />
        <YAxis type="category" dataKey="source" tick={{ fontSize: 9, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} tickLine={false} axisLine={false} width={160} />
        <Tooltip content={<BarTooltip />} />
        <Bar dataKey="amount" fill="#C8922A" isAnimationActive animationDuration={800} />
      </BarChart>
    </ResponsiveContainer>
  )
}

const PIE_COLORS = ['#C41E3A', '#C8922A', '#9CA3AF']

function PriorityPieChart({ stats }: { stats: ReturnType<typeof getSummaryStats> }) {
  const data = useMemo(() => [
    { name: 'High', value: stats.highPriority },
    { name: 'Medium', value: stats.mediumPriority },
    { name: 'Low', value: stats.lowPriority },
  ].filter((d) => d.value > 0), [stats])

  return (
    <div className="flex items-center gap-6">
      <ResponsiveContainer width="50%" height={200}>
        <PieChart>
          <Pie data={data} cx="50%" cy="50%" outerRadius={80} dataKey="value" stroke="none" isAnimationActive animationDuration={800}>
            {data.map((_, index) => (
              <Cell key={index} fill={PIE_COLORS[index % PIE_COLORS.length]} />
            ))}
          </Pie>
          <Tooltip content={<BarTooltip />} />
        </PieChart>
      </ResponsiveContainer>
      <div className="space-y-2">
        {data.map((entry, i) => (
          <div key={entry.name} className="flex items-center gap-2">
            <div className="w-3 h-3" style={{ backgroundColor: PIE_COLORS[i] }} />
            <span className="font-ibm text-xs text-charcoal">{entry.name}: {entry.value}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function BarTooltip({ active, payload, label }: { active?: boolean; payload?: Array<{ dataKey: string; value: number; color: string; name?: string }>; label?: string }) {
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
