'use client'

import { useMemo, useState } from 'react'
import { motion } from 'framer-motion'
import { Download, ArrowLeft, SlidersHorizontal, X } from 'lucide-react'
import Link from 'next/link'
import { ORGANIZATIONS, formatMoney, getSummaryStats, type Organization } from '@/lib/data'

const STATUS_STYLES: Record<string, string> = {
  'HIGH REVIEW PRIORITY': 'bg-red-primary text-white',
  'MEDIUM REVIEW PRIORITY': 'bg-amber-warn text-white',
  'LOW REVIEW PRIORITY': 'bg-[#6B7280] text-white',
}

type SortKey = 'score' | 'totalFunding' | 'dependencyRatio' | 'name' | 'daysSinceActivity'
type SortDir = 'asc' | 'desc'

export default function OversightPage() {
  const [statusFilter, setStatusFilter] = useState<string | null>(null)
  const [sectorFilter, setSectorFilter] = useState<string | null>(null)
  const [provinceFilter, setProvinceFilter] = useState<string | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>('score')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [searchTerm, setSearchTerm] = useState('')

  const sectors = useMemo(() => [...new Set(ORGANIZATIONS.map((o) => o.sector))].sort(), [])
  const provinces = useMemo(() => [...new Set(ORGANIZATIONS.map((o) => o.province))].sort(), [])

  const filteredOrgs = useMemo(() => {
    let result = ORGANIZATIONS
    if (statusFilter) result = result.filter((o) => o.reviewStatus === statusFilter)
    if (sectorFilter) result = result.filter((o) => o.sector === sectorFilter)
    if (provinceFilter) result = result.filter((o) => o.province === provinceFilter)
    if (searchTerm) {
      const lower = searchTerm.toLowerCase()
      result = result.filter((o) => o.name.toLowerCase().includes(lower) || o.bnRoot.includes(lower))
    }
    return result
  }, [statusFilter, sectorFilter, provinceFilter, searchTerm])

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

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir((d) => d === 'asc' ? 'desc' : 'asc')
    else { setSortKey(key); setSortDir('desc') }
  }

  const exportCSV = () => {
    const headers = ['Name', 'BN', 'Sector', 'Province', 'Total Funding', 'Dependency %', 'Score', 'Status', 'Criterion A', 'Criterion B', 'Both Criteria', 'Flags']
    const rows = sortedOrgs.map((o) => [
      o.name, o.bnRoot, o.sector, o.province,
      o.totalFunding, o.dependencyRatio, o.score,
      o.reviewStatus, o.criterionA ? 'Yes' : 'No',
      o.criterionB ? 'Yes' : 'No', o.bothCriteria ? 'Yes' : 'No',
      o.flags.map((f) => f.code).join('; '),
    ])
    const csv = [headers.join(','), ...rows.map((r) => r.map((v) => `"${v}"`).join(','))].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `oversight-export-${new Date().toISOString().slice(0, 10)}.csv`
    link.click()
    URL.revokeObjectURL(url)
  }

  const hasActiveFilters = statusFilter || sectorFilter || provinceFilter || searchTerm

  return (
    <div className="min-h-screen bg-white">
      {/* Header */}
      <nav className="sticky top-0 z-50 bg-white border-b border-red-dim">
        <div className="max-w-7xl mx-auto px-6 flex items-center h-14 justify-between">
          <div className="flex items-center gap-4">
            <Link href="/" className="p-2 hover:bg-red-dim/30 transition">
              <ArrowLeft className="w-4 h-4 text-red-primary" />
            </Link>
            <div className="font-syne font-700 text-red-primary text-lg tracking-tight">RRI</div>
            <span className="font-ibm text-xs text-muted-foreground uppercase tracking-wider">Oversight Dashboard</span>
          </div>
          <button
            onClick={exportCSV}
            className="flex items-center gap-2 px-4 py-2 bg-red-primary text-white font-ibm text-xs uppercase tracking-wider hover:bg-red-primary/90 transition"
          >
            <Download className="w-3.5 h-3.5" /> Export CSV
          </button>
        </div>
      </nav>

      <div className="max-w-7xl mx-auto px-6 py-10 space-y-8">
        {/* Summary stats */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          {[
            { label: 'Total', value: stats.totalOrgs },
            { label: 'High Priority', value: stats.highPriority },
            { label: 'Both Criteria', value: stats.bothCriteria },
            { label: 'Avg Score', value: stats.avgScore },
            { label: 'Flags Raised', value: stats.totalFlagsRaised },
          ].map((card) => (
            <div key={card.label} className="bg-card-bg border border-red-dim p-4 border-l-4 border-l-red-primary">
              <div className="font-ibm text-2xl font-700 text-charcoal tabular-nums">{card.value}</div>
              <div className="font-syne text-xs font-600 text-charcoal mt-1">{card.label}</div>
            </div>
          ))}
        </div>

        {/* Filters */}
        <div className="border border-red-dim bg-card-bg p-4 space-y-3">
          <div className="flex items-center gap-3 flex-wrap">
            <SlidersHorizontal className="w-3.5 h-3.5 text-muted-foreground" />
            <input
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              placeholder="Search by name or BN..."
              className="px-3 py-1.5 border border-red-dim font-ibm text-xs bg-white text-charcoal outline-none flex-1 min-w-[200px]"
            />
            <select value={statusFilter ?? ''} onChange={(e) => setStatusFilter(e.target.value || null)} className="px-3 py-1.5 border border-red-dim font-ibm text-xs bg-white text-charcoal outline-none">
              <option value="">All Statuses</option>
              <option value="HIGH REVIEW PRIORITY">High Priority</option>
              <option value="MEDIUM REVIEW PRIORITY">Medium Priority</option>
              <option value="LOW REVIEW PRIORITY">Low Priority</option>
            </select>
            <select value={sectorFilter ?? ''} onChange={(e) => setSectorFilter(e.target.value || null)} className="px-3 py-1.5 border border-red-dim font-ibm text-xs bg-white text-charcoal outline-none">
              <option value="">All Sectors</option>
              {sectors.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
            <select value={provinceFilter ?? ''} onChange={(e) => setProvinceFilter(e.target.value || null)} className="px-3 py-1.5 border border-red-dim font-ibm text-xs bg-white text-charcoal outline-none">
              <option value="">All Provinces</option>
              {provinces.map((p) => <option key={p} value={p}>{p}</option>)}
            </select>
            {hasActiveFilters && (
              <button
                onClick={() => { setStatusFilter(null); setSectorFilter(null); setProvinceFilter(null); setSearchTerm('') }}
                className="flex items-center gap-1 font-ibm text-[11px] text-red-primary"
              >
                <X className="w-3 h-3" /> Clear
              </button>
            )}
          </div>
          <p className="font-ibm text-[11px] text-muted-foreground">
            Showing {sortedOrgs.length} of {ORGANIZATIONS.length} organizations
          </p>
        </div>

        {/* Table */}
        <div className="border border-red-dim bg-card-bg overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="bg-charcoal">
                  {[
                    { key: 'name' as SortKey, label: 'Organization' },
                    { key: null, label: 'Sector' },
                    { key: null, label: 'Province' },
                    { key: 'totalFunding' as SortKey, label: 'Funding' },
                    { key: 'dependencyRatio' as SortKey, label: 'Dep %' },
                    { key: 'daysSinceActivity' as SortKey, label: 'Days Inactive' },
                    { key: 'score' as SortKey, label: 'Score' },
                    { key: null, label: 'Status' },
                    { key: null, label: 'A' },
                    { key: null, label: 'B' },
                    { key: null, label: 'Flags' },
                  ].map((h) => (
                    <th
                      key={h.label}
                      className={`text-left px-3 py-3 font-ibm text-[10px] uppercase tracking-wider text-white font-500 ${h.key ? 'cursor-pointer hover:text-red-dim select-none' : ''}`}
                      onClick={h.key ? () => toggleSort(h.key!) : undefined}
                    >
                      <span className="flex items-center gap-1">
                        {h.label}
                        {h.key && sortKey === h.key && <span className="text-red-dim">{sortDir === 'desc' ? '↓' : '↑'}</span>}
                      </span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedOrgs.map((org, i) => (
                  <motion.tr
                    key={org.id}
                    initial={{ opacity: 0 }}
                    whileInView={{ opacity: 1 }}
                    viewport={{ once: true }}
                    transition={{ delay: i * 0.03 }}
                    className="border-b border-red-dim hover:bg-red-dim/20 transition"
                  >
                    <td className="px-3 py-2.5">
                      <span className="font-syne text-sm font-600 text-charcoal">{org.name}</span>
                      <div className="font-ibm text-[10px] text-muted-foreground">{org.bnRoot}</div>
                    </td>
                    <td className="px-3 py-2.5 font-ibm text-xs text-charcoal">{org.sector}</td>
                    <td className="px-3 py-2.5 font-ibm text-xs text-charcoal">{org.province}</td>
                    <td className="px-3 py-2.5 font-ibm text-xs tabular-nums text-charcoal">{formatMoney(org.totalFunding)}</td>
                    <td className="px-3 py-2.5 font-ibm text-xs tabular-nums text-charcoal">{org.dependencyRatio}%</td>
                    <td className="px-3 py-2.5 font-ibm text-xs tabular-nums text-charcoal">{org.daysSinceActivity}d</td>
                    <td className="px-3 py-2.5 font-ibm text-xs tabular-nums font-600 text-red-primary">{org.score}</td>
                    <td className="px-3 py-2.5">
                      <span className={`font-ibm text-[9px] uppercase tracking-wider px-2 py-0.5 ${STATUS_STYLES[org.reviewStatus]}`}>
                        {org.reviewStatus.replace(' REVIEW PRIORITY', '')}
                      </span>
                    </td>
                    <td className="px-3 py-2.5">
                      <span className={`font-ibm text-[10px] px-1.5 py-0.5 ${org.criterionA ? 'bg-red-primary text-white' : 'text-muted-foreground'}`}>
                        {org.criterionA ? 'Y' : 'N'}
                      </span>
                    </td>
                    <td className="px-3 py-2.5">
                      <span className={`font-ibm text-[10px] px-1.5 py-0.5 ${org.criterionB ? 'bg-red-primary text-white' : 'text-muted-foreground'}`}>
                        {org.criterionB ? 'Y' : 'N'}
                      </span>
                    </td>
                    <td className="px-3 py-2.5 font-ibm text-xs tabular-nums text-charcoal">{org.flags.length}</td>
                  </motion.tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  )
}
