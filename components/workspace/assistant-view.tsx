'use client'

import { useState, useCallback, useMemo } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { Send, MessageSquare, FileText, Loader2 } from 'lucide-react'
import { useWorkspace } from '@/lib/workspace-context'
import { ORGANIZATIONS, getFilteredOrgs, formatMoney, type Organization } from '@/lib/data'

interface ChatMessage {
  role: 'user' | 'assistant'
  content: string
  evidence?: string
  isMemo?: boolean
}

const SUGGESTED_QUESTIONS = [
  'How many organizations exceed 80% dependency?',
  'Which sector has the highest average dependency?',
  'What is the total funding in both-criteria cases?',
  'Which funding source appears most in high-priority cases?',
  'What should a reviewer inspect next?',
  'Describe the common pattern among zombie organizations.',
  'Generate a review memo for the selected organization.',
  'Which provinces have the most flagged cases?',
]

function answerQuestion(question: string, orgs: Organization[], selectedOrg: Organization | null): { answer: string; evidence: string; isMemo?: boolean } {
  const lowerQ = question.toLowerCase()

  if (lowerQ.includes('80%') && lowerQ.includes('dependency')) {
    const count = orgs.filter((o) => o.dependencyRatio >= 80).length
    const names = orgs.filter((o) => o.dependencyRatio >= 80).map((o) => o.name).join(', ')
    return {
      answer: `${count} organizations exceed 80% public funding dependency: ${names}.`,
      evidence: `Field: dependency_ratio. Filter: >= 80. Result set: ${orgs.length} organizations in current filter scope.`,
    }
  }

  if (lowerQ.includes('sector') && (lowerQ.includes('highest') || lowerQ.includes('average'))) {
    const sectorDeps: Record<string, number[]> = {}
    for (const o of orgs) {
      if (!sectorDeps[o.sector]) sectorDeps[o.sector] = []
      sectorDeps[o.sector].push(o.dependencyRatio)
    }
    const avgBySector = Object.entries(sectorDeps).map(([sector, deps]) => ({
      sector,
      avg: Math.round(deps.reduce((a, b) => a + b, 0) / deps.length),
    })).sort((a, b) => b.avg - a.avg)
    const top = avgBySector[0]
    return {
      answer: `${top.sector} has the highest average dependency at ${top.avg}%. Full ranking: ${avgBySector.map((s) => `${s.sector}: ${s.avg}%`).join(', ')}.`,
      evidence: `Fields: sector, dependency_ratio. Aggregation: mean by sector. Result set: ${orgs.length} organizations.`,
    }
  }

  if (lowerQ.includes('total funding') && lowerQ.includes('both')) {
    const bothOrgs = orgs.filter((o) => o.bothCriteria)
    const total = bothOrgs.reduce((s, o) => s + o.totalFunding, 0)
    return {
      answer: `Total funding in both-criteria cases: ${formatMoney(total)} across ${bothOrgs.length} organizations.`,
      evidence: `Fields: total_funding, criterion_a, criterion_b. Filter: both_criteria = true. Result set: ${orgs.length} organizations.`,
    }
  }

  if (lowerQ.includes('funding source') || lowerQ.includes('funder')) {
    const sourceCounts: Record<string, number> = {}
    for (const o of orgs.filter((o) => o.reviewStatus === 'HIGH REVIEW PRIORITY')) {
      for (const ev of o.fundingEvents) {
        sourceCounts[ev.source] = (sourceCounts[ev.source] || 0) + 1
      }
    }
    const sorted = Object.entries(sourceCounts).sort((a, b) => b[1] - a[1])
    const top = sorted[0]
    return {
      answer: top
        ? `"${top[0]}" appears most frequently in high-priority cases with ${top[1]} funding events. Top 3 sources: ${sorted.slice(0, 3).map(([s, c]) => `${s} (${c})`).join(', ')}.`
        : 'No high-priority cases found in current filter scope.',
      evidence: `Fields: funding_events.source, review_status. Filter: review_status = HIGH REVIEW PRIORITY. Result set: ${orgs.length} organizations.`,
    }
  }

  if (lowerQ.includes('province') || lowerQ.includes('provinces')) {
    const provinceCounts: Record<string, number> = {}
    for (const o of orgs.filter((o) => o.criterionA || o.criterionB)) {
      provinceCounts[o.province] = (provinceCounts[o.province] || 0) + 1
    }
    const sorted = Object.entries(provinceCounts).sort((a, b) => b[1] - a[1])
    return {
      answer: `Province breakdown of flagged organizations: ${sorted.map(([p, c]) => `${p}: ${c}`).join(', ')}.`,
      evidence: `Fields: province, criterion_a, criterion_b. Filter: criterion_a OR criterion_b = true. Result set: ${orgs.length} organizations.`,
    }
  }

  if (lowerQ.includes('pattern') || lowerQ.includes('common')) {
    const highOrgs = orgs.filter((o) => o.reviewStatus === 'HIGH REVIEW PRIORITY')
    const avgDep = highOrgs.length > 0 ? Math.round(highOrgs.reduce((s, o) => s + o.dependencyRatio, 0) / highOrgs.length) : 0
    const avgMonths = highOrgs.length > 0 ? Math.round(highOrgs.reduce((s, o) => s + o.monthsPostFundingActivity, 0) / highOrgs.length) : 0
    const zeroEmp = highOrgs.filter((o) => o.employees === 0).length
    const noAddress = highOrgs.filter((o) => !o.hasAddress).length

    return {
      answer: `Common pattern among ${highOrgs.length} high-priority organizations: average dependency of ${avgDep}%, average post-funding activity of ${avgMonths} months, ${zeroEmp} with zero employees, ${noAddress} with no verifiable address. The typical zombie profile in this dataset is an organization that received substantial government funding (>$500K), showed dependency above 80%, ceased filing within 6–10 months, and had minimal or zero reported employees.`,
      evidence: `Fields: dependency_ratio, months_post_funding_activity, employees, has_address. Filter: review_status = HIGH REVIEW PRIORITY. Aggregation: mean, count. Result set: ${highOrgs.length} high-priority organizations.`,
    }
  }

  if (lowerQ.includes('memo') || lowerQ.includes('review memo')) {
    const target = selectedOrg || orgs.sort((a, b) => b.score - a.score)[0]
    if (!target) {
      return { answer: 'No organization available for memo generation.', evidence: 'No result set.' }
    }
    const memo = [
      `REVIEW MEMO — ${target.name}`,
      `BN: ${target.bnRoot} | Score: ${target.score}/100 | Status: ${target.reviewStatus}`,
      ``,
      `SUMMARY: ${target.name} received ${formatMoney(target.totalFunding)} in public funding with ${target.dependencyRatio}% dependency. ${target.criterionA ? `Post-funding inactivity within ${target.monthsPostFundingActivity} months. ` : ''}${target.criterionB ? `Dependency exceeds threshold. ` : ''}${target.bothCriteria ? 'Both criteria met.' : ''}`,
      ``,
      `FLAGS (${target.flags.length}):`,
      ...target.flags.map((f) => `- ${f.code}: ${f.explanation}`),
      ``,
      `SCORE: ${target.scoreComponents.map((c) => `${c.name}: ${c.points}/${c.maxPoints}`).join(' | ')}`,
      ``,
      `SOURCES: ${target.dataSources.join(', ')}`,
    ].join('\n')

    return {
      answer: memo,
      evidence: `Generated from structured profile data for ${target.id}. Fields: all profile fields. Model: deterministic scoring v1.0.`,
      isMemo: true,
    }
  }

  if (lowerQ.includes('inspect') || lowerQ.includes('next') || lowerQ.includes('review')) {
    const topCases = [...orgs].sort((a, b) => b.score - a.score).slice(0, 3)
    if (selectedOrg) {
      return {
        answer: `For ${selectedOrg.name}: 1) Verify the ${selectedOrg.flags[0]?.code || 'primary flag'} against original funding agreements. 2) Cross-reference CRA filing status for ${selectedOrg.lastFilingYear + 1}. 3) Check corporate registry for dissolution or name change. ${selectedOrg.relatedEntities.length > 0 ? `4) Review related entities: ${selectedOrg.relatedEntities.join(', ')}.` : ''}`,
        evidence: `Fields: flags, last_filing_year, related_entities. Organization: ${selectedOrg.id}. Score: ${selectedOrg.score}/100.`,
      }
    }
    return {
      answer: `Priority review order: ${topCases.map((o, i) => `${i + 1}. ${o.name} (score: ${o.score}, ${formatMoney(o.totalFunding)} at ${o.dependencyRatio}% dependency)`).join('. ')}. Start with the highest-scoring case and verify CRA filing status.`,
      evidence: `Fields: score, total_funding, dependency_ratio. Sort: score DESC. Limit: 3. Result set: ${orgs.length} organizations.`,
    }
  }

  return {
    answer: `Based on the current result set of ${orgs.length} organizations: ${orgs.filter((o) => o.bothCriteria).length} meet both criteria, ${orgs.filter((o) => o.reviewStatus === 'HIGH REVIEW PRIORITY').length} are high priority, with total flagged funding of ${formatMoney(orgs.filter((o) => o.criterionA || o.criterionB).reduce((s, o) => s + o.totalFunding, 0))}.`,
    evidence: `Fields: criterion_a, criterion_b, review_status, total_funding. Result set: ${orgs.length} organizations in current filter scope.`,
  }
}

export default function AssistantView() {
  const { criteriaFilter, selectedOrgId } = useWorkspace()
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [thinking, setThinking] = useState(false)

  const filteredOrgs = useMemo(() => getFilteredOrgs(criteriaFilter), [criteriaFilter])
  const selectedOrg = useMemo(() => ORGANIZATIONS.find((o) => o.id === selectedOrgId) || null, [selectedOrgId])

  const sendMessage = useCallback((question: string) => {
    const q = question.trim()
    if (!q) return

    setMessages((prev) => [...prev, { role: 'user', content: q }])
    setInput('')
    setThinking(true)

    setTimeout(() => {
      const { answer, evidence, isMemo } = answerQuestion(q, filteredOrgs, selectedOrg)
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: answer, evidence, isMemo },
      ])
      setThinking(false)
    }, 800 + Math.random() * 700)
  }, [filteredOrgs, selectedOrg])

  return (
    <div className="flex flex-col h-[calc(100vh-280px)] min-h-[500px]">
      {/* Header */}
      <div className="bg-card-bg border border-red-dim p-4 flex items-center justify-between flex-shrink-0">
        <div>
          <h2 className="font-syne text-lg font-600 text-charcoal">Assistant</h2>
          <p className="font-ibm text-[11px] text-muted-foreground mt-0.5">
            Constrained to current result set ({filteredOrgs.length} organizations)
            {selectedOrg && ` · Focused on: ${selectedOrg.name}`}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 bg-green-500" />
          <span className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground">Active</span>
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4 border-x border-red-dim">
        {messages.length === 0 && (
          <div className="flex items-center justify-center h-full">
            <div className="text-center max-w-sm">
              <MessageSquare className="w-10 h-10 text-red-dim mx-auto mb-4" />
              <p className="font-syne text-base text-muted-foreground">Ask a question about the current data</p>
              <p className="font-ibm text-xs text-muted-foreground mt-2">
                The assistant can only answer questions answerable from the result-set object in scope.
              </p>
            </div>
          </div>
        )}

        <AnimatePresence>
          {messages.map((msg, i) => (
            <motion.div
              key={i}
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
            >
              <div className={`max-w-[80%] ${msg.role === 'user' ? 'bg-charcoal text-white' : 'bg-card-bg border border-red-dim'} p-4`}>
                {msg.isMemo ? (
                  <div>
                    <div className="flex items-center gap-2 mb-2">
                      <FileText className="w-3.5 h-3.5 text-amber-warn" />
                      <span className="font-ibm text-[10px] uppercase tracking-wider text-amber-warn font-600">Generated Memo</span>
                    </div>
                    <pre className="font-ibm text-xs whitespace-pre-wrap leading-relaxed text-charcoal">{msg.content}</pre>
                  </div>
                ) : (
                  <p className="font-ibm text-sm">{msg.content}</p>
                )}
                {msg.evidence && (
                  <div className="mt-3 pt-2 border-t border-red-dim">
                    <span className="font-ibm text-[10px] uppercase tracking-wider text-muted-foreground">Evidence used:</span>
                    <p className="font-ibm text-[11px] text-muted-foreground mt-1">{msg.evidence}</p>
                  </div>
                )}
              </div>
            </motion.div>
          ))}

          {thinking && (
            <motion.div
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              className="flex justify-start"
            >
              <div className="bg-card-bg border border-red-dim p-4 flex items-center gap-2">
                <Loader2 className="w-3.5 h-3.5 text-red-primary animate-spin" />
                <span className="font-ibm text-xs text-muted-foreground">Analyzing result set...</span>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {/* Suggested questions */}
      <div className="flex gap-2 flex-wrap p-3 border-x border-red-dim bg-card-bg/50">
        {SUGGESTED_QUESTIONS.map((q) => (
          <button
            key={q}
            onClick={() => sendMessage(q)}
            className="font-ibm text-[11px] px-3 py-1.5 border border-red-dim text-charcoal hover:bg-red-dim/30 transition truncate max-w-[280px]"
          >
            {q}
          </button>
        ))}
      </div>

      {/* Input */}
      <div className="flex gap-0 border border-red-dim flex-shrink-0">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && !thinking && sendMessage(input)}
          placeholder="Ask about the current data set..."
          className="flex-1 px-4 py-3 font-ibm text-sm bg-white text-charcoal outline-none border-0"
          disabled={thinking}
        />
        <button
          onClick={() => sendMessage(input)}
          disabled={thinking}
          className="px-5 bg-red-primary text-white hover:bg-red-primary/90 transition flex items-center disabled:opacity-50"
        >
          <Send className="w-4 h-4" />
        </button>
      </div>
    </div>
  )
}
