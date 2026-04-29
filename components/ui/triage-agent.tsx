'use client'

import { useState, useEffect, useCallback } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { Play, Loader2, CheckCircle2 } from 'lucide-react'

type ScanPhase = 'idle' | 'scanning' | 'complete'

const scanLines = [
  { text: 'INITIALIZING SCAN...', delay: 0 },
  { text: 'CROSS-REFERENCING FILINGS...', delay: 800 },
  { text: 'CHECKING CORPORATE REGISTRY...', delay: 1600 },
  { text: 'ANOMALIES DETECTED: 12', delay: 2400, isResult: true },
]

interface TriageResult {
  organization: string
  score: number
  flag: string
  funding: string
}

const triageResults: TriageResult[] = [
  { organization: 'Northgate Solutions Inc.', score: 94, flag: 'ZOMBIE', funding: '$2.4M' },
  { organization: 'Westfield Charitable Foundation', score: 91, flag: 'ZOMBIE', funding: '$1.8M' },
  { organization: 'Summit Research Corp', score: 88, flag: 'HIGH RISK', funding: '$2.1M' },
  { organization: 'Clearwater Innovations', score: 86, flag: 'ZOMBIE', funding: '$1.1M' },
  { organization: 'Prairie Community Trust', score: 82, flag: 'HIGH RISK', funding: '$890K' },
  { organization: 'Northern Light Society', score: 79, flag: 'ZOMBIE', funding: '$560K' },
]

function TerminalLine({ text, delay, isResult }: { text: string; delay: number; isResult?: boolean }) {
  const [visible, setVisible] = useState(false)
  const [displayText, setDisplayText] = useState('')

  useEffect(() => {
    const showTimer = setTimeout(() => {
      setVisible(true)
      let charIndex = 0
      const typeTimer = setInterval(() => {
        charIndex++
        setDisplayText(text.slice(0, charIndex))
        if (charIndex >= text.length) clearInterval(typeTimer)
      }, 25)
      return () => clearInterval(typeTimer)
    }, delay)
    return () => clearTimeout(showTimer)
  }, [text, delay])

  if (!visible) return null

  return (
    <div className={`font-ibm text-sm ${isResult ? 'text-red-primary font-600' : 'text-charcoal/70'}`}>
      <span className="text-red-primary mr-2">{'>'}</span>
      {displayText}
      {displayText.length < text.length && (
        <span className="inline-block w-2 h-4 bg-red-primary ml-0.5 animate-terminal-blink" />
      )}
    </div>
  )
}

export default function TriageAgent() {
  const [phase, setPhase] = useState<ScanPhase>('idle')
  const [showResults, setShowResults] = useState(false)

  const runScan = useCallback(() => {
    setPhase('scanning')
    setShowResults(false)
    setTimeout(() => {
      setPhase('complete')
      setTimeout(() => setShowResults(true), 600)
    }, 3200)
  }, [])

  return (
    <div className="border border-red-dim bg-card-bg">
      <div className="p-6 border-b border-red-dim flex items-center justify-between">
        <div>
          <h3 className="font-syne text-lg font-600 text-charcoal">Triage Agent</h3>
          <p className="font-ibm text-xs text-muted-foreground mt-1 uppercase tracking-wider">
            Automated risk scanning and prioritization
          </p>
        </div>
        <button
          onClick={runScan}
          disabled={phase === 'scanning'}
          className="flex items-center gap-2 px-5 py-2.5 bg-red-primary text-white font-ibm text-xs uppercase tracking-wider hover:bg-red-primary/90 transition disabled:opacity-50 disabled:cursor-wait"
        >
          {phase === 'idle' && <><Play className="w-3.5 h-3.5" /> Run Triage</>}
          {phase === 'scanning' && <><Loader2 className="w-3.5 h-3.5 animate-spin" /> Scanning...</>}
          {phase === 'complete' && <><CheckCircle2 className="w-3.5 h-3.5" /> Complete</>}
        </button>
      </div>

      {phase !== 'idle' && (
        <div className="p-6 border-b border-red-dim bg-charcoal/[0.02]">
          <div className="space-y-2">
            {scanLines.map((line) => (
              <TerminalLine key={line.text} text={line.text} delay={line.delay} isResult={line.isResult} />
            ))}
          </div>
        </div>
      )}

      <AnimatePresence>
        {showResults && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            transition={{ duration: 0.4 }}
            className="overflow-hidden"
          >
            <table className="w-full">
              <thead>
                <tr className="border-b border-red-dim bg-charcoal">
                  <th className="text-left px-5 py-2.5 font-ibm text-[10px] uppercase tracking-wider text-white font-500">Organization</th>
                  <th className="text-right px-5 py-2.5 font-ibm text-[10px] uppercase tracking-wider text-white font-500">Score</th>
                  <th className="text-left px-5 py-2.5 font-ibm text-[10px] uppercase tracking-wider text-white font-500">Flag</th>
                  <th className="text-right px-5 py-2.5 font-ibm text-[10px] uppercase tracking-wider text-white font-500">Funding</th>
                </tr>
              </thead>
              <tbody>
                {triageResults.map((result, index) => (
                  <motion.tr
                    key={result.organization}
                    initial={{ opacity: 0, x: -10 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: index * 0.08, duration: 0.3 }}
                    className="border-b border-red-dim hover:bg-red-dim/30 transition"
                  >
                    <td className="px-5 py-3 font-syne text-sm font-500 text-charcoal">{result.organization}</td>
                    <td className="px-5 py-3 text-right font-ibm text-sm tabular-nums text-red-primary font-600">{result.score}</td>
                    <td className="px-5 py-3">
                      <span className={`font-ibm text-[10px] uppercase tracking-wider px-2 py-0.5 ${
                        result.flag === 'ZOMBIE' ? 'bg-red-primary text-white' : 'bg-amber-warn text-white'
                      }`}>
                        {result.flag}
                      </span>
                    </td>
                    <td className="px-5 py-3 text-right font-ibm text-sm tabular-nums text-charcoal">{result.funding}</td>
                  </motion.tr>
                ))}
              </tbody>
            </table>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}
