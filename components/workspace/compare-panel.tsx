'use client'

import { useMemo } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { X, ArrowRight } from 'lucide-react'
import { useWorkspace } from '@/lib/workspace-context'
import { ORGANIZATIONS } from '@/lib/data'

export default function ComparePanel() {
  const { compareSlots, removeFromCompare, openCompare, activeView } = useWorkspace()

  const orgs = useMemo(() => {
    return compareSlots.map((id) => ORGANIZATIONS.find((o) => o.id === id)).filter(Boolean) as typeof ORGANIZATIONS
  }, [compareSlots])

  if (orgs.length === 0 || activeView === 'compare') return null

  return (
    <motion.div
      initial={{ y: 100, opacity: 0 }}
      animate={{ y: 0, opacity: 1 }}
      exit={{ y: 100, opacity: 0 }}
      className="fixed bottom-6 right-6 z-40 bg-white border border-red-primary shadow-2xl p-4 min-w-[300px]"
    >
      <div className="flex items-center justify-between mb-3">
        <span className="font-syne text-sm font-600 text-charcoal">Compare ({orgs.length}/3)</span>
        <button
          onClick={openCompare}
          disabled={orgs.length < 2}
          className="flex items-center gap-1.5 px-3 py-1.5 bg-red-primary text-white font-ibm text-[10px] uppercase tracking-wider hover:bg-red-primary/90 transition disabled:opacity-40"
        >
          Compare <ArrowRight className="w-3 h-3" />
        </button>
      </div>

      <AnimatePresence>
        {orgs.map((org) => (
          <motion.div
            key={org.id}
            initial={{ opacity: 0, x: -10 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: 10 }}
            className="flex items-center justify-between gap-3 py-2 border-b border-red-dim last:border-b-0"
          >
            <div className="min-w-0">
              <p className="font-syne text-xs font-600 text-charcoal truncate">{org.name}</p>
              <p className="font-ibm text-[10px] text-muted-foreground">Score: {org.score}</p>
            </div>
            <button onClick={() => removeFromCompare(org.id)} className="p-1 hover:bg-red-dim/30 transition flex-shrink-0">
              <X className="w-3 h-3 text-muted-foreground" />
            </button>
          </motion.div>
        ))}
      </AnimatePresence>
    </motion.div>
  )
}
