'use client'

import { createContext, useContext, useState, useCallback, type ReactNode } from 'react'
import type { Organization } from '@/lib/data'

type ViewName = 'discover' | 'investigate' | 'compare' | 'assistant'
type CriteriaFilter = 'all' | 'criterionA' | 'criterionB' | 'both'

interface WorkspaceState {
  activeView: ViewName
  setActiveView: (view: ViewName) => void
  criteriaFilter: CriteriaFilter
  setCriteriaFilter: (filter: CriteriaFilter) => void
  selectedOrgId: string | null
  openProfile: (id: string) => void
  compareSlots: string[]
  addToCompare: (id: string) => void
  removeFromCompare: (id: string) => void
  clearCompare: () => void
  openCompare: () => void
  openAssistantForOrg: (id: string) => void
}

const WorkspaceContext = createContext<WorkspaceState | null>(null)

export function useWorkspace() {
  const ctx = useContext(WorkspaceContext)
  if (!ctx) throw new Error('useWorkspace must be used within WorkspaceProvider')
  return ctx
}

export function WorkspaceProvider({ children }: { children: ReactNode }) {
  const [activeView, setActiveView] = useState<ViewName>('discover')
  const [criteriaFilter, setCriteriaFilter] = useState<CriteriaFilter>('all')
  const [selectedOrgId, setSelectedOrgId] = useState<string | null>(null)
  const [compareSlots, setCompareSlots] = useState<string[]>([])

  const openProfile = useCallback((id: string) => {
    setSelectedOrgId(id)
    setActiveView('investigate')
  }, [])

  const addToCompare = useCallback((id: string) => {
    setCompareSlots((prev) => {
      if (prev.includes(id)) return prev
      if (prev.length >= 3) return [...prev.slice(1), id]
      return [...prev, id]
    })
  }, [])

  const removeFromCompare = useCallback((id: string) => {
    setCompareSlots((prev) => prev.filter((s) => s !== id))
  }, [])

  const clearCompare = useCallback(() => setCompareSlots([]), [])

  const openCompare = useCallback(() => {
    setActiveView('compare')
  }, [])

  const openAssistantForOrg = useCallback((id: string) => {
    setSelectedOrgId(id)
    setActiveView('assistant')
  }, [])

  return (
    <WorkspaceContext.Provider
      value={{
        activeView,
        setActiveView,
        criteriaFilter,
        setCriteriaFilter,
        selectedOrgId,
        openProfile,
        compareSlots,
        addToCompare,
        removeFromCompare,
        clearCompare,
        openCompare,
        openAssistantForOrg,
      }}
    >
      {children}
    </WorkspaceContext.Provider>
  )
}
