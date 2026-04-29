'use client'

import dynamic from 'next/dynamic'
import Link from 'next/link'
import HeroSection from '@/components/ui/hero-section'
import SectionHeader from '@/components/ui/section-header'
import { WorkspaceProvider, useWorkspace } from '@/lib/workspace-context'

const HeroFeatureGrid = dynamic(() => import('@/components/ui/hero-feature-grid'), { ssr: false })
const DiscoverView = dynamic(() => import('@/components/workspace/discover-view'), { ssr: false })
const InvestigateView = dynamic(() => import('@/components/workspace/investigate-view'), { ssr: false })
const CompareView = dynamic(() => import('@/components/workspace/compare-view'), { ssr: false })
const AssistantView = dynamic(() => import('@/components/workspace/assistant-view'), { ssr: false })
const ComparePanel = dynamic(() => import('@/components/workspace/compare-panel'), { ssr: false })

export default function Home() {
  return (
    <WorkspaceProvider>
      <main className="bg-white">
        <HeroSection />
        <div id="below-fold">
          <HeroFeatureGrid />
          <Divider />
          <WorkspaceSection />
        </div>
      </main>
    </WorkspaceProvider>
  )
}

const NAV_TABS = [
  { key: 'discover' as const, label: 'Discover' },
  { key: 'investigate' as const, label: 'Investigate' },
  { key: 'compare' as const, label: 'Compare' },
  { key: 'assistant' as const, label: 'Assistant' },
]

function WorkspaceSection() {
  const { activeView, setActiveView } = useWorkspace()

  return (
    <section className="min-h-screen">
      {/* Sticky nav */}
      <nav className="sticky top-0 z-50 bg-white border-b border-red-dim">
        <div className="max-w-7xl mx-auto px-6 flex items-center h-14">
          <div className="font-syne font-700 text-red-primary text-lg mr-10 tracking-tight">RRI</div>
          <div className="flex gap-0">
            {NAV_TABS.map((tab) => (
              <button
                key={tab.key}
                onClick={() => setActiveView(tab.key)}
                className={`font-syne text-sm px-5 py-4 border-b-2 transition-colors ${
                  activeView === tab.key
                    ? 'border-red-primary text-red-primary font-600'
                    : 'border-transparent text-charcoal/60 hover:text-charcoal'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>
        </div>
      </nav>

      {/* View content */}
      <div className="max-w-7xl mx-auto px-6 py-10">
        {activeView === 'discover' && <DiscoverView />}
        {activeView === 'investigate' && <InvestigateView />}
        {activeView === 'compare' && <CompareView />}
        {activeView === 'assistant' && <AssistantView />}
      </div>

      {/* Floating compare panel */}
      <ComparePanel />

      {/* Footer */}
      <footer className="border-t border-red-dim py-12 bg-white">
        <div className="max-w-7xl mx-auto px-6 flex items-center justify-between">
          <div>
            <span className="font-syne text-lg font-700 text-red-primary tracking-tight">Recipient Risk Intelligence</span>
            <p className="font-ibm text-xs text-muted-foreground mt-1 uppercase tracking-wider">AI for Accountability Hackathon 2026</p>
          </div>
          <div className="flex items-center gap-6">
            <Link href="/oversight" className="font-ibm text-xs text-muted-foreground uppercase tracking-wider hover:text-red-primary transition">
              Oversight
            </Link>
            <Link href="/binder" className="font-ibm text-xs text-muted-foreground uppercase tracking-wider hover:text-red-primary transition">
              Committee Binder
            </Link>
            <div className="font-ibm text-xs text-muted-foreground uppercase tracking-wider">BCO AgencyMax</div>
          </div>
        </div>
      </footer>
    </section>
  )
}

function Divider() {
  return (
    <div className="max-w-7xl mx-auto px-6">
      <div className="h-px bg-[rgba(196,30,58,0.2)]" />
    </div>
  )
}
