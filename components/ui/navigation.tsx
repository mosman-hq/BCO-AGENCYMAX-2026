'use client'

import { useState } from 'react'

const tabs = ['Summary', 'Triage', 'Oversight', 'Committee Binder']

export default function Navigation() {
  const [active, setActive] = useState('Summary')

  return (
    <nav className="sticky top-0 z-50 bg-white border-b border-red-dim">
      <div className="max-w-7xl mx-auto px-6 flex items-center h-14">
        <div className="font-syne font-700 text-red-primary text-lg mr-10 tracking-tight">
          RRI
        </div>
        <div className="flex gap-0">
          {tabs.map((tab) => (
            <button
              key={tab}
              onClick={() => setActive(tab)}
              className={`font-syne text-sm px-5 py-4 border-b-2 transition-colors ${
                active === tab
                  ? 'border-red-primary text-red-primary font-600'
                  : 'border-transparent text-charcoal/60 hover:text-charcoal'
              }`}
            >
              {tab}
            </button>
          ))}
        </div>
      </div>
    </nav>
  )
}
