'use client'

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'

const timelineData = [
  { month: 'Jan', zombie: 2400000, atRisk: 1800000, clean: 4200000 },
  { month: 'Feb', zombie: 1900000, atRisk: 2100000, clean: 3800000 },
  { month: 'Mar', zombie: 3100000, atRisk: 1600000, clean: 5100000 },
  { month: 'Apr', zombie: 2800000, atRisk: 2400000, clean: 4600000 },
  { month: 'May', zombie: 3600000, atRisk: 2900000, clean: 3900000 },
  { month: 'Jun', zombie: 2200000, atRisk: 1700000, clean: 4400000 },
  { month: 'Jul', zombie: 4100000, atRisk: 3200000, clean: 5200000 },
  { month: 'Aug', zombie: 3400000, atRisk: 2600000, clean: 4100000 },
  { month: 'Sep', zombie: 2700000, atRisk: 2000000, clean: 3600000 },
  { month: 'Oct', zombie: 3800000, atRisk: 3100000, clean: 4800000 },
  { month: 'Nov', zombie: 4500000, atRisk: 2800000, clean: 3700000 },
  { month: 'Dec', zombie: 3200000, atRisk: 2300000, clean: 4000000 },
]

function formatAmount(value: number): string {
  if (value >= 1000000) return `$${(value / 1000000).toFixed(1)}M`
  if (value >= 1000) return `$${(value / 1000).toFixed(0)}K`
  return `$${value}`
}

function CustomTooltip({ active, payload, label }: {
  active?: boolean
  payload?: Array<{ dataKey: string; value: number; color: string }>
  label?: string
}) {
  if (!active || !payload?.length) return null

  return (
    <div className="bg-charcoal text-white border border-red-primary p-3 shadow-lg">
      <p className="font-ibm text-xs font-600 mb-2 uppercase tracking-wider">{label}</p>
      {payload.map((entry) => (
        <div key={entry.dataKey} className="flex items-center gap-2 text-xs font-ibm">
          <div className="w-2.5 h-2.5" style={{ backgroundColor: entry.color }} />
          <span className="text-white/70 capitalize">{entry.dataKey}:</span>
          <span className="font-500 tabular-nums">{formatAmount(entry.value)}</span>
        </div>
      ))}
    </div>
  )
}

export default function FundingTimeline() {
  return (
    <div className="border border-red-dim bg-card-bg p-6">
      <ResponsiveContainer width="100%" height={360}>
        <BarChart data={timelineData} margin={{ top: 10, right: 10, bottom: 10, left: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(196,30,58,0.08)" vertical={false} />
          <XAxis
            dataKey="month"
            tick={{ fontSize: 11, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }}
            tickLine={false}
            axisLine={false}
          />
          <YAxis
            tickFormatter={formatAmount}
            tick={{ fontSize: 10, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }}
            tickLine={false}
            axisLine={false}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend
            wrapperStyle={{ fontFamily: 'IBM Plex Mono', fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.05em' }}
          />
          <Bar dataKey="zombie" fill="#C41E3A" name="Confirmed Zombie" isAnimationActive animationDuration={800} />
          <Bar dataKey="atRisk" fill="#C8922A" name="At Risk" isAnimationActive animationDuration={800} animationBegin={200} />
          <Bar dataKey="clean" fill="#9CA3AF" name="Clean" isAnimationActive animationDuration={800} animationBegin={400} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
