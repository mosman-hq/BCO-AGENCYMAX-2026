'use client'

import { motion } from 'framer-motion'
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceArea,
  ReferenceLine,
  Label,
} from 'recharts'

interface EntityPoint {
  name: string
  dependency: number
  monthsActive: number
  funding: string
  lastFiling: string
  zone: 'zombie' | 'atRisk' | 'clean'
}

const zombieEntities: EntityPoint[] = [
  { name: 'Northgate Solutions', dependency: 94, monthsActive: 4, funding: '$2.4M', lastFiling: '2023-11', zone: 'zombie' },
  { name: 'Clearwater Innovations', dependency: 91, monthsActive: 7, funding: '$1.1M', lastFiling: '2023-12', zone: 'zombie' },
  { name: 'Northern Light Society', dependency: 83, monthsActive: 3, funding: '$560K', lastFiling: '2023-10', zone: 'zombie' },
  { name: 'Westfield Foundation', dependency: 89, monthsActive: 2, funding: '$1.8M', lastFiling: '2023-09', zone: 'zombie' },
  { name: 'Prairie Community Trust', dependency: 87, monthsActive: 10, funding: '$890K', lastFiling: '2024-01', zone: 'zombie' },
  { name: 'Summit Research Corp', dependency: 92, monthsActive: 5, funding: '$2.1M', lastFiling: '2023-08', zone: 'zombie' },
]

const atRiskEntities: EntityPoint[] = [
  { name: 'Capital Bridge Group', dependency: 65, monthsActive: 8, funding: '$3.2M', lastFiling: '2024-02', zone: 'atRisk' },
  { name: 'Pine Valley Industries', dependency: 58, monthsActive: 5, funding: '$420K', lastFiling: '2024-03', zone: 'atRisk' },
  { name: 'Maple Grove Services', dependency: 62, monthsActive: 14, funding: '$750K', lastFiling: '2024-01', zone: 'atRisk' },
  { name: 'Eastern Shore Co-op', dependency: 55, monthsActive: 9, funding: '$380K', lastFiling: '2024-02', zone: 'atRisk' },
]

const cleanEntities: EntityPoint[] = [
  { name: 'Atlantic Health Network', dependency: 35, monthsActive: 42, funding: '$1.2M', lastFiling: '2024-03', zone: 'clean' },
  { name: 'BC Arts Council', dependency: 28, monthsActive: 55, funding: '$680K', lastFiling: '2024-03', zone: 'clean' },
  { name: 'Prairies Education Trust', dependency: 40, monthsActive: 38, funding: '$920K', lastFiling: '2024-02', zone: 'clean' },
  { name: 'National Youth Services', dependency: 22, monthsActive: 48, funding: '$1.5M', lastFiling: '2024-03', zone: 'clean' },
  { name: 'Quebec Cultural Society', dependency: 45, monthsActive: 30, funding: '$540K', lastFiling: '2024-01', zone: 'clean' },
  { name: 'Ontario Green Fund', dependency: 18, monthsActive: 52, funding: '$2.8M', lastFiling: '2024-03', zone: 'clean' },
]

function CustomTooltip({ active, payload }: { active?: boolean; payload?: Array<{ payload: EntityPoint }> }) {
  if (!active || !payload?.length) return null
  const data = payload[0].payload

  return (
    <div className="bg-charcoal text-white border border-red-primary p-3 shadow-lg max-w-[220px]">
      <p className="font-syne text-sm font-600">{data.name}</p>
      <div className="font-ibm text-xs space-y-1 mt-2 text-white/80">
        <p>Funding: {data.funding}</p>
        <p>Dependency: {data.dependency}%</p>
        <p>Last filing: {data.lastFiling}</p>
        <p>Active: {data.monthsActive} months</p>
      </div>
    </div>
  )
}

export default function ZombieQuadrant() {
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.6 }}
      className="border border-red-dim bg-card-bg p-6"
    >
      <ResponsiveContainer width="100%" height={420}>
        <ScatterChart margin={{ top: 20, right: 30, bottom: 30, left: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(196,30,58,0.08)" />

          <XAxis
            type="number"
            dataKey="dependency"
            domain={[0, 100]}
            tick={{ fontSize: 11, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }}
            tickLine={false}
          >
            <Label value="Funding Dependency %" position="bottom" offset={10} style={{ fontSize: 11, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} />
          </XAxis>

          <YAxis
            type="number"
            dataKey="monthsActive"
            domain={[0, 60]}
            tick={{ fontSize: 11, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }}
            tickLine={false}
          >
            <Label value="Months Post-Funding Activity" angle={-90} position="left" offset={0} style={{ fontSize: 11, fontFamily: 'IBM Plex Mono', fill: '#6B7280' }} />
          </YAxis>

          <ReferenceArea
            x1={70}
            x2={100}
            y1={0}
            y2={12}
            fill="rgba(196,30,58,0.08)"
            strokeOpacity={0}
          />

          <ReferenceLine x={70} stroke="#C41E3A" strokeDasharray="6 4" strokeWidth={1.5} />
          <ReferenceLine y={12} stroke="#C41E3A" strokeDasharray="6 4" strokeWidth={1.5} />

          <Tooltip content={<CustomTooltip />} />

          <Scatter name="Zombie" data={zombieEntities} fill="#C41E3A" className="animate-pulse-glow">
            {zombieEntities.map((_, index) => (
              <motion.circle
                key={index}
                initial={{ scale: 0, opacity: 0 }}
                whileInView={{ scale: 1, opacity: 1 }}
                viewport={{ once: true }}
                transition={{ delay: index * 0.03, duration: 0.4 }}
              />
            ))}
          </Scatter>
          <Scatter name="At Risk" data={atRiskEntities} fill="#C8922A" />
          <Scatter name="Clean" data={cleanEntities} fill="#6B7280" />
        </ScatterChart>
      </ResponsiveContainer>

      <div className="flex items-center gap-6 mt-4 font-ibm text-xs">
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 bg-red-primary" />
          <span className="text-muted-foreground uppercase tracking-wider">Zombie Zone</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 bg-amber-warn" />
          <span className="text-muted-foreground uppercase tracking-wider">At Risk</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-3 h-3 bg-[#6B7280]" />
          <span className="text-muted-foreground uppercase tracking-wider">Clean</span>
        </div>
        <span className="ml-auto text-muted-foreground uppercase tracking-wider">
          Zombie Zone: &gt;70% dependency, &lt;12mo activity
        </span>
      </div>
    </motion.div>
  )
}
