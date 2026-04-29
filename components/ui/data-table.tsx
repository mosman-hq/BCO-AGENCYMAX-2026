'use client'

import { motion } from 'framer-motion'

interface RecipientRow {
  organization: string
  funding: string
  dependency: string
  lastFiling: string
  daysSince: string
  status: 'CONFIRMED ZOMBIE' | 'HIGH RISK' | 'WATCH'
}

const rows: RecipientRow[] = [
  { organization: 'Northgate Solutions Inc.', funding: '$2.4M', dependency: '94%', lastFiling: '2023-11-12', daysSince: '34 days', status: 'CONFIRMED ZOMBIE' },
  { organization: 'Prairie Community Trust', funding: '$890K', dependency: '87%', lastFiling: '2024-01-05', daysSince: '18 days', status: 'HIGH RISK' },
  { organization: 'Clearwater Innovations', funding: '$1.1M', dependency: '91%', lastFiling: '2023-12-20', daysSince: '28 days', status: 'CONFIRMED ZOMBIE' },
  { organization: 'Capital Bridge Group', funding: '$3.2M', dependency: '78%', lastFiling: '2024-02-01', daysSince: '9 days', status: 'WATCH' },
  { organization: 'Northern Light Society', funding: '$560K', dependency: '83%', lastFiling: '2023-10-30', daysSince: '47 days', status: 'CONFIRMED ZOMBIE' },
  { organization: 'Westfield Charitable Foundation', funding: '$1.8M', dependency: '89%', lastFiling: '2023-09-15', daysSince: '62 days', status: 'CONFIRMED ZOMBIE' },
  { organization: 'Pine Valley Industries', funding: '$420K', dependency: '71%', lastFiling: '2024-03-01', daysSince: '5 days', status: 'WATCH' },
  { organization: 'Summit Research Corp', funding: '$2.1M', dependency: '92%', lastFiling: '2023-08-22', daysSince: '85 days', status: 'HIGH RISK' },
]

const statusStyles: Record<string, string> = {
  'CONFIRMED ZOMBIE': 'bg-red-primary text-white',
  'HIGH RISK': 'bg-amber-warn text-white',
  'WATCH': 'bg-[#856404] text-white',
}

export default function DataTable() {
  return (
    <div className="border border-red-dim bg-card-bg overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="border-b border-red-dim bg-charcoal">
              <th className="text-left px-5 py-3 font-ibm text-[10px] uppercase tracking-wider text-white font-500">Organization</th>
              <th className="text-right px-5 py-3 font-ibm text-[10px] uppercase tracking-wider text-white font-500">Funding Received</th>
              <th className="text-right px-5 py-3 font-ibm text-[10px] uppercase tracking-wider text-white font-500">Dependency %</th>
              <th className="text-left px-5 py-3 font-ibm text-[10px] uppercase tracking-wider text-white font-500">Last Filing</th>
              <th className="text-right px-5 py-3 font-ibm text-[10px] uppercase tracking-wider text-white font-500">Days Since</th>
              <th className="text-left px-5 py-3 font-ibm text-[10px] uppercase tracking-wider text-white font-500">Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => (
              <motion.tr
                key={row.organization}
                initial={{ opacity: 0, x: -10 }}
                whileInView={{ opacity: 1, x: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.3, delay: index * 0.05 }}
                className="border-b border-red-dim group cursor-pointer hover:-translate-y-px transition-transform"
              >
                <td className="px-5 py-4 border-l-2 border-l-transparent group-hover:border-l-red-primary transition-colors">
                  <span className="font-syne text-sm font-600 text-charcoal">{row.organization}</span>
                </td>
                <td className="px-5 py-4 text-right">
                  <span className="font-ibm text-sm tabular-nums text-charcoal">{row.funding}</span>
                </td>
                <td className="px-5 py-4 text-right">
                  <span className="font-ibm text-sm tabular-nums text-charcoal">{row.dependency}</span>
                </td>
                <td className="px-5 py-4">
                  <span className="font-ibm text-sm tabular-nums text-muted-foreground">{row.lastFiling}</span>
                </td>
                <td className="px-5 py-4 text-right">
                  <span className="font-ibm text-sm tabular-nums text-charcoal">{row.daysSince}</span>
                </td>
                <td className="px-5 py-4">
                  <span className={`font-ibm text-[10px] uppercase tracking-wider px-2.5 py-1 inline-block ${statusStyles[row.status]}`}>
                    {row.status}
                  </span>
                </td>
              </motion.tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
