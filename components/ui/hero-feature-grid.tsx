'use client'

import { AlertTriangle, ArrowRight, Network, MapPin, Radio } from 'lucide-react'
import DottedMap from 'dotted-map'
import { AreaChart, Area, XAxis, YAxis, CartesianGrid } from 'recharts'
import { Card } from '@/components/ui/card'
import * as React from "react"
import * as RechartsPrimitive from "recharts"
import { cn } from "@/lib/utils"

export default function HeroFeatureGrid() {
  return (
    <section className="py-20 bg-white">
      <div className="max-w-7xl mx-auto px-6 grid grid-cols-1 md:grid-cols-2 md:grid-rows-2">

        {/* 1. MAP — Top Left */}
        <div className="relative overflow-hidden bg-card-bg border border-red-dim p-6">
          <div className="flex items-center gap-2 text-sm text-muted-foreground mb-4">
            <MapPin className="w-4 h-4 text-red-primary" />
            <span className="font-ibm text-xs uppercase tracking-wider">Distribution Map</span>
          </div>
          <h3 className="font-syne text-xl font-600 text-charcoal">
            Funding Distribution Across Canada{" "}
            <span className="text-muted-foreground font-400">
              Track where public money flows — and where it disappears
            </span>
          </h3>
          <div className="relative mt-6">
            <div className="absolute top-16 left-1/2 -translate-x-1/2 z-10 px-3 py-1.5 bg-white text-charcoal text-xs font-ibm font-500 shadow-md flex items-center gap-2 border border-red-dim">
              🚨 Last flagged entity: Ontario
            </div>
            <FundingMap />
          </div>
        </div>

        {/* 2. LIVE TRIAGE FEED — Top Right */}
        <div className="flex flex-col justify-between gap-4 p-6 border border-red-dim bg-card-bg">
          <div>
            <span className="text-xs flex items-center gap-2 text-muted-foreground font-ibm uppercase tracking-wider">
              <Radio className="w-4 h-4 text-red-primary" /> Live Feed
            </span>
            <h3 className="font-syne text-xl font-600 text-charcoal mt-2">
              Live Triage Feed{" "}
              <span className="text-muted-foreground font-400">
                AI agent surfacing high-risk recipients in real time
              </span>
            </h3>
          </div>
          <div className="flex justify-center items-center w-full">
            <TriageFeedCard />
          </div>
        </div>

        {/* 3. AREA CHART — Bottom Left */}
        <div className="border border-red-dim bg-card-bg p-6 space-y-4">
          <div className="flex items-center gap-2 text-sm text-muted-foreground mb-2">
            <AlertTriangle className="w-4 h-4 text-red-primary" />
            <span className="font-ibm text-xs uppercase tracking-wider">Flagged Funding</span>
          </div>
          <h3 className="font-syne text-xl font-600 text-charcoal">
            Flagged Funding Over Time{" "}
            <span className="text-muted-foreground font-400">
              Monthly volume of high-risk disbursements identified
            </span>
          </h3>
          <FlaggedFundingChart />
        </div>

        {/* 4. FEATURE CARDS — Bottom Right */}
        <div className="grid sm:grid-cols-2 bg-card-bg">
          <FeatureCard
            icon={<AlertTriangle className="w-4 h-4 text-red-primary" />}
            title="Zombie Detection"
            subtitle="AI-Powered"
            description="Automatically cross-references funding records with corporate dissolution and bankruptcy filings."
          />
          <FeatureCard
            icon={<Network className="w-4 h-4 text-red-primary" />}
            title="Dependency Scoring"
            subtitle="Real-Time"
            description="Flags entities where public funding exceeds 70% of total revenue with no evidence of delivery."
          />
        </div>
      </div>
    </section>
  )
}

function FeatureCard({ icon, title, subtitle, description }: {
  icon: React.ReactNode
  title: string
  subtitle: string
  description: string
}) {
  return (
    <div className="relative flex flex-col gap-3 p-5 border border-red-dim bg-white transition min-h-[200px]">
      <div className="flex items-center gap-4">
        <div>
          <span className="text-xs flex items-center gap-2 text-muted-foreground font-ibm uppercase tracking-wider mb-3">
            {icon}
            {title}
          </span>
          <h3 className="font-syne text-lg font-600 text-charcoal">
            {subtitle}{" "}
            <span className="text-muted-foreground font-400">{description}</span>
          </h3>
        </div>
      </div>

      <Card className="absolute bottom-0 right-0 w-24 h-20 sm:w-32 sm:h-28 md:w-40 md:h-32 border-8 border-r-0 border-b-0 border-red-dim overflow-hidden bg-card-bg" />

      <div className="absolute bottom-2 right-2 p-3 flex items-center gap-2 border border-red-dim hover:-rotate-45 transition z-10 bg-white">
        <ArrowRight className="w-4 h-4 text-red-primary" />
      </div>
    </div>
  )
}

// ─── Map ───────────────────────────────
const map = new DottedMap({ height: 55, grid: 'diagonal' })
const points = map.getPoints()

function FundingMap() {
  return (
    <svg viewBox="0 0 120 60" className="w-full h-auto" style={{ color: 'rgba(196,30,58,0.4)' }}>
      {points.map((point, i) => (
        <circle key={i} cx={point.x} cy={point.y} r={0.15} fill="currentColor" />
      ))}
    </svg>
  )
}

// ─── Chart ─────────────────────────────
const chartData = [
  { month: 'Jan', confirmed: 12, atRisk: 34 },
  { month: 'Feb', confirmed: 18, atRisk: 41 },
  { month: 'Mar', confirmed: 9, atRisk: 28 },
  { month: 'Apr', confirmed: 24, atRisk: 55 },
  { month: 'May', confirmed: 31, atRisk: 62 },
  { month: 'Jun', confirmed: 27, atRisk: 48 },
]

const chartConfig = {
  confirmed: {
    label: 'Confirmed Zombies',
    color: '#C41E3A',
  },
  atRisk: {
    label: 'At Risk',
    color: '#C8922A',
  },
} satisfies ChartConfig

function FlaggedFundingChart() {
  return (
    <ChartContainer className="h-52 w-full aspect-auto" config={chartConfig}>
      <AreaChart data={chartData}>
        <defs>
          <linearGradient id="fillConfirmed" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#C41E3A" stopOpacity={0.6} />
            <stop offset="95%" stopColor="#C41E3A" stopOpacity={0.05} />
          </linearGradient>
          <linearGradient id="fillAtRisk" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#C8922A" stopOpacity={0.6} />
            <stop offset="95%" stopColor="#C8922A" stopOpacity={0.05} />
          </linearGradient>
        </defs>
        <XAxis dataKey="month" tick={{ fontSize: 11, fontFamily: 'IBM Plex Mono' }} tickLine={false} axisLine={false} />
        <YAxis hide />
        <CartesianGrid vertical={false} strokeDasharray="3 3" stroke="rgba(196,30,58,0.1)" />
        <ChartTooltip cursor={false} content={<ChartTooltipContent />} />
        <Area strokeWidth={2} dataKey="atRisk" type="monotone" fill="url(#fillAtRisk)" stroke="#C8922A" />
        <Area strokeWidth={2} dataKey="confirmed" type="monotone" fill="url(#fillConfirmed)" stroke="#C41E3A" />
      </AreaChart>
    </ChartContainer>
  )
}

// ─── Triage Feed ───────────────────────
interface TriageMessage {
  title: string
  time: string
  content: string
  colorFrom: string
  colorTo: string
}

const triageMessages: TriageMessage[] = [
  {
    title: "ZOMBIE FLAGGED",
    time: "2m ago",
    content: "Northgate Solutions Inc. dissolved 34 days post-funding. $2.4M unaccounted.",
    colorFrom: "#dc2626",
    colorTo: "#7f1d1d",
  },
  {
    title: "HIGH DEPENDENCY",
    time: "5m ago",
    content: "Prairie Community Trust — 94% revenue from federal transfers.",
    colorFrom: "#ef4444",
    colorTo: "#c2410c",
  },
  {
    title: "FILING LAPSED",
    time: "9m ago",
    content: "Clearwater Innovations Ltd. missed T2 deadline. Last grant: $890K.",
    colorFrom: "#ea580c",
    colorTo: "#b91c1c",
  },
  {
    title: "GHOST ENTITY",
    time: "13m ago",
    content: "No employees. No address. $1.1M received Q3 2023.",
    colorFrom: "#991b1b",
    colorTo: "#292524",
  },
  {
    title: "LOOP DETECTED",
    time: "17m ago",
    content: "Circular transfer between 3 registered charities. Total: $640K.",
    colorFrom: "#dc2626",
    colorTo: "#991b1b",
  },
  {
    title: "RISK ESCALATED",
    time: "21m ago",
    content: "Capital Bridge Group flagged by triage agent. Score: 94/100.",
    colorFrom: "#ef4444",
    colorTo: "#7f1d1d",
  },
]

function TriageFeedCard() {
  return (
    <div className="w-full max-w-sm h-[280px] bg-white p-2 overflow-hidden relative">
      <div className="absolute inset-x-0 bottom-0 h-12 bg-gradient-to-t from-white to-transparent z-10" />
      <div className="space-y-2 relative z-0">
        {triageMessages.map((msg, i) => (
          <div
            key={i}
            className="flex gap-3 items-start p-3 border border-red-dim transform transition duration-300 ease-in-out cursor-pointer animate-scaleUp"
            style={{
              animationDelay: `${i * 300}ms`,
              animationFillMode: "forwards",
              opacity: 0,
            }}
          >
            <div
              className="w-8 h-8 min-w-[2rem] min-h-[2rem] flex-shrink-0"
              style={{
                background: `linear-gradient(135deg, ${msg.colorFrom}, ${msg.colorTo})`,
              }}
            />
            <div className="flex flex-col min-w-0">
              <div className="flex items-center gap-2 text-xs font-ibm font-600 text-charcoal">
                {msg.title}
                <span className="text-xs text-muted-foreground before:content-['·'] before:mr-1">
                  {msg.time}
                </span>
              </div>
              <p className="text-xs font-ibm text-muted-foreground mt-0.5 line-clamp-1">
                {msg.content}
              </p>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ─── Chart primitives (shadcn chart) ───
const THEMES = { light: "", dark: ".dark" } as const

export type ChartConfig = {
  [k in string]: {
    label?: React.ReactNode
    icon?: React.ComponentType
  } & (
    | { color?: string; theme?: never }
    | { color?: never; theme: Record<keyof typeof THEMES, string> }
  )
}

type ChartContextProps = { config: ChartConfig }

const ChartContext = React.createContext<ChartContextProps | null>(null)

function useChart() {
  const context = React.useContext(ChartContext)
  if (!context) throw new Error("useChart must be used within a <ChartContainer />")
  return context
}

const ChartContainer = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div"> & {
    config: ChartConfig
    children: React.ComponentProps<typeof RechartsPrimitive.ResponsiveContainer>["children"]
  }
>(({ id, className, children, config, ...props }, ref) => {
  const uniqueId = React.useId()
  const chartId = `chart-${id || uniqueId.replace(/:/g, "")}`

  return (
    <ChartContext.Provider value={{ config }}>
      <div
        data-chart={chartId}
        ref={ref}
        className={cn(
          "flex aspect-video justify-center text-xs [&_.recharts-cartesian-axis-tick_text]:fill-muted-foreground [&_.recharts-cartesian-grid_line[stroke='#ccc']]:stroke-border/50 [&_.recharts-curve.recharts-tooltip-cursor]:stroke-border [&_.recharts-dot[stroke='#fff']]:stroke-transparent [&_.recharts-layer]:outline-none [&_.recharts-polar-grid_[stroke='#ccc']]:stroke-border [&_.recharts-radial-bar-background-sector]:fill-muted [&_.recharts-rectangle.recharts-tooltip-cursor]:fill-muted [&_.recharts-reference-line_[stroke='#ccc']]:stroke-border [&_.recharts-sector[stroke='#fff']]:stroke-transparent [&_.recharts-sector]:outline-none [&_.recharts-surface]:outline-none",
          className,
        )}
        {...props}
      >
        <ChartStyle id={chartId} config={config} />
        <RechartsPrimitive.ResponsiveContainer>
          {children}
        </RechartsPrimitive.ResponsiveContainer>
      </div>
    </ChartContext.Provider>
  )
})
ChartContainer.displayName = "Chart"

function ChartStyle({ id, config }: { id: string; config: ChartConfig }) {
  const colorConfig = Object.entries(config).filter(([, c]) => c.theme || c.color)
  if (!colorConfig.length) return null

  return (
    <style
      dangerouslySetInnerHTML={{
        __html: Object.entries(THEMES)
          .map(
            ([theme, prefix]) =>
              `${prefix} [data-chart=${id}] {\n${colorConfig
                .map(([key, itemConfig]) => {
                  const color = itemConfig.theme?.[theme as keyof typeof itemConfig.theme] || itemConfig.color
                  return color ? `  --color-${key}: ${color};` : null
                })
                .filter(Boolean)
                .join("\n")}\n}`
          )
          .join("\n"),
      }}
    />
  )
}

const ChartTooltip = RechartsPrimitive.Tooltip as unknown as React.FC<RechartsPrimitive.TooltipProps<number, string>>

const ChartTooltipContent = React.forwardRef<
  HTMLDivElement,
  {
    active?: boolean
    payload?: Array<{ dataKey?: string; name?: string; value?: number; color?: string; payload?: Record<string, unknown> }>
    label?: React.ReactNode
    hideLabel?: boolean
    hideIndicator?: boolean
    indicator?: "line" | "dot" | "dashed"
  } & React.ComponentProps<"div">
>(({ active, payload, className, label, hideLabel = false, hideIndicator = false, indicator = "dot" }, ref) => {
  const { config } = useChart()

  if (!active || !payload?.length) return null

  return (
    <div
      ref={ref}
      className={cn(
        "grid min-w-[8rem] items-start gap-1.5 border border-red-dim bg-white px-2.5 py-1.5 text-xs shadow-xl",
        className,
      )}
    >
      {!hideLabel && label && <div className="font-ibm font-500 text-charcoal">{label}</div>}
      <div className="grid gap-1.5">
        {payload.map((item) => {
          const key = item.dataKey || item.name || "value"
          const itemConfig = config[key]
          const indicatorColor = item.color

          return (
            <div key={key} className={cn("flex w-full items-center gap-2", indicator === "dot" && "items-center")}>
              {!hideIndicator && (
                <div
                  className="h-2.5 w-2.5 shrink-0"
                  style={{ backgroundColor: indicatorColor }}
                />
              )}
              <div className="flex flex-1 justify-between leading-none items-center">
                <span className="text-muted-foreground">{itemConfig?.label || item.name}</span>
                {item.value !== undefined && (
                  <span className="font-ibm font-500 tabular-nums text-charcoal ml-3">
                    {item.value.toLocaleString()}
                  </span>
                )}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
})
ChartTooltipContent.displayName = "ChartTooltip"
