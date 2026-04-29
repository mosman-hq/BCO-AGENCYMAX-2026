'use client'

import { useEffect, useRef, useState } from 'react'
import { motion, useMotionValue, useTransform, animate } from 'framer-motion'
import { ChevronDown } from 'lucide-react'
import { CircuitBg } from '@/components/ui/circuit-bg'

interface AnimatedCounterProps {
  target: number
  prefix?: string
  suffix?: string
  duration?: number
}

function AnimatedCounter({ target, prefix = '', suffix = '', duration = 1.5 }: AnimatedCounterProps) {
  const motionValue = useMotionValue(0)
  const rounded = useTransform(motionValue, (latest) => {
    if (target >= 1000000) {
      return `${prefix}${(latest / 1000000).toFixed(1)}M${suffix}`
    }
    if (target >= 1000) {
      return `${prefix}${Math.round(latest).toLocaleString()}${suffix}`
    }
    return `${prefix}${Math.round(latest)}${suffix}`
  })
  const [display, setDisplay] = useState(`${prefix}0${suffix}`)

  useEffect(() => {
    const unsubscribe = rounded.on('change', setDisplay)
    const controls = animate(motionValue, target, {
      duration,
      ease: 'easeOut',
    })
    return () => {
      unsubscribe()
      controls.stop()
    }
  }, [motionValue, target, rounded, duration])

  return <span>{display}</span>
}

const stats = [
  { label: 'Recipients Flagged', value: 2847, prefix: '', suffix: '' },
  { label: 'Total Exposure ($CAD)', value: 4200000, prefix: '$', suffix: '' },
  { label: 'Over 80% Dependency', value: 312, prefix: '', suffix: '' },
  { label: 'Confirmed Zombies', value: 89, prefix: '', suffix: '' },
]

export default function HeroSection() {
  const scrollRef = useRef<HTMLDivElement>(null)

  const handleScrollClick = () => {
    scrollRef.current?.closest('main')?.querySelector('#below-fold')?.scrollIntoView({ behavior: 'smooth' })
  }

  return (
    <section className="relative h-screen w-full overflow-hidden">
      <CircuitBg />

      <div className="absolute inset-0 bg-black/50" />

      <div className="relative z-10 flex flex-col items-center justify-center h-full px-6">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, ease: 'easeOut' }}
          className="text-center"
        >
          <h1 className="font-syne text-5xl md:text-7xl lg:text-8xl font-800 text-white tracking-tight leading-none">
            RECIPIENT RISK
            <br />
            <span className="text-red-primary">INTELLIGENCE</span>
          </h1>
          <p className="font-ibm text-red-dim text-sm md:text-base mt-6 tracking-wider uppercase">
            Tracking where public money disappears
          </p>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: 40 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, delay: 0.3, ease: 'easeOut' }}
          className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-16 w-full max-w-5xl"
        >
          {stats.map((stat, index) => (
            <motion.div
              key={stat.label}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5, delay: 0.5 + index * 0.1 }}
              className="bg-white/95 border-l-4 border-l-red-primary p-5"
            >
              <div className="font-ibm text-3xl md:text-4xl font-700 text-charcoal tabular-nums">
                <AnimatedCounter
                  target={stat.value}
                  prefix={stat.prefix}
                  suffix={stat.suffix}
                />
              </div>
              <div className="font-ibm text-xs text-muted-foreground mt-2 uppercase tracking-wider">
                {stat.label}
              </div>
            </motion.div>
          ))}
        </motion.div>

        <motion.div
          ref={scrollRef}
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 1.5 }}
          className="absolute bottom-10 flex flex-col items-center cursor-pointer"
          onClick={handleScrollClick}
        >
          <span className="font-ibm text-xs text-red-dim uppercase tracking-[0.2em] mb-3">
            Scroll to explore
          </span>
          <ChevronDown className="w-6 h-6 text-red-dim animate-chevron" />
        </motion.div>
      </div>
    </section>
  )
}
