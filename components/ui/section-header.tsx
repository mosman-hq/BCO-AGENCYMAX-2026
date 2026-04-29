interface SectionHeaderProps {
  title: string
  subtitle?: string
}

export default function SectionHeader({ title, subtitle }: SectionHeaderProps) {
  return (
    <div className="flex items-start gap-4 mb-10">
      <div className="w-1 h-10 bg-red-primary flex-shrink-0 mt-1" />
      <div>
        <h2 className="font-syne text-2xl md:text-3xl font-700 uppercase tracking-tight text-charcoal">
          {title}
        </h2>
        {subtitle && (
          <p className="font-ibm text-sm text-muted-foreground mt-2">{subtitle}</p>
        )}
      </div>
    </div>
  )
}
