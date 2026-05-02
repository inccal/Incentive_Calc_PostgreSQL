import { motion } from 'framer-motion'

/**
 * IncentiveSlabTable — 2×N grid of target achievement % ranges and incentive %.
 *
 * Props:
 *   slabs: Array<{ minPercent: number, maxPercent: number|null, incentivePercent: number }>
 *   compact: boolean — smaller padding (default: false)
 */
const IncentiveSlabTable = ({ slabs, compact = false }) => {
  if (!slabs || !Array.isArray(slabs) || slabs.length === 0) {
    return (
      <div className={`rounded-2xl border border-dashed border-slate-300/60 bg-slate-50/30 flex items-center justify-center ${compact ? 'py-4 px-3' : 'py-8 px-6'}`}>
        <div className="text-center">
          <svg className={`mx-auto text-slate-300 mb-2 ${compact ? 'h-6 w-6' : 'h-8 w-8'}`} fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.5}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6.75h16.5M3.75 12h16.5m-16.5 5.25H12" />
          </svg>
          <p className={`font-medium text-slate-400 ${compact ? 'text-[10px]' : 'text-xs'}`}>Incentive slabs not configured</p>
        </div>
      </div>
    )
  }

  const formatRange = (slab) => {
    if (slab.maxPercent == null) return `${slab.minPercent}%+`
    return `${slab.minPercent}-${slab.maxPercent}%`
  }

  const slabColors = [
    { bg: 'from-red-50 to-rose-50', border: 'border-red-200/60', text: 'text-red-700' },
    { bg: 'from-amber-50 to-orange-50', border: 'border-amber-200/60', text: 'text-amber-700' },
    { bg: 'from-yellow-50 to-lime-50', border: 'border-yellow-200/60', text: 'text-yellow-700' },
    { bg: 'from-emerald-50 to-green-50', border: 'border-emerald-200/60', text: 'text-emerald-700' },
    { bg: 'from-teal-50 to-cyan-50', border: 'border-teal-200/60', text: 'text-teal-700' },
    { bg: 'from-blue-50 to-indigo-50', border: 'border-blue-200/60', text: 'text-blue-700' },
    { bg: 'from-violet-50 to-purple-50', border: 'border-violet-200/60', text: 'text-violet-700' },
    { bg: 'from-pink-50 to-fuchsia-50', border: 'border-pink-200/60', text: 'text-pink-700' },
  ]

  const getColor = (index) => slabColors[index % slabColors.length]

  const cellClass = (index) => {
    const color = getColor(index)
    return `text-center rounded-xl border transition-all duration-300 bg-gradient-to-br ${color.bg} ${color.border} hover:shadow-sm ${compact ? 'px-2 py-2' : 'px-3 py-3'}`
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: [0.25, 0.46, 0.45, 0.94] }}
      className="rounded-2xl border border-slate-200/80 bg-white shadow-sm overflow-hidden"
    >
      <div className={`bg-gradient-to-r from-slate-800 via-indigo-900/95 to-slate-800 ${compact ? 'px-4 py-2.5' : 'px-5 py-3'}`}>
        <div className="flex items-center gap-2">
          <div className={`flex items-center justify-center rounded-lg bg-white/10 ${compact ? 'h-6 w-6' : 'h-8 w-8'}`}>
            <svg className={`text-indigo-200 ${compact ? 'h-3.5 w-3.5' : 'h-4.5 w-4.5'}`} fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={1.8}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 3v11.25A2.25 2.25 0 006 16.5h2.25M3.75 3h-1.5m1.5 0h16.5m0 0h1.5m-1.5 0v11.25A2.25 2.25 0 0118 16.5h-2.25m-7.5 0h7.5m-7.5 0l-1 3m8.5-3l1 3m0 0l.5 1.5m-.5-1.5h-9.5m0 0l-.5 1.5m.75-9l3-1.5L12 12m0 0l3 1.5m-3-1.5V18" />
            </svg>
          </div>
          <h4 className={`font-semibold text-white ${compact ? 'text-xs' : 'text-sm'}`}>
            Incentive Slabs
          </h4>
        </div>
      </div>

      <div className={compact ? 'p-3' : 'p-4'}>
        <div className="grid gap-2" style={{ gridTemplateColumns: `repeat(${slabs.length}, minmax(0, 1fr))` }}>
          {slabs.map((slab, index) => {
            const color = getColor(index)
            return (
              <motion.div
                key={`range-${index}`}
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: index * 0.05, duration: 0.3 }}
                className={cellClass(index)}
              >
                <p className={`font-medium text-slate-500 mb-0.5 ${compact ? 'text-[9px]' : 'text-[10px]'}`}>Target Achievement</p>
                <p className={`font-bold ${color.text} ${compact ? 'text-xs' : 'text-sm'}`}>
                  {formatRange(slab)}
                </p>
              </motion.div>
            )
          })}

          {slabs.map((slab, index) => {
            const color = getColor(index)
            return (
              <motion.div
                key={`incentive-${index}`}
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: 0.1 + index * 0.05, duration: 0.3 }}
                className={cellClass(index)}
              >
                <p className={`font-medium text-slate-500 mb-0.5 ${compact ? 'text-[9px]' : 'text-[10px]'}`}>Incentive %</p>
                <p className={`font-extrabold ${color.text} ${compact ? 'text-sm' : 'text-lg'}`}>
                  {slab.incentivePercent}%
                </p>
              </motion.div>
            )
          })}
        </div>
      </div>
    </motion.div>
  )
}

export default IncentiveSlabTable
