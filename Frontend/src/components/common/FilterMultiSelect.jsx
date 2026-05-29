import { useState, useRef, useEffect } from 'react';

/**
 * Multi-select dropdown with checkboxes for table filters.
 * Empty selectedValues = no filter (show all).
 */
const FilterMultiSelect = ({
  label,
  options,
  selectedValues = [],
  onChange,
  allLabel = 'All',
  className = '',
}) => {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);

  useEffect(() => {
    const onDocClick = (e) => {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    };
    document.addEventListener('mousedown', onDocClick);
    return () => document.removeEventListener('mousedown', onDocClick);
  }, []);

  const toggle = (value) => {
    const next = selectedValues.includes(value)
      ? selectedValues.filter((v) => v !== value)
      : [...selectedValues, value];
    onChange(next);
  };

  const triggerLabel =
    selectedValues.length === 0
      ? allLabel
      : selectedValues.length === 1
        ? options.find((o) => o.value === selectedValues[0])?.label ?? '1 selected'
        : `${selectedValues.length} selected`;

  return (
    <div ref={ref} className={`relative ${className}`}>
      {label && (
        <span className="sr-only">{label}</span>
      )}
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        aria-expanded={open}
        aria-haspopup="listbox"
        className="flex min-w-[8.5rem] items-center justify-between gap-2 px-3 py-2 bg-white border border-slate-200 rounded-lg text-sm text-slate-700 hover:border-slate-300 focus:outline-none focus:ring-2 focus:ring-blue-500/20"
      >
        <span className="truncate">{triggerLabel}</span>
        <svg
          className={`h-4 w-4 shrink-0 text-slate-400 transition-transform ${open ? 'rotate-180' : ''}`}
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
          aria-hidden
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {open && (
        <div
          role="listbox"
          className="absolute left-0 z-50 mt-1 max-h-64 min-w-full overflow-y-auto rounded-lg border border-slate-200 bg-white py-1 shadow-lg"
        >
          <label className="flex cursor-pointer items-center gap-2 px-3 py-2 text-sm hover:bg-slate-50">
            <input
              type="checkbox"
              checked={selectedValues.length === 0}
              onChange={() => onChange([])}
              className="rounded text-blue-600 focus:ring-blue-500"
            />
            <span className="font-medium text-slate-700">{allLabel}</span>
          </label>
          <div className="my-1 border-t border-slate-100" />
          {options.map((opt) => (
            <label
              key={opt.value}
              className="flex cursor-pointer items-center gap-2 px-3 py-2 text-sm hover:bg-slate-50"
            >
              <input
                type="checkbox"
                checked={selectedValues.includes(opt.value)}
                onChange={() => toggle(opt.value)}
                className="rounded text-blue-600 focus:ring-blue-500"
              />
              <span className="text-slate-700">{opt.label}</span>
            </label>
          ))}
        </div>
      )}
    </div>
  );
};

export default FilterMultiSelect;
