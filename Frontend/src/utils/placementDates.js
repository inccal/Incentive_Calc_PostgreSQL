/**
 * Format a date-only placement field (DOJ/DOQ) stored as UTC midnight.
 * Uses UTC calendar components so display matches the uploaded sheet date.
 */
export function formatPlacementDate(value) {
  if (!value) return '-';
  const d = new Date(value);
  if (isNaN(d.getTime())) return '-';
  if (d.getUTCFullYear() === 1990 && d.getUTCMonth() === 0 && d.getUTCDate() === 1) {
    return '-';
  }
  return d.toLocaleDateString('en-US', {
    timeZone: 'UTC',
    month: 'numeric',
    day: 'numeric',
    year: 'numeric',
  });
}

/** YYYY-MM-DD for <input type="date" /> from UTC-stored date-only value */
export function toPlacementInputDate(value) {
  if (!value) return '';
  const d = new Date(value);
  if (isNaN(d.getTime())) return '';
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}
