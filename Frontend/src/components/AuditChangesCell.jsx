import { formatAuditChanges } from "../utils/auditLogFormat";

export default function AuditChangesCell({ changes, action, useS1RoleLabels = false }) {
  const lines = formatAuditChanges(changes, action, { s1Labels: useS1RoleLabels });

  return (
    <div className="text-sm text-slate-700 space-y-1 min-w-[240px] max-w-lg whitespace-normal">
      {lines.map((line, i) => (
        <div
          key={i}
          className={i === 0 && lines.length > 1 ? "font-medium text-slate-800" : "text-slate-600 pl-0"}
        >
          {line}
        </div>
      ))}
    </div>
  );
}
