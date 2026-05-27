const FIELD_LABELS = {
  name: "Name",
  email: "Email",
  role: "Role",
  isActive: "Account status",
  level: "Level",
  team: "Team",
  teamId: "Team",
  manager: "Manager",
  managerId: "Manager",
  vbid: "VB ID",
  comment: "Comment",
  targetType: "Target type",
  provider: "Sign-in method",
  reason: "Reason",
  yearlyTarget: "Yearly target",
  color: "Color",
  placementType: "Placement type",
  billingStatus: "Billing status",
};

const ROLE_LABELS = {
  TEAM_LEAD: "Team Lead",
  EMPLOYEE: "Employee",
  SUPER_ADMIN: "Head",
  S1_ADMIN: "S1 Admin",
  LIMITED_ACCESS: "Limited Access",
};

const ACTION_LABELS = {
  USER_UPDATED: "User updated",
  USER_CREATED: "User created",
  USER_DEACTIVATED: "User deactivated",
  LOGIN_ATTEMPT: "Sign-in",
};

const SKIP_KEYS = new Set(["entraObjectId", "employeeProfile"]);

function formatScalar(key, value) {
  if (value === null || value === undefined || value === "") return "—";
  if (key === "role") return ROLE_LABELS[value] || String(value);
  if (key === "isActive") return value ? "Active" : "Inactive";
  if (key === "provider" && value === "MICROSOFT_ENTRA_ID") return "Microsoft";
  if (key === "targetType") {
    if (value === "PLACEMENTS") return "Placements";
    if (value === "REVENUE") return "Revenue";
  }
  if (typeof value === "boolean") return value ? "Yes" : "No";
  return String(value);
}

function flattenUserSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== "object") return {};
  const profile = snapshot.employeeProfile;
  const flat = {
    name: snapshot.name,
    email: snapshot.email,
    role: snapshot.role,
    isActive: snapshot.isActive,
  };
  if (profile && typeof profile === "object") {
    if (profile.level != null) flat.level = profile.level;
    if (profile.team?.name) flat.team = profile.team.name;
    else if (profile.teamId) flat.teamId = profile.teamId;
    if (profile.manager?.name) flat.manager = profile.manager.name;
    else if (profile.managerId) flat.managerId = profile.managerId;
    if (profile.vbid != null) flat.vbid = profile.vbid;
    if (profile.comment != null) flat.comment = profile.comment;
    if (profile.targetType != null) flat.targetType = profile.targetType;
  }
  if (snapshot.vbid != null && flat.vbid == null) flat.vbid = snapshot.vbid;
  return flat;
}

function diffFlat(before, after) {
  const lines = [];
  const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
  for (const key of keys) {
    if (SKIP_KEYS.has(key)) continue;
    const oldVal = before[key];
    const newVal = after[key];
    if (JSON.stringify(oldVal) === JSON.stringify(newVal)) continue;
    const label = FIELD_LABELS[key] || key.replace(/([A-Z])/g, " $1").replace(/^./, (s) => s.toUpperCase());
    lines.push(`${label}: ${formatScalar(key, oldVal)} → ${formatScalar(key, newVal)}`);
  }
  return lines;
}

function formatLoginChanges(changes) {
  if (changes.reason) {
    const lines = [`Microsoft sign-in failed`];
    if (changes.email) lines.push(`Email: ${changes.email}`);
    if (changes.reason) lines.push(`Reason: ${changes.reason}`);
    return lines;
  }
  const lines = ["Signed in with Microsoft"];
  if (changes.email) lines.push(`Account: ${changes.email}`);
  return lines;
}

function formatCreateChanges(changes) {
  const lines = [];
  if (changes.name) lines.push(`Created user: ${changes.name}`);
  if (changes.email) lines.push(`Email: ${changes.email}`);
  if (changes.role) lines.push(`Role: ${formatScalar("role", changes.role)}`);
  if (changes.level) lines.push(`Level: ${changes.level}`);
  if (changes.vbid) lines.push(`VB ID: ${changes.vbid}`);
  if (changes.teamId) lines.push(`Team assigned`);
  if (changes.managerId) lines.push(`Manager assigned`);
  return lines.length ? lines : ["New user created"];
}

function formatGenericFlat(changes) {
  const lines = [];
  for (const [key, value] of Object.entries(changes)) {
    if (SKIP_KEYS.has(key) || value === null || value === undefined) continue;
    if (typeof value === "object") continue;
    const label = FIELD_LABELS[key] || key.replace(/_/g, " ");
    lines.push(`${label}: ${formatScalar(key, value)}`);
  }
  return lines;
}

/**
 * Turn audit log `changes` JSON into short, non-technical lines.
 * @returns {string[]}
 */
export function formatAuditChanges(changes, action = "") {
  if (!changes) return ["No details recorded"];
  if (typeof changes === "string") {
    try {
      return formatAuditChanges(JSON.parse(changes), action);
    } catch {
      return [changes];
    }
  }
  if (typeof changes !== "object") return [String(changes)];

  if (changes.before && changes.after) {
    const lines = diffFlat(
      flattenUserSnapshot(changes.before),
      flattenUserSnapshot(changes.after)
    );
    const who =
      changes.after?.name ||
      changes.before?.name ||
      changes.after?.email ||
      changes.before?.email;
    if (lines.length === 0) {
      return who ? [`Updated ${who} (no visible field changes)`] : ["User updated"];
    }
    return who ? [`Updated ${who}`, ...lines] : lines;
  }

  if (changes.provider === "MICROSOFT_ENTRA_ID" || action === "LOGIN_ATTEMPT") {
    return formatLoginChanges(changes);
  }

  if (action === "USER_CREATED" || (changes.name && changes.email && changes.role)) {
    return formatCreateChanges(changes);
  }

  const generic = formatGenericFlat(changes);
  return generic.length ? generic : ["Action completed"];
}

export function formatAuditAction(action) {
  if (!action) return "—";
  return ACTION_LABELS[action] || action.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}
