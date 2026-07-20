import prisma from "../prisma.js";
import { Role } from "../generated/client/index.js";

const hierarchyError = (message) => Object.assign(new Error(message), { statusCode: 400 });

const normalizeLevel = (level) => String(level || "").trim().toUpperCase();

export function roleForHierarchyLevel(level, fallbackRole = Role.EMPLOYEE) {
  const normalized = normalizeLevel(level);
  if (normalized === "L2" || normalized === "L3") return Role.TEAM_LEAD;
  if (normalized === "L4") return Role.EMPLOYEE;
  return fallbackRole;
}

/** Resolve a team's Head from its existing L2 assignments. */
export function inferTeamHeadFromEmployees(employees = [], teamName = "team") {
  const candidates = new Map();
  for (const profile of employees) {
    if (String(profile.level || "").trim().toUpperCase() !== "L2") continue;
    const managerId = profile.managerId || profile.user?.managerId || null;
    if (!managerId) continue;
    const manager = profile.manager || profile.user?.manager || null;
    if (manager?.role && manager.role !== Role.SUPER_ADMIN) continue;
    const candidate = candidates.get(managerId) || {
      id: managerId,
      name: manager?.name || null,
      l2Count: 0,
    };
    candidate.l2Count += 1;
    candidates.set(managerId, candidate);
  }

  const ranked = [...candidates.values()].sort((a, b) => b.l2Count - a.l2Count);
  if (ranked.length > 1 && ranked[0].l2Count === ranked[1].l2Count) {
    const names = [...candidates.values()].map((head) => head.name || head.id).join(", ");
    throw hierarchyError(`${teamName} has conflicting Head assignments: ${names}`);
  }
  return ranked[0] || null;
}

/** Prefer the team's stored owner; infer only for legacy rows not migrated yet. */
export function teamHeadFromRecord(team) {
  if (team?.headId && team?.head?.role === Role.SUPER_ADMIN) {
    return { id: team.headId, name: team.head.name || null };
  }
  return inferTeamHeadFromEmployees(team?.employees || [], team?.name || "team");
}

/** Query the destination team and resolve its Head, excluding the member being moved. */
export async function resolveTeamHead(teamId, { excludeUserId, allowMissing = false } = {}) {
  if (!teamId) return null;
  const team = await prisma.team.findUnique({
    where: { id: teamId },
    select: {
      name: true,
      headId: true,
      head: { select: { id: true, name: true, role: true, isActive: true } },
      employees: {
        where: {
          deletedAt: null,
          ...(excludeUserId ? { id: { not: excludeUserId } } : {}),
        },
        select: {
          id: true,
          level: true,
          managerId: true,
          manager: { select: { id: true, name: true, role: true } },
          user: {
            select: {
              managerId: true,
              manager: { select: { id: true, name: true, role: true } },
            },
          },
        },
      },
    },
  });
  if (!team) throw hierarchyError("Selected team was not found");
  const head = teamHeadFromRecord(team);
  if (head && team.head && !team.head.isActive) {
    throw hierarchyError(`The configured Head for ${team.name} is inactive`);
  }
  if (!head && !allowMissing) {
    throw hierarchyError(`No Head is configured for ${team.name}`);
  }
  return head;
}

/**
 * Resolve a manager that is valid for the destination team and level.
 * A supplied manager is preserved only while it remains valid. When exactly
 * one valid choice exists it is selected automatically; ambiguous choices are
 * returned to the user instead of silently attaching the wrong manager.
 */
export async function resolveHierarchyManager({
  teamId,
  level,
  requestedManagerId,
  excludeUserId,
}) {
  if (!teamId) return null;
  const normalizedLevel = normalizeLevel(level);
  if (normalizedLevel === "L2") {
    const head = await resolveTeamHead(teamId, { excludeUserId });
    return head.id;
  }

  const allowedManagerLevels =
    normalizedLevel === "L3" ? ["L2"] :
    normalizedLevel === "L4" ? ["L2", "L3"] : [];
  if (allowedManagerLevels.length === 0) {
    throw hierarchyError("Select a valid hierarchy level (L2, L3, or L4)");
  }

  const candidates = await prisma.employeeProfile.findMany({
    where: {
      teamId,
      isActive: true,
      deletedAt: null,
      level: { in: allowedManagerLevels },
      user: { isActive: true },
      ...(excludeUserId ? { id: { not: excludeUserId } } : {}),
    },
    select: { id: true, user: { select: { name: true } } },
    orderBy: { user: { name: "asc" } },
  });

  if (requestedManagerId && candidates.some((candidate) => candidate.id === requestedManagerId)) {
    return requestedManagerId;
  }
  if (candidates.length === 1) return candidates[0].id;
  if (candidates.length === 0) {
    const requiredLevel = normalizedLevel === "L3" ? "an L2 Team Lead" : "an L2 or L3 manager";
    throw hierarchyError(`This team has no active ${requiredLevel} available`);
  }
  const names = candidates.map((candidate) => candidate.user.name).join(", ");
  throw hierarchyError(`Select a manager for this member. Available managers: ${names}`);
}

/** Prevent an edit from leaving existing reports under an incompatible lead. */
export async function assertDirectReportsRemainValid({
  userId,
  currentTeamId,
  targetTeamId,
  targetLevel,
}) {
  const directReports = await prisma.employeeProfile.findMany({
    where: {
      id: { not: userId },
      isActive: true,
      deletedAt: null,
      OR: [
        { managerId: userId },
        { user: { managerId: userId } },
      ],
    },
    select: { level: true, user: { select: { name: true } } },
    orderBy: { user: { name: "asc" } },
  });
  if (directReports.length === 0) return;

  if (currentTeamId !== targetTeamId) {
    const names = directReports.map((report) => report.user.name).join(", ");
    throw hierarchyError(`Reassign this member's direct reports before changing teams: ${names}`);
  }

  const normalizedTargetLevel = normalizeLevel(targetLevel);
  const allowedReportLevels =
    normalizedTargetLevel === "L2" ? new Set(["L3", "L4"]) :
    normalizedTargetLevel === "L3" ? new Set(["L4"]) :
    new Set();
  const incompatible = directReports.filter(
    (report) => !allowedReportLevels.has(normalizeLevel(report.level)),
  );
  if (incompatible.length > 0) {
    const names = incompatible.map((report) => report.user.name).join(", ");
    throw hierarchyError(`Reassign incompatible direct reports before changing this level: ${names}`);
  }
}
