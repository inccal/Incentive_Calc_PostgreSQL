import prisma from "../prisma.js";
import { Role } from "../generated/client/index.js";

const hierarchyError = (message) => Object.assign(new Error(message), { statusCode: 400 });

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

/** Query the destination team and resolve its Head, excluding the member being moved. */
export async function resolveTeamHead(teamId, { excludeUserId, allowMissing = false } = {}) {
  if (!teamId) return null;
  const team = await prisma.team.findUnique({
    where: { id: teamId },
    select: {
      name: true,
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
  const head = inferTeamHeadFromEmployees(team.employees, team.name);
  if (!head && !allowMissing) {
    throw hierarchyError(`No Head is configured for ${team.name}; assign its first L2 to a Head`);
  }
  return head;
}
