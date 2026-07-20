import prisma from "../src/prisma.js";
import { teamHeadFromRecord } from "../src/services/teamHierarchyService.js";

const apply = process.argv.includes("--apply");

async function main() {
  const teams = await prisma.team.findMany({
    where: { isActive: true },
    select: {
      id: true,
      name: true,
      headId: true,
      head: { select: { id: true, name: true, role: true } },
      employees: {
        where: { deletedAt: null },
        select: {
          id: true,
          level: true,
          managerId: true,
          manager: { select: { id: true, name: true, role: true } },
          user: {
            select: {
              name: true,
              managerId: true,
              manager: { select: { id: true, name: true, role: true } },
            },
          },
        },
      },
    },
  });

  const ownershipChanges = [];
  const assignmentChanges = [];
  const unresolvedTeams = [];
  for (const team of teams) {
    const head = teamHeadFromRecord(team);
    if (!head) {
      unresolvedTeams.push(team.name);
      continue;
    }
    if (team.headId !== head.id) ownershipChanges.push({ team, head });
    for (const profile of team.employees) {
      if (String(profile.level || "").trim().toUpperCase() !== "L2") continue;
      if (profile.managerId !== head.id || profile.user.managerId !== head.id) {
        assignmentChanges.push({ team, profile, head });
      }
    }
  }

  console.log(`Mode: ${apply ? "APPLY" : "DRY RUN"}`);
  console.log(`Team owners requiring changes: ${ownershipChanges.length}`);
  for (const change of ownershipChanges) {
    console.log(
      `- ${change.team.name}: ${change.team.head?.name || "-"} -> ${change.head.name || change.head.id}`,
    );
  }
  console.log(`L2 Head assignments requiring changes: ${assignmentChanges.length}`);
  for (const change of assignmentChanges) {
    console.log(
      `- ${change.profile.user.name} (${change.team.name}): ` +
      `${change.profile.manager?.name || change.profile.user.manager?.name || "-"} -> ${change.head.name || change.head.id}`,
    );
  }
  if (unresolvedTeams.length > 0) {
    console.log(`Teams without a configured/inferable Head: ${unresolvedTeams.join(", ")}`);
  }
  if (!apply) {
    console.log("No database changes made. Re-run with --apply after reviewing this output.");
    return;
  }

  const operations = [
    ...ownershipChanges.map((change) => prisma.team.update({
      where: { id: change.team.id },
      data: { headId: change.head.id },
    })),
    ...assignmentChanges.flatMap((change) => [
      prisma.employeeProfile.update({
        where: { id: change.profile.id },
        data: { managerId: change.head.id },
      }),
      prisma.user.update({
        where: { id: change.profile.id },
        data: { managerId: change.head.id },
      }),
    ]),
  ];
  if (operations.length > 0) await prisma.$transaction(operations);
  console.log(
    `Applied ${ownershipChanges.length} team owner and ${assignmentChanges.length} L2 Head assignment updates.`,
  );
}

main()
  .catch((error) => {
    console.error(error.message || error);
    process.exitCode = 1;
  })
  .finally(() => prisma.$disconnect());
