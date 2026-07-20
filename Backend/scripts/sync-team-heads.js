import prisma from "../src/prisma.js";
import { inferTeamHeadFromEmployees } from "../src/services/teamHierarchyService.js";

const apply = process.argv.includes("--apply");

async function main() {
  const teams = await prisma.team.findMany({
    where: { isActive: true },
    select: {
      id: true,
      name: true,
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

  const changes = [];
  for (const team of teams) {
    const head = inferTeamHeadFromEmployees(team.employees, team.name);
    if (!head) continue;
    for (const profile of team.employees) {
      if (String(profile.level || "").trim().toUpperCase() !== "L2") continue;
      if (profile.managerId !== head.id || profile.user.managerId !== head.id) {
        changes.push({ team, profile, head });
      }
    }
  }

  console.log(`Mode: ${apply ? "APPLY" : "DRY RUN"}`);
  console.log(`L2 Head assignments requiring changes: ${changes.length}`);
  for (const change of changes) {
    console.log(
      `- ${change.profile.user.name} (${change.team.name}): ` +
      `${change.profile.manager?.name || change.profile.user.manager?.name || "-"} -> ${change.head.name || change.head.id}`,
    );
  }
  if (!apply) {
    console.log("No database changes made. Re-run with --apply after reviewing this output.");
    return;
  }

  await prisma.$transaction(changes.flatMap((change) => [
    prisma.employeeProfile.update({
      where: { id: change.profile.id },
      data: { managerId: change.head.id },
    }),
    prisma.user.update({
      where: { id: change.profile.id },
      data: { managerId: change.head.id },
    }),
  ]));
  console.log(`Applied ${changes.length} L2 Head assignment updates.`);
}

main()
  .catch((error) => {
    console.error(error.message || error);
    process.exitCode = 1;
  })
  .finally(() => prisma.$disconnect());
