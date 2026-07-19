import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import prisma from "../src/prisma.js";

const apply = process.argv.includes("--apply");
const here = path.dirname(fileURLToPath(import.meta.url));
const hierarchyPath = path.resolve(here, "../hierarchy_data.json");
const normalize = (value) => String(value ?? "").trim().toLowerCase();

function collectAssignments(data) {
  const assignments = [];
  const add = (person, teamName, manager, defaultLevel) => {
    if (!person) return;
    assignments.push({
      person,
      teamName: teamName || null,
      manager: manager || null,
      level: person.level || defaultLevel || null,
    });
  };
  const walkReports = (reports, teamName, manager, defaultLevel) => {
    for (const report of reports || []) {
      add(report, teamName, manager, defaultLevel);
      walkReports(report.subLeads, teamName, report, "L3");
      walkReports(report.leads, teamName, report, "L3");
      walkReports(report.members, teamName, report, "L4");
    }
  };

  for (const head of data.hierarchy || []) {
    for (const team of head.teams || []) {
      walkReports(team.leads, team.name, head, "L2");
      walkReports(team.members, team.name, head, "L4");
    }
  }
  return assignments;
}

function buildIndex(rows, getter) {
  const index = new Map();
  for (const row of rows) {
    const key = normalize(getter(row));
    if (!key) continue;
    const matches = index.get(key) || [];
    matches.push(row);
    index.set(key, matches);
  }
  return index;
}

function resolveUser(person, indexes) {
  const candidates = [
    ["email", indexes.email, person.email],
    ["VBID", indexes.vbid, person.vbid],
    ["name", indexes.name, person.name],
  ];
  for (const [field, index, value] of candidates) {
    const matches = index.get(normalize(value)) || [];
    if (matches.length === 1) return { user: matches[0] };
    if (matches.length > 1) return { error: `ambiguous ${field} '${value}'` };
  }
  return { error: `no existing user matched email/VBID/name` };
}

async function main() {
  const data = JSON.parse(fs.readFileSync(hierarchyPath, "utf8"));
  const [users, teams] = await Promise.all([
    prisma.user.findMany({
      select: {
        id: true, name: true, email: true, vbid: true, managerId: true,
        employeeProfile: { select: { id: true, vbid: true, teamId: true, managerId: true, level: true } },
      },
    }),
    prisma.team.findMany({ select: { id: true, name: true } }),
  ]);
  const indexes = {
    email: buildIndex(users, (u) => u.email),
    vbid: buildIndex(users, (u) => u.vbid || u.employeeProfile?.vbid),
    name: buildIndex(users, (u) => u.name),
  };
  const teamIndex = buildIndex(teams, (team) => team.name);
  const resolvedById = new Map();
  const warnings = [];

  for (const assignment of collectAssignments(data)) {
    const result = resolveUser(assignment.person, indexes);
    if (!result.user) {
      warnings.push(`${assignment.person.name || assignment.person.email}: ${result.error}`);
      continue;
    }
    if (!result.user.employeeProfile) {
      warnings.push(`${result.user.name}: existing user has no employee profile`);
      continue;
    }
    if (resolvedById.has(result.user.id)) {
      warnings.push(`${result.user.name}: appears more than once in hierarchy; skipped duplicate`);
      continue;
    }
    resolvedById.set(result.user.id, { ...assignment, user: result.user });
  }

  const changes = [];
  for (const item of resolvedById.values()) {
    let teamId = null;
    if (item.teamName) {
      const matches = teamIndex.get(normalize(item.teamName)) || [];
      if (matches.length !== 1) {
        warnings.push(`${item.user.name}: team '${item.teamName}' was not uniquely matched`);
        continue;
      }
      teamId = matches[0].id;
    }
    let managerId = null;
    if (item.manager) {
      const managerResult = resolveUser(item.manager, indexes);
      if (!managerResult.user) {
        warnings.push(`${item.user.name}: manager '${item.manager.name}' ${managerResult.error}`);
        continue;
      }
      managerId = managerResult.user.id;
    }
    const profile = item.user.employeeProfile;
    const fields = {
      teamId: [profile.teamId, teamId],
      managerId: [profile.managerId, managerId],
      userManagerId: [item.user.managerId, managerId],
      level: [profile.level, item.level],
    };
    if (Object.values(fields).some(([before, after]) => before !== after)) {
      changes.push({ ...item, teamId, managerId, fields });
    }
  }

  console.log(`Mode: ${apply ? "APPLY" : "DRY RUN"}`);
  console.log(`Hierarchy assignments matched: ${resolvedById.size}`);
  console.log(`Assignments requiring changes: ${changes.length}`);
  for (const change of changes) {
    const changedFields = Object.entries(change.fields)
      .filter(([, [before, after]]) => before !== after)
      .map(([field, [before, after]]) => `${field}: ${before ?? "-"} -> ${after ?? "-"}`)
      .join(", ");
    console.log(`- ${change.user.name}: ${changedFields}`);
  }
  if (warnings.length) {
    console.warn(`Warnings (${warnings.length}):`);
    warnings.forEach((warning) => console.warn(`- ${warning}`));
  }
  if (!apply) {
    console.log("No database changes made. Re-run with --apply after reviewing this output.");
    return;
  }

  await prisma.$transaction(changes.flatMap((change) => [
    prisma.employeeProfile.update({
      where: { id: change.user.id },
      data: { teamId: change.teamId, managerId: change.managerId, level: change.level },
    }),
    prisma.user.update({
      where: { id: change.user.id },
      data: { managerId: change.managerId },
    }),
  ]));
  console.log(`Applied ${changes.length} hierarchy assignment updates.`);
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(() => prisma.$disconnect());
