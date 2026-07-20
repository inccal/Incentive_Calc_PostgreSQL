import crypto from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import prisma from "../src/prisma.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const backendRoot = path.resolve(here, "..");
const args = process.argv.slice(2);

function getArg(name) {
  const index = args.indexOf(name);
  if (index === -1) return null;
  const value = args[index + 1];
  if (!value || value.startsWith("--")) {
    throw new Error(`${name} requires a file path`);
  }
  return value;
}

function timestamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function personData(profile, includeRole = false) {
  const person = {
    name: profile.user.name,
    email: profile.user.email,
    level: profile.level,
    vbid: profile.vbid || profile.user.vbid || null,
  };
  if (includeRole) person.role = profile.user.role;
  return person;
}

function comparePeople(a, b) {
  return a.user.name.localeCompare(b.user.name, undefined, { sensitivity: "base" });
}

export function buildHierarchy(profiles, teams) {
  const profileById = new Map(profiles.map((profile) => [profile.id, profile]));
  const roots = profiles.filter(
    (profile) => profile.level?.toUpperCase() === "L1" || profile.user.role === "SUPER_ADMIN",
  );
  const rootIds = new Set(roots.map((root) => root.id));
  const errors = [];

  for (const profile of profiles) {
    if (profile.managerId !== profile.user.managerId) {
      errors.push(
        `${profile.user.name}: EmployeeProfile.managerId (${profile.managerId || "-"}) ` +
        `does not match User.managerId (${profile.user.managerId || "-"})`,
      );
    }
    if (!rootIds.has(profile.id) && !profile.teamId && profile.user.role !== "S1_ADMIN") {
      errors.push(`${profile.user.name}: non-L1 employee has no team assignment`);
    }
    if (rootIds.has(profile.id) && (profile.level?.toUpperCase() !== "L1" || profile.user.role !== "SUPER_ADMIN")) {
      errors.push(`${profile.user.name}: hierarchy root must have level L1 and role SUPER_ADMIN`);
    }
  }

  function resolveRoot(profile, teamId) {
    const visited = new Set([profile.id]);
    let current = profile;
    while (!rootIds.has(current.id)) {
      if (!current.managerId) {
        errors.push(`${profile.user.name}: reporting chain ends without an L1/SUPER_ADMIN manager`);
        return null;
      }
      const manager = profileById.get(current.managerId);
      if (!manager) {
        errors.push(`${profile.user.name}: manager ${current.managerId} has no active employee profile`);
        return null;
      }
      if (visited.has(manager.id)) {
        errors.push(`${profile.user.name}: reporting chain contains a cycle at ${manager.user.name}`);
        return null;
      }
      if (!rootIds.has(manager.id) && manager.teamId !== teamId) {
        errors.push(
          `${profile.user.name}: manager ${manager.user.name} belongs to a different team`,
        );
        return null;
      }
      visited.add(manager.id);
      current = manager;
    }
    return current;
  }

  const teamsByRoot = new Map(roots.map((root) => [root.id, []]));
  const exportedIds = new Set(roots.map((root) => root.id));

  for (const team of teams) {
    const teamProfiles = profiles.filter((profile) => profile.teamId === team.id);
    if (teamProfiles.length === 0) continue;
    const teamProfileIds = new Set(teamProfiles.map((profile) => profile.id));
    const resolvedRoots = new Map();

    for (const profile of teamProfiles) {
      const root = resolveRoot(profile, team.id);
      if (root) resolvedRoots.set(root.id, root);
    }
    if (resolvedRoots.size !== 1) {
      errors.push(
        `${team.name}: expected exactly one L1 owner, found ${resolvedRoots.size}`,
      );
      continue;
    }
    const root = [...resolvedRoots.values()][0];
    const childrenByManager = new Map();
    for (const profile of teamProfiles) {
      const children = childrenByManager.get(profile.managerId) || [];
      children.push(profile);
      childrenByManager.set(profile.managerId, children);
    }

    const topLevel = (childrenByManager.get(root.id) || []).sort(comparePeople);
    const unreachable = teamProfiles.filter(
      (profile) => profile.managerId !== root.id && !teamProfileIds.has(profile.managerId),
    );
    for (const profile of unreachable) {
      errors.push(`${profile.user.name}: manager is not in team ${team.name} or its L1 owner`);
    }

    function buildLead(profile) {
      exportedIds.add(profile.id);
      const node = personData(profile);
      const children = (childrenByManager.get(profile.id) || []).sort(comparePeople);
      const level = profile.level?.toUpperCase();
      if (!["L2", "L3"].includes(level) || profile.user.role !== "TEAM_LEAD") {
        errors.push(`${profile.user.name}: lead must have level L2/L3 and role TEAM_LEAD`);
      }
      const subLeads = children.filter((child) => child.level?.toUpperCase() === "L3");
      const members = children.filter((child) => child.level?.toUpperCase() === "L4");
      const unsupportedChildren = children.filter((child) => !subLeads.includes(child) && !members.includes(child));
      for (const child of unsupportedChildren) {
        errors.push(`${child.user.name}: unsupported level ${child.level || "-"} below ${profile.user.name}`);
      }
      if (level === "L3" && subLeads.length) {
        errors.push(`${profile.user.name}: L3 lead has another L3 lead below it; seed format supports L4 members only`);
      }
      if (subLeads.length) node.subLeads = subLeads.map(buildLead);
      if (members.length) {
        node.members = members.map((member) => {
          exportedIds.add(member.id);
          if (member.user.role !== "EMPLOYEE") {
            errors.push(`${member.user.name}: L4 member must have role EMPLOYEE`);
          }
          if ((childrenByManager.get(member.id) || []).length > 0) {
            errors.push(`${member.user.name}: L4/employee member has direct reports`);
          }
          return personData(member);
        });
      }
      return node;
    }

    if (topLevel.length === 0) {
      errors.push(`${team.name}: no lead reports directly to ${root.user.name}`);
      continue;
    }
    for (const lead of topLevel) {
      if (lead.level?.toUpperCase() !== "L2") {
        errors.push(`${lead.user.name}: top-level team lead must have level L2`);
      }
    }
    const teamNode = {
      name: team.name,
      color: team.color || null,
      leads: topLevel.map(buildLead),
    };
    teamsByRoot.get(root.id).push(teamNode);
  }

  const expectedIds = new Set(
    profiles
      .filter((profile) => rootIds.has(profile.id) || profile.teamId)
      .map((profile) => profile.id),
  );
  const omitted = [...expectedIds].filter((id) => !exportedIds.has(id));
  for (const id of omitted) {
    errors.push(`${profileById.get(id)?.user.name || id}: would be omitted from the exported hierarchy`);
  }
  if (errors.length) {
    throw new Error(`Hierarchy export validation failed:\n- ${[...new Set(errors)].join("\n- ")}`);
  }

  return {
    hierarchy: roots.sort(comparePeople).map((root) => ({
      ...personData(root, true),
      teams: (teamsByRoot.get(root.id) || []).sort((a, b) => a.name.localeCompare(b.name)),
    })),
  };
}

async function main() {
  if (args.includes("--help")) {
    console.log("Usage: npm run export-hierarchy -- [--output <path>]");
    console.log("Default: Backend/hierarchy_snapshots/hierarchy-<timestamp>.json");
    console.log("This command reads the database and never overwrites hierarchy_data.json.");
    return;
  }

  const requestedOutput = getArg("--output");
  const outputPath = requestedOutput
    ? path.resolve(process.cwd(), requestedOutput)
    : path.join(backendRoot, "hierarchy_snapshots", `hierarchy-${timestamp()}.json`);
  const liveHierarchyPath = path.join(backendRoot, "hierarchy_data.json");
  if (path.resolve(outputPath).toLowerCase() === path.resolve(liveHierarchyPath).toLowerCase()) {
    throw new Error("Refusing to overwrite Backend/hierarchy_data.json; choose a snapshot path");
  }
  if (fs.existsSync(outputPath)) {
    throw new Error(`Refusing to overwrite existing snapshot: ${outputPath}`);
  }

  const [profiles, teams] = await Promise.all([
    prisma.employeeProfile.findMany({
      where: { deletedAt: null },
      select: {
        id: true,
        teamId: true,
        managerId: true,
        level: true,
        vbid: true,
        user: {
          select: {
            name: true,
            email: true,
            role: true,
            vbid: true,
            managerId: true,
          },
        },
      },
    }),
    prisma.team.findMany({
      select: { id: true, name: true, color: true },
      orderBy: { name: "asc" },
    }),
  ]);

  const data = buildHierarchy(profiles, teams);
  const json = `${JSON.stringify(data, null, 2)}\n`;
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  const temporaryPath = `${outputPath}.tmp-${process.pid}`;
  fs.writeFileSync(temporaryPath, json, { encoding: "utf8", flag: "wx" });
  fs.renameSync(temporaryPath, outputPath);

  const memberCount = data.hierarchy.reduce(
    (total, head) => total + head.teams.reduce((sum, team) => {
      const count = (people) => people.reduce(
        (n, person) => n + 1 + count(person.subLeads || []) + count(person.members || []),
        0,
      );
      return sum + count(team.leads || []);
    }, 0),
    data.hierarchy.length,
  );
  const digest = crypto.createHash("sha256").update(json).digest("hex");
  console.log(`Hierarchy snapshot written: ${outputPath}`);
  console.log(`Exported people: ${memberCount}`);
  console.log(`Exported teams: ${data.hierarchy.reduce((n, head) => n + head.teams.length, 0)}`);
  console.log(`SHA-256: ${digest}`);
  console.log("Copy this snapshot off the VPS; it is a hierarchy snapshot, not a full database backup.");
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main()
    .catch((error) => {
      console.error(error.message || error);
      process.exitCode = 1;
    })
    .finally(() => prisma.$disconnect());
}
