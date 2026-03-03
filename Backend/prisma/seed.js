/**
 * Seed database from hierarchy_data.json.
 * Creates: S1 Admin, then all Users, Teams, and EmployeeProfiles from the hierarchy.
 * Default password for all users: 123456
 * Run: npm run prisma:seed (or node prisma/seed.js)
 */

import prismaPkg from "../src/generated/client/index.js";
const { PrismaClient, Role } = prismaPkg;
import bcrypt from "bcryptjs";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const prisma = new PrismaClient();
const DEFAULT_PASSWORD = "123456";

function loadHierarchy() {
  const filePath = path.join(__dirname, "../hierarchy_data.json");
  if (!fs.existsSync(filePath)) {
    throw new Error("hierarchy_data.json not found at Backend/hierarchy_data.json");
  }
  const raw = fs.readFileSync(filePath, "utf-8");
  const data = JSON.parse(raw);
  if (!data.hierarchy || !Array.isArray(data.hierarchy)) {
    throw new Error("hierarchy_data.json must have a 'hierarchy' array");
  }
  return data.hierarchy;
}

function safeVbid(vbid) {
  if (vbid == null || String(vbid).trim() === "") return null;
  return String(vbid).trim();
}

async function ensureUser(db, { email, name, passwordHash, role, vbid, managerId }) {
  const emailNorm = email.trim().toLowerCase();
  let user = await db.user.findUnique({ where: { email: emailNorm } });
  if (user) {
    const updateData = { name: name.trim(), role, vbid: vbid ?? null };
    if (managerId !== undefined) updateData.managerId = managerId;
    await db.user.update({ where: { id: user.id }, data: updateData });
    return user;
  }
  user = await db.user.create({
    data: {
      email: emailNorm,
      name: name.trim(),
      passwordHash,
      role,
      vbid: vbid ?? null,
      managerId: managerId ?? null,
    },
  });
  return user;
}

async function ensureProfile(db, { userId, teamId, managerId, level, vbid }) {
  const data = {
    teamId: teamId ?? null,
    managerId: managerId ?? null,
    level: level || null,
    vbid: vbid ?? null,
    targetType: "REVENUE",
  };
  const existing = await db.employeeProfile.findUnique({
    where: { id: userId },
  });
  if (existing) {
    try {
      await db.employeeProfile.update({
        where: { id: userId },
        data,
      });
    } catch (err) {
      if (err.code === "P2002") {
        await db.employeeProfile.update({
          where: { id: userId },
          data: { ...data, vbid: null },
        });
      } else {
        throw err;
      }
    }
    return;
  }
  try {
    await db.employeeProfile.create({
      data: {
        id: userId,
        teamId: data.teamId,
        managerId: data.managerId,
        level: data.level,
        vbid: null,
        targetType: data.targetType,
      },
    });
  } catch (err) {
    if (err.code === "P2002") {
      try {
        await db.employeeProfile.update({
          where: { id: userId },
          data: { ...data, vbid: data.vbid || null },
        });
      } catch (updateErr) {
        if (updateErr.code === "P2025") {
          const now = new Date();
          try {
            await db.$executeRaw`
              INSERT INTO dbo.EmployeeProfile (id, teamId, managerId, level, vbid, targetType, isActive, createdAt, updatedAt)
              VALUES (${userId}, ${data.teamId}, ${data.managerId}, ${data.level}, ${data.vbid}, ${data.targetType}, 1, ${now}, ${now})
            `;
          } catch (insertErr) {
            try {
              await db.employeeProfile.update({
                where: { id: userId },
                data: { ...data, vbid: data.vbid || null },
              });
            } catch (_) {}
          }
        } else {
          throw updateErr;
        }
      }
      return;
    }
    throw err;
  }
  if (data.vbid) {
    try {
      await db.employeeProfile.update({
        where: { id: userId },
        data: { vbid: data.vbid },
      });
    } catch (err) {
      if (err.code !== "P2002") throw err;
    }
  }
}

async function ensureTeam(db, name, color) {
  let team = await db.team.findUnique({ where: { name } });
  if (!team) {
    team = await db.team.create({
      data: { name, color: color || null, yearlyTarget: 0 },
    });
  }
  return team;
}

async function processMembers(db, members, teamId, managerId, passwordHash) {
  if (!members || !Array.isArray(members)) return;
  for (const m of members) {
    const vbid = safeVbid(m.vbid);
    const user = await ensureUser(db, {
      email: m.email,
      name: m.name,
      passwordHash,
      role: Role.EMPLOYEE,
      vbid,
      managerId,
    });
    await ensureProfile(db, {
      userId: user.id,
      teamId,
      managerId,
      level: m.level || "L4",
      vbid,
    });
  }
}

async function processSubLeads(db, subLeads, teamId, l2UserId, passwordHash) {
  if (!subLeads || !Array.isArray(subLeads)) return;
  for (const l3 of subLeads) {
    const vbid = safeVbid(l3.vbid);
    const l3User = await ensureUser(db, {
      email: l3.email,
      name: l3.name,
      passwordHash,
      role: Role.TEAM_LEAD,
      vbid,
      managerId: l2UserId,
    });
    await ensureProfile(db, {
      userId: l3User.id,
      teamId,
      managerId: l2UserId,
      level: l3.level || "L3",
      vbid,
    });
    if (l3.members) {
      await processMembers(db, l3.members, teamId, l3User.id, passwordHash);
    }
  }
}

async function processLeads(db, leads, teamId, l1UserId, passwordHash) {
  if (!leads || !Array.isArray(leads)) return;
  for (const l2 of leads) {
    const vbid = safeVbid(l2.vbid);
    const l2User = await ensureUser(db, {
      email: l2.email,
      name: l2.name,
      passwordHash,
      role: Role.TEAM_LEAD,
      vbid,
      managerId: l1UserId,
    });
    await ensureProfile(db, {
      userId: l2User.id,
      teamId,
      managerId: l1UserId,
      level: l2.level || "L2",
      vbid,
    });
    if (l2.subLeads) {
      await processSubLeads(db, l2.subLeads, teamId, l2User.id, passwordHash);
    }
    if (l2.members) {
      await processMembers(db, l2.members, teamId, l2User.id, passwordHash);
    }
  }
}

async function main() {
  const passwordHash = await bcrypt.hash(DEFAULT_PASSWORD, 10);
  const hierarchy = loadHierarchy();
  console.log(`Seeding from hierarchy_data.json (${hierarchy.length} top-level entries)...`);

  const db = prisma;
  const s1Email = "admin@vbeyond.com";
  if (!(await db.user.findUnique({ where: { email: s1Email } }))) {
    const s1 = await db.user.create({
      data: {
        email: s1Email,
        name: "S1 Admin",
        passwordHash,
        role: Role.S1_ADMIN,
      },
    });
    await db.employeeProfile.create({
      data: { id: s1.id, targetType: "REVENUE" },
    });
    console.log("Created S1 Admin:", s1Email);
  }

  for (const l1 of hierarchy) {
    const vbid = safeVbid(l1.vbid);
    const l1User = await ensureUser(db, {
      email: l1.email,
      name: l1.name,
      passwordHash,
      role: Role.SUPER_ADMIN,
      vbid,
      managerId: null,
    });
    await ensureProfile(db, {
      userId: l1User.id,
      teamId: null,
      managerId: null,
      level: l1.level || "L1",
      vbid,
    });

    if (l1.teams && Array.isArray(l1.teams)) {
      for (const t of l1.teams) {
        const team = await ensureTeam(db, t.name, t.color);
        if (t.leads) {
          await processLeads(db, t.leads, team.id, l1User.id, passwordHash);
        }
      }
    }
  }

  console.log("Seed completed. All users have password:", DEFAULT_PASSWORD);
}

main()
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });
