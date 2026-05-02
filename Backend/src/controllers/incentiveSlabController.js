import prisma from "../prisma.js";

/**
 * Validate slabs array structure
 */
function validateSlabs(slabs) {
  if (!Array.isArray(slabs) || slabs.length < 1 || slabs.length > 8) {
    throw Object.assign(new Error("Slabs must be an array of 1-8 entries"), { statusCode: 400 });
  }
  for (const s of slabs) {
    if (s.minPercent == null || s.incentivePercent == null) {
      throw Object.assign(new Error("Each slab must have minPercent and incentivePercent"), { statusCode: 400 });
    }
    if (typeof s.minPercent !== "number" || typeof s.incentivePercent !== "number") {
      throw Object.assign(new Error("minPercent and incentivePercent must be numbers"), { statusCode: 400 });
    }
    if (s.maxPercent != null && typeof s.maxPercent !== "number") {
      throw Object.assign(new Error("maxPercent must be a number or null"), { statusCode: 400 });
    }
  }
  return true;
}

/**
 * Get the logged-in user's own slab configuration.
 */
export async function getMySlabs(userId) {
  const slab = await prisma.incentiveSlab.findUnique({
    where: { userId },
  });
  return slab; // null if not configured
}

/**
 * Get a specific user's slab configuration.
 */
export async function getSlabForUser(userId) {
  const slab = await prisma.incentiveSlab.findUnique({
    where: { userId },
  });
  return slab;
}

/**
 * Get all users across the organisation with their slab status for the allocation page.
 * Returns: name, email, team, level, role, slab status, current slab data.
 */
export async function getAllUsersForSlabAllocation() {
  const users = await prisma.user.findMany({
    where: {
      isActive: true,
      role: { in: ["TEAM_LEAD", "EMPLOYEE"] },
    },
    include: {
      employeeProfile: {
        include: {
          team: { select: { id: true, name: true, color: true } },
        },
      },
      incentiveSlab: true,
    },
    orderBy: [{ name: "asc" }],
  });

  return users.map((u) => ({
    id: u.id,
    name: u.name,
    email: u.email,
    role: u.role,
    level: u.employeeProfile?.level || null,
    teamId: u.employeeProfile?.teamId || null,
    teamName: u.employeeProfile?.team?.name || null,
    teamColor: u.employeeProfile?.team?.color || null,
    hasSlabConfigured: !!u.incentiveSlab,
    slabs: u.incentiveSlab?.slabs || null,
    slabUpdatedAt: u.incentiveSlab?.updatedAt || null,
  }));
}

/**
 * Bulk assign the same slab configuration to multiple users.
 */
export async function bulkAssignSlabs(userIds, slabs, assignedById) {
  validateSlabs(slabs);

  if (!Array.isArray(userIds) || userIds.length === 0) {
    throw Object.assign(new Error("userIds must be a non-empty array"), { statusCode: 400 });
  }

  // Verify all users exist
  const existingUsers = await prisma.user.findMany({
    where: { id: { in: userIds }, isActive: true },
    select: { id: true },
  });
  const existingIds = new Set(existingUsers.map((u) => u.id));
  const missing = userIds.filter((id) => !existingIds.has(id));
  if (missing.length > 0) {
    throw Object.assign(new Error(`Users not found: ${missing.join(", ")}`), { statusCode: 404 });
  }

  // Upsert slabs for each user
  const results = await prisma.$transaction(
    userIds.map((userId) =>
      prisma.incentiveSlab.upsert({
        where: { userId },
        create: {
          userId,
          slabs,
          assignedById,
        },
        update: {
          slabs,
          assignedById,
        },
      })
    )
  );

  return { updated: results.length };
}

/**
 * Update a single user's slab configuration.
 */
export async function updateSlabForUser(userId, slabs, assignedById) {
  validateSlabs(slabs);

  const user = await prisma.user.findUnique({ where: { id: userId } });
  if (!user || !user.isActive) {
    throw Object.assign(new Error("User not found"), { statusCode: 404 });
  }

  const result = await prisma.incentiveSlab.upsert({
    where: { userId },
    create: { userId, slabs, assignedById },
    update: { slabs, assignedById },
  });

  return result;
}

/**
 * Delete a user's slab configuration.
 */
export async function deleteSlabForUser(userId) {
  const existing = await prisma.incentiveSlab.findUnique({ where: { userId } });
  if (!existing) {
    throw Object.assign(new Error("No slab configuration found for this user"), { statusCode: 404 });
  }
  await prisma.incentiveSlab.delete({ where: { userId } });
  return { deleted: true };
}

// ========== TEMPLATE FUNCTIONS ==========

/**
 * List all slab templates.
 */
export async function listTemplates() {
  return prisma.slabTemplate.findMany({
    orderBy: { name: "asc" },
  });
}

/**
 * Create a new slab template.
 */
export async function createTemplate(name, slabs) {
  validateSlabs(slabs);

  if (!name || typeof name !== "string" || name.trim().length === 0) {
    throw Object.assign(new Error("Template name is required"), { statusCode: 400 });
  }

  const existing = await prisma.slabTemplate.findUnique({ where: { name: name.trim() } });
  if (existing) {
    throw Object.assign(new Error("A template with this name already exists"), { statusCode: 409 });
  }

  return prisma.slabTemplate.create({
    data: { name: name.trim(), slabs },
  });
}

/**
 * Update a slab template.
 */
export async function updateTemplate(id, name, slabs) {
  validateSlabs(slabs);

  const existing = await prisma.slabTemplate.findUnique({ where: { id } });
  if (!existing) {
    throw Object.assign(new Error("Template not found"), { statusCode: 404 });
  }

  return prisma.slabTemplate.update({
    where: { id },
    data: {
      ...(name ? { name: name.trim() } : {}),
      slabs,
    },
  });
}

/**
 * Delete a slab template.
 */
export async function deleteTemplate(id) {
  const existing = await prisma.slabTemplate.findUnique({ where: { id } });
  if (!existing) {
    throw Object.assign(new Error("Template not found"), { statusCode: 404 });
  }
  await prisma.slabTemplate.delete({ where: { id } });
  return { deleted: true };
}
