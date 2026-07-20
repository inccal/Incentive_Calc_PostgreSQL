import { Role } from "../generated/client/index.js";
import prisma from "../prisma.js";
import {
  assertDirectReportsRemainValid,
  resolveHierarchyManager,
  roleForHierarchyLevel,
} from "../services/teamHierarchyService.js";

const COMMENT_MAX_LENGTH = 1000;
const ENTRA_ONLY_PASSWORD_HASH = "MICROSOFT_ENTRA_ID_ONLY";

/** Human-readable user fields for audit logs (names only, no internal IDs). */
function userAuditSnapshot(user) {
  const profile = user?.employeeProfile;
  return {
    name: user?.name ?? null,
    email: user?.email ?? null,
    role: user?.role ?? null,
    isActive: user?.isActive ?? null,
    level: profile?.level ?? null,
    team: profile?.team?.name ?? null,
    manager: profile?.manager?.name ?? null,
    vbid: profile?.vbid ?? user?.vbid ?? null,
    comment: profile?.comment ?? null,
    targetType: profile?.targetType ?? null,
  };
}

/** Enforce L1 → L2 → L3 → L4 reporting (Team Lead cannot report to Senior Recruiter). */
async function assertValidManagerAssignment({ managerId, level, role }) {
  if (!managerId) return;
  const lvl = String(level || "").trim().toUpperCase();
  const manager = await prisma.user.findUnique({
    where: { id: managerId },
    include: { employeeProfile: { select: { level: true } } },
  });
  if (!manager) {
    const err = new Error("Manager not found");
    err.statusCode = 400;
    throw err;
  }
  const mgrLvl = String(manager.employeeProfile?.level || "").trim().toUpperCase();
  const isL2 = lvl === "L2" || (!lvl && role === Role.TEAM_LEAD);
  if (isL2 && manager.role !== Role.SUPER_ADMIN) {
    const err = new Error(
      "Team Lead (L2) can only report to a Head (Super Admin), not to Senior Recruiter or Recruiter."
    );
    err.statusCode = 400;
    throw err;
  }
  if (lvl === "L3" && mgrLvl !== "L2") {
    const err = new Error("Senior Recruiter (L3) must report to a Team Lead (L2).");
    err.statusCode = 400;
    throw err;
  }
  if (lvl === "L4" || (role === Role.EMPLOYEE && !lvl)) {
    if (mgrLvl !== "L2" && mgrLvl !== "L3") {
      const err = new Error("Recruiter (L4) must report to an L2 or L3 manager.");
      err.statusCode = 400;
      throw err;
    }
  }
}

// const prisma = new PrismaClient();

export async function listUsersWithRelations({ page = 1, pageSize = 25, actor, role }) {
  const skip = (page - 1) * pageSize;
  
  let targetRoles = [Role.TEAM_LEAD, Role.EMPLOYEE, Role.LIMITED_ACCESS];
  if (actor && actor.role === Role.S1_ADMIN) {
    targetRoles.push(Role.SUPER_ADMIN);
  }

  if (role) {
      if (targetRoles.includes(role)) {
          targetRoles = [role];
      } else {
          // If requested role is not allowed, return empty
          targetRoles = [];
      }
  }
  
  const where = { 
    role: { in: targetRoles },
  };

  if (actor && actor.role === Role.SUPER_ADMIN) {
    const ownedTeams = await prisma.team.findMany({
      where: {
        isActive: true,
        OR: [
          { headId: actor.id },
          // Legacy fallback until every installation has run the migration.
          { employees: { some: { deletedAt: null, user: { managerId: actor.id } } } },
        ],
      },
      select: { id: true },
    });
    where.employeeProfile = {
      teamId: { in: ownedTeams.map((team) => team.id) },
      isActive: true,
      deletedAt: null,
    };
  }

  const [total, users] = await Promise.all([
    prisma.user.count({ where }),
    prisma.user.findMany({
      where,
      include: {
        manager: true,
        employeeProfile: {
          include: { team: true, manager: true },
        },
      },
      orderBy: { name: "asc" },
      skip,
      take: pageSize,
    }),
  ]);

  const data = users.map((u) => {
    const manager = u.employeeProfile?.manager || u.manager || null;
    return ({
    id: u.id,
    email: u.email,
    name: u.name,
    role: u.role,
    isActive: u.isActive,
    vbid: u.vbid || u.employeeProfile?.vbid || null,
    level: u.employeeProfile?.level || null,
    team: u.employeeProfile?.team
      ? {
          id: u.employeeProfile.team.id,
          name: u.employeeProfile.team.name,
        }
      : null,
    managerId: u.employeeProfile?.managerId || u.managerId || null,
    teamId: u.employeeProfile?.teamId || null,
    manager: manager
      ? {
          id: manager.id,
          name: manager.name,
        }
      : null,
    yearlyTarget: null,
    targetType: u.employeeProfile?.targetType || "REVENUE",
    });
  });

  return {
    data,
    pagination: {
      page,
      pageSize,
      total,
      totalPages: Math.ceil(total / pageSize),
    },
  };
}

export async function getUserById(id) {
  const user = await prisma.user.findUnique({
    where: { id },
    include: {
      manager: true,
      employeeProfile: {
        include: { team: true, manager: true },
      },
    },
  });

  if (!user) {
    const error = new Error("User not found");
    error.statusCode = 404;
    throw error;
  }

  const manager = user.employeeProfile?.manager || user.manager || null;
  const managerId = user.employeeProfile?.managerId || user.managerId || null;
  return {
    id: user.id,
    email: user.email,
    name: user.name,
    role: user.role,
    isActive: user.isActive,
    vbid: user.vbid,
    managerId,
    manager: manager ? { id: manager.id, name: manager.name } : null,
    employeeProfile: user.employeeProfile
      ? { ...user.employeeProfile, managerId, manager }
      : null,
  };
}

export async function createUserWithProfile(payload, actorId) {
  const {
    email,
    name,
    role,
    teamId,
    managerId,
    level,
    targetType,
    vbid,
  } = payload;

  if (!email || !name || !role) {
    const error = new Error("Missing required fields");
    error.statusCode = 400;
    throw error;
  }

  // Require vbid for ALL roles
  if (!vbid || !vbid.trim()) {
    const error = new Error("VB ID is required");
    error.statusCode = 400;
    throw error;
  }

  if (![Role.S1_ADMIN, Role.SUPER_ADMIN, Role.TEAM_LEAD, Role.LIMITED_ACCESS, Role.EMPLOYEE].includes(role)) {
    const error = new Error("Invalid role");
    error.statusCode = 400;
    throw error;
  }

  // Validate vbid uniqueness - check both User.vbid and EmployeeProfile.vbid
  const trimmedVbid = vbid.trim();
  const existingUser = await prisma.user.findFirst({
    where: { vbid: trimmedVbid },
  });
  if (existingUser) {
    const error = new Error("VB ID already in use");
    error.statusCode = 409;
    throw error;
  }
  
  const existingProfile = await prisma.employeeProfile.findUnique({
    where: { vbid: trimmedVbid },
  });
  if (existingProfile) {
    const error = new Error("VB ID already in use");
    error.statusCode = 409;
    throw error;
  }

  const fallbackLevel = role === Role.TEAM_LEAD ? "L2" : role === Role.EMPLOYEE ? "L4" : "";
  const normalizedLevel = String(level || fallbackLevel).trim().toUpperCase();
  const resolvedRole = [Role.S1_ADMIN, Role.SUPER_ADMIN].includes(role)
    ? role
    : roleForHierarchyLevel(normalizedLevel, role);
  const resolvedManagerId = [Role.S1_ADMIN, Role.SUPER_ADMIN].includes(resolvedRole)
    ? null
    : await resolveHierarchyManager({
        teamId: teamId || null,
        level: normalizedLevel,
        requestedManagerId: managerId || null,
      });

  await assertValidManagerAssignment({ managerId: resolvedManagerId, level, role: resolvedRole });

  try {
    const user = await prisma.user.create({
      data: {
        email: email.toLowerCase(),
        passwordHash: ENTRA_ONLY_PASSWORD_HASH,
        name,
        role: resolvedRole,
        managerId: resolvedManagerId,
        vbid: trimmedVbid, // vbid stored on User for all roles (including SUPER_ADMIN and S1_ADMIN)
        employeeProfile:
          resolvedRole === Role.S1_ADMIN || resolvedRole === Role.SUPER_ADMIN
            ? undefined
            : {
                create: {
                  teamId: teamId || null,
                  managerId: resolvedManagerId,
                  level: normalizedLevel || null,
                  targetType: targetType || "REVENUE",
                  vbid: trimmedVbid, // Canonical vbid on EmployeeProfile for non-admin roles
                },
              },
      },
      include: {
        employeeProfile: {
          include: { team: true, manager: true },
        },
      },
    });

    await prisma.auditLog.create({
      data: {
        actorId,
        action: "USER_CREATED",
        entityType: "User",
        entityId: user.id,
        changes: userAuditSnapshot(user),
      },
    });

    return user;
  } catch (err) {
    if (err.code === "P2002") {
      if (err.meta?.target?.includes("vbid")) {
        const error = new Error("VB ID already in use");
        error.statusCode = 409;
        throw error;
      }
      const error = new Error("Email already in use");
      error.statusCode = 409;
      throw error;
    }
    throw err;
  }
}

export async function updateUserWithProfile(id, body, actor) {
  const {
    email,
    name,
    role,
    teamId: rawTeamId,
    managerId: rawManagerId,
    level: rawLevel,
    vbid,
    targetType,
    isActive,
    comment,
  } = body;

  const teamId = rawTeamId === "" ? null : rawTeamId;
  const managerId = rawManagerId === "" ? null : rawManagerId;
  const level = rawLevel === "" ? null : rawLevel;
  if (comment !== undefined && comment !== null && String(comment).length > COMMENT_MAX_LENGTH) {
    const err = new Error(`Comment must be at most ${COMMENT_MAX_LENGTH} characters`);
    err.statusCode = 400;
    throw err;
  }
  if (actor.role !== Role.SUPER_ADMIN && actor.role !== Role.S1_ADMIN) {
    if (
      role ||
      rawTeamId !== undefined ||
      rawManagerId !== undefined ||
      rawLevel !== undefined ||
      targetType !== undefined ||
      typeof isActive === "boolean" ||
      comment !== undefined
    ) {
      const error = new Error("Unauthorized to change sensitive fields");
      error.statusCode = 403;
      throw error;
    }
  }

  const user = await prisma.user.findUnique({
    where: { id },
    include: {
      employeeProfile: {
        include: { team: true, manager: true },
      },
    },
  });
  if (!user) {
    const error = new Error("User not found");
    error.statusCode = 404;
    throw error;
  }

  const data = {};
  if (email) data.email = email.toLowerCase();
  if (name) data.name = name;
  if (role) {
    if (actor.role === Role.SUPER_ADMIN) {
      data.role = role;
    } else if (
      actor.role === Role.S1_ADMIN &&
      user.role !== Role.SUPER_ADMIN &&
      user.role !== Role.S1_ADMIN &&
      [Role.TEAM_LEAD, Role.EMPLOYEE, Role.LIMITED_ACCESS].includes(role)
    ) {
      data.role = role;
    }
  }
  if (typeof isActive === "boolean") {
    if (actor.role === Role.SUPER_ADMIN) {
      data.isActive = isActive;
    } else if (
      actor.role === Role.S1_ADMIN &&
      user.role !== Role.S1_ADMIN
    ) {
      // S1 can toggle active/inactive for all managed roles including heads.
      data.isActive = isActive;
    }
  }
  let employeeProfileUpdate = undefined;

  if (actor.role === Role.SUPER_ADMIN && comment !== undefined) {
    const error = new Error("Only S1_ADMIN can update or create comments");
    error.statusCode = 403;
    throw error;
  }

  if (actor.role === Role.SUPER_ADMIN || actor.role === Role.S1_ADMIN) {
    // Validate vbid uniqueness if being updated - check both User.vbid and EmployeeProfile.vbid
    let finalVbid = vbid !== undefined ? (vbid === "" ? null : vbid.trim()) : (user.employeeProfile?.vbid ?? user.vbid ?? null);
    
    // Require vbid if not already set on the user
    if (!finalVbid || !finalVbid.trim()) {
      // Check if user already has a vbid
      const existingVbid = user.vbid || user.employeeProfile?.vbid;
      if (!existingVbid) {
        const error = new Error("VB ID is required");
        error.statusCode = 400;
        throw error;
      }
      // Use existing vbid if not being updated
      finalVbid = existingVbid;
    } else {
      const trimmedVbid = finalVbid.trim();
      
      // Check User.vbid uniqueness
      const existingUser = await prisma.user.findFirst({
        where: {
          vbid: trimmedVbid,
          id: { not: user.id },
        },
      });
      if (existingUser) {
        const error = new Error("VB ID already in use");
        error.statusCode = 409;
        throw error;
      }
      
      // Check EmployeeProfile.vbid uniqueness
      const existingProfile = await prisma.employeeProfile.findFirst({
        where: {
          vbid: trimmedVbid,
          id: { not: user.id },
        },
      });
      if (existingProfile) {
        const error = new Error("VB ID already in use");
        error.statusCode = 409;
        throw error;
      }
      
      finalVbid = trimmedVbid;
    }

    const requestedTargetRole = role ?? user.role;
    const storedOrRequestedLevel =
      level !== undefined ? level : (user.employeeProfile?.level ?? null);
    const targetLevel = storedOrRequestedLevel || (
      requestedTargetRole === Role.TEAM_LEAD
        ? "L2"
        : requestedTargetRole === Role.EMPLOYEE
          ? "L4"
          : null
    );
    const targetTeamId =
      teamId !== undefined ? teamId : (user.employeeProfile?.teamId ?? null);
    const normalizedTargetLevel = String(targetLevel || "").trim().toUpperCase();
    const targetRole = [Role.S1_ADMIN, Role.SUPER_ADMIN].includes(requestedTargetRole)
      ? requestedTargetRole
      : roleForHierarchyLevel(normalizedTargetLevel, requestedTargetRole);
    const requestedManagerId =
      managerId !== undefined ? managerId : (user.employeeProfile?.managerId ?? user.managerId ?? null);
    const resolvedManagerId = [Role.S1_ADMIN, Role.SUPER_ADMIN].includes(targetRole)
      ? null
      : await resolveHierarchyManager({
          teamId: targetTeamId,
          level: normalizedTargetLevel,
          requestedManagerId,
          excludeUserId: user.id,
        });

    if (
      user.employeeProfile &&
      (targetTeamId !== user.employeeProfile.teamId ||
        normalizedTargetLevel !== String(user.employeeProfile.level || "").trim().toUpperCase())
    ) {
      await assertDirectReportsRemainValid({
        userId: user.id,
        currentTeamId: user.employeeProfile.teamId,
        targetTeamId,
        targetLevel: normalizedTargetLevel,
      });
    }

    await assertValidManagerAssignment({
      managerId: resolvedManagerId,
      level: targetLevel,
      role: targetRole,
    });

    // Level is canonical: L2/L3 are Team Leads and L4 is an Employee.
    data.role = targetRole;

    employeeProfileUpdate =
      targetRole === Role.SUPER_ADMIN || targetRole === Role.S1_ADMIN
        ? user.employeeProfile
          ? {
              update: {
                isActive:
                  typeof isActive === "boolean"
                    ? isActive
                    : user.employeeProfile.isActive,
                deletedAt:
                  isActive === false
                    ? new Date()
                    : user.employeeProfile.deletedAt,
              },
            }
          : undefined
        : {
            upsert: {
              create: {
                teamId: targetTeamId,
                managerId: resolvedManagerId || null,
                level: normalizedTargetLevel || null,
                vbid: finalVbid,
                targetType: targetType || "REVENUE",
                comment: user.employeeProfile?.comment ?? null,
                isActive:
                  typeof isActive === "boolean"
                    ? isActive
                    : user.employeeProfile?.isActive ?? true,
                deletedAt:
                  isActive === false
                    ? new Date()
                    : user.employeeProfile?.deletedAt || null,
              },
              update: {
                teamId: targetTeamId,
                managerId: resolvedManagerId,
                level: normalizedTargetLevel || null,
                vbid: finalVbid,
                targetType: targetType !== undefined ? targetType : (user.employeeProfile?.targetType ?? "REVENUE"),
                comment: user.employeeProfile?.comment ?? null,
                isActive:
                  typeof isActive === "boolean"
                    ? isActive
                    : user.employeeProfile?.isActive ?? true,
                deletedAt:
                  isActive === false
                    ? new Date()
                    : user.employeeProfile?.deletedAt || null,
              },
            },
          };
    
    // Sync vbid to User model (denormalized)
    if (vbid !== undefined) {
      data.vbid = finalVbid;
    }

    // Keep User.managerId in sync with profile (used by hierarchy queries)
    data.managerId = resolvedManagerId;
  }

  if (actor.role === Role.S1_ADMIN && user.employeeProfile && comment !== undefined) {
    const commentValue = comment === "" ? null : comment;
    if (employeeProfileUpdate?.upsert?.update) {
      employeeProfileUpdate.upsert.update.comment = commentValue;
    } else if (!employeeProfileUpdate) {
      employeeProfileUpdate = {
        update: {
          comment: commentValue,
        },
      };
    }
  }

  const before = userAuditSnapshot(user);

  const updated = await prisma.user.update({
    where: { id },
    data: {
      ...data,
      employeeProfile: employeeProfileUpdate,
    },
    include: {
      employeeProfile: {
        include: { team: true, manager: true },
      },
    },
  });

  await prisma.auditLog.create({
    data: {
      actorId: actor.id,
      action: "USER_UPDATED",
      entityType: "User",
      entityId: updated.id,
      changes: {
        before,
        after: userAuditSnapshot(updated),
      },
    },
  });

  return updated;
}

/**
 * Bulk update comment on employee profiles. Only S1_ADMIN; no restriction on which users.
 */
export async function updateBulkComment(actorId, { userIds, comment }) {
  if (!Array.isArray(userIds) || userIds.length === 0) {
    const error = new Error("userIds must be a non-empty array");
    error.statusCode = 400;
    throw error;
  }

  const rawComment = comment == null ? "" : String(comment);
  const commentValue = rawComment.trim() === "" ? null : rawComment.trim();
  if (commentValue !== null && commentValue.length > COMMENT_MAX_LENGTH) {
    const error = new Error(`Comment must be at most ${COMMENT_MAX_LENGTH} characters`);
    error.statusCode = 400;
    throw error;
  }

  const actor = await prisma.user.findUnique({
    where: { id: actorId },
    include: { employeeProfile: true },
  });
  if (!actor || actor.role !== Role.S1_ADMIN) {
    const error = new Error("Forbidden: only S1_ADMIN can bulk-update comments");
    error.statusCode = 403;
    throw error;
  }

  const allowedUserIds = userIds;

  if (allowedUserIds.length === 0) {
    const error = new Error(
      "None of the selected members could be updated. You may not have permission to update their comments."
    );
    error.statusCode = 403;
    throw error;
  }

  await prisma.employeeProfile.updateMany({
    where: { id: { in: allowedUserIds } },
    data: { comment: commentValue },
  });

  return { updated: allowedUserIds.length, userIds: allowedUserIds };
}

export async function softDeleteUser(id, actorId) {
  // Check if actor has permission (get actor details first)
  const actor = await prisma.user.findUnique({ where: { id: actorId }, include: { employeeProfile: true } });
  
  if (actor.role === Role.SUPER_ADMIN) {
      // Perform same scope check as update
      const targetUser = await prisma.user.findUnique({
          where: { id },
          include: { employeeProfile: true }
      });
      
      if (!targetUser) return null; // Or throw not found

      const actorTeamId = actor.employeeProfile?.teamId;
      let hasAccess = false;
      
      if (actorTeamId) {
          if (targetUser.employeeProfile?.teamId === actorTeamId) hasAccess = true;
      } else {
           const hierarchyCheck = await prisma.user.findFirst({
            where: {
              id,
              OR: [
                { managerId: actor.id },
                { manager: { managerId: actor.id } },
                { manager: { manager: { managerId: actor.id } } },
              ],
            },
          });
          if (hierarchyCheck) hasAccess = true;
      }
      
      if (!hasAccess) {
          const error = new Error("Forbidden: Access denied to this user");
          error.statusCode = 403;
          throw error;
      }
  }

  const profile = await prisma.employeeProfile.findUnique({
    where: { id },
  });

  if (profile) {
    await prisma.employeeProfile.update({
      where: { id },
      data: {
        isActive: false,
        deletedAt: new Date(),
      },
    });
  }

  const user = await prisma.user.update({
    where: { id },
    data: {
      isActive: false,
    },
  });

  await prisma.refreshToken.updateMany({
    where: { userId: id },
    data: { isRevoked: true },
  });

  await prisma.auditLog.create({
    data: {
      actorId,
      action: "USER_DELETED",
      entityType: "User",
      entityId: id,
      changes: {
        isActive: false,
      },
    },
  });

  return user;
}
