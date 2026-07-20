import express from "express";
import { Role } from "../generated/client/index.js";
import prisma from "../prisma.js";
import { authenticate, requireRole } from "../middleware/auth.js";
import { cacheMiddleware, clearCacheMiddleware } from "../middleware/cache.js";
import {
  listUsersWithRelations,
  createUserWithProfile,
  updateUserWithProfile,
  updateBulkComment,
  softDeleteUser,
  getUserById,
} from "../controllers/userController.js";

const router = express.Router();
// const prisma = new PrismaClient();

router.use(authenticate);
router.use(clearCacheMiddleware);

router.get("/", requireRole(Role.SUPER_ADMIN, Role.S1_ADMIN), cacheMiddleware(60), async (req, res, next) => {
  try {
    const page = Number(req.query.page || 1);
    const pageSize = Number(req.query.pageSize || 25);
    const { role } = req.query;
    const result = await listUsersWithRelations({ page, pageSize, actor: req.user, role });
    res.json(result);
  } catch (err) {
    next(err);
  }
});

router.post("/", requireRole(Role.SUPER_ADMIN), async (req, res, next) => {
  try {
    const user = await createUserWithProfile(req.body, req.user.id);
    res.status(201).json({
      id: user.id,
      email: user.email,
      name: user.name,
      role: user.role,
    });
  } catch (err) {
    if (err.statusCode) {
      return res.status(err.statusCode).json({ error: err.message });
    }
    next(err);
  }
});

router.patch(
  "/bulk-comment",
  requireRole(Role.S1_ADMIN),
  async (req, res, next) => {
    try {
      const { userIds, comment } = req.body || {};
      const result = await updateBulkComment(req.user.id, { userIds: userIds || [], comment });
      res.json(result);
    } catch (err) {
      if (err.statusCode) {
        return res.status(err.statusCode).json({ error: err.message });
      }
      next(err);
    }
  }
);

router.get("/:id", requireRole(Role.SUPER_ADMIN, Role.S1_ADMIN), async (req, res, next) => {
  try {
    const { id } = req.params;
    const user = await getUserById(id);
    res.json(user);
  } catch (err) {
    next(err);
  }
});

router.put("/:id", async (req, res, next) => {
  try {
    const { id } = req.params;
    if (req.user.role !== Role.SUPER_ADMIN && req.user.role !== Role.S1_ADMIN && req.user.id !== id) {
      return res.status(403).json({ error: "Forbidden" });
    }

    const updated = await updateUserWithProfile(id, req.body, req.user);

    res.json({
      id: updated.id,
      email: updated.email,
      name: updated.name,
      role: updated.role,
      isActive: updated.isActive,
    });
  } catch (err) {
    if (err.statusCode) {
      return res.status(err.statusCode).json({ error: err.message });
    }
    next(err);
  }
});

router.patch(
  "/:id/deactivate",
  requireRole(Role.SUPER_ADMIN, Role.S1_ADMIN),
  async (req, res, next) => {
    try {
      const { id } = req.params;

      const user = await prisma.user.update({
        where: { id },
        data: {
          isActive: false,
          employeeProfile: {
            update: {
              isActive: false,
              deletedAt: new Date(),
            },
          },
        },
        include: { employeeProfile: true },
      });

      await prisma.refreshToken.updateMany({
        where: { userId: id, isRevoked: false },
        data: { isRevoked: true },
      });

      res.json({ id: user.id, isActive: user.isActive });
    } catch (err) {
      next(err);
    }
  }
);

async function hardDeleteUserById(id) {
  await prisma.$transaction(async (tx) => {
    const batchIds = (
      await tx.placementImportBatch.findMany({
        where: { uploaderId: id },
        select: { id: true },
      })
    ).map((b) => b.id);

    if (batchIds.length > 0) {
      await tx.personalPlacement.deleteMany({ where: { batchId: { in: batchIds } } });
      await tx.teamPlacement.deleteMany({ where: { batchId: { in: batchIds } } });
      await tx.placementImportBatch.deleteMany({ where: { id: { in: batchIds } } });
    }

    await tx.personalPlacement.deleteMany({ where: { employeeId: id } });
    await tx.teamPlacement.deleteMany({ where: { leadId: id } });
    await tx.refreshToken.deleteMany({ where: { userId: id } });
    await tx.incentiveSlab.deleteMany({ where: { userId: id } });
    await tx.auditLog.updateMany({ where: { actorId: id }, data: { actorId: null } });
    await tx.employeeProfile.updateMany({ where: { managerId: id }, data: { managerId: null } });
    await tx.user.updateMany({ where: { managerId: id }, data: { managerId: null } });
    await tx.employeeProfile.deleteMany({ where: { id } });
    await tx.user.delete({ where: { id } });
  });
}

router.delete(
  "/:id",
  requireRole(Role.SUPER_ADMIN, Role.S1_ADMIN),
  async (req, res, next) => {
    try {
      const { id } = req.params;
      const exists = await prisma.user.findUnique({ where: { id }, select: { id: true } });
      if (!exists) {
        return res.status(404).json({ error: "User not found" });
      }

      await hardDeleteUserById(id);
      res.status(204).send();
    } catch (err) {
      if (err.code === "P2025") {
        return res.status(404).json({ error: "User not found" });
      }
      if (err.code === "P2003") {
        return res.status(400).json({
          error:
            "Cannot delete user: they are still referenced elsewhere (e.g. another user’s manager). Reassign those links and try again.",
        });
      }
      next(err);
    }
  }
);

export default router;
