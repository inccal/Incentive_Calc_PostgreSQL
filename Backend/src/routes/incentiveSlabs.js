import express from "express";
import { Role } from "../generated/client/index.js";
import { authenticate, requireRole } from "../middleware/auth.js";
import {
  getMySlabs,
  getSlabForUser,
  getAllUsersForSlabAllocation,
  bulkAssignSlabs,
  updateSlabForUser,
  deleteSlabForUser,
  listTemplates,
  createTemplate,
  updateTemplate,
  deleteTemplate,
} from "../controllers/incentiveSlabController.js";

const router = express.Router();

router.use(authenticate);

// ========== USER-FACING ROUTES ==========

// Get logged-in user's own slab
router.get("/me", async (req, res, next) => {
  try {
    const slab = await getMySlabs(req.user.id);
    res.json(slab || { slabs: null });
  } catch (err) {
    next(err);
  }
});

// Get a specific user's slab (admin, super admin, or team lead viewing subordinate)
router.get(
  "/user/:userId",
  requireRole(Role.S1_ADMIN, Role.SUPER_ADMIN, Role.TEAM_LEAD, Role.EMPLOYEE),
  async (req, res, next) => {
    try {
      const slab = await getSlabForUser(req.params.userId);
      res.json(slab || { slabs: null });
    } catch (err) {
      if (err.statusCode) return res.status(err.statusCode).json({ error: err.message });
      next(err);
    }
  }
);

// ========== S1 ADMIN ONLY ROUTES ==========

// Get all users across org for the allocation page
router.get(
  "/org-users",
  requireRole(Role.S1_ADMIN),
  async (req, res, next) => {
    try {
      const users = await getAllUsersForSlabAllocation();
      res.json(users);
    } catch (err) {
      next(err);
    }
  }
);

// Bulk assign slabs to multiple users
router.post(
  "/bulk",
  requireRole(Role.S1_ADMIN),
  async (req, res, next) => {
    try {
      const { userIds, slabs } = req.body;
      const result = await bulkAssignSlabs(userIds, slabs, req.user.id);
      res.json(result);
    } catch (err) {
      if (err.statusCode) return res.status(err.statusCode).json({ error: err.message });
      next(err);
    }
  }
);

// Update a single user's slab
router.put(
  "/user/:userId",
  requireRole(Role.S1_ADMIN),
  async (req, res, next) => {
    try {
      const { slabs } = req.body;
      const result = await updateSlabForUser(req.params.userId, slabs, req.user.id);
      res.json(result);
    } catch (err) {
      if (err.statusCode) return res.status(err.statusCode).json({ error: err.message });
      next(err);
    }
  }
);

// Delete a user's slab configuration
router.delete(
  "/user/:userId",
  requireRole(Role.S1_ADMIN),
  async (req, res, next) => {
    try {
      await deleteSlabForUser(req.params.userId);
      res.status(204).send();
    } catch (err) {
      if (err.statusCode) return res.status(err.statusCode).json({ error: err.message });
      next(err);
    }
  }
);

// ========== TEMPLATE ROUTES (S1 ADMIN ONLY) ==========

router.get(
  "/templates",
  requireRole(Role.S1_ADMIN),
  async (req, res, next) => {
    try {
      const templates = await listTemplates();
      res.json(templates);
    } catch (err) {
      next(err);
    }
  }
);

router.post(
  "/templates",
  requireRole(Role.S1_ADMIN),
  async (req, res, next) => {
    try {
      const { name, slabs } = req.body;
      const template = await createTemplate(name, slabs);
      res.status(201).json(template);
    } catch (err) {
      if (err.statusCode) return res.status(err.statusCode).json({ error: err.message });
      next(err);
    }
  }
);

router.put(
  "/templates/:id",
  requireRole(Role.S1_ADMIN),
  async (req, res, next) => {
    try {
      const { name, slabs } = req.body;
      const template = await updateTemplate(req.params.id, name, slabs);
      res.json(template);
    } catch (err) {
      if (err.statusCode) return res.status(err.statusCode).json({ error: err.message });
      next(err);
    }
  }
);

router.delete(
  "/templates/:id",
  requireRole(Role.S1_ADMIN),
  async (req, res, next) => {
    try {
      await deleteTemplate(req.params.id);
      res.status(204).send();
    } catch (err) {
      if (err.statusCode) return res.status(err.statusCode).json({ error: err.message });
      next(err);
    }
  }
);

export default router;
