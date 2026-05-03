import { PrismaClient } from "./generated/client/index.js";

/**
 * Reuse one client per process. Hot reload / duplicate module loads can otherwise
 * open extra pools and hit host limits (e.g. Neon session pooler: EMAXCONNSESSION).
 *
 * If you still see "max clients reached", lower Prisma's pool via DATABASE_URL, e.g.
 *   ...?connection_limit=5&pool_timeout=20
 * and close other clients (Prisma Studio, second dev server) using the same URL.
 */
const globalForPrisma = globalThis;
const prisma =
  globalForPrisma.__prismaClient ??
  new PrismaClient({
    log: ["error"],
  });
if (process.env.NODE_ENV !== "production") {
  globalForPrisma.__prismaClient = prisma;
}

export default prisma;
