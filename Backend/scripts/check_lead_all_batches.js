// Checks whether stale rows from OLDER import batches are still sitting in the DB
// alongside the latest (correct) import, causing the overview page's "pick first
// non-null value" logic to surface an outdated summary number instead of the
// most recent one.
//
// Usage:
//   node scripts/check_lead_all_batches.js <leadOrEmployeeId> [--team|--personal]
//
// Defaults to --team (TeamPlacement). Pass --personal to check PersonalPlacement instead.

import prisma from "../src/prisma.js";

async function main() {
  const id = process.argv[2];
  const mode = process.argv.includes("--personal") ? "personal" : "team";

  if (!id) {
    console.log("Usage: node scripts/check_lead_all_batches.js <leadOrEmployeeId> [--team|--personal]");
    return;
  }

  const rows = mode === "team"
    ? await prisma.teamPlacement.findMany({
        where: { leadId: id },
        select: { id: true, batchId: true, createdAt: true, plcId: true, candidateName: true, placementDone: true, placementAchPercent: true },
        orderBy: { createdAt: "asc" },
      })
    : await prisma.personalPlacement.findMany({
        where: { employeeId: id },
        select: { id: true, batchId: true, createdAt: true, plcId: true, candidateName: true, achieved: true, targetAchievedPercent: true },
        orderBy: { createdAt: "asc" },
      });

  console.log(`Total ${mode} rows for id=${id} across ALL batches ever imported: ${rows.length}`);

  const doneField = mode === "team" ? "placementDone" : "achieved";
  const byBatch = new Map();
  rows.forEach((r) => {
    const key = r.batchId || "(no batch)";
    if (!byBatch.has(key)) byBatch.set(key, []);
    byBatch.get(key).push(r);
  });

  for (const [batchId, rs] of byBatch) {
    const doneVals = [...new Set(rs.map((r) => (r[doneField] == null ? "NULL" : r[doneField].toString())))];
    console.log(`  batch ${batchId} | ${rs.length} rows | first createdAt ${rs[0].createdAt.toISOString()} | "${doneField}" values seen: ${doneVals.join(", ")}`);
  }

  if (byBatch.size > 1) {
    console.log(`\n>>> WARNING: rows from ${byBatch.size} different import batches exist for this id. Older batches were not cleaned up when re-imported, which can cause stale summary values to surface on overview pages that pick "any row with a non-null value" without preferring the most recent batch.`);
  }
}

main()
  .catch((e) => console.error("Failed:", e))
  .finally(() => prisma.$disconnect());
