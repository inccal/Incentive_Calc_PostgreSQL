// Diagnostic: checks why imported "achieved" / "targetAchievedPercent" summary
// fields are null on placement rows for a given import batch, causing the
// dashboard to fall back to counting placement rows instead of reading the
// value stored from the sheet.
//
// Usage:
//   node scripts/diagnose_summary_import.js <batchId>
//   node scripts/diagnose_summary_import.js            (checks the most recent batch of each type)

import prisma from "../src/prisma.js";

async function inspectBatch(batch) {
  console.log(`\n=== Batch ${batch.id} (${batch.type}, uploaded ${batch.createdAt.toISOString()}) ===`);

  const errors = batch.errors;
  if (Array.isArray(errors) && errors.length > 0) {
    console.log(`Row-level errors recorded during import (${errors.length}):`);
    errors.slice(0, 20).forEach((e) => console.log(`  row ${e.rowIndex}: ${e.message}`));
    if (errors.length > 20) console.log(`  ...and ${errors.length - 20} more`);
  } else {
    console.log("No row-level errors recorded for this batch.");
  }

  const isPersonal = batch.type === "PERSONAL";
  const rows = isPersonal
    ? await prisma.personalPlacement.findMany({
        where: { batchId: batch.id },
        select: { employeeId: true, recruiterName: true, vbCode: true, achieved: true, targetAchievedPercent: true, yearlyTarget: true },
      })
    : await prisma.teamPlacement.findMany({
        where: { batchId: batch.id },
        select: { leadId: true, recruiterName: true, vbCode: true, placementDone: true, placementAchPercent: true, yearlyPlacementTarget: true },
      });

  const doneField = isPersonal ? "achieved" : "placementDone";
  const nullCount = rows.filter((r) => r[doneField] == null).length;
  console.log(`Rows in batch: ${rows.length}. Rows with "${doneField}" NULL: ${nullCount}`);

  if (nullCount > 0) {
    const byPerson = new Map();
    rows.forEach((r) => {
      const key = r.employeeId || r.leadId;
      if (!byPerson.has(key)) byPerson.set(key, { name: r.recruiterName, vbCode: r.vbCode, total: 0, nullDone: 0 });
      const entry = byPerson.get(key);
      entry.total += 1;
      if (r[doneField] == null) entry.nullDone += 1;
    });
    console.log("\nPer-employee breakdown (only those with at least one NULL):");
    for (const [id, info] of byPerson) {
      if (info.nullDone > 0) {
        console.log(`  employee ${id} (${info.name || "?"}, vb=${info.vbCode || "?"}): ${info.nullDone}/${info.total} rows missing "${doneField}"`);
      }
    }
  }
}

async function main() {
  const batchId = process.argv[2];

  if (batchId) {
    const batch = await prisma.placementImportBatch.findUnique({ where: { id: batchId } });
    if (!batch) {
      console.log(`No PlacementImportBatch found with id ${batchId}`);
      return;
    }
    await inspectBatch(batch);
  } else {
    const latestPersonal = await prisma.placementImportBatch.findFirst({
      where: { type: "PERSONAL" },
      orderBy: { createdAt: "desc" },
    });
    const latestTeam = await prisma.placementImportBatch.findFirst({
      where: { type: "TEAM" },
      orderBy: { createdAt: "desc" },
    });
    if (latestPersonal) await inspectBatch(latestPersonal);
    if (latestTeam) await inspectBatch(latestTeam);
    if (!latestPersonal && !latestTeam) console.log("No import batches found at all.");
  }

  // Sanity check: do Team table names look right?
  const teams = await prisma.team.findMany({ select: { name: true } });
  console.log(`\nTeams in DB (${teams.length}):`, teams.map((t) => t.name));
}

main()
  .catch((e) => console.error("Diagnostic failed:", e))
  .finally(() => prisma.$disconnect());
