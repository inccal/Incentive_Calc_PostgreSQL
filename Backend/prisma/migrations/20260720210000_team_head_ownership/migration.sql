-- A team must retain its L1 owner even when L2 members move between teams.
ALTER TABLE "Team" ADD COLUMN "headId" TEXT;

-- Backfill from the most common valid L2 -> SUPER_ADMIN assignment in each
-- team. This also tolerates one stale L2 link such as a recently moved lead.
WITH owner_counts AS (
  SELECT
    ep."teamId",
    COALESCE(ep."managerId", u."managerId") AS "headId",
    COUNT(*) AS lead_count
  FROM "EmployeeProfile" ep
  JOIN "User" u ON u.id = ep.id
  JOIN "User" head ON head.id = COALESCE(ep."managerId", u."managerId")
  WHERE ep."teamId" IS NOT NULL
    AND UPPER(COALESCE(ep.level, '')) = 'L2'
    AND head.role = 'SUPER_ADMIN'
  GROUP BY ep."teamId", COALESCE(ep."managerId", u."managerId")
), ranked_owners AS (
  SELECT
    "teamId",
    "headId",
    ROW_NUMBER() OVER (PARTITION BY "teamId" ORDER BY lead_count DESC, "headId") AS rank
  FROM owner_counts
)
UPDATE "Team" team
SET "headId" = owner."headId"
FROM ranked_owners owner
WHERE owner."teamId" = team.id AND owner.rank = 1;

ALTER TABLE "Team"
  ADD CONSTRAINT "Team_headId_fkey"
  FOREIGN KEY ("headId") REFERENCES "User"(id)
  ON DELETE SET NULL ON UPDATE CASCADE;

CREATE INDEX "Team_headId_idx" ON "Team"("headId");
