-- Some legacy workbook rows legitimately have no DOJ. Preserve those rows and
-- render DOJ as blank instead of rejecting the complete placement import.
ALTER TABLE "PersonalPlacement"
  ALTER COLUMN "doj" DROP NOT NULL;

ALTER TABLE "TeamPlacement"
  ALTER COLUMN "doj" DROP NOT NULL;
