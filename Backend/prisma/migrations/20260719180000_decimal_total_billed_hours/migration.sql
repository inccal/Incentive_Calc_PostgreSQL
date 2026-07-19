-- Excel source files contain fractional billed hours. Preserve them instead of
-- rejecting an entire import when PostgreSQL receives a non-integer value.
ALTER TABLE "PersonalPlacement"
  ALTER COLUMN "totalBilledHours" TYPE DECIMAL(14, 2)
  USING "totalBilledHours"::DECIMAL(14, 2);

ALTER TABLE "TeamPlacement"
  ALTER COLUMN "totalBilledHours" TYPE DECIMAL(14, 2)
  USING "totalBilledHours"::DECIMAL(14, 2);
