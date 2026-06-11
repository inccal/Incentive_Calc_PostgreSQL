-- AlterTable: store manual team target/achieved (not calculated from members)
ALTER TABLE "Team" ADD COLUMN IF NOT EXISTS "achievedValue" DECIMAL(14,2) NOT NULL DEFAULT 0;
ALTER TABLE "Team" ADD COLUMN IF NOT EXISTS "targetType" "TargetType" NOT NULL DEFAULT 'PLACEMENTS';
