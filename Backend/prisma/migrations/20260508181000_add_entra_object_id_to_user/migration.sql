-- Add stable Microsoft Entra object mapping on users.
ALTER TABLE "User"
ADD COLUMN "entraObjectId" TEXT;

CREATE UNIQUE INDEX "User_entraObjectId_key"
ON "User"("entraObjectId");
