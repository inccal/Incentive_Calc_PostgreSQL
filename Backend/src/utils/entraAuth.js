import { createRemoteJWKSet, jwtVerify, decodeJwt } from "jose";

const clientId =
  process.env.AZURE_CLIENT_ID ||
  process.env.ENTRA_CLIENT_ID ||
  process.env.MICROSOFT_CLIENT_ID;

// ─── Multi-tenant JWKS ────────────────────────────────────────────────────────
// The 'common' endpoint returns signing keys valid for ALL Azure AD tenants.
// This allows users from any organizational directory to authenticate.
const multiTenantJwks = createRemoteJWKSet(
  new URL("https://login.microsoftonline.com/common/discovery/v2.0/keys")
);

// Optional: allowlist of tenant IDs that are permitted to log in.
// Leave empty (or unset AZURE_ALLOWED_TENANT_IDS) to allow ALL tenants.
// Set to a comma-separated list in .env to restrict to specific tenants:
//   AZURE_ALLOWED_TENANT_IDS=tenant-id-1,tenant-id-2
function getAllowedTenantIds() {
  const raw = process.env.AZURE_ALLOWED_TENANT_IDS || "";
  return raw
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);
}

export async function verifyEntraIdToken(idToken) {
  if (!clientId) {
    const error = new Error("Microsoft Entra ID is not configured on the server");
    error.statusCode = 500;
    throw error;
  }

  // Decode first (without verifying) to read the tenant ID from the token.
  // This is safe because jwtVerify below will cryptographically verify everything.
  let unverifiedPayload;
  try {
    unverifiedPayload = decodeJwt(idToken);
  } catch {
    const error = new Error("Malformed Microsoft ID token");
    error.statusCode = 401;
    throw error;
  }

  const tokenTenantId = unverifiedPayload.tid;
  if (!tokenTenantId) {
    const error = new Error("Microsoft ID token is missing tenant information");
    error.statusCode = 401;
    throw error;
  }

  // ── Optional tenant allowlist check ─────────────────────────────────────────
  const allowedTenants = getAllowedTenantIds();
  if (allowedTenants.length > 0 && !allowedTenants.includes(tokenTenantId)) {
    const error = new Error(
      "Your Microsoft account belongs to an organization that is not authorized to access this application."
    );
    error.statusCode = 403;
    throw error;
  }

  // ── Cryptographic verification ───────────────────────────────────────────────
  // The issuer is tenant-specific — we build it dynamically from the token's tid.
  const tenantIssuer = `https://login.microsoftonline.com/${tokenTenantId}/v2.0`;

  const { payload } = await jwtVerify(idToken, multiTenantJwks, {
    issuer: tenantIssuer,
    audience: clientId,
  });

  const email =
    payload.preferred_username ||
    payload.email ||
    payload.upn ||
    payload.unique_name;

  if (!email) {
    const error = new Error("Microsoft account did not provide an email address");
    error.statusCode = 401;
    throw error;
  }

  return {
    email: String(email).toLowerCase(),
    name: payload.name || email,
    objectId: payload.oid || payload.sub,
    tenantId: payload.tid,
  };
}
