import { createRemoteJWKSet, jwtVerify } from "jose";

const tenantId =
  process.env.AZURE_TENANT_ID ||
  process.env.ENTRA_TENANT_ID ||
  process.env.MICROSOFT_TENANT_ID;

const clientId =
  process.env.AZURE_CLIENT_ID ||
  process.env.ENTRA_CLIENT_ID ||
  process.env.MICROSOFT_CLIENT_ID;

const issuer = tenantId
  ? `https://login.microsoftonline.com/${tenantId}/v2.0`
  : null;

const jwks = tenantId
  ? createRemoteJWKSet(
      new URL(`https://login.microsoftonline.com/${tenantId}/discovery/v2.0/keys`)
    )
  : null;

export async function verifyEntraIdToken(idToken) {
  if (!tenantId || !clientId || !issuer || !jwks) {
    const error = new Error("Microsoft Entra ID is not configured on the server");
    error.statusCode = 500;
    throw error;
  }

  const { payload } = await jwtVerify(idToken, jwks, {
    issuer,
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
