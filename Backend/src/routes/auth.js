import crypto from "crypto";
import express from "express";
import prisma from "../prisma.js";
import {
  signAccessToken,
  signRefreshToken,
  verifyRefreshToken,
} from "../utils/jwt.js";
import { authenticate } from "../middleware/auth.js";
import { createAuditLog } from "../controllers/auditLogController.js";
import { verifyEntraIdToken } from "../utils/entraAuth.js";

const router = express.Router();

async function createRefreshToken(userId) {
  const tokenRecord = await prisma.refreshToken.create({
    data: {
      userId,
      token: "",
      expiresAt: new Date(
        Date.now() +
          (parseInt(process.env.JWT_REFRESH_TTL_DAYS || "30", 10) ||
            30) *
            24 *
            60 *
            60 *
            1000
      ),
    },
  });

  const token = signRefreshToken({ id: userId }, tokenRecord.id);

  await prisma.refreshToken.update({
    where: { id: tokenRecord.id },
    data: { token },
  });

  return token;
}

function getClientMeta(req) {
  return {
    ipAddress: req.ip || req.headers["x-forwarded-for"] || req.socket.remoteAddress,
    userAgent: req.headers["user-agent"],
  };
}

function buildAuthUser(user) {
  const profile = user.employeeProfile;
  const manager = profile?.manager || user.manager || null;

  return {
    id: user.id,
    email: user.email,
    name: user.name,
    role: user.role,
    isActive: user.isActive,
    mfaEnabled: false,
    team: profile?.team
      ? {
          id: profile.team.id,
          name: profile.team.name,
          color: profile.team.color,
        }
      : null,
    manager: manager
      ? {
          id: manager.id,
          name: manager.name,
          email: manager.email,
        }
      : null,
    level: profile?.level,
    yearlyTarget: null,
  };
}

const ENTRA_OAUTH_STATE_TTL_MS = 15 * 60 * 1000;

function entraOAuthStateSecret() {
  return (
    process.env.OAUTH_STATE_SECRET ||
    process.env.JWT_ACCESS_SECRET ||
    process.env.JWT_REFRESH_SECRET ||
    "dev-access-secret"
  );
}

/** Signed state carried in the OAuth URL (no cookie needed — avoids devices that drop cookies on return). */
function createEntraOAuthState() {
  const payload = Buffer.from(
    JSON.stringify({
      ts: Date.now(),
      n: crypto.randomBytes(12).toString("hex"),
    })
  ).toString("base64url");
  const sig = crypto
    .createHmac("sha256", entraOAuthStateSecret())
    .update(payload)
    .digest("base64url");
  return `${payload}.${sig}`;
}

function verifyEntraOAuthState(state) {
  if (!state || typeof state !== "string") return false;
  const dot = state.lastIndexOf(".");
  if (dot <= 0) return false;
  const payloadB64 = state.slice(0, dot);
  const sig = state.slice(dot + 1);
  const expectedSig = crypto
    .createHmac("sha256", entraOAuthStateSecret())
    .update(payloadB64)
    .digest("base64url");
  if (!sig || expectedSig.length !== sig.length) return false;
  try {
    if (!crypto.timingSafeEqual(Buffer.from(expectedSig, "utf8"), Buffer.from(sig, "utf8"))) {
      return false;
    }
  } catch {
    return false;
  }
  let data;
  try {
    data = JSON.parse(Buffer.from(payloadB64, "base64url").toString("utf8"));
  } catch {
    return false;
  }
  if (typeof data.ts !== "number") return false;
  if (Date.now() - data.ts > ENTRA_OAUTH_STATE_TTL_MS) return false;
  return true;
}

function buildEntraAuthorizeUrl({ clientId, redirectUri, state }) {
  const params = new URLSearchParams({
    client_id: clientId,
    response_type: "code",
    redirect_uri: redirectUri,
    response_mode: "query",
    scope: "openid profile email",
    prompt: "select_account",
    state,
  });

  // Use the 'organizations' endpoint so users from ANY Azure AD tenant can log in.
  // Change to 'common' if you also want to support personal Microsoft accounts.
  const authorizeUrl = new URL(
    "https://login.microsoftonline.com/organizations/oauth2/v2.0/authorize"
  );
  authorizeUrl.search = params.toString();
  return authorizeUrl.toString();
}

async function issueAppSession(res, user) {
  const accessToken = signAccessToken(user);
  const refreshToken = await createRefreshToken(user.id);
  const accessTtlMs = 24 * 60 * 60 * 1000;

  res.cookie("accessToken", accessToken, {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/",
    maxAge: accessTtlMs,
  });
  res.cookie("refreshToken", refreshToken, {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/api/auth",
    maxAge:
      (parseInt(process.env.JWT_REFRESH_TTL_DAYS || "30", 10) || 30) *
      24 *
      60 *
      60 *
      1000,
  });

  return {
    accessToken,
    user: buildAuthUser(user),
  };
}

async function resolveActiveUserFromEntra(entraUser) {
  let user = null;
  if (entraUser.objectId) {
    user = await prisma.user.findFirst({
      where: { entraObjectId: entraUser.objectId },
      include: {
        manager: true,
        employeeProfile: { include: { team: true, manager: true } },
      },
    });
  }

  if (!user) {
    user = await prisma.user.findFirst({
      where: { email: { equals: entraUser.email, mode: "insensitive" } },
      include: {
        manager: true,
        employeeProfile: { include: { team: true, manager: true } },
      },
    });
    if (user && !user.entraObjectId && entraUser.objectId) {
      user = await prisma.user.update({
        where: { id: user.id },
        data: { entraObjectId: entraUser.objectId, email: entraUser.email },
        include: {
          manager: true,
          employeeProfile: { include: { team: true, manager: true } },
        },
      });
    }
  }

  return user;
}

router.get("/entra/login", (req, res) => {
  const clientId =
    process.env.AZURE_CLIENT_ID ||
    process.env.ENTRA_CLIENT_ID ||
    process.env.MICROSOFT_CLIENT_ID;
  const redirectUri =
    process.env.AZURE_REDIRECT_URI ||
    process.env.ENTRA_REDIRECT_URI ||
    process.env.MICROSOFT_REDIRECT_URI;

  if (!clientId || !redirectUri) {
    return res
      .status(500)
      .json({ error: "Microsoft Entra ID is not configured on the server" });
  }

  const state = createEntraOAuthState();

  const redirectTarget = buildEntraAuthorizeUrl({ clientId, redirectUri, state });
  if (!redirectTarget.startsWith("https://login.microsoftonline.com/")) {
    return res.status(500).json({ error: "Invalid Microsoft Entra authorization URL configuration" });
  }

  return res.redirect(302, redirectTarget);
});

router.get("/entra/authorize-url", (req, res) => {
  const clientId =
    process.env.AZURE_CLIENT_ID ||
    process.env.ENTRA_CLIENT_ID ||
    process.env.MICROSOFT_CLIENT_ID;
  const redirectUri =
    process.env.AZURE_REDIRECT_URI ||
    process.env.ENTRA_REDIRECT_URI ||
    process.env.MICROSOFT_REDIRECT_URI;

  if (!clientId || !redirectUri) {
    return res
      .status(500)
      .json({ error: "Microsoft Entra ID is not configured on the server" });
  }

  const state = createEntraOAuthState();

  const authorizeUrl = buildEntraAuthorizeUrl({ clientId, redirectUri, state });
  if (!authorizeUrl.startsWith("https://login.microsoftonline.com/")) {
    return res.status(500).json({ error: "Invalid Microsoft Entra authorization URL configuration" });
  }

  return res.json({ authorizeUrl });
});

router.get("/entra/callback", async (req, res, next) => {
  const { ipAddress, userAgent } = getClientMeta(req);

  try {
    const { code, state, admin_consent, error, error_description } = req.query || {};

    // ── Admin consent redirect (not a login attempt) ─────────────────────────
    // When an admin from another tenant grants consent via the /adminconsent URL,
    // Azure redirects here with admin_consent=True — there is no `code`.
    if (admin_consent === "True") {
      const frontendUrl = (
        process.env.FRONTEND_URL || "https://inccalvbeyond.com"
      ).replace(/\/$/, "");
      return res.redirect(`${frontendUrl}/?admin_consent=success`);
    }

    // ── OAuth error returned by Azure (e.g. user cancelled login) ────────────
    if (error) {
      const frontendUrl = (
        process.env.FRONTEND_URL || "https://inccalvbeyond.com"
      ).replace(/\/$/, "");
      return res.redirect(
        `${frontendUrl}/?login=error&reason=${encodeURIComponent(error_description || error)}`
      );
    }

    if (!code || typeof code !== "string") {
      return res.status(400).json({ error: "Authorization code is missing" });
    }
    if (!verifyEntraOAuthState(state)) {
      return res.status(400).json({ error: "Invalid login state. Please try again." });
    }


    const clientId =
      process.env.AZURE_CLIENT_ID ||
      process.env.ENTRA_CLIENT_ID ||
      process.env.MICROSOFT_CLIENT_ID;
    const clientSecret =
      process.env.AZURE_CLIENT_SECRET ||
      process.env.ENTRA_CLIENT_SECRET ||
      process.env.MICROSOFT_CLIENT_SECRET;
    const redirectUri =
      process.env.AZURE_REDIRECT_URI ||
      process.env.ENTRA_REDIRECT_URI ||
      process.env.MICROSOFT_REDIRECT_URI;

    if (!clientId || !clientSecret || !redirectUri) {
      return res
        .status(500)
        .json({ error: "Microsoft Entra ID is not configured on the server" });
    }

    // Use the 'organizations' token endpoint for multi-tenant support.
    const tokenEndpoint = "https://login.microsoftonline.com/organizations/oauth2/v2.0/token";
    const tokenParams = new URLSearchParams({
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: "authorization_code",
      code,
      redirect_uri: redirectUri,
      scope: "openid profile email",
    });
    const tokenResponse = await fetch(tokenEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: tokenParams.toString(),
    });
    const tokenData = await tokenResponse.json().catch(() => ({}));
    if (!tokenResponse.ok || !tokenData.id_token) {
      console.error("[Entra Token Exchange Failed]", {
        status: tokenResponse.status,
        error: tokenData?.error,
        error_description: tokenData?.error_description,
        error_codes: tokenData?.error_codes,
      });
      const detail = tokenData?.error_description || tokenData?.error || "unknown_error";
      return res.status(401).json({
        error: "Failed to exchange Microsoft authorization code",
        ...(process.env.NODE_ENV !== "production" ? { detail } : {}),
      });
    }

    const entraUser = await verifyEntraIdToken(tokenData.id_token);
    const user = await resolveActiveUserFromEntra(entraUser);

    if (!user || !user.isActive) {
      await createAuditLog({
        actorId: null,
        action: "LOGIN_ATTEMPT",
        module: "AUTH",
        entityType: "User",
        entityId: entraUser.email,
        status: "FAILURE",
        ipAddress,
        userAgent,
        changes: {
          provider: "MICROSOFT_ENTRA_ID",
          reason: "User not found or inactive",
          email: entraUser.email,
        },
      });
      return res.status(401).json({
        error: "Your Microsoft account is not active in this application. Contact your admin.",
      });
    }

    await createAuditLog({
      actorId: user.id,
      action: "LOGIN_ATTEMPT",
      module: "AUTH",
      entityType: "User",
      entityId: user.id,
      status: "SUCCESS",
      ipAddress,
      userAgent,
      changes: {
        provider: "MICROSOFT_ENTRA_ID",
        email: user.email,
        entraObjectId: entraUser.objectId || null,
      },
    });

    const frontendUrl =
      (process.env.FRONTEND_URL || process.env.CLIENT_ORIGIN || "http://localhost:5173").replace(
        /\/$/,
        ""
      );
    await issueAppSession(res, user);
    return res.redirect(`${frontendUrl}/?login=success`);
  } catch (err) {
    next(err);
  }
});

router.post("/logout", async (req, res) => {
  const refreshToken = req.cookies.refreshToken;
  const cookieOptions = {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/api/auth",
  };
  if (refreshToken) {
    try {
      const payload = verifyRefreshToken(refreshToken);
      if (payload?.jti) {
        await prisma.refreshToken.updateMany({
          where: { id: payload.jti },
          data: { isRevoked: true },
        });
      }
    } catch {
      // Token invalid or expired; still clear cookie
    }
  }
  res.clearCookie("accessToken", {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/",
  });
  res.clearCookie("refreshToken", cookieOptions);
  res.json({ message: "Logged out" });
});

router.post("/refresh", async (req, res, next) => {
  try {
    const refreshToken = req.cookies.refreshToken;
    if (!refreshToken) return res.status(401).json({ error: "No refresh token" });

    let payload;
    try {
      payload = verifyRefreshToken(refreshToken);
    } catch (err) {
      return res.status(401).json({ error: "Session expired. Please login again." });
    }

    if (!payload || !payload.jti) {
      return res.status(401).json({ error: "Invalid refresh token payload" });
    }

    const tokenRecord = await prisma.refreshToken.findUnique({
      where: { id: payload.jti },
    });

    if (!tokenRecord || tokenRecord.token !== refreshToken) {
      return res.status(401).json({ error: "Invalid refresh token" });
    }

    if (tokenRecord.isRevoked) {
      return res.status(401).json({ error: "Session expired. Please login again." });
    }
    
    const user = await prisma.user.findUnique({
        where: { id: payload.sub },
    });

    if (!user || !user.isActive) {
        return res.status(401).json({ error: "User inactive or not found" });
    }

    const accessToken = signAccessToken(user);
    // Optionally rotate refresh token here
    res
      .cookie("accessToken", accessToken, {
        httpOnly: true,
        secure: process.env.NODE_ENV === "production",
        sameSite: "lax",
        path: "/",
        maxAge: 24 * 60 * 60 * 1000,
      })
      .json({ accessToken });
  } catch (err) {
    next(err);
  }
});

router.get("/me", authenticate, async (req, res, next) => {
  try {
    const user = await prisma.user.findUnique({
      where: { id: req.user.id },
      include: {
        manager: true,
        employeeProfile: {
          include: {
            team: true,
            manager: true,
          },
        },
      },
    });
    if (!user || !user.isActive) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    return res.json({ user: buildAuthUser(user) });
  } catch (err) {
    next(err);
  }
});

router.post("/login", (req, res) => {
  res.status(410).json({ error: "Password login is disabled. Use Microsoft Entra ID." });
});

router.post("/forgot-password", (req, res) => {
  res.status(410).json({
    error: "Password reset is disabled. Use Microsoft Entra ID to sign in.",
  });
});

router.post("/reset-password", (req, res) => {
  res.status(410).json({
    error: "Password reset is disabled. Use Microsoft Entra ID to sign in.",
  });
});

export default router;
