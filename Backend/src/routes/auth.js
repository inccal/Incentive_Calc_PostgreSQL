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
    manager: profile?.manager
      ? {
          id: profile.manager.id,
          name: profile.manager.name,
          email: profile.manager.email,
        }
      : null,
    level: profile?.level,
    yearlyTarget: null,
  };
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
        employeeProfile: { include: { team: true, manager: true } },
      },
    });
  }

  if (!user) {
    user = await prisma.user.findFirst({
      where: { email: { equals: entraUser.email, mode: "insensitive" } },
      include: {
        employeeProfile: { include: { team: true, manager: true } },
      },
    });
    if (user && !user.entraObjectId && entraUser.objectId) {
      user = await prisma.user.update({
        where: { id: user.id },
        data: { entraObjectId: entraUser.objectId, email: entraUser.email },
        include: {
          employeeProfile: { include: { team: true, manager: true } },
        },
      });
    }
  }

  return user;
}

router.get("/entra/login", (req, res) => {
  const tenantId =
    process.env.AZURE_TENANT_ID ||
    process.env.ENTRA_TENANT_ID ||
    process.env.MICROSOFT_TENANT_ID;
  const clientId =
    process.env.AZURE_CLIENT_ID ||
    process.env.ENTRA_CLIENT_ID ||
    process.env.MICROSOFT_CLIENT_ID;
  const redirectUri =
    process.env.AZURE_REDIRECT_URI ||
    process.env.ENTRA_REDIRECT_URI ||
    process.env.MICROSOFT_REDIRECT_URI;

  if (!tenantId || !clientId || !redirectUri) {
    return res
      .status(500)
      .json({ error: "Microsoft Entra ID is not configured on the server" });
  }

  const state = Buffer.from(
    JSON.stringify({ ts: Date.now(), nonce: Math.random().toString(36).slice(2) })
  ).toString("base64url");
  res.cookie("entraState", state, {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/api/auth/entra/callback",
    maxAge: 10 * 60 * 1000,
  });

  const params = new URLSearchParams({
    client_id: clientId,
    response_type: "code",
    redirect_uri: redirectUri,
    response_mode: "query",
    scope: "openid profile email",
    prompt: "select_account",
    state,
  });

  const authorizeUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/authorize?${params.toString()}`;
  return res.redirect(authorizeUrl);
});

router.get("/entra/callback", async (req, res, next) => {
  const { ipAddress, userAgent } = getClientMeta(req);

  try {
    const { code, state } = req.query || {};
    if (!code || typeof code !== "string") {
      return res.status(400).json({ error: "Authorization code is missing" });
    }
    if (!state || typeof state !== "string" || state !== req.cookies?.entraState) {
      return res.status(400).json({ error: "Invalid login state. Please try again." });
    }
    res.clearCookie("entraState", { path: "/api/auth/entra/callback" });

    const tenantId =
      process.env.AZURE_TENANT_ID ||
      process.env.ENTRA_TENANT_ID ||
      process.env.MICROSOFT_TENANT_ID;
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

    if (!tenantId || !clientId || !clientSecret || !redirectUri) {
      return res
        .status(500)
        .json({ error: "Microsoft Entra ID is not configured on the server" });
    }

    const tokenEndpoint = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`;
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
