import { createContext, useContext, useEffect, useState } from "react";
import { apiRequest, clearAuthStorage, API_BASE_URL } from "../api/client";

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let mounted = true;

    const initializeSession = async () => {
      // Microsoft login establishes the authoritative session in HTTP-only
      // cookies. A token left in localStorage by a previous account/refresh
      // must not override a newly issued cookie on a shared browser.
      clearAuthStorage();

      const stored = localStorage.getItem("user");
      if (stored) {
        try {
          if (mounted) setUser(JSON.parse(stored));
        } catch {
          if (mounted) setUser(null);
        }
      }

      try {
        const response = await apiRequest("/auth/me", { method: "GET" }, {
          skipAuth: false,
          suppressSessionExpiredEvent: true,
        });
        if (response.ok) {
          const data = await response.json();
          if (mounted && data?.user) {
            setUser(data.user);
            localStorage.setItem("user", JSON.stringify(data.user));
          }
        } else if (response.status === 401 || response.status === 403) {
          clearAuthStorage();
          localStorage.removeItem("user");
          if (mounted) setUser(null);
        }
      } catch {
        // Ignore bootstrap errors; user remains logged out.
      } finally {
        if (mounted) setLoading(false);
      }
    };

    initializeSession();

    const handleSessionExpired = () => {
      clearAuthStorage();
      setUser(null);
      localStorage.removeItem("user");
      alert("Session expired. Please login again.");
    };

    window.addEventListener("auth:session-expired", handleSessionExpired);

    return () => {
      mounted = false;
      window.removeEventListener("auth:session-expired", handleSessionExpired);
    };
  }, []);

  const loginWithMicrosoft = async () => {
    const response = await apiRequest("/auth/entra/authorize-url", { method: "GET" }, { skipAuth: true });
    if (!response.ok) {
      const data = await response.json().catch(() => ({}));
      throw new Error(data.error || "Failed to initialize Microsoft login");
    }

    const data = await response.json();
    if (!data?.authorizeUrl) {
      throw new Error("Microsoft authorization URL is missing");
    }
    window.location.assign(data.authorizeUrl);
  };

  const login = async () => {
    const stored = localStorage.getItem("user");
    if (stored) return JSON.parse(stored);
    throw new Error("Password login is disabled. Use Microsoft Entra ID.");
  };

  const logout = async () => {
    // End the visible session immediately. Server-side token revocation is
    // still attempted, but a slow network must not trap the user on the page.
    clearAuthStorage();
    setUser(null);
    localStorage.removeItem("user");
    try {
      await apiRequest(
        "/auth/logout",
        {
          method: "POST",
        },
        { skipAuth: true }
      );
    } catch {}
  };

  const value = {
    user,
    loading,
    login,
    loginWithMicrosoft,
    logout,
    API_BASE_URL,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  return useContext(AuthContext);
}
