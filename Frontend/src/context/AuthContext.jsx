import { createContext, useContext, useEffect, useState } from "react";
import { apiRequest, clearAuthStorage, API_BASE_URL } from "../api/client";

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let mounted = true;

    const initializeSession = async () => {
      const stored = localStorage.getItem("user");
      if (stored) {
        try {
          if (mounted) setUser(JSON.parse(stored));
        } catch {
          if (mounted) setUser(null);
        }
      }

      try {
        const response = await apiRequest("/auth/me", { method: "GET" }, { skipAuth: false });
        if (response.ok) {
          const data = await response.json();
          if (mounted && data?.user) {
            setUser(data.user);
            localStorage.setItem("user", JSON.stringify(data.user));
          }
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

  const loginWithMicrosoft = () => {
    window.location.href = `${API_BASE_URL}/auth/entra/login`;
  };

  const login = async () => {
    const stored = localStorage.getItem("user");
    if (stored) return JSON.parse(stored);
    throw new Error("Password login is disabled. Use Microsoft Entra ID.");
  };

  const logout = async () => {
    try {
      await apiRequest(
        "/auth/logout",
        {
          method: "POST",
        },
        { skipAuth: true }
      );
    } catch {}
    clearAuthStorage();
    setUser(null);
    localStorage.removeItem("user");
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
