import { useState, useEffect } from "react";
import { apiRequest } from "../api/client";

const UserCreationModal = ({ isOpen, onClose, editingUser, onSuccess, teams = [], allMembers = [], defaultRole = "EMPLOYEE", useS1RoleLabels = false }) => {
  const [formData, setFormData] = useState({
    name: "",
    email: "",
    role: defaultRole,
    teamId: "",
    managerId: "",
    level: "",
    vbid: "",
    isActive: true,
  });

  const [error, setError] = useState("");

  // Level takes precedence over role (L3 users may have TEAM_LEAD role in DB)
  const getUiRole = (role, level) => {
    const lvl = (level || "").toUpperCase();
    if (lvl === "L3") return "SENIOR_RECRUITER";
    if (lvl === "L2" || role === "TEAM_LEAD") return "TEAM_LEAD";
    return "RECRUITER";
  };

  const getManagerOptions = (teamId, levelOverride = formData.level) => {
    if (!teamId) return [];
    const byId = new Map();
    const add = (person) => {
      const id = person.userId || person.id;
      if (!id || !person.name) return;
      if (editingUser && String(id) === String(editingUser.id)) return;
      byId.set(String(id), { userId: id, name: person.name });
    };

    const assigneeLevel = (levelOverride || "L4").toUpperCase();
    const team = teams.find((t) => String(t.id) === String(teamId));
    if (team) {
      if (assigneeLevel === "L2" && team.headId) {
        add({ userId: team.headId, name: team.headName || "Team Head" });
      }
      (team.leads || []).forEach((lead) => {
        const lvl = (lead.level || "L2").toUpperCase();
        if (assigneeLevel === "L3" && lvl === "L2") add(lead);
        else if (assigneeLevel === "L4" && (lvl === "L2" || lvl === "L3")) add(lead);
      });
      (team.members || []).forEach((m) => {
        const lvl = (m.level || "").toUpperCase();
        if (assigneeLevel === "L4" && (lvl === "L3" || lvl === "L2")) add(m);
        else if (assigneeLevel === "L3" && lvl === "L2") add(m);
      });
    }
    (allMembers || []).forEach((m) => {
      if (String(m.team?.id) !== String(teamId) && String(m.teamId) !== String(teamId)) return;
      const lvl = (m.level || "").toUpperCase();
      if (assigneeLevel === "L4") {
        if (lvl === "L3" || lvl === "L2" || m.role === "TEAM_LEAD") {
          add({ userId: m.id, name: m.name });
        }
      } else if (assigneeLevel === "L3") {
        if (m.role === "TEAM_LEAD" || lvl === "L2") add({ userId: m.id, name: m.name });
      }
    });

    return Array.from(byId.values()).sort((a, b) => a.name.localeCompare(b.name));
  };

  useEffect(() => {
    if (editingUser) {
      setFormData({
        name: editingUser.name,
        email: editingUser.email,
        role: editingUser.role,
        teamId: editingUser.teamId || editingUser.employeeProfile?.teamId || editingUser.team?.id || "",
        managerId: editingUser.managerId || editingUser.employeeProfile?.managerId || editingUser.manager?.id || "",
        level: editingUser.employeeProfile?.level || editingUser.level || "",
        vbid: editingUser.employeeProfile?.vbid || editingUser.vbid || "",
        isActive: editingUser.isActive !== false,
      });
    } else {
      // Determine default level based on defaultRole
      const defaultLevel = defaultRole === "TEAM_LEAD" ? "L2" : "L4";
      
      setFormData({
        name: "",
        email: "",
        role: defaultRole,
        teamId: teams.length === 1 ? teams[0].id : "",
        managerId: "",
        level: defaultLevel,
        vbid: "",
        isActive: true,
      });
    }
    setError("");
  }, [editingUser, isOpen, teams, defaultRole]);

  const handleUiRoleChange = (uiRole) => {
    let newRole = "EMPLOYEE";
    let newLevel = "L4";

    switch (uiRole) {
      case "TEAM_LEAD":
        newRole = "TEAM_LEAD";
        newLevel = "L2";
        break;
      case "SENIOR_RECRUITER":
        newRole = "TEAM_LEAD";
        newLevel = "L3";
        break;
      case "RECRUITER":
      default:
        newRole = "EMPLOYEE";
        newLevel = "L4";
        break;
    }

    setFormData(prev => ({
      ...prev,
      role: newRole,
      level: newLevel,
      managerId: newLevel === "L2"
        ? (teams.find((team) => String(team.id) === String(prev.teamId))?.headId || "")
        : prev.managerId,
    }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError("");
    
    try {
      const payload = { ...formData };
      const lvl = (payload.level || "").toUpperCase();
      if (payload.teamId && lvl !== "L2" && !payload.managerId) {
        throw new Error("Select a manager for this team before saving.");
      }
      const url = editingUser ? `/users/${editingUser.id}` : "/users";
      const method = editingUser ? "PUT" : "POST";
      const response = await apiRequest(url, {
        method,
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to save user");
      }

      onSuccess();
      onClose();
    } catch (err) {
      setError(err.message);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4 backdrop-blur-sm animate-fadeIn">
      <div className="bg-white rounded-2xl shadow-2xl max-w-md w-full p-6 transform transition-all scale-100">
        <h2 className="text-xl font-bold text-slate-800 mb-4 flex items-center gap-2">
          {editingUser ? (
            <>
              <svg className="w-6 h-6 text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
              </svg>
              Edit User
            </>
          ) : (
            <>
              <svg className="w-6 h-6 text-emerald-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M18 9v3m0 0v3m0-3h3m-3 0h-3m-2-5a4 4 0 11-8 0 4 4 0 018 0zM3 20a6 6 0 0112 0v1H3v-1z" />
              </svg>
              Create New Member/Lead
            </>
          )}
        </h2>
        {editingUser && (
          <div className="mb-4">
            <span
              className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium border ${
                formData.isActive
                  ? "bg-green-100 text-green-700 border-green-200"
                  : "bg-red-100 text-red-700 border-red-200"
              }`}
            >
              {formData.isActive ? "Active" : "Inactive"}
            </span>
          </div>
        )}
        
        {error && (
          <div className="mb-4 p-3 bg-red-50 border border-red-200 text-red-600 rounded-lg text-sm flex items-center gap-2">
            <svg className="w-4 h-4 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
              <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
            </svg>
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1 uppercase tracking-wide">Name</label>
            <input
              type="text"
              required
              value={formData.name}
              onChange={(e) => setFormData({ ...formData, name: e.target.value })}
              className="w-full px-3 py-2 border border-slate-300 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all"
              placeholder="Full Name"
            />
          </div>
          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1 uppercase tracking-wide">Email</label>
            <input
              type="email"
              required
              value={formData.email}
              onChange={(e) => setFormData({ ...formData, email: e.target.value })}
              className="w-full px-3 py-2 border border-slate-300 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all"
              placeholder="email@example.com"
            />
          </div>
          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1 uppercase tracking-wide">VB ID</label>
            <input
              type="text"
              required
              value={formData.vbid}
              onChange={(e) => setFormData({ ...formData, vbid: e.target.value.trim() })}
              className="w-full px-3 py-2 border border-slate-300 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all"
              placeholder="Enter VB ID"
            />
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs font-semibold text-slate-700 mb-1 uppercase tracking-wide">Role</label>
              <select
                value={getUiRole(formData.role, formData.level)}
                onChange={(e) => handleUiRoleChange(e.target.value)}
                className="w-full px-3 py-2 border border-slate-300 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all bg-white"
              >
                <option value="RECRUITER">{useS1RoleLabels ? "L4 - Recruiter" : "Recruiter"}</option>
                <option value="SENIOR_RECRUITER">{useS1RoleLabels ? "L3 - Senior Recruiter" : "Senior Recruiter"}</option>
                <option value="TEAM_LEAD">{useS1RoleLabels ? "L2 - Team Lead" : "Team Lead"}</option>
              </select>
            </div>
            <div>
              <label className="block text-xs font-semibold text-slate-700 mb-1 uppercase tracking-wide">Team</label>
              <select
                value={formData.teamId}
                onChange={(e) => {
                  const newTeamId = e.target.value;
                  const level = (formData.level || "L4").toUpperCase();
                  const managerOptions = getManagerOptions(newTeamId, level);
                  setFormData({ 
                    ...formData, 
                    teamId: newTeamId, 
                    managerId: level === "L2"
                      ? (teams.find((team) => String(team.id) === String(newTeamId))?.headId || "")
                      : (managerOptions.length === 1 ? managerOptions[0].userId : ""),
                  });
                }}
                className="w-full px-3 py-2 border border-slate-300 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all bg-white"
              >
                <option value="">No Team</option>
                {teams.map((team) => (
                  <option key={team.id} value={team.id}>
                    {team.name}
                  </option>
                ))}
              </select>
            </div>
            
            {formData.teamId && (
              <div>
                <label className="block text-xs font-semibold text-slate-700 mb-1 uppercase tracking-wide">
                  {(formData.level || "L4").toUpperCase() === "L2" ? "Head (Automatic)" : "Manager (Required)"}
                </label>
                <select
                  value={formData.managerId}
                  onChange={(e) => setFormData({ ...formData, managerId: e.target.value })}
                  disabled={(formData.level || "L4").toUpperCase() === "L2"}
                  className="w-full px-3 py-2 border border-slate-300 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all bg-white"
                >
                  <option value="">Select Manager</option>
                  {getManagerOptions(formData.teamId).map((mgr) => (
                      <option key={mgr.userId} value={mgr.userId}>
                        {mgr.name}
                      </option>
                    ))}
                </select>
              </div>
            )}
          </div>

          {editingUser && (
            <div>
              <label className="block text-xs font-semibold text-slate-700 mb-1 uppercase tracking-wide">Status</label>
              <select
                value={formData.isActive ? "ACTIVE" : "INACTIVE"}
                onChange={(e) => setFormData({ ...formData, isActive: e.target.value === "ACTIVE" })}
                className="w-full px-3 py-2 border border-slate-300 rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all bg-white"
              >
                <option value="ACTIVE">Active</option>
                <option value="INACTIVE">Inactive</option>
              </select>
            </div>
          )}
          
          <div className="flex justify-end gap-3 mt-6 pt-4 border-t border-slate-100">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-slate-600 hover:bg-slate-100 rounded-xl font-medium transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-6 py-2 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700 text-white rounded-xl font-medium shadow-lg shadow-blue-500/30 transition-all transform hover:scale-[1.02]"
            >
              {editingUser ? "Update User" : "Create Member"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};

export default UserCreationModal;
