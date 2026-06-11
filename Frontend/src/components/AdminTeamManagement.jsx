import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext";
import { useTeams } from "../hooks/useTeams";
import { Skeleton } from "./common/Skeleton";
import CalculationService from "../utils/calculationService";

/** URL-safe slug from team name (matches backend resolution). */
function teamSlug(team) {
  return (team?.name ?? "").toLowerCase().replace(/\s+/g, "-").replace(/[^a-z0-9-]/g, "") || team?.id;
}

const AdminTeamManagement = () => {
  const { user } = useAuth();
  const navigate = useNavigate();
  const { teams, isLoading, error, createTeam, deleteTeam, updateTeam } = useTeams();
  const canEditTeamMetrics = user?.role === "S1_ADMIN" || user?.role === "SUPER_ADMIN";
  
  // Create Team Modal
  const [showModal, setShowModal] = useState(false);
  const [formData, setFormData] = useState({
    name: "",
  });

  // Edit team target/achieved modal
  const [editTeam, setEditTeam] = useState(null);
  const [editForm, setEditForm] = useState({
    yearlyTarget: "",
    achievedValue: "",
    targetType: "PLACEMENTS",
  });

  const handleSubmit = (e) => {
    e.preventDefault();
    createTeam(formData, {
      onSuccess: () => {
        setShowModal(false);
        setFormData({ name: "" });
      },
      onError: (err) => {
        alert(err.message);
      }
    });
  };

  const handleDelete = (teamId) => {
    if (!window.confirm("Are you sure? This will fail if the team has active members.")) return;
    deleteTeam(teamId, {
      onError: (err) => {
        alert(err.message);
      }
    });
  };

  const openEditModal = (team) => {
    setEditTeam(team);
    setEditForm({
      yearlyTarget: String(team.yearlyTarget ?? 0),
      achievedValue: String(team.achievedValue ?? team.totalPlacements ?? team.totalRevenue ?? 0),
      targetType: team.targetType || "PLACEMENTS",
    });
  };

  const handleEditSubmit = (e) => {
    e.preventDefault();
    if (!editTeam) return;
    updateTeam(
      {
        teamId: editTeam.id,
        data: {
          yearlyTarget: Number(editForm.yearlyTarget) || 0,
          achievedValue: Number(editForm.achievedValue) || 0,
          targetType: editForm.targetType,
        },
      },
      {
        onSuccess: () => setEditTeam(null),
        onError: (err) => alert(err.message),
      }
    );
  };

  if (isLoading) {
    return (
      <div className="p-6 bg-slate-50 min-h-screen">
        <div className="max-w-7xl mx-auto">
          <div className="flex justify-between items-center mb-6">
             <div className="flex gap-4">
                <Skeleton className="h-8 w-8 rounded" />
                <Skeleton className="h-8 w-48 rounded" />
             </div>
             <Skeleton className="h-10 w-32 rounded-lg" />
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {[1, 2, 3, 4, 5, 6].map((i) => (
              <div key={i} className="bg-white rounded-xl shadow-sm border border-slate-100 p-6">
                <div className="flex justify-between items-start mb-4">
                   <div className="flex items-center gap-3">
                      <Skeleton className="h-12 w-12 rounded-lg" />
                      <div>
                         <Skeleton className="h-5 w-32 rounded mb-1" />
                         <Skeleton className="h-3 w-20 rounded" />
                      </div>
                   </div>
                   <Skeleton className="h-8 w-8 rounded-lg" />
                </div>
                <div className="space-y-2 mt-4">
                   <Skeleton className="h-4 w-full rounded" />
                   <Skeleton className="h-4 w-2/3 rounded" />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  if (error) {
     return <div className="p-8 text-center text-red-600">Error loading teams: {error.message}</div>;
  }

  return (
    <div className="p-6 bg-slate-50 min-h-screen">
      <div className="max-w-7xl mx-auto">
        <div className="flex justify-between items-center mb-6">
          <div className="flex items-center gap-4">
            <button
              onClick={() => navigate(user?.role === 'S1_ADMIN' ? '/admin/dashboard' : '/team')}
              className="text-slate-500 hover:text-slate-700 transition-colors"
            >
              <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
              </svg>
            </button>
            <div>
              <h2 className="text-2xl font-bold text-slate-800">Team Management</h2>
              <p className="text-slate-600">Create and manage teams</p>
            </div>
          </div>
          <div className="flex gap-3">
            <button
              onClick={() => navigate('/admin/incentive-slabs')}
              className="bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-2 rounded-lg shadow-sm transition-colors flex items-center gap-2"
            >
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 7h6m0 10v-3m-3 3h.01M9 17h.01M9 14h.01M12 14h.01M15 11h.01M12 11h.01M9 11h.01M7 21h10a2 2 0 002-2V5a2 2 0 00-2-2H7a2 2 0 00-2 2v14a2 2 0 002 2z" />
              </svg>
              Manage Incentive Slabs
            </button>
            <button
              onClick={() => setShowModal(true)}
              className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg shadow-sm transition-colors flex items-center gap-2"
            >
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
              </svg>
              Create New Team
            </button>
          </div>
        </div>

        {error && (
          <div className="bg-red-100 text-red-700 p-4 rounded-lg mb-6">
            {error}
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {teams.map((team) => (
            <div key={team.id} className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden hover:shadow-md transition-shadow">
              <div className="h-2 w-full bg-blue-500"></div>
              <div className="p-6">
                <div className="flex justify-between items-start mb-4">
                  <h3 className="text-xl font-bold text-slate-800">{team.name}</h3>
                  {canEditTeamMetrics && (
                    <button
                      type="button"
                      onClick={() => openEditModal(team)}
                      className="text-slate-400 hover:text-blue-600 p-1 rounded-lg hover:bg-blue-50 transition-colors"
                      title="Edit target & achieved"
                    >
                      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
                      </svg>
                    </button>
                  )}
                </div>
                
                <div className="space-y-3 mb-6">
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-500">Total Target</span>
                    <span className="font-medium text-slate-700">
                      {team.targetType === 'PLACEMENTS' 
                        ? `${team.yearlyTarget || 0} Placements` 
                        : CalculationService.formatCurrency(team.yearlyTarget)}
                    </span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-500">Achieved</span>
                    <span className="font-medium text-emerald-600">
                      {team.targetType === 'PLACEMENTS'
                        ? `${team.totalPlacements || 0} Placements`
                        : CalculationService.formatCurrency(team.totalRevenue)}
                    </span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-500">Leads</span>
                    <span className="font-medium text-slate-700">{team.leads?.length || 0}</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-500">Members</span>
                    <span className="font-medium text-slate-700">{team.members?.length || 0}</span>
                  </div>
                </div>

                <div className="pt-4 border-t border-slate-100 flex justify-between items-center">
                  <button
                    onClick={() => navigate(`/admin/teams/${teamSlug(team)}`)}
                    className="text-blue-600 hover:text-blue-800 text-sm font-medium flex items-center gap-1"
                  >
                    Manage Team
                    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                    </svg>
                  </button>
                  <button
                    onClick={() => handleDelete(team.id)}
                    className="text-red-600 hover:text-red-800 text-sm font-medium flex items-center gap-1"
                  >
                    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                    </svg>
                    Delete
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>

        {editTeam && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <div className="bg-white rounded-xl shadow-xl max-w-md w-full p-6 animate-fadeIn">
              <h2 className="text-xl font-bold text-slate-800 mb-1">Edit Team Metrics</h2>
              <p className="text-sm text-slate-500 mb-4">{editTeam.name}</p>
              <form onSubmit={handleEditSubmit} className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Target Type</label>
                  <select
                    value={editForm.targetType}
                    onChange={(e) => setEditForm({ ...editForm, targetType: e.target.value })}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
                  >
                    <option value="PLACEMENTS">Placement Based</option>
                    <option value="REVENUE">Revenue Based</option>
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Total Target</label>
                  <input
                    type="number"
                    min="0"
                    step="any"
                    required
                    value={editForm.yearlyTarget}
                    onChange={(e) => setEditForm({ ...editForm, yearlyTarget: e.target.value })}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Achieved</label>
                  <input
                    type="number"
                    min="0"
                    step="any"
                    required
                    value={editForm.achievedValue}
                    onChange={(e) => setEditForm({ ...editForm, achievedValue: e.target.value })}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
                  />
                </div>
                <div className="flex justify-end gap-3 mt-6">
                  <button
                    type="button"
                    onClick={() => setEditTeam(null)}
                    className="px-4 py-2 text-slate-600 hover:bg-slate-100 rounded-lg font-medium"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg font-medium shadow-sm"
                  >
                    Save
                  </button>
                </div>
              </form>
            </div>
          </div>
        )}

        {showModal && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <div className="bg-white rounded-xl shadow-xl max-w-md w-full p-6 animate-fadeIn">
              <h2 className="text-xl font-bold text-slate-800 mb-4">Create New Team</h2>
              <form onSubmit={handleSubmit} className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1">Team Name</label>
                  <input
                    type="text"
                    required
                    value={formData.name}
                    onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
                    placeholder="e.g. Titans"
                  />
                </div>

                <div className="flex justify-end gap-3 mt-6">
                  <button
                    type="button"
                    onClick={() => setShowModal(false)}
                    className="px-4 py-2 text-slate-600 hover:bg-slate-100 rounded-lg font-medium"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg font-medium shadow-sm"
                  >
                    Create Team
                  </button>
                </div>
              </form>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default AdminTeamManagement;
