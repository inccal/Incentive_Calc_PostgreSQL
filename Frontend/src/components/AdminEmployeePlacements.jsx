import { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { apiRequest } from "../api/client";
import CalculationService from "../utils/calculationService";
import { formatPlacementDate } from "../utils/placementDates";

const AdminEmployeePlacements = () => {
  const { id: userId } = useParams();
  const navigate = useNavigate();
  const [placements, setPlacements] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [userName, setUserName] = useState("");

  // Modal states
  const [showModal, setShowModal] = useState(false);
  const [editingId, setEditingId] = useState(null); // If null, it's adding mode
  const [createSource, setCreateSource] = useState("personal");
  const [formBaseline, setFormBaseline] = useState(null);
  const [savingPlacement, setSavingPlacement] = useState(false);
  const [placementFormError, setPlacementFormError] = useState(null);
  const [pageMessage, setPageMessage] = useState(null);
  const [selectedIds, setSelectedIds] = useState([]);
  const [targetUser, setTargetUser] = useState(null);

  // Split by source so we show Personal (recruiter sheet) vs Team (lead sheet) clearly
  const personalPlacements = placements.filter((p) => p.source === "personal");
  const teamPlacements = placements.filter((p) => p.source === "team");
  const legacyPlacements = placements.filter((p) => !p.source || p.source === "legacy");
  const filteredPlacements = placements;

  // Show Team section only for team leads (L2/L3); hide for L4 and other roles
  const isTeamLead = targetUser?.role === "TEAM_LEAD";

  const initialFormState = {
    candidateName: "",
    candidateId: "",
    placementYear: new Date().getFullYear(),
    clientName: "",
    plcId: "",
    doi: "",
    doq: "",
    doj: "",
    placementType: "",
    billedHours: "",
    revenue: "",
    recruiterName: "",
    teamLead: "",
    placementSharing: "",
    totalRevenue: "",
    revenueAsLead: "",
    billingStatus: "PENDING",
    incentivePayoutEta: "",
    incentiveAmountInr: "",
    incentivePaidInr: "",
    placementBalanceIncentiveAmount: "",
  };

  const [formData, setFormData] = useState(initialFormState);

  const emptyPersonalSummaryDraft = () => ({
    yearlyTarget: "",
    achieved: "",
    targetAchievedPercent: "",
    totalRevenueGenerated: "",
    slabQualified: "",
    totalIncentiveInr: "",
    totalIncentivePaidInr: "",
    totalBalanceIncentiveAmount: "",
  });
  const emptyTeamSummaryDraft = () => ({
    yearlyPlacementTarget: "",
    placementDone: "",
    placementAchPercent: "",
    yearlyRevenueTarget: "",
    revenueAch: "",
    revenueTargetAchievedPercent: "",
    totalRevenueGenerated: "",
    slabQualified: "",
    totalIncentiveInr: "",
    totalIncentivePaidInr: "",
    totalBalanceIncentiveAmount: "",
  });

  const [personalSummaryDraft, setPersonalSummaryDraft] = useState(emptyPersonalSummaryDraft);
  const [teamSummaryDraft, setTeamSummaryDraft] = useState(emptyTeamSummaryDraft);
  const [personalSummaryBaseline, setPersonalSummaryBaseline] = useState(emptyPersonalSummaryDraft);
  const [teamSummaryBaseline, setTeamSummaryBaseline] = useState(emptyTeamSummaryDraft);
  const [personalSummaryError, setPersonalSummaryError] = useState(null);
  const [teamSummaryError, setTeamSummaryError] = useState(null);
  const [personalSummarySuccess, setPersonalSummarySuccess] = useState("");
  const [teamSummarySuccess, setTeamSummarySuccess] = useState("");
  const [savingPersonalSummary, setSavingPersonalSummary] = useState(false);
  const [savingTeamSummary, setSavingTeamSummary] = useState(false);
  const [summaryLoading, setSummaryLoading] = useState(false);

  const fmtSummaryVal = (v) => (v != null && v !== "" ? String(v) : "");
  const normalizeForDirty = (obj) =>
    JSON.stringify(
      Object.keys(obj || {})
        .sort()
        .reduce((acc, key) => {
          const value = obj[key];
          acc[key] = value == null ? "" : String(value).trim();
          return acc;
        }, {})
    );

  const isDirty = (draft, baseline) => normalizeForDirty(draft) !== normalizeForDirty(baseline);
  const personalSummaryDirty = isDirty(personalSummaryDraft, personalSummaryBaseline);
  const teamSummaryDirty = isDirty(teamSummaryDraft, teamSummaryBaseline);

  const loadSheetSummaries = async () => {
    if (!userId) return;
    setSummaryLoading(true);
    setPersonalSummaryError(null);
    setTeamSummaryError(null);
    setPersonalSummarySuccess("");
    setTeamSummarySuccess("");
    try {
      const pRes = await apiRequest(`/dashboard/personal-placements?userId=${userId}`);
      if (pRes.ok) {
        const data = await pRes.json();
        const s = data.summary || {};
        const nextPersonalSummary = {
          yearlyTarget: fmtSummaryVal(s.yearlyTarget),
          achieved: fmtSummaryVal(s.achieved),
          targetAchievedPercent: fmtSummaryVal(s.targetAchievedPercent),
          totalRevenueGenerated: fmtSummaryVal(s.totalRevenueGenerated),
          slabQualified: fmtSummaryVal(s.slabQualified),
          totalIncentiveInr: fmtSummaryVal(s.totalIncentiveInr),
          totalIncentivePaidInr: fmtSummaryVal(s.totalIncentivePaidInr),
          totalBalanceIncentiveAmount: fmtSummaryVal(s.totalBalanceIncentiveAmount),
        };
        setPersonalSummaryDraft(nextPersonalSummary);
        setPersonalSummaryBaseline(nextPersonalSummary);
      } else {
        const err = await pRes.json().catch(() => ({}));
        setPersonalSummaryError({
          error: err.error || "Could not load personal sheet summary",
          hint: err.hint || "Check network or permissions.",
        });
      }

      const role = targetUser?.role;
      if (role === "TEAM_LEAD") {
        const tRes = await apiRequest(`/dashboard/team-placements?leadId=${userId}`);
        if (tRes.ok) {
          const data = await tRes.json();
          const s = data.summary || {};
          const nextTeamSummary = {
            yearlyPlacementTarget: fmtSummaryVal(s.yearlyPlacementTarget),
            placementDone: fmtSummaryVal(s.placementDone),
            placementAchPercent: fmtSummaryVal(s.placementAchPercent),
            yearlyRevenueTarget: fmtSummaryVal(s.yearlyRevenueTarget),
            revenueAch: fmtSummaryVal(s.revenueAch),
            revenueTargetAchievedPercent: fmtSummaryVal(s.revenueTargetAchievedPercent),
            totalRevenueGenerated: fmtSummaryVal(s.totalRevenueGenerated),
            slabQualified: fmtSummaryVal(s.slabQualified),
            totalIncentiveInr: fmtSummaryVal(s.totalIncentiveInr),
            totalIncentivePaidInr: fmtSummaryVal(s.totalIncentivePaidInr),
            totalBalanceIncentiveAmount: fmtSummaryVal(s.totalBalanceIncentiveAmount),
          };
          setTeamSummaryDraft(nextTeamSummary);
          setTeamSummaryBaseline(nextTeamSummary);
        } else {
          const err = await tRes.json().catch(() => ({}));
          setTeamSummaryError({
            error: err.error || "Could not load team sheet summary",
            hint: err.hint || "Check network or permissions.",
          });
        }
      } else {
        const emptyTeamSummary = emptyTeamSummaryDraft();
        setTeamSummaryDraft(emptyTeamSummary);
        setTeamSummaryBaseline(emptyTeamSummary);
      }
    } catch (e) {
      setPersonalSummaryError({ error: e.message || "Failed to load summaries", hint: "" });
    } finally {
      setSummaryLoading(false);
    }
  };

  useEffect(() => {
    if (!userId) return;
    loadSheetSummaries();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- reload team block when role is known
  }, [userId, targetUser?.role]);

  const buildPersonalSummaryPayload = () => {
    const body = { userId };
    const keys = [
      "yearlyTarget",
      "achieved",
      "targetAchievedPercent",
      "totalRevenueGenerated",
      "slabQualified",
      "totalIncentiveInr",
      "totalIncentivePaidInr",
      "totalBalanceIncentiveAmount",
    ];
    for (const k of keys) {
      const raw = personalSummaryDraft[k];
      const t = raw == null ? "" : String(raw).trim();
      body[k] = t === "" ? null : t;
    }
    return body;
  };

  const buildTeamSummaryPayload = () => {
    const body = { leadId: userId };
    const keys = [
      "yearlyPlacementTarget",
      "placementDone",
      "placementAchPercent",
      "yearlyRevenueTarget",
      "revenueAch",
      "revenueTargetAchievedPercent",
      "totalRevenueGenerated",
      "slabQualified",
      "totalIncentiveInr",
      "totalIncentivePaidInr",
      "totalBalanceIncentiveAmount",
    ];
    for (const k of keys) {
      const raw = teamSummaryDraft[k];
      const t = raw == null ? "" : String(raw).trim();
      body[k] = t === "" ? null : t;
    }
    return body;
  };

  const handleSavePersonalSummary = async () => {
    if (!personalSummaryDirty || savingPersonalSummary || summaryLoading) return;
    setSavingPersonalSummary(true);
    setPersonalSummaryError(null);
    setPersonalSummarySuccess("");
    try {
      const res = await apiRequest("/placements/summary/personal", {
        method: "PATCH",
        body: JSON.stringify(buildPersonalSummaryPayload()),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setPersonalSummaryError({
          error: data.error || "Save failed",
          hint: data.hint || "",
          detail: data.detail,
        });
        return;
      }
      setPersonalSummaryBaseline({ ...personalSummaryDraft });
      setPersonalSummarySuccess("Personal summary saved.");
      fetchPlacements({ silent: true });
    } catch (e) {
      setPersonalSummaryError({ error: e.message || "Save failed", hint: "" });
    } finally {
      setSavingPersonalSummary(false);
    }
  };

  const handleSaveTeamSummary = async () => {
    if (!teamSummaryDirty || savingTeamSummary || summaryLoading) return;
    setSavingTeamSummary(true);
    setTeamSummaryError(null);
    setTeamSummarySuccess("");
    try {
      const res = await apiRequest("/placements/summary/team", {
        method: "PATCH",
        body: JSON.stringify(buildTeamSummaryPayload()),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setTeamSummaryError({
          error: data.error || "Save failed",
          hint: data.hint || "",
          detail: data.detail,
        });
        return;
      }
      setTeamSummaryBaseline({ ...teamSummaryDraft });
      setTeamSummarySuccess("Team summary saved.");
      fetchPlacements({ silent: true });
    } catch (e) {
      setTeamSummaryError({ error: e.message || "Save failed", hint: "" });
    } finally {
      setSavingTeamSummary(false);
    }
  };

  // Auto-calculate Days Completed - REMOVED as per request to delete field
  // useEffect(() => {
  //   if (formData.doj) {
  //     const diffDays = CalculationService.calculateDaysDifference(formData.doj);
  //     setFormData(prev => ({ ...prev, daysCompleted: diffDays }));
  //   }
  // }, [formData.doj]);

  const fetchPlacements = async (opts = {}) => {
    const silent = !!opts.silent;
    try {
      if (!silent) setLoading(true);
      const response = await apiRequest(`/placements/user/${userId}`);
      if (!response.ok) throw new Error("Failed to fetch placements");
      const data = await response.json();
      setPlacements(data);
    } catch (err) {
      setError(err.message);
    } finally {
      if (!silent) setLoading(false);
    }
  };

  useEffect(() => {
    fetchPlacements();
    // Fetch detailed user info
    const fetchUserDetails = async () => {
      try {
        const response = await apiRequest(`/users/${userId}`);
        if (response.ok) {
          const data = await response.json();
          setTargetUser(data);
          setUserName(data.name);
        }
      } catch (err) {
        console.error("Failed to fetch user details", err);
      }
    };
    if (userId) fetchUserDetails();
  }, [userId]);

  const handleEdit = (placement) => {
    const source = placement.source === "team" ? "team" : "personal";
    const nextFormData = {
      candidateName: placement.candidateName,
      candidateId: placement.candidateId || "",
      placementYear: placement.placementYear || new Date().getFullYear(),
      clientName: placement.clientName,
      plcId: placement.plcId || "",
      doq: placement.doq ? placement.doq.split('T')[0] : "",
      doj: placement.doj ? placement.doj.split('T')[0] : "",
      placementType: placement.placementType,
      billedHours: placement.billedHours || "",
      revenue: placement.revenue || "",
      recruiterName: placement.recruiterName || "",
      teamLead: placement.teamLead || "",
      placementSharing: placement.placementSharing || "",
      totalRevenue: placement.totalRevenue || "",
      revenueAsLead: placement.revenueAsLead || "",
      billingStatus: placement.billingStatus,
      collectionStatus: placement.collectionStatus || "",
      incentivePayoutEta: placement.incentivePayoutEta ? placement.incentivePayoutEta.split('T')[0] : "",
      incentiveAmountInr: placement.incentiveAmountInr || "",
      incentivePaidInr: (placement.incentivePaidInr !== undefined && placement.incentivePaidInr !== null) ? String(placement.incentivePaidInr) : "",
      placementBalanceIncentiveAmount: (placement.placementBalanceIncentiveAmount !== undefined && placement.placementBalanceIncentiveAmount !== null) ? String(placement.placementBalanceIncentiveAmount) : "",
    };
    setEditingId(placement.id);
    setCreateSource(source);
    setFormData(nextFormData);
    setFormBaseline(sanitizePlacementPayload(nextFormData, source));
    setPlacementFormError(null);
    setShowModal(true);
  };

  const handleAddNew = (source = "personal") => {
    setEditingId(null);
    setCreateSource(source);
    const nextFormData = {
      ...initialFormState,
      teamLead: source === "team" ? (userName || targetUser?.name || "") : "",
    };
    setFormData(nextFormData);
    setFormBaseline(sanitizePlacementPayload(nextFormData, source));
    setPlacementFormError(null);
    setShowModal(true);
  };

  const sanitizePlacementPayload = (data, source = createSource) => {
    const payload = { ...data };
    delete payload.candidateId;
    delete payload.doi;
    delete payload.totalRevenue;
    delete payload.revenueAsLead;
    delete payload.incentivePayoutEta;
    if (source === "personal") {
      delete payload.recruiterName;
      delete payload.placementSharing;
    }
    return payload;
  };

  const buildPlacementPayload = () => sanitizePlacementPayload(formData, createSource);
  const placementFormDirty =
    formBaseline != null && normalizeForDirty(buildPlacementPayload()) !== normalizeForDirty(formBaseline);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!placementFormDirty || savingPlacement) return;
    setSavingPlacement(true);
    setPlacementFormError(null);
    try {
      const url = editingId
        ? `/placements/${editingId}`
        : `/placements/user/${userId}/${createSource}`;
      
      const method = editingId ? "PUT" : "POST";

      const response = await apiRequest(url, {
        method,
        body: JSON.stringify(buildPlacementPayload()),
      });

      const data = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(data.error || "Failed to save placement");

      setShowModal(false);
      setFormData(initialFormState);
      setEditingId(null);
      setCreateSource("personal");
      setFormBaseline(null);
      setPageMessage({
        type: "success",
        text: editingId ? "Placement updated successfully." : "Placement created successfully.",
      });
      fetchPlacements();
    } catch (err) {
      setPlacementFormError(err.message || "Failed to save placement");
    } finally {
      setSavingPlacement(false);
    }
  };

  const handleBulkDelete = async () => {
    if (selectedIds.length === 0) return;
    if (!window.confirm(`Are you sure you want to delete ${selectedIds.length} placements?`)) return;

    try {
      const response = await apiRequest("/placements/bulk", {
        method: "DELETE",
        body: JSON.stringify({ placementIds: selectedIds }),
      });
      if (!response.ok) throw new Error("Failed to delete placements");
      
      setSelectedIds([]);
      fetchPlacements();
    } catch (err) {
      alert(err.message);
    }
  };

  const handleDelete = async (id) => {
    if (!window.confirm("Are you sure you want to delete this placement?")) return;
    try {
      await apiRequest(`/placements/${id}`, { method: "DELETE" });
      fetchPlacements();
    } catch (err) {
      alert(err.message);
    }
  };

  const handleSelectAll = (e) => {
    if (e.target.checked) {
      setSelectedIds(filteredPlacements.map(p => p.id));
    } else {
      setSelectedIds([]);
    }
  };

  const handleSelectAllInSection = (sectionPlacements) => {
    setSelectedIds((prev) => {
      const sectionIds = sectionPlacements.map((p) => p.id);
      const allSelected = sectionIds.length > 0 && sectionIds.every((id) => prev.includes(id));
      if (allSelected) return prev.filter((id) => !sectionIds.includes(id));
      return [...new Set([...prev, ...sectionIds])];
    });
  };

  const handleSelectOne = (id) => {
    setSelectedIds(prev => 
      prev.includes(id) ? prev.filter(pId => pId !== id) : [...prev, id]
    );
  };

  const memberVbid = (() => {
    const fromUser = targetUser?.vbid || targetUser?.employeeProfile?.vbid;
    if (fromUser != null && String(fromUser).trim() !== "") return String(fromUser).trim();
    const row = placements.find((p) => p.vbCode != null && String(p.vbCode).trim() !== "");
    return row ? String(row.vbCode).trim() : null;
  })();

  return (
    <div className="p-6 bg-slate-50 min-h-screen">
      <div className="max-w-[95%] mx-auto">
        <div className="flex items-center justify-between mb-8">
          <div className="flex items-center gap-4">
            <button
              onClick={() => navigate(-1)}
              className="text-slate-500 hover:text-slate-700 transition-colors"
            >
              <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
              </svg>
            </button>
            <h1 className="text-3xl font-bold text-slate-900">Placement Management</h1>
          </div>
          <div className="flex flex-wrap justify-end gap-3">
            {selectedIds.length > 0 && (
              <button
                onClick={handleBulkDelete}
                className="px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-lg shadow-sm transition-colors flex items-center gap-2 animate-fadeIn"
              >
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                </svg>
                Delete Selected ({selectedIds.length})
              </button>
            )}
            <button
              onClick={() => handleAddNew("personal")}
              className="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg shadow-sm transition-colors flex items-center gap-2"
            >
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
              </svg>
              Create Personal Placement
            </button>
            {isTeamLead && (
              <button
                onClick={() => handleAddNew("team")}
                className="px-4 py-2 bg-violet-600 hover:bg-violet-700 text-white rounded-lg shadow-sm transition-colors flex items-center gap-2"
              >
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
                </svg>
                Create Team Placement
              </button>
            )}
          </div>
        </div>

        {!loading && (userName || targetUser) && (
          <div className="mb-6 flex flex-col gap-3 rounded-xl border border-indigo-200 bg-gradient-to-r from-indigo-50 to-white px-4 py-4 shadow-sm sm:flex-row sm:flex-wrap sm:items-center sm:justify-between sm:gap-4">
            <div className="flex min-w-0 flex-1 flex-col gap-3 sm:flex-row sm:items-center sm:gap-6">
              <div className="min-w-0">
                <p className="text-[10px] font-bold uppercase tracking-wider text-indigo-600">Member name</p>
                <p className="truncate text-xl font-bold text-slate-900 sm:text-2xl">
                  {userName || targetUser?.name || "—"}
                </p>
              </div>
              <div className="hidden h-10 w-px shrink-0 bg-indigo-200 sm:block" aria-hidden />
              <div className="min-w-0">
                <p className="text-[10px] font-bold uppercase tracking-wider text-indigo-600">VB ID</p>
                <p className="font-mono text-lg font-semibold tracking-tight text-slate-800 sm:text-xl">
                  {memberVbid || "—"}
                </p>
              </div>
            </div>
            {targetUser?.email && (
              <p className="shrink-0 truncate text-xs text-slate-500 sm:max-w-xs sm:text-right" title={targetUser.email}>
                {targetUser.email}
              </p>
            )}
          </div>
        )}

        {pageMessage && (
          <div
            className={`mb-6 flex items-center gap-3 rounded-xl border px-4 py-3 text-sm shadow-sm animate-fadeIn ${
              pageMessage.type === "success"
                ? "border-emerald-200 bg-emerald-50 text-emerald-800"
                : "border-rose-200 bg-rose-50 text-rose-800"
            }`}
          >
            <span className={`flex h-5 w-5 items-center justify-center rounded-full text-xs font-bold ${
              pageMessage.type === "success" ? "bg-emerald-100 text-emerald-700" : "bg-rose-100 text-rose-700"
            }`}>
              {pageMessage.type === "success" ? "✓" : "!"}
            </span>
            <span className="font-medium">{pageMessage.text}</span>
            <button
              type="button"
              onClick={() => setPageMessage(null)}
              className="ml-auto rounded px-2 py-1 text-xs opacity-70 hover:bg-white/70 hover:opacity-100"
            >
              Close
            </button>
          </div>
        )}

        {loading ? (
          <div className="text-center py-12">Loading...</div>
        ) : error ? (
          <div className="text-center py-12 text-red-600">{error}</div>
        ) : (
          <div className="space-y-8">
            {/* Sheet summary editors — separate saves; IDs come only from this page URL */}
            <div className="grid gap-6 lg:grid-cols-2">
              <div className="rounded-xl border border-indigo-200 bg-white p-5 shadow-sm">
                <h2 className="text-base font-bold text-indigo-950">Personal sheet summary (Members placement)</h2>
                <p className="text-xs text-slate-600 mt-1 mb-4">
                  Mirrors the yellow summary block from the personal import. Saves to all personal placement rows for this user. Uses user id from this page only.
                </p>
                {personalSummaryError && (
                  <div className="mb-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
                    <p className="font-semibold">{personalSummaryError.error}</p>
                    {personalSummaryError.hint && <p className="mt-1 text-xs text-rose-700">{personalSummaryError.hint}</p>}
                  </div>
                )}
                {personalSummarySuccess && !personalSummaryDirty && (
                  <div className="mb-3 rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm font-medium text-emerald-800">
                    ✓ {personalSummarySuccess}
                  </div>
                )}
                {summaryLoading ? (
                  <p className="text-sm text-slate-500">Loading summary…</p>
                ) : (
                  <div className="grid gap-3 sm:grid-cols-2">
                    {[
                      ["yearlyTarget", "Yearly target"],
                      ["achieved", "Achieved"],
                      ["targetAchievedPercent", "Target achieved %"],
                      ["totalRevenueGenerated", "Total revenue (USD)"],
                      ["slabQualified", "Slab qualified"],
                      ["totalIncentiveInr", "Total incentive (INR)"],
                      ["totalIncentivePaidInr", "Incentive paid (INR)"],
                      ["totalBalanceIncentiveAmount", "Balance incentive (INR)"],
                    ].map(([key, label]) => (
                      <label key={key} className="block text-xs">
                        <span className="font-medium text-slate-600">{label}</span>
                        <input
                          className="mt-1 w-full rounded border border-slate-200 px-2 py-1.5 text-sm"
                          value={personalSummaryDraft[key]}
                          onChange={(e) => setPersonalSummaryDraft((d) => ({ ...d, [key]: e.target.value }))}
                        />
                      </label>
                    ))}
                  </div>
                )}
                <button
                  type="button"
                  disabled={savingPersonalSummary || summaryLoading || !personalSummaryDirty}
                  onClick={handleSavePersonalSummary}
                  className="mt-4 inline-flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-indigo-700 disabled:cursor-not-allowed disabled:bg-slate-200 disabled:text-slate-500 disabled:shadow-none"
                >
                  {savingPersonalSummary && <span className="h-3.5 w-3.5 rounded-full border-2 border-white/40 border-t-white animate-spin" />}
                  {savingPersonalSummary ? "Saving…" : personalSummaryDirty ? "Save personal summary" : "No changes to save"}
                </button>
              </div>

              {isTeamLead ? (
                <div className="rounded-xl border border-violet-200 bg-white p-5 shadow-sm">
                  <h2 className="text-base font-bold text-violet-950">Team sheet summary (Team lead placement)</h2>
                  <p className="text-xs text-slate-600 mt-1 mb-4">
                    Mirrors the team import summary block. Saves to all team placement rows for this lead. Same user id as this page; do not mix with the personal form.
                  </p>
                  {teamSummaryError && (
                    <div className="mb-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800">
                      <p className="font-semibold">{teamSummaryError.error}</p>
                      {teamSummaryError.hint && <p className="mt-1 text-xs text-rose-700">{teamSummaryError.hint}</p>}
                    </div>
                  )}
                  {teamSummarySuccess && !teamSummaryDirty && (
                    <div className="mb-3 rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm font-medium text-emerald-800">
                      ✓ {teamSummarySuccess}
                    </div>
                  )}
                  {summaryLoading ? (
                    <p className="text-sm text-slate-500">Loading summary…</p>
                  ) : (
                    <div className="grid gap-3 sm:grid-cols-2">
                      {[
                        ["yearlyPlacementTarget", "Yearly placement target"],
                        ["placementDone", "Placement done"],
                        ["placementAchPercent", "Placement ach %"],
                        ["yearlyRevenueTarget", "Yearly revenue target"],
                        ["revenueAch", "Revenue ach"],
                        ["revenueTargetAchievedPercent", "Revenue target achieved %"],
                        ["totalRevenueGenerated", "Total revenue (USD)"],
                        ["slabQualified", "Slab qualified"],
                        ["totalIncentiveInr", "Total incentive (INR)"],
                        ["totalIncentivePaidInr", "Incentive paid (INR)"],
                        ["totalBalanceIncentiveAmount", "Balance incentive (INR)"],
                      ].map(([key, label]) => (
                        <label key={key} className="block text-xs">
                          <span className="font-medium text-slate-600">{label}</span>
                          <input
                            className="mt-1 w-full rounded border border-slate-200 px-2 py-1.5 text-sm"
                            value={teamSummaryDraft[key]}
                            onChange={(e) => setTeamSummaryDraft((d) => ({ ...d, [key]: e.target.value }))}
                          />
                        </label>
                      ))}
                    </div>
                  )}
                  <button
                    type="button"
                    disabled={savingTeamSummary || summaryLoading || !teamSummaryDirty}
                    onClick={handleSaveTeamSummary}
                    className="mt-4 inline-flex items-center gap-2 rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-violet-700 disabled:cursor-not-allowed disabled:bg-slate-200 disabled:text-slate-500 disabled:shadow-none"
                  >
                    {savingTeamSummary && <span className="h-3.5 w-3.5 rounded-full border-2 border-white/40 border-t-white animate-spin" />}
                    {savingTeamSummary ? "Saving…" : teamSummaryDirty ? "Save team summary" : "No changes to save"}
                  </button>
                </div>
              ) : (
                <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50/80 p-5 text-sm text-slate-500">
                  <p className="font-medium text-slate-700">Team sheet summary</p>
                  <p className="mt-1 text-xs">Shown only for users with role Team Lead.</p>
                </div>
              )}
            </div>

            {/* Personal (Recruiter sheet) placements */}
            <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
              <div className="bg-indigo-50 border-b border-indigo-100 px-4 py-2">
                <h2 className="text-sm font-semibold text-indigo-900">Personal (Recruiter) Placements</h2>
                <p className="text-xs text-indigo-700 mt-0.5">From the recruiter / members placement sheet. Empty if no personal sheet was uploaded.</p>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-slate-50 text-slate-600 text-xs uppercase tracking-wider text-left border-b border-slate-200">
                      <th className="py-3 px-2 w-8">
                        <input
                          type="checkbox"
                          className="w-4 h-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                          onChange={() => handleSelectAllInSection(personalPlacements)}
                          checked={personalPlacements.length > 0 && personalPlacements.every((p) => selectedIds.includes(p.id))}
                        />
                      </th>
                      <th className="py-3 px-2 font-medium">Candidate Name</th>
                      <th className="py-3 px-2 font-medium">Recruiter Name</th>
                      <th className="py-3 px-2 font-medium">Lead</th>
                      <th className="py-3 px-2 font-medium">Split With</th>
                      <th className="py-3 px-2 font-medium">Placement Year</th>
                      <th className="py-3 px-2 font-medium">DOJ</th>
                      <th className="py-3 px-2 font-medium">DOQ</th>
                      <th className="py-3 px-2 font-medium">Client</th>
                      <th className="py-3 px-2 font-medium">PLC ID</th>
                      <th className="py-3 px-2 font-medium">Placement Type</th>
                      <th className="py-3 px-2 font-medium">Billing Status</th>
                      <th className="py-3 px-2 font-medium">Collection Status</th>
                      <th className="py-3 px-2 font-medium">Total Billed Hours</th>
                      <th className="py-3 px-2 font-medium">Revenue (USD)</th>
                      <th className="py-3 px-2 font-medium">Revenue -Lead (USD)</th>
                      <th className="py-3 px-2 font-medium">Incentive amount (INR)</th>
                      <th className="py-3 px-2 font-medium">Incentive Paid (INR)</th>
                      <th className="py-3 px-2 font-medium">Balance Incentive Amount</th>
                      <th className="py-3 px-2 font-medium text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {personalPlacements.length === 0 ? (
                      <tr>
                        <td colSpan="100%" className="px-6 py-8 text-center text-slate-500">
                          <span className="font-medium">No personal (recruiter) placements</span>
                        </td>
                      </tr>
                    ) : (
                      personalPlacements.map((p) => (
                        <tr key={p.id} className={`hover:bg-slate-50 ${selectedIds.includes(p.id) ? "bg-blue-50/50" : ""}`}>
                          <td className="py-3 px-2">
                            <input
                              type="checkbox"
                              className="w-4 h-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                              checked={selectedIds.includes(p.id)}
                              onChange={() => handleSelectOne(p.id)}
                            />
                          </td>
                          <td className="py-3 px-2 font-medium text-slate-800">{p.candidateName}</td>
                          <td className="py-3 px-2 text-slate-600">{p.recruiterName || userName || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.teamLead || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.placementSharing || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.placementYear || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{formatPlacementDate(p.doj)}</td>
                          <td className="py-3 px-2 text-slate-600">{p.doq ? formatPlacementDate(p.doq) : "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.clientName}</td>
                          <td className="py-3 px-2 text-slate-600">{p.plcId || "-"}</td>
                          <td className="py-3 px-2">
                            <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${(p.placementType && (p.placementType.toUpperCase().includes("PERMANENT") || p.placementType.toUpperCase().includes("FTE"))) ? "bg-purple-100 text-purple-700" : "bg-orange-100 text-orange-700"}`}>
                              {p.placementType || "-"}
                            </span>
                          </td>
                          <td className="py-3 px-2">
                            <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${p.billingStatus === "BILLED" ? "bg-green-100 text-green-700" : p.billingStatus === "PENDING" ? "bg-yellow-100 text-yellow-700" : "bg-red-100 text-red-700"}`}>
                              {p.billingStatus === "BILLED" ? "Completed" : p.billingStatus === "PENDING" ? "Pending" : p.billingStatus}
                            </span>
                          </td>
                          <td className="py-3 px-2 text-slate-600">{p.collectionStatus || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.billedHours || "-"}</td>
                          <td className="py-3 px-2 text-emerald-600 font-medium">{CalculationService.formatCurrency(p.revenue)}</td>
                          <td className="py-3 px-2 text-slate-600">{p.revenueAsLead || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.incentiveAmountInr ? CalculationService.formatCurrency(p.incentiveAmountInr, "INR") : "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.incentivePaidInr || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.placementBalanceIncentiveAmount || "-"}</td>
                          <td className="py-3 px-2 text-right flex gap-2 justify-end">
                            <button onClick={() => handleEdit(p)} className="text-blue-600 hover:text-blue-800 text-xs">Edit</button>
                            <button onClick={() => handleDelete(p.id)} className="text-red-500 hover:text-red-700 text-xs">Del</button>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Team (Lead sheet) placements – only for team leads (L2/L3); hidden for L4 and other roles */}
            {isTeamLead && (
              <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
                <div className="bg-violet-50 border-b border-violet-100 px-4 py-2">
                  <h2 className="text-sm font-semibold text-violet-900">Team (Lead) Placements</h2>
                  <p className="text-xs text-violet-700 mt-0.5">From the team lead placement sheet. Only present for team leads.</p>
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-slate-50 text-slate-600 text-xs uppercase tracking-wider text-left border-b border-slate-200">
                        <th className="py-3 px-2 w-8">
                          <input
                            type="checkbox"
                            className="w-4 h-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                            onChange={() => handleSelectAllInSection(teamPlacements)}
                            checked={teamPlacements.length > 0 && teamPlacements.every((p) => selectedIds.includes(p.id))}
                          />
                        </th>
                        <th className="py-3 px-2 font-medium">Candidate Name</th>
                        <th className="py-3 px-2 font-medium">Recruiter Name</th>
                        <th className="py-3 px-2 font-medium">Lead</th>
                        <th className="py-3 px-2 font-medium">Split With</th>
                        <th className="py-3 px-2 font-medium">Placement Year</th>
                        <th className="py-3 px-2 font-medium">DOJ</th>
                        <th className="py-3 px-2 font-medium">DOQ</th>
                        <th className="py-3 px-2 font-medium">Client</th>
                        <th className="py-3 px-2 font-medium">PLC ID</th>
                        <th className="py-3 px-2 font-medium">Placement Type</th>
                        <th className="py-3 px-2 font-medium">Billing Status</th>
                        <th className="py-3 px-2 font-medium">Collection Status</th>
                        <th className="py-3 px-2 font-medium">Total Billed Hours</th>
                        <th className="py-3 px-2 font-medium">Revenue (USD)</th>
                        <th className="py-3 px-2 font-medium">Revenue -Lead (USD)</th>
                        <th className="py-3 px-2 font-medium">Incentive amount (INR)</th>
                        <th className="py-3 px-2 font-medium">Incentive Paid (INR)</th>
                        <th className="py-3 px-2 font-medium">Balance Incentive Amount</th>
                        <th className="py-3 px-2 font-medium text-right">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100">
                      {teamPlacements.length === 0 ? (
                        <tr>
                          <td colSpan="100%" className="px-6 py-8 text-center text-slate-500">
                            <span className="font-medium">No team (lead) placements</span>
                          </td>
                        </tr>
                      ) : (
                        teamPlacements.map((p) => (
                          <tr key={p.id} className={`hover:bg-slate-50 ${selectedIds.includes(p.id) ? "bg-blue-50/50" : ""}`}>
                            <td className="py-3 px-2">
                              <input
                                type="checkbox"
                                className="w-4 h-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                                checked={selectedIds.includes(p.id)}
                                onChange={() => handleSelectOne(p.id)}
                              />
                            </td>
                            <td className="py-3 px-2 font-medium text-slate-800">{p.candidateName}</td>
                            <td className="py-3 px-2 text-slate-600">{p.recruiterName || userName || "-"}</td>
                            <td className="py-3 px-2 text-slate-600">{p.teamLead || "-"}</td>
                            <td className="py-3 px-2 text-slate-600">{p.placementSharing || "-"}</td>
                            <td className="py-3 px-2 text-slate-600">{p.placementYear || "-"}</td>
                            <td className="py-3 px-2 text-slate-600">{formatPlacementDate(p.doj)}</td>
                            <td className="py-3 px-2 text-slate-600">{p.doq ? formatPlacementDate(p.doq) : "-"}</td>
                            <td className="py-3 px-2 text-slate-600">{p.clientName}</td>
                            <td className="py-3 px-2 text-slate-600">{p.plcId || "-"}</td>
                            <td className="py-3 px-2">
                              <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${(p.placementType && (p.placementType.toUpperCase().includes("PERMANENT") || p.placementType.toUpperCase().includes("FTE"))) ? "bg-purple-100 text-purple-700" : "bg-orange-100 text-orange-700"}`}>
                                {p.placementType || "-"}
                              </span>
                            </td>
                            <td className="py-3 px-2">
                              <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${p.billingStatus === "BILLED" ? "bg-green-100 text-green-700" : p.billingStatus === "PENDING" ? "bg-yellow-100 text-yellow-700" : "bg-red-100 text-red-700"}`}>
                                {p.billingStatus === "BILLED" ? "Completed" : p.billingStatus === "PENDING" ? "Pending" : p.billingStatus}
                              </span>
                            </td>
                            <td className="py-3 px-2 text-slate-600">{p.collectionStatus || "-"}</td>
                            <td className="py-3 px-2 text-slate-600">{p.billedHours || "-"}</td>
                            <td className="py-3 px-2 text-emerald-600 font-medium">{CalculationService.formatCurrency(p.revenue)}</td>
                            <td className="py-3 px-2 text-slate-600">{p.revenueAsLead || "-"}</td>
                            <td className="py-3 px-2 text-slate-600">{p.incentiveAmountInr ? CalculationService.formatCurrency(p.incentiveAmountInr, "INR") : "-"}</td>
                            <td className="py-3 px-2 text-slate-600">{p.incentivePaidInr || "-"}</td>
                            <td className="py-3 px-2 text-slate-600">{p.placementBalanceIncentiveAmount || "-"}</td>
                            <td className="py-3 px-2 text-right flex gap-2 justify-end">
                              <button onClick={() => handleEdit(p)} className="text-blue-600 hover:text-blue-800 text-xs">Edit</button>
                              <button onClick={() => handleDelete(p.id)} className="text-red-500 hover:text-red-700 text-xs">Del</button>
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Legacy placements (if any) */}
            {legacyPlacements.length > 0 && (
              <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
                <div className="bg-slate-100 border-b border-slate-200 px-4 py-2">
                  <h2 className="text-sm font-semibold text-slate-800">Legacy / Other Placements</h2>
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-slate-50 text-slate-600 text-xs uppercase tracking-wider text-left border-b border-slate-200">
                        <th className="py-3 px-2 w-8">
                          <input
                            type="checkbox"
                            className="w-4 h-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                            onChange={() => handleSelectAllInSection(legacyPlacements)}
                            checked={legacyPlacements.length > 0 && legacyPlacements.every((p) => selectedIds.includes(p.id))}
                          />
                        </th>
                        <th className="py-3 px-2 font-medium">Candidate Name</th>
                        <th className="py-3 px-2 font-medium">Recruiter Name</th>
                        <th className="py-3 px-2 font-medium">Lead</th>
                        <th className="py-3 px-2 font-medium">Split With</th>
                        <th className="py-3 px-2 font-medium">Placement Year</th>
                        <th className="py-3 px-2 font-medium">DOJ</th>
                        <th className="py-3 px-2 font-medium">DOQ</th>
                        <th className="py-3 px-2 font-medium">Client</th>
                        <th className="py-3 px-2 font-medium">PLC ID</th>
                        <th className="py-3 px-2 font-medium">Placement Type</th>
                        <th className="py-3 px-2 font-medium">Billing Status</th>
                        <th className="py-3 px-2 font-medium">Collection Status</th>
                        <th className="py-3 px-2 font-medium">Total Billed Hours</th>
                        <th className="py-3 px-2 font-medium">Revenue (USD)</th>
                        <th className="py-3 px-2 font-medium">Revenue -Lead (USD)</th>
                        <th className="py-3 px-2 font-medium">Incentive amount (INR)</th>
                        <th className="py-3 px-2 font-medium">Incentive Paid (INR)</th>
                        <th className="py-3 px-2 font-medium">Balance Incentive Amount</th>
                        <th className="py-3 px-2 font-medium text-right">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100">
                      {legacyPlacements.map((p) => (
                        <tr key={p.id} className={`hover:bg-slate-50 ${selectedIds.includes(p.id) ? "bg-blue-50/50" : ""}`}>
                          <td className="py-3 px-2">
                            <input
                              type="checkbox"
                              className="w-4 h-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                              checked={selectedIds.includes(p.id)}
                              onChange={() => handleSelectOne(p.id)}
                            />
                          </td>
                          <td className="py-3 px-2 font-medium text-slate-800">{p.candidateName}</td>
                          <td className="py-3 px-2 text-slate-600">{p.recruiterName || userName || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.teamLead || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.placementSharing || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.placementYear || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{formatPlacementDate(p.doj)}</td>
                          <td className="py-3 px-2 text-slate-600">{p.doq ? formatPlacementDate(p.doq) : "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.clientName}</td>
                          <td className="py-3 px-2 text-slate-600">{p.plcId || "-"}</td>
                          <td className="py-3 px-2">
                            <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${(p.placementType && (p.placementType.toUpperCase().includes("PERMANENT") || p.placementType.toUpperCase().includes("FTE"))) ? "bg-purple-100 text-purple-700" : "bg-orange-100 text-orange-700"}`}>
                              {p.placementType || "-"}
                            </span>
                          </td>
                          <td className="py-3 px-2">
                            <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${p.billingStatus === "BILLED" ? "bg-green-100 text-green-700" : p.billingStatus === "PENDING" ? "bg-yellow-100 text-yellow-700" : "bg-red-100 text-red-700"}`}>
                              {p.billingStatus === "BILLED" ? "Completed" : p.billingStatus === "PENDING" ? "Pending" : p.billingStatus}
                            </span>
                          </td>
                          <td className="py-3 px-2 text-slate-600">{p.collectionStatus || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.billedHours || "-"}</td>
                          <td className="py-3 px-2 text-emerald-600 font-medium">{CalculationService.formatCurrency(p.revenue)}</td>
                          <td className="py-3 px-2 text-slate-600">{p.revenueAsLead || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.incentiveAmountInr ? CalculationService.formatCurrency(p.incentiveAmountInr, "INR") : "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.incentivePaidInr || "-"}</td>
                          <td className="py-3 px-2 text-slate-600">{p.placementBalanceIncentiveAmount || "-"}</td>
                          <td className="py-3 px-2 text-right flex gap-2 justify-end">
                            <button onClick={() => handleEdit(p)} className="text-blue-600 hover:text-blue-800 text-xs">Edit</button>
                            <button onClick={() => handleDelete(p.id)} className="text-red-500 hover:text-red-700 text-xs">Del</button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Add/Edit Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-xl shadow-xl max-w-4xl w-full p-6 animate-fadeIn overflow-y-auto max-h-[90vh]">
            <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
              <div>
                <h2 className="text-xl font-bold text-slate-800">
                  {editingId
                    ? `Edit ${createSource === "team" ? "Team" : "Personal"} Placement`
                    : `Create ${createSource === "team" ? "Team" : "Personal"} Placement`}
                </h2>
                <p className="text-xs text-slate-500">
                  {createSource === "team"
                    ? "This will save into TeamPlacement for this team lead."
                    : "This will save into PersonalPlacement for this member."}
                </p>
              </div>
              <span className={`rounded-full px-3 py-1 text-xs font-bold uppercase tracking-wider ${
                createSource === "team" ? "bg-violet-100 text-violet-700" : "bg-indigo-100 text-indigo-700"
              }`}>
                {createSource === "team" ? "Team sheet" : "Personal sheet"}
              </span>
            </div>
            {placementFormError && (
              <div className="mb-4 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm font-medium text-rose-800">
                {placementFormError}
              </div>
            )}
            <form onSubmit={handleSubmit} className="grid grid-cols-3 gap-4">
              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">Candidate Name</label>
                <input required type="text" className="w-full px-3 py-2 border rounded-lg text-sm"
                  value={formData.candidateName} onChange={e => setFormData({...formData, candidateName: e.target.value})} />
              </div>
              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">Placement Year</label>
                <input required type="number" className="w-full px-3 py-2 border rounded-lg text-sm"
                  value={formData.placementYear} onChange={e => setFormData({...formData, placementYear: e.target.value})} />
              </div>

              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">Client</label>
                <input required type="text" className="w-full px-3 py-2 border rounded-lg text-sm"
                  value={formData.clientName} onChange={e => setFormData({...formData, clientName: e.target.value})} />
              </div>
              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">PLC ID</label>
                <input type="text" className="w-full px-3 py-2 border rounded-lg text-sm"
                  value={formData.plcId || ""} onChange={e => setFormData({...formData, plcId: e.target.value})} />
              </div>
              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">Placement Type</label>
                <input type="text" className="w-full px-3 py-2 border rounded-lg text-sm" placeholder="e.g. C2C, PERMANENT, CONTRACT"
                  value={formData.placementType || ""} onChange={e => setFormData({...formData, placementType: e.target.value})} />
              </div>

              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">Date of Joining (DOJ)</label>
                <input required type="date" className="w-full px-3 py-2 border rounded-lg text-sm"
                  value={formData.doj} onChange={e => setFormData({...formData, doj: e.target.value})} />
              </div>
              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">Date of Quit (DOQ)</label>
                <input type="date" className="w-full px-3 py-2 border rounded-lg text-sm"
                  value={formData.doq} onChange={e => setFormData({...formData, doq: e.target.value})} />
              </div>


              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">Total Billed Hours</label>
                <input type="number" className="w-full px-3 py-2 border rounded-lg text-sm"
                  value={formData.billedHours} onChange={e => setFormData({...formData, billedHours: e.target.value})} />
              </div>
              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">Revenue (USD)</label>
                <input required type="number" className="w-full px-3 py-2 border rounded-lg text-sm"
                  value={formData.revenue} onChange={e => setFormData({...formData, revenue: e.target.value})} />
              </div>
              {createSource === "team" && (
                <div>
                  <label className="block text-xs font-medium text-slate-700 mb-1">Recruiter Name</label>
                  <input type="text" className="w-full px-3 py-2 border rounded-lg text-sm"
                    value={formData.recruiterName || ""} onChange={e => setFormData({...formData, recruiterName: e.target.value})} />
                </div>
              )}
              {createSource === "team" && (
                <>
                  <div>
                    <label className="block text-xs font-medium text-slate-700 mb-1">Lead Name</label>
                    <input type="text" className="w-full px-3 py-2 border rounded-lg text-sm"
                      value={formData.teamLead} onChange={e => setFormData({...formData, teamLead: e.target.value})} />
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-slate-700 mb-1">Split With</label>
                    <input type="text" className="w-full px-3 py-2 border rounded-lg text-sm"
                      value={formData.placementSharing} onChange={e => setFormData({...formData, placementSharing: e.target.value})} />
                  </div>
                </>
              )}

              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">Billing Status</label>
                <select className="w-full px-3 py-2 border rounded-lg text-sm"
                  value={formData.billingStatus} onChange={e => setFormData({...formData, billingStatus: e.target.value})}>
                  <option value="PENDING">Pending</option>
                  <option value="BILLED">Billed</option>
                  <option value="HOLD">Hold</option>
                  <option value="CANCELLED">Cancelled</option>
                </select>
              </div>
              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">Collection Status</label>
                <input type="text" className="w-full px-3 py-2 border rounded-lg text-sm"
                  value={formData.collectionStatus || ""} onChange={e => setFormData({...formData, collectionStatus: e.target.value})} />
              </div>
              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">Incentive amount (INR)</label>
                <input type="number" className="w-full px-3 py-2 border rounded-lg text-sm"
                  value={formData.incentiveAmountInr} onChange={e => setFormData({...formData, incentiveAmountInr: e.target.value})} />
              </div>

              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">Incentive Paid (INR)</label>
                <input type="text" className="w-full px-3 py-2 border rounded-lg text-sm"
                  value={formData.incentivePaidInr || ''} onChange={e => setFormData({...formData, incentivePaidInr: e.target.value})} />
              </div>
              <div>
                <label className="block text-xs font-medium text-slate-700 mb-1">Balance Incentive Amount (INR)</label>
                <input type="text" className="w-full px-3 py-2 border rounded-lg text-sm"
                  value={formData.placementBalanceIncentiveAmount || ''} onChange={e => setFormData({...formData, placementBalanceIncentiveAmount: e.target.value})} />
              </div>
              <div className="col-span-3 flex justify-end gap-3 mt-6 border-t pt-4">
                <button
                  type="button"
                  onClick={() => {
                    setShowModal(false);
                    setPlacementFormError(null);
                    setFormBaseline(null);
                  }}
                  disabled={savingPlacement}
                  className="px-4 py-2 text-slate-600 hover:bg-slate-100 rounded-lg disabled:opacity-50"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={!placementFormDirty || savingPlacement}
                  className="inline-flex items-center gap-2 rounded-lg bg-blue-600 px-4 py-2 font-semibold text-white transition hover:bg-blue-700 disabled:cursor-not-allowed disabled:bg-slate-200 disabled:text-slate-500"
                >
                  {savingPlacement && <span className="h-4 w-4 rounded-full border-2 border-white/40 border-t-white animate-spin" />}
                  {savingPlacement
                    ? "Saving…"
                    : placementFormDirty
                      ? (editingId ? "Update Placement" : "Create Placement")
                      : "No changes to save"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

    </div>
  );
};

export default AdminEmployeePlacements;
