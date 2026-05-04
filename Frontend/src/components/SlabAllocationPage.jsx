import React, { useState, useEffect, useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { apiRequest } from '../api/client';
import { Skeleton, TableRowSkeleton } from './common/Skeleton';
import IncentiveSlabTable from './common/IncentiveSlabTable';

const SlabAllocationPage = () => {
    const [users, setUsers] = useState([]);
    const [templates, setTemplates] = useState([]);
    const [loading, setLoading] = useState(true);
    const [selectedUserIds, setSelectedUserIds] = useState(new Set());
    const [searchTerm, setSearchTerm] = useState('');
    const [roleFilter, setRoleFilter] = useState('ALL');
    const [teamFilter, setTeamFilter] = useState('ALL');
    const [statusFilter, setStatusFilter] = useState('ALL');
    
    // Editor State
    const [isEditorOpen, setIsEditorOpen] = useState(false);
    const [editingSlabs, setEditingSlabs] = useState([
        { minPercent: 0, maxPercent: 34, incentivePercent: 0 },
        { minPercent: 35, maxPercent: 65, incentivePercent: 1.25 },
        { minPercent: 66, maxPercent: 90, incentivePercent: 1.75 },
        { minPercent: 91, maxPercent: 110, incentivePercent: 2.25 },
        { minPercent: 111, maxPercent: null, incentivePercent: 3.75 }
    ]);
    const [saving, setSaving] = useState(false);
    const [message, setMessage] = useState({ type: '', text: '' });
    const [selectedMembersOpen, setSelectedMembersOpen] = useState(false);
    /** Collapsed on small screens so slab rows get space; full width preview on lg+ */
    const [livePreviewOpen, setLivePreviewOpen] = useState(false);

    useEffect(() => {
        fetchData();
    }, []);

    const fetchData = async () => {
        setLoading(true);
        try {
            const [usersRes, templatesRes] = await Promise.all([
                apiRequest('/incentive-slabs/org-users'),
                apiRequest('/incentive-slabs/templates')
            ]);
            
            if (usersRes.ok) setUsers(await usersRes.json());
            if (templatesRes.ok) setTemplates(await templatesRes.json());
        } catch (err) {
            console.error('Error fetching data:', err);
            setMessage({ type: 'error', text: 'Failed to load data' });
        } finally {
            setLoading(false);
        }
    };

    const teams = useMemo(() => {
        const uniqueTeams = new Set();
        users.forEach(u => { if (u.teamName) uniqueTeams.add(u.teamName); });
        return Array.from(uniqueTeams).sort();
    }, [users]);

    const filteredUsers = useMemo(() => {
        return users.filter(u => {
            const matchesSearch = u.name.toLowerCase().includes(searchTerm.toLowerCase()) || 
                                u.email.toLowerCase().includes(searchTerm.toLowerCase());
            const matchesRole = roleFilter === 'ALL' || u.role === roleFilter;
            const matchesTeam = teamFilter === 'ALL' || u.teamName === teamFilter;
            const matchesStatus = statusFilter === 'ALL' || 
                                (statusFilter === 'CONFIGURED' ? u.hasSlabConfigured : !u.hasSlabConfigured);
            return matchesSearch && matchesRole && matchesTeam && matchesStatus;
        });
    }, [users, searchTerm, roleFilter, teamFilter, statusFilter]);

    const selectedMembersList = useMemo(
        () => users.filter((u) => selectedUserIds.has(u.id)),
        [users, selectedUserIds]
    );

    const selectedMembersSummary = useMemo(() => {
        if (selectedMembersList.length === 0) return '';
        if (selectedMembersList.length === 1) return selectedMembersList[0].name;
        const first = selectedMembersList.slice(0, 2).map((u) => u.name).join(', ');
        const rest = selectedMembersList.length - 2;
        return rest > 0 ? `${first} +${rest} more` : first;
    }, [selectedMembersList]);

    const toggleUserSelection = (userId) => {
        const newSelected = new Set(selectedUserIds);
        if (newSelected.has(userId)) newSelected.delete(userId);
        else newSelected.add(userId);
        setSelectedUserIds(newSelected);
    };

    const selectAllFiltered = () => {
        if (selectedUserIds.size === filteredUsers.length) {
            setSelectedUserIds(new Set());
        } else {
            setSelectedUserIds(new Set(filteredUsers.map(u => u.id)));
        }
    };

    const handleSlabChange = (index, field, value) => {
        const newSlabs = [...editingSlabs];
        newSlabs[index][field] = value === '' ? null : Number(value);
        setEditingSlabs(newSlabs);
    };

    const addSlab = () => {
        if (editingSlabs.length >= 8) return;
        const lastSlab = editingSlabs[editingSlabs.length - 1];
        const newMin = lastSlab.maxPercent ? lastSlab.maxPercent + 1 : lastSlab.minPercent + 10;
        setEditingSlabs([...editingSlabs, { minPercent: newMin, maxPercent: null, incentivePercent: 0 }]);
    };

    const removeSlab = (index) => {
        if (editingSlabs.length <= 1) return;
        setEditingSlabs(editingSlabs.filter((_, i) => i !== index));
    };

    const applyTemplate = (template) => {
        setEditingSlabs(template.slabs);
    };

    const handleSave = async () => {
        if (selectedUserIds.size === 0) return;
        setSaving(true);
        try {
            const res = await apiRequest('/incentive-slabs/bulk', {
                method: 'POST',
                body: JSON.stringify({
                    userIds: Array.from(selectedUserIds),
                    slabs: editingSlabs
                })
            });
            if (res.ok) {
                setMessage({ type: 'success', text: `Successfully updated ${selectedUserIds.size} users` });
                setIsEditorOpen(false);
                setSelectedUserIds(new Set());
                fetchData();
            } else {
                const data = await res.json();
                setMessage({ type: 'error', text: data.error || 'Failed to save' });
            }
        } catch (err) {
            setMessage({ type: 'error', text: 'Error saving: ' + err.message });
        } finally {
            setSaving(false);
        }
    };

    return (
        <div className="min-h-screen bg-slate-50 p-6 md:p-8">
            <div className="max-w-7xl mx-auto">
                <header className="flex justify-between items-center mb-8">
                    <div>
                        <h1 className="text-3xl font-bold text-slate-900 tracking-tight">Advanced Slab Allocation</h1>
                        <p className="text-slate-500 mt-1">Configure performance incentive slabs for all organization members</p>
                    </div>
                    <div className="flex items-center gap-4">
                        <div className="bg-white px-4 py-2 rounded-xl border border-slate-200 shadow-sm flex items-center gap-3">
                            <div className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Status</div>
                            <div className="h-4 w-px bg-slate-200"></div>
                            <div className="flex items-center gap-1.5">
                                <span className="h-2 w-2 rounded-full bg-emerald-500"></span>
                                <span className="text-sm font-bold text-slate-700">
                                    {users.filter(u => u.hasSlabConfigured).length}/{users.length}
                                </span>
                                <span className="text-xs text-slate-400 font-medium">Configured</span>
                            </div>
                        </div>
                    </div>
                </header>

                {message.text && (
                    <motion.div 
                        initial={{ opacity: 0, y: -10 }}
                        animate={{ opacity: 1, y: 0 }}
                        className={`mb-6 p-4 rounded-xl border flex items-center gap-3 ${
                            message.type === 'success' ? 'bg-emerald-50 border-emerald-200 text-emerald-800' : 'bg-rose-50 border-rose-200 text-rose-800'
                        }`}
                    >
                        {message.type === 'success' ? (
                            <svg className="w-5 h-5 text-emerald-500" fill="currentColor" viewBox="0 0 20 20"><path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" /></svg>
                        ) : (
                            <svg className="w-5 h-5 text-rose-500" fill="currentColor" viewBox="0 0 20 20"><path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z" clipRule="evenodd" /></svg>
                        )}
                        <span className="font-medium">{message.text}</span>
                        <button onClick={() => setMessage({ type: '', text: '' })} className="ml-auto opacity-60 hover:opacity-100">×</button>
                    </motion.div>
                )}

                <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 lg:items-start">
                    {/* User List Panel — scroll inside card so the slab editor stays in view */}
                    <div className="lg:col-span-2 space-y-6 min-h-0">
                        <div className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden flex flex-col max-h-[calc(100dvh-10rem)] lg:max-h-[calc(100dvh-8.5rem)]">
                            <div className="p-4 border-b border-slate-100 bg-slate-50/50 flex flex-wrap gap-4 items-center shrink-0">
                                <div className="relative flex-1 min-w-[200px]">
                                    <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" /></svg>
                                    <input 
                                        type="text" 
                                        placeholder="Search by name or email..."
                                        value={searchTerm}
                                        onChange={(e) => setSearchTerm(e.target.value)}
                                        className="w-full pl-10 pr-4 py-2 bg-white border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20"
                                    />
                                </div>
                                <select 
                                    value={roleFilter} 
                                    onChange={(e) => setRoleFilter(e.target.value)}
                                    className="px-3 py-2 bg-white border border-slate-200 rounded-lg text-sm focus:outline-none"
                                >
                                    <option value="ALL">All Roles</option>
                                    <option value="TEAM_LEAD">Team Leads</option>
                                    <option value="EMPLOYEE">Employees</option>
                                    <option value="SUPER_ADMIN">Super Admins</option>
                                </select>
                                <select 
                                    value={teamFilter} 
                                    onChange={(e) => setTeamFilter(e.target.value)}
                                    className="px-3 py-2 bg-white border border-slate-200 rounded-lg text-sm focus:outline-none"
                                >
                                    <option value="ALL">All Teams</option>
                                    {teams.map(t => <option key={t} value={t}>{t}</option>)}
                                </select>
                                <select 
                                    value={statusFilter} 
                                    onChange={(e) => setStatusFilter(e.target.value)}
                                    className="px-3 py-2 bg-white border border-slate-200 rounded-lg text-sm focus:outline-none"
                                >
                                    <option value="ALL">All Status</option>
                                    <option value="CONFIGURED">Configured</option>
                                    <option value="NOT_CONFIGURED">Not Set</option>
                                </select>
                            </div>

                            <div className="overflow-x-auto overflow-y-auto flex-1 min-h-0">
                                <table className="w-full text-left border-collapse">
                                    <thead>
                                        <tr className="bg-slate-50/80 text-slate-500 text-[10px] font-bold uppercase tracking-widest border-b border-slate-100">
                                            <th className="px-6 py-4 w-12">
                                                <input 
                                                    type="checkbox" 
                                                    checked={selectedUserIds.size === filteredUsers.length && filteredUsers.length > 0}
                                                    onChange={selectAllFiltered}
                                                    className="rounded text-blue-600 focus:ring-blue-500"
                                                />
                                            </th>
                                            <th className="px-6 py-4">Employee</th>
                                            <th className="px-6 py-4">Team & Level</th>
                                            <th className="px-6 py-4">Status</th>
                                            <th className="px-6 py-4 text-right">Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-slate-100">
                                        {loading ? (
                                            Array.from({ length: 5 }).map((_, i) => (
                                                <TableRowSkeleton key={i} />
                                            ))
                                        ) : filteredUsers.length === 0 ? (
                                            <tr>
                                                <td colSpan="5" className="px-6 py-12 text-center">
                                                    <div className="text-slate-400 italic">No members found matching your filters</div>
                                                </td>
                                            </tr>
                                        ) : (
                                            filteredUsers.map((user) => (
                                                <tr 
                                                    key={user.id} 
                                                    className={`hover:bg-slate-50/50 transition-colors ${selectedUserIds.has(user.id) ? 'bg-blue-50/30' : ''}`}
                                                >
                                                    <td className="px-6 py-4">
                                                        <input 
                                                            type="checkbox" 
                                                            checked={selectedUserIds.has(user.id)}
                                                            onChange={() => toggleUserSelection(user.id)}
                                                            className="rounded text-blue-600 focus:ring-blue-500"
                                                        />
                                                    </td>
                                                    <td className="px-6 py-4">
                                                        <div className="font-semibold text-slate-800">{user.name}</div>
                                                        <div className="text-xs text-slate-400">{user.email}</div>
                                                    </td>
                                                    <td className="px-6 py-4">
                                                        <div className="flex flex-col gap-1">
                                                            <span className="text-sm font-medium text-slate-600">
                                                                {user.teamName || 'No Team'}
                                                            </span>
                                                            <span className="text-[10px] px-2 py-0.5 rounded-full bg-slate-100 text-slate-500 w-fit font-bold">
                                                                {user.level || 'NO LEVEL'}
                                                            </span>
                                                        </div>
                                                    </td>
                                                    <td className="px-6 py-4">
                                                        {user.hasSlabConfigured ? (
                                                            <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-emerald-100 text-emerald-700 text-xs font-bold">
                                                                <span className="h-1.5 w-1.5 rounded-full bg-emerald-500"></span>
                                                                CONFIGURED
                                                            </span>
                                                        ) : (
                                                            <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-slate-200 text-slate-500 text-xs font-bold">
                                                                <span className="h-1.5 w-1.5 rounded-full bg-slate-400"></span>
                                                                NOT SET
                                                            </span>
                                                        )}
                                                    </td>
                                                    <td className="px-6 py-4 text-right">
                                                        <button 
                                                            onClick={() => {
                                                                setSelectedUserIds(new Set([user.id]));
                                                                if (user.slabs) setEditingSlabs(user.slabs);
                                                                setIsEditorOpen(true);
                                                            }}
                                                            className="text-blue-600 hover:text-blue-800 text-sm font-bold uppercase tracking-wider"
                                                        >
                                                            Edit
                                                        </button>
                                                    </td>
                                                </tr>
                                            ))
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>

                    {/* Allocation Panel — sticky + internal scroll keeps preview & save visible */}
                    <div className="lg:col-span-1 w-full min-w-0 lg:sticky lg:top-4 lg:z-20">
                        <div className="bg-white rounded-2xl border border-slate-200 overflow-hidden flex flex-col max-h-[calc(100dvh-4.5rem)] lg:max-h-none lg:overflow-visible shadow-lg ring-1 ring-slate-200/60">
                            <div className="p-5 border-b border-slate-100 bg-gradient-to-br from-indigo-900 to-slate-900 text-white shrink-0">
                                <h3 className="font-bold text-lg">Incentive Slab Editor</h3>
                                <p className="text-indigo-200 text-xs mt-1">
                                    {selectedUserIds.size === 0 
                                        ? 'Select users from the list to begin' 
                                        : `${selectedUserIds.size} user${selectedUserIds.size === 1 ? '' : 's'} selected`}
                                </p>
                                {selectedMembersList.length > 0 && (
                                    <div className="mt-3 border-t border-white/15 pt-3">
                                        <button
                                            type="button"
                                            onClick={() => setSelectedMembersOpen((o) => !o)}
                                            aria-expanded={selectedMembersOpen}
                                            className="flex w-full items-start gap-2 rounded-lg py-1.5 text-left outline-none ring-white/30 transition-colors hover:bg-white/5 focus-visible:ring-2"
                                        >
                                            <span className="mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-white/10 text-white">
                                                <svg
                                                    className={`h-4 w-4 transition-transform duration-200 ${selectedMembersOpen ? 'rotate-180' : ''}`}
                                                    fill="none"
                                                    stroke="currentColor"
                                                    viewBox="0 0 24 24"
                                                    aria-hidden
                                                >
                                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                                                </svg>
                                            </span>
                                            <span className="min-w-0 flex-1">
                                                <span className="block text-[10px] font-bold uppercase tracking-widest text-indigo-200/90">
                                                    Selected members
                                                </span>
                                                {!selectedMembersOpen && (
                                                    <span className="mt-1 block text-xs font-medium leading-snug text-white/95 line-clamp-2">
                                                        {selectedMembersSummary}
                                                    </span>
                                                )}
                                                <span className="mt-0.5 block text-[10px] text-indigo-200/70">
                                                    {selectedMembersOpen ? 'Tap to hide list' : 'Tap to show full list'}
                                                </span>
                                            </span>
                                        </button>
                                        <AnimatePresence initial={false}>
                                            {selectedMembersOpen && (
                                                <motion.div
                                                    initial={{ height: 0, opacity: 0 }}
                                                    animate={{ height: 'auto', opacity: 1 }}
                                                    exit={{ height: 0, opacity: 0 }}
                                                    transition={{ duration: 0.2 }}
                                                    className="overflow-hidden"
                                                >
                                                    <ul className="mt-2 max-h-[min(40vh,14rem)] space-y-2 overflow-y-auto border-t border-white/10 pt-3 pr-1 text-xs text-white/95 custom-scrollbar">
                                                        {selectedMembersList.map((u) => (
                                                            <li key={u.id} className="flex items-start gap-2 rounded-md py-0.5" title={u.email}>
                                                                <span className="mt-1.5 h-1 w-1 shrink-0 rounded-full bg-indigo-300" />
                                                                <span className="min-w-0 leading-snug">
                                                                    <span className="font-semibold">{u.name}</span>
                                                                    <span className="block text-[10px] text-indigo-200/80 break-words">{u.email}</span>
                                                                </span>
                                                            </li>
                                                        ))}
                                                    </ul>
                                                </motion.div>
                                            )}
                                        </AnimatePresence>
                                    </div>
                                )}
                            </div>

                            <div className="flex min-h-0 flex-1 flex-col overflow-hidden lg:flex-none lg:min-h-0 lg:overflow-visible">
                                <div className="min-h-0 flex-1 space-y-4 overflow-y-auto p-4 sm:p-6 sm:space-y-6 pr-2 custom-scrollbar lg:flex-none lg:min-h-0 lg:overflow-visible">
                                {/* Templates */}
                                <div className="shrink-0">
                                    <label className="block text-xs font-bold text-slate-400 uppercase tracking-widest mb-3">Quick Templates</label>
                                    <div className="flex flex-wrap gap-2">
                                        {templates.map(tpl => (
                                            <button 
                                                key={tpl.id}
                                                onClick={() => applyTemplate(tpl)}
                                                className="px-3 py-1.5 bg-slate-100 hover:bg-indigo-100 hover:text-indigo-700 rounded-lg text-xs font-bold text-slate-600 transition-colors border border-transparent hover:border-indigo-200"
                                            >
                                                {tpl.name}
                                            </button>
                                        ))}
                                        {templates.length === 0 && <span className="text-xs italic text-slate-400">No templates found</span>}
                                    </div>
                                </div>

                                {/* Slab Inputs — primary scroll region on narrow viewports */}
                                <div className="min-h-0 space-y-3 pb-2">
                                    <div className="flex shrink-0 items-center justify-between mb-1">
                                        <label className="block text-xs font-bold text-slate-400 uppercase tracking-widest">Configuration</label>
                                        <button 
                                            onClick={addSlab} 
                                            disabled={editingSlabs.length >= 8}
                                            className="text-indigo-600 hover:text-indigo-800 text-[10px] font-bold uppercase"
                                        >
                                            + Add Slab
                                        </button>
                                    </div>
                                    
                                    <div className="space-y-2 pr-1">
                                        {editingSlabs.map((slab, idx) => (
                                            <div key={idx} className="relative flex flex-col gap-2 rounded-xl border border-slate-100 bg-slate-50/50 p-2.5 sm:gap-3 sm:p-3 pr-10 sm:pr-10">
                                                <button 
                                                    type="button"
                                                    onClick={() => removeSlab(idx)}
                                                    disabled={editingSlabs.length <= 1}
                                                    title={editingSlabs.length <= 1 ? 'At least one slab required' : 'Remove this slab'}
                                                    aria-label="Remove slab"
                                                    className="absolute right-2 top-2 flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border border-rose-200 bg-white text-lg font-bold leading-none text-rose-600 shadow-sm transition-colors hover:bg-rose-50 hover:border-rose-300 disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:bg-white"
                                                >
                                                    ×
                                                </button>
                                                <div className="grid grid-cols-2 gap-3">
                                                    <div>
                                                        <label className="block text-[10px] font-bold text-slate-400 uppercase mb-1">Range (%)</label>
                                                        <div className="flex items-center gap-1">
                                                            <input 
                                                                type="number" 
                                                                value={slab.minPercent} 
                                                                onChange={(e) => handleSlabChange(idx, 'minPercent', e.target.value)}
                                                                className="w-full px-2 py-1.5 text-xs border border-slate-200 rounded focus:ring-1 focus:ring-blue-500/30"
                                                                placeholder="Min"
                                                            />
                                                            <span className="text-slate-300">-</span>
                                                            <input 
                                                                type="number" 
                                                                value={slab.maxPercent === null ? '' : slab.maxPercent} 
                                                                onChange={(e) => handleSlabChange(idx, 'maxPercent', e.target.value)}
                                                                className="w-full px-2 py-1.5 text-xs border border-slate-200 rounded focus:ring-1 focus:ring-blue-500/30"
                                                                placeholder="Max (empty for +)"
                                                            />
                                                        </div>
                                                    </div>
                                                    <div>
                                                        <label className="block text-[10px] font-bold text-slate-400 uppercase mb-1">Incentive (%)</label>
                                                        <input 
                                                            type="number" 
                                                            step="0.01"
                                                            value={slab.incentivePercent} 
                                                            onChange={(e) => handleSlabChange(idx, 'incentivePercent', e.target.value)}
                                                            className="w-full px-2 py-1.5 text-xs border border-slate-200 rounded focus:ring-1 focus:ring-blue-500/30 font-bold text-indigo-700"
                                                            placeholder="0.00"
                                                        />
                                                    </div>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                                </div>

                                {/* Preview + save — compact on mobile; toggle preview so slab rows stay visible */}
                                <div className="shrink-0 space-y-3 border-t border-slate-100 bg-slate-50/80 p-3 sm:p-5 sm:space-y-4">
                                    <div>
                                        <button
                                            type="button"
                                            onClick={() => setLivePreviewOpen((o) => !o)}
                                            aria-expanded={livePreviewOpen}
                                            className="mb-2 flex w-full items-center justify-between gap-2 rounded-lg py-2 text-left lg:hidden"
                                        >
                                            <span className="text-xs font-bold uppercase tracking-widest text-slate-400">
                                                Live preview
                                            </span>
                                            <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md border border-slate-200 bg-white text-slate-600">
                                                <svg
                                                    className={`h-4 w-4 transition-transform ${livePreviewOpen ? 'rotate-180' : ''}`}
                                                    fill="none"
                                                    stroke="currentColor"
                                                    viewBox="0 0 24 24"
                                                    aria-hidden
                                                >
                                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                                                </svg>
                                            </span>
                                        </button>
                                        <label className="mb-2 hidden text-xs font-bold uppercase tracking-widest text-slate-400 lg:mb-3 lg:block">
                                            Live Preview
                                        </label>
                                        <div
                                            className={`origin-top rounded-lg border border-slate-100 bg-white custom-scrollbar sm:scale-95 ${
                                                livePreviewOpen
                                                    ? 'block max-h-52 overflow-x-auto overflow-y-auto p-2'
                                                    : 'hidden'
                                            } lg:block lg:max-h-none lg:overflow-x-auto lg:overflow-y-visible lg:p-2`}
                                        >
                                            <IncentiveSlabTable slabs={editingSlabs} compact />
                                        </div>
                                    </div>

                                    <button 
                                        onClick={handleSave}
                                        disabled={selectedUserIds.size === 0 || saving}
                                        className={`w-full py-3 rounded-xl font-bold text-sm shadow-lg transition-all flex items-center justify-center gap-2 ${
                                            selectedUserIds.size === 0 
                                                ? 'bg-slate-200 text-slate-400 cursor-not-allowed shadow-none' 
                                                : 'bg-indigo-600 hover:bg-indigo-700 text-white shadow-indigo-500/20 active:scale-[0.98]'
                                        }`}
                                    >
                                        {saving ? (
                                            <span className="h-4 w-4 border-2 border-white/30 border-t-white rounded-full animate-spin"></span>
                                        ) : (
                                            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2.5" /></svg>
                                        )}
                                        Apply Configuration
                                    </button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            
            <style dangerouslySetInnerHTML={{ __html: `
                .custom-scrollbar::-webkit-scrollbar { width: 4px; }
                .custom-scrollbar::-webkit-scrollbar-track { background: transparent; }
                .custom-scrollbar::-webkit-scrollbar-thumb { background: #e2e8f0; border-radius: 10px; }
                .custom-scrollbar::-webkit-scrollbar-thumb:hover { background: #cbd5e1; }
            `}} />
        </div>
    );
};

export default SlabAllocationPage;
