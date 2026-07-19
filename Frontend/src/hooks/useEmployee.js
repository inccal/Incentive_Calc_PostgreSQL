import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiRequest } from '../api/client'

export const useEmployeeDetails = (employeeId, userRole, currentUserId) => {
  return useQuery({
    // Scope cached employee data to the authenticated viewer as well as the
    // target, preventing a previous login's cached response from being reused.
    queryKey: ['employee', employeeId, userRole, currentUserId],
    queryFn: async () => {
      // Employee-like roles can only view themselves. Always use the dedicated
      // self endpoint so URL slugs, duplicate names, or stale navigation state
      // can never select another employee record.
      const isSelf = ['EMPLOYEE', 'LIMITED_ACCESS'].includes(userRole)
      let endpoint = isSelf
        ? '/dashboard/employee'
        : `/dashboard/employee/${employeeId}`
      
      const response = await apiRequest(endpoint)
      if (!response.ok) {
        const data = await response.json().catch(() => ({}))
        throw new Error(data.error || 'Failed to load employee details')
      }
      return response.json()
    },
    enabled: !!employeeId || (['EMPLOYEE', 'LIMITED_ACCESS'].includes(userRole) && !employeeId),
  })
}

export const useUpdateVbid = () => {
  const queryClient = useQueryClient()
  
  return useMutation({
    mutationFn: async ({ employeeId, vbid }) => {
      const response = await apiRequest(`/users/${employeeId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ vbid })
      })
      if (!response.ok) throw new Error('Failed to update VBID')
      return response.json()
    },
    onSuccess: (data, variables) => {
      queryClient.invalidateQueries({ queryKey: ['employee', variables.employeeId] })
    }
  })
}
