import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext'

const LoginForm = () => {
  const [error, setError] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const navigate = useNavigate()
  const { loginWithMicrosoft, user, loading } = useAuth()

  useEffect(() => {
    if (loading || !user) return
    if (user.role === 'S1_ADMIN') return navigate('/admin/dashboard', { replace: true })
    if (user.role === 'SUPER_ADMIN') return navigate('/team', { replace: true })
    if (user.role === 'TEAM_LEAD') return navigate('/teamlead', { replace: true })
    if (user.role === 'EMPLOYEE') {
      const slug = (user.name ?? '').toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '') || user.id
      return navigate(`/employee/${slug}`, { replace: true, state: { employeeId: user.id } })
    }
  }, [loading, user, navigate])

  const handleSubmit = async (e) => {
    e.preventDefault()
    setError('')
    setSubmitting(true)

    try {
      loginWithMicrosoft()
    } catch (err) {
      setError(err.message || 'Microsoft Entra ID login failed')
      setSubmitting(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center p-4 relative overflow-hidden bg-gradient-to-br from-white via-blue-50/30 to-indigo-50/40">
      {/* Clean Light Modern Background */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        {/* Soft gradient overlay */}
        <div className="absolute inset-0 bg-gradient-to-br from-blue-50/40 via-transparent to-purple-50/30"></div>
        
        {/* Large soft gradient orbs */}
        <div className="absolute -top-32 -right-32 w-96 h-96 bg-gradient-to-br from-blue-100/30 to-indigo-100/20 rounded-full blur-3xl animate-float-gentle"></div>
        <div className="absolute -bottom-32 -left-32 w-[30rem] h-[30rem] bg-gradient-to-br from-indigo-100/25 to-purple-100/20 rounded-full blur-3xl animate-float-gentle-delayed"></div>
        <div className="absolute top-1/3 right-1/4 w-80 h-80 bg-gradient-to-br from-cyan-100/20 to-blue-100/15 rounded-full blur-3xl animate-float-gentle-slow"></div>
        
        {/* Subtle decorative elements */}
        <div className="absolute top-20 left-1/4 w-1 h-1 bg-blue-400/30 rounded-full"></div>
        <div className="absolute bottom-40 right-1/3 w-1 h-1 bg-indigo-400/30 rounded-full"></div>
        <div className="absolute top-1/2 left-1/2 w-1 h-1 bg-purple-400/30 rounded-full"></div>
        
        {/* Light pattern overlay */}
        <div className="absolute inset-0 opacity-[0.02]" style={{backgroundImage: 'radial-gradient(circle at 1px 1px, #3b82f6 1px, transparent 0)', backgroundSize: '80px 80px'}}></div>
      </div>
      
      <div className="bg-white rounded-lg shadow-2xl overflow-hidden max-w-5xl w-full flex flex-col md:flex-row relative z-10">
      {/* Left Section - Welcome Back */}
      <div className="bg-gradient-to-br from-blue-400 via-blue-500 to-blue-600 md:w-1/2 p-12 flex flex-col justify-center relative overflow-hidden">
        {/* Decorative circles */}
        <div className="absolute top-10 right-10 w-32 h-32 bg-blue-300 rounded-full opacity-30"></div>
        <div className="absolute top-5 right-20 w-20 h-20 bg-blue-200 rounded-full opacity-20"></div>
        <div className="absolute bottom-20 left-10 w-40 h-40 bg-blue-700 rounded-full opacity-20"></div>
        <div className="absolute top-1/2 left-1/4 w-24 h-24 bg-blue-300 rounded-full opacity-15"></div>
        
        {/* Company Name / simple "V" logo */}
        <div className="absolute top-8 left-8 flex items-center space-x-2 text-white">
          {/* place small V icon (vlogo.png) under public; or overwrite this file with any image */}
          <img src="/logo.png" alt="V Logo" className="w-20 h-20 object-contain" />
          <span className="text-sm font-semibold uppercase tracking-wide">Vbeyond Corporation</span>
        </div>

        {/* Main Content */}
        <div className="relative z-10 text-white">
          <p className="text-sm mb-3 opacity-90">Nice to see you again</p>
          <h1 className="text-5xl font-bold mb-6 tracking-wide">WELCOME BACK</h1>
          <p className="text-sm leading-relaxed opacity-80 max-w-md">
           
          </p>
        </div>
      </div>

      {/* Right Section - Login Form */}
      <div className="md:w-1/2 p-12 flex flex-col justify-center">
        <div className="max-w-md w-full mx-auto">
          <h2 className="text-2xl font-semibold text-gray-800 mb-2">Login Account</h2>
          <div className="w-16 h-1 bg-blue-500 mb-8"></div>

          <form onSubmit={handleSubmit} className="space-y-6">
            {/* Error Message */}
            {error && (
              <div className="bg-red-50 border border-red-200 text-red-600 px-4 py-3 rounded-lg text-sm">
                {error}
              </div>
            )}

            <p className="text-sm text-gray-500">
              Sign in with your company Microsoft Entra ID account. Passwords are managed by Microsoft.
            </p>

            {/* Submit Button */}
            <button
              type="submit"
              disabled={submitting}
              className="w-full bg-blue-500 hover:bg-blue-600 disabled:opacity-70 disabled:cursor-not-allowed text-white font-semibold py-3 px-6 rounded-full transition-all duration-300 transform hover:scale-[1.02] shadow-md hover:shadow-lg uppercase tracking-wide"
            >
              {submitting ? 'Opening Microsoft...' : 'Sign in with Microsoft'}
            </button>
          </form>
        </div>
      </div>
      </div>
    </div>
  )
}

export default LoginForm

