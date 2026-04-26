import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import { Activity, ArrowRight, Bell, Database, Inbox, Settings, Target, Zap } from 'lucide-react'
import SourcesPage from './components/SourcesPage'
import TargetPage from './components/TargetPage'
import StatusPage from './components/StatusPage'
import SettingsPage from './components/SettingsPage'
import MappingPage from './components/MappingPage'
import AlertsPage from './components/AlertsPage'
import DLQPage from './components/DLQPage'

const NAV = [
  { to: '/',         icon: Activity,    label: 'Status'   },
  { to: '/sources',  icon: Database,    label: 'Sources'  },
  { to: '/target',   icon: Target,      label: 'Target'   },
  { to: '/mappings', icon: ArrowRight,  label: 'Mappings' },
  { to: '/alerts',   icon: Bell,        label: 'Alerts'   },
  { to: '/dlq',      icon: Inbox,       label: 'DLQ'      },
  { to: '/settings', icon: Settings,    label: 'Settings' },
]

export default function App() {
  return (
    <BrowserRouter>
      <div style={styles.shell}>
        {/* Sidebar */}
        <aside style={styles.sidebar}>
          <div style={styles.logo}>
            <Zap size={22} color="#60a5fa" />
            <span style={styles.logoText}>Dremio CDC</span>
          </div>
          <nav style={styles.nav}>
            {NAV.map(({ to, icon: Icon, label }) => (
              <NavLink key={to} to={to} end={to === '/'} style={({ isActive }) => ({
                ...styles.navLink,
                ...(isActive ? styles.navLinkActive : {}),
              })}>
                <Icon size={16} />
                <span>{label}</span>
              </NavLink>
            ))}
          </nav>
          <div style={styles.sidebarFooter}>
            <span style={{ color: '#64748b', fontSize: 11 }}>v1.0</span>
          </div>
        </aside>

        {/* Main */}
        <main style={styles.main}>
          <Routes>
            <Route path="/"         element={<StatusPage />} />
            <Route path="/sources"  element={<SourcesPage />} />
            <Route path="/target"   element={<TargetPage />} />
            <Route path="/mappings" element={<MappingPage />} />
            <Route path="/alerts"   element={<AlertsPage />} />
            <Route path="/dlq"      element={<DLQPage />} />
            <Route path="/settings" element={<SettingsPage />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}

const styles: Record<string, React.CSSProperties> = {
  shell: {
    display: 'flex', height: '100vh', background: '#0f172a', color: '#e2e8f0',
    fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
    fontSize: 14,
  },
  sidebar: {
    width: 200, background: '#0f172a', borderRight: '1px solid #1e293b',
    display: 'flex', flexDirection: 'column', flexShrink: 0,
  },
  logo: {
    display: 'flex', alignItems: 'center', gap: 10,
    padding: '20px 16px 16px', borderBottom: '1px solid #1e293b',
  },
  logoText: { fontWeight: 700, fontSize: 15, color: '#f1f5f9', letterSpacing: '-0.3px' },
  nav: { padding: '12px 8px', flex: 1, display: 'flex', flexDirection: 'column', gap: 2 },
  navLink: {
    display: 'flex', alignItems: 'center', gap: 10,
    padding: '8px 10px', borderRadius: 6, color: '#94a3b8',
    textDecoration: 'none', transition: 'all 0.15s',
  },
  navLinkActive: { background: '#1e293b', color: '#60a5fa' },
  sidebarFooter: { padding: '12px 16px', borderTop: '1px solid #1e293b' },
  main: { flex: 1, overflow: 'auto', background: '#0f172a' },
}
