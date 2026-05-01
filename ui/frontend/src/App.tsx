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
        <aside style={styles.sidebar}>
          <div style={styles.logoArea}>
            <Zap size={20} color="var(--primary)" />
            <span style={styles.logoText}>Dremio CDC</span>
          </div>
          <nav style={styles.nav}>
            {NAV.map(({ to, icon: Icon, label }) => (
              <NavLink key={to} to={to} end={to === '/'} style={({ isActive }) => ({
                ...styles.navLink,
                ...(isActive ? styles.navLinkActive : {}),
              })}>
                <Icon size={15} />
                <span>{label}</span>
              </NavLink>
            ))}
          </nav>
          <div style={styles.sidebarFooter}>
            <span style={{ color: 'var(--sidebar-border)', fontSize: 11 }}>v1.6</span>
          </div>
        </aside>

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
    display: 'flex',
    height: '100vh',
    background: 'var(--background)',
    color: 'var(--foreground)',
  },
  sidebar: {
    width: 210,
    background: 'var(--sidebar)',
    borderRight: '1px solid rgba(255,255,255,0.08)',
    display: 'flex',
    flexDirection: 'column',
    flexShrink: 0,
  },
  logoArea: {
    display: 'flex',
    alignItems: 'center',
    gap: 10,
    padding: '20px 16px 16px',
    borderBottom: '1px solid rgba(255,255,255,0.08)',
  },
  logoText: {
    fontWeight: 700,
    fontSize: 15,
    color: 'var(--sidebar-foreground)',
    letterSpacing: '-0.3px',
  },
  nav: {
    padding: '10px 8px',
    flex: 1,
    display: 'flex',
    flexDirection: 'column',
    gap: 1,
  },
  navLink: {
    display: 'flex',
    alignItems: 'center',
    gap: 10,
    padding: '7px 10px',
    borderRadius: 6,
    color: 'rgba(255,255,255,0.6)',
    textDecoration: 'none',
    fontSize: 13,
    fontWeight: 500,
    transition: 'all 0.12s',
  },
  navLinkActive: {
    background: 'var(--sidebar-accent)',
    color: 'var(--sidebar-foreground)',
    borderLeft: '3px solid var(--primary)',
    paddingLeft: 7,
  },
  sidebarFooter: {
    padding: '12px 16px',
    borderTop: '1px solid rgba(255,255,255,0.08)',
  },
  main: {
    flex: 1,
    overflow: 'auto',
    background: 'var(--background)',
  },
}
