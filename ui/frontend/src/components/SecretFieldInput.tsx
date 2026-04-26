/**
 * SecretFieldInput — credential field with a mode picker.
 *
 * Stores/emits the raw YAML-ready value:
 *   Direct  → the typed string (shown masked)
 *   Env var → ${VAR_NAME}
 *   Vault   → vault:secret/path#field
 *
 * Mode is auto-detected from the incoming value.
 */
import { useEffect, useRef, useState } from 'react'
import { ChevronDown, KeyRound, Lock } from 'lucide-react'

type Mode = 'direct' | 'env' | 'vault'

function detectMode(v: string): Mode {
  if (v.startsWith('vault:')) return 'vault'
  if (v.startsWith('${'))     return 'env'
  return 'direct'
}

interface Props {
  value: string
  onChange: (v: string) => void
  placeholder?: string
  isPassword?: boolean   // if true, direct-mode input uses type="password"
  inputStyle?: React.CSSProperties
}

export default function SecretFieldInput({ value, onChange, placeholder, isPassword = true, inputStyle = {} }: Props) {
  const [open, setOpen] = useState(false)
  const dropRef = useRef<HTMLDivElement>(null)

  const mode = detectMode(value)

  // Env var: strip ${ } wrapper for the text input
  const envVar = mode === 'env' ? value.slice(2, -1) : ''

  // Vault: parse vault:path#field
  const vaultRaw = mode === 'vault' ? value.slice(6) : ''
  const hashIdx = vaultRaw.lastIndexOf('#')
  const vaultPath  = hashIdx >= 0 ? vaultRaw.slice(0, hashIdx) : vaultRaw
  const vaultField = hashIdx >= 0 ? vaultRaw.slice(hashIdx + 1) : ''

  // Close dropdown on outside click
  useEffect(() => {
    const h = (e: MouseEvent) => {
      if (dropRef.current && !dropRef.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [])

  const switchMode = (m: Mode) => {
    setOpen(false)
    if (m === 'direct') { onChange(''); return }
    if (m === 'env')    { onChange('${'); return }
    if (m === 'vault')  { onChange('vault:#'); return }
  }

  const base: React.CSSProperties = {
    background: '#0f172a', border: '1px solid #334155', borderRadius: 6,
    padding: '8px 12px', color: '#e2e8f0', fontSize: 13, outline: 'none',
    boxSizing: 'border-box', width: '100%', ...inputStyle,
  }

  const modeColors: Record<Mode, string> = {
    direct: '#475569',
    env:    '#0369a1',
    vault:  '#7c3aed',
  }

  const modeLabels: Record<Mode, string> = {
    direct: 'Direct',
    env:    'Env',
    vault:  'Vault',
  }

  return (
    <div style={{ position: 'relative', display: 'flex', gap: 6, alignItems: 'flex-start', flexDirection: 'column' }}>
      <div style={{ display: 'flex', gap: 6, width: '100%', alignItems: 'center' }}>
        {/* Main input area — changes shape based on mode */}
        <div style={{ flex: 1, position: 'relative' }}>
          {mode === 'direct' && (
            <input
              style={base}
              type={isPassword ? 'password' : 'text'}
              value={value}
              onChange={e => onChange(e.target.value)}
              placeholder={placeholder}
            />
          )}

          {mode === 'env' && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 0 }}>
              <span style={{
                background: '#0c2233', border: '1px solid #0369a1', borderRight: 'none',
                borderRadius: '6px 0 0 6px', padding: '8px 10px', color: '#38bdf8',
                fontSize: 13, fontFamily: 'monospace', whiteSpace: 'nowrap', flexShrink: 0,
              }}>${'{'}</span>
              <input
                style={{ ...base, borderRadius: '0 6px 6px 0', borderLeft: '1px solid #0369a1', fontFamily: 'monospace' }}
                value={envVar}
                onChange={e => onChange(e.target.value ? `\${${e.target.value}}` : '\${')}
                placeholder="MY_ENV_VAR"
              />
            </div>
          )}

          {mode === 'vault' && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              <div style={{ display: 'flex', gap: 6 }}>
                <div style={{ flex: 1 }}>
                  <input
                    style={{ ...base, fontFamily: 'monospace', borderColor: '#7c3aed' }}
                    value={vaultPath}
                    onChange={e => onChange(`vault:${e.target.value}#${vaultField}`)}
                    placeholder="secret/prod/myapp"
                  />
                </div>
                <div style={{ width: 130 }}>
                  <input
                    style={{ ...base, fontFamily: 'monospace', borderColor: '#7c3aed' }}
                    value={vaultField}
                    onChange={e => onChange(`vault:${vaultPath}#${e.target.value}`)}
                    placeholder="field"
                  />
                </div>
              </div>
              {(vaultPath || vaultField) && (
                <div style={{ fontSize: 11, color: '#a78bfa', fontFamily: 'monospace', paddingLeft: 2 }}>
                  vault:{vaultPath}#{vaultField}
                </div>
              )}
            </div>
          )}
        </div>

        {/* Mode picker button */}
        <div ref={dropRef} style={{ position: 'relative', flexShrink: 0 }}>
          <button
            type="button"
            onClick={() => setOpen(o => !o)}
            title="Change secrets provider"
            style={{
              display: 'flex', alignItems: 'center', gap: 4,
              background: '#0f172a', border: `1px solid ${modeColors[mode]}`,
              borderRadius: 6, padding: '7px 10px', cursor: 'pointer',
              color: modeColors[mode], fontSize: 11, fontWeight: 600,
            }}
          >
            {mode === 'vault' ? <Lock size={12} /> : mode === 'env' ? <KeyRound size={12} /> : <Lock size={12} color="#334155" />}
            {modeLabels[mode]}
            <ChevronDown size={11} />
          </button>

          {open && (
            <div style={{
              position: 'absolute', top: '100%', right: 0, zIndex: 200, marginTop: 4,
              background: '#1e293b', border: '1px solid #334155', borderRadius: 8,
              boxShadow: '0 8px 24px rgba(0,0,0,0.5)', minWidth: 200, overflow: 'hidden',
            }}>
              <ModeOption
                active={mode === 'direct'}
                label="Direct value"
                description="Stored in config.yml"
                color="#475569"
                onClick={() => switchMode('direct')}
              />
              <ModeOption
                active={mode === 'env'}
                label="Environment variable"
                description={'${MY_VAR} resolved at startup'}
                color="#0369a1"
                onClick={() => switchMode('env')}
              />
              <ModeOption
                active={mode === 'vault'}
                label="HashiCorp Vault"
                description="vault:path#field"
                color="#7c3aed"
                onClick={() => switchMode('vault')}
              />
            </div>
          )}
        </div>
      </div>

      {mode === 'vault' && (
        <div style={{ fontSize: 11, color: '#64748b', paddingLeft: 2 }}>
          Path: <code style={{ color: '#94a3b8' }}>secret/prod/myapp</code>&nbsp;&nbsp;·&nbsp;&nbsp;
          Field: the key name inside that secret (e.g. <code style={{ color: '#94a3b8' }}>password</code>)
        </div>
      )}
      {mode === 'env' && (
        <div style={{ fontSize: 11, color: '#64748b', paddingLeft: 2 }}>
          Set <code style={{ color: '#94a3b8' }}>export {envVar || 'MY_VAR'}=yourvalue</code> before starting the daemon.
        </div>
      )}
    </div>
  )
}

function ModeOption({ active, label, description, color, onClick }: {
  active: boolean; label: string; description: string; color: string; onClick: () => void
}) {
  return (
    <div
      onClick={onClick}
      style={{
        padding: '10px 14px', cursor: 'pointer', borderBottom: '1px solid #0f172a',
        background: active ? '#0f172a' : 'transparent',
      }}
    >
      <div style={{ color: active ? color : '#e2e8f0', fontWeight: active ? 700 : 500, fontSize: 13 }}>
        {active && '✓ '}{label}
      </div>
      <div style={{ color: '#475569', fontSize: 11, marginTop: 2, fontFamily: 'monospace' }}>{description}</div>
    </div>
  )
}
