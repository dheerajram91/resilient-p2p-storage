/** Minimal structured logger — timestamped, level-prefixed, no dependencies. */

type Level = 'info' | 'warn' | 'error' | 'debug'

const scopeName = { value: 'node' }

export function setScope(name: string): void {
  scopeName.value = name
}

function emit(level: Level, msg: string, extra?: Record<string, unknown>): void {
  const ts = new Date().toISOString()
  const base = `${ts} [${level.toUpperCase()}] (${scopeName.value}) ${msg}`
  if (extra && Object.keys(extra).length > 0) {
    // eslint-disable-next-line no-console
    console.log(base, JSON.stringify(extra))
  } else {
    // eslint-disable-next-line no-console
    console.log(base)
  }
}

export const log = {
  info: (msg: string, extra?: Record<string, unknown>) => emit('info', msg, extra),
  warn: (msg: string, extra?: Record<string, unknown>) => emit('warn', msg, extra),
  error: (msg: string, extra?: Record<string, unknown>) => emit('error', msg, extra),
  debug: (msg: string, extra?: Record<string, unknown>) => {
    if (process.env.DEBUG) emit('debug', msg, extra)
  }
}
