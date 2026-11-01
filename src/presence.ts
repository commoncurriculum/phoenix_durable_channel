import type { Channel } from "./channel"
import { decodePresenceDiff, decodePresenceState } from "./wire"

export interface PresenceMeta {
  phx_ref: string
  [key: string]: unknown
}

export interface PresenceEntry {
  metas: Array<PresenceMeta>
  [key: string]: unknown
}

export type PresenceState = Record<string, PresenceEntry>
export type PresenceDiff = { joins: PresenceState; leaves: PresenceState }
type OnJoin = (key: string, current: PresenceEntry | undefined, joined: PresenceEntry) => void
type OnLeave = (key: string, current: PresenceEntry, left: PresenceEntry) => void

const clone = <T>(value: T): T => structuredClone(value)
// Presence keys come from the server. With no prototype, a key like "toString" or
// "__proto__" is just a key (phoenix.js 1.7 crashes on them, CVE-2026-56812).
const presenceState = (state: PresenceState = {}): PresenceState =>
  Object.assign(Object.create(null), clone(state))

/** Tracks Phoenix.Presence state on a channel, as phoenix.js's Presence does. */
export class Presence {
  state: PresenceState = presenceState()
  pendingDiffs: Array<PresenceDiff> = []
  joinRef: string | null = null
  private caller: { onJoin: OnJoin; onLeave: OnLeave; onSync: () => void } = {
    onJoin: () => {},
    onLeave: () => {},
    onSync: () => {},
  }

  constructor(
    readonly channel: Channel,
    options: { events?: { state: string; diff: string } } = {},
  ) {
    const events = options.events ?? {
      state: "presence_state",
      diff: "presence_diff",
    }

    channel.on(events.state, (newState) => {
      const { onJoin, onLeave, onSync } = this.caller
      this.joinRef = channel.joinRef()
      this.state = Presence.syncState(this.state, decodePresenceState(newState), onJoin, onLeave)
      for (const diff of this.pendingDiffs)
        this.state = Presence.syncDiff(this.state, diff, onJoin, onLeave)
      this.pendingDiffs = []
      onSync()
    })

    channel.on(events.diff, (diff) => {
      const { onJoin, onLeave, onSync } = this.caller
      if (this.inPendingSyncState()) {
        this.pendingDiffs.push(decodePresenceDiff(diff))
      } else {
        this.state = Presence.syncDiff(this.state, decodePresenceDiff(diff), onJoin, onLeave)
        onSync()
      }
    })
  }

  onJoin(callback: OnJoin): void {
    this.caller.onJoin = callback
  }

  onLeave(callback: OnLeave): void {
    this.caller.onLeave = callback
  }

  onSync(callback: () => void): void {
    this.caller.onSync = callback
  }

  list(): Array<PresenceEntry>
  list<T>(by: (key: string, presence: PresenceEntry) => T): Array<T>
  list<T>(by?: (key: string, presence: PresenceEntry) => T): Array<T | PresenceEntry> {
    return by ? Presence.list(this.state, by) : Presence.list(this.state)
  }

  inPendingSyncState(): boolean {
    return this.joinRef === null || this.joinRef !== this.channel.joinRef()
  }

  static syncState(
    currentState: PresenceState,
    newState: PresenceState,
    onJoin?: OnJoin,
    onLeave?: OnLeave,
  ): PresenceState {
    const state = presenceState(currentState)
    const next = presenceState(newState)
    const joins = presenceState()
    const leaves = presenceState()

    for (const [key, presence] of Object.entries(state)) if (!next[key]) leaves[key] = presence
    for (const [key, newPresence] of Object.entries(next)) {
      const currentPresence = state[key]
      if (!currentPresence) {
        joins[key] = newPresence
        continue
      }
      const newRefs = newPresence.metas.map((meta) => meta.phx_ref)
      const currentRefs = currentPresence.metas.map((meta) => meta.phx_ref)
      const joinedMetas = newPresence.metas.filter((meta) => !currentRefs.includes(meta.phx_ref))
      const leftMetas = currentPresence.metas.filter((meta) => !newRefs.includes(meta.phx_ref))
      if (joinedMetas.length > 0) joins[key] = { ...newPresence, metas: joinedMetas }
      if (leftMetas.length > 0) leaves[key] = { ...clone(currentPresence), metas: leftMetas }
    }
    return Presence.syncDiff(state, { joins, leaves }, onJoin, onLeave)
  }

  static syncDiff(
    currentState: PresenceState,
    diff: PresenceDiff,
    onJoin: OnJoin = () => {},
    onLeave: OnLeave = () => {},
  ): PresenceState {
    const state = presenceState(currentState)
    const joins = presenceState(diff.joins)
    const leaves = presenceState(diff.leaves)

    for (const [key, newPresence] of Object.entries(joins)) {
      const currentPresence = state[key]
      const joined = clone(newPresence)
      if (currentPresence) {
        const joinedRefs = joined.metas.map((meta) => meta.phx_ref)
        joined.metas.unshift(
          ...currentPresence.metas.filter((meta) => !joinedRefs.includes(meta.phx_ref)),
        )
      }
      state[key] = joined
      onJoin(key, currentPresence, newPresence)
    }
    for (const [key, leftPresence] of Object.entries(leaves)) {
      const currentPresence = state[key]
      if (!currentPresence) continue
      const refsToRemove = leftPresence.metas.map((meta) => meta.phx_ref)
      currentPresence.metas = currentPresence.metas.filter(
        (meta) => !refsToRemove.includes(meta.phx_ref),
      )
      onLeave(key, currentPresence, leftPresence)
      if (currentPresence.metas.length === 0) delete state[key]
    }
    return state
  }

  static list(presences: PresenceState): Array<PresenceEntry>
  static list<T>(
    presences: PresenceState,
    chooser: (key: string, presence: PresenceEntry) => T,
  ): Array<T>
  static list<T>(
    presences: PresenceState,
    chooser?: (key: string, presence: PresenceEntry) => T,
  ): Array<T | PresenceEntry> {
    return Object.entries(presences).map(([key, presence]) =>
      chooser ? chooser(key, presence) : presence,
    )
  }
}
