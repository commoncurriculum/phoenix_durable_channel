import { expect, test } from "vitest"
import {
  Presence,
  type PresenceEntry,
  type PresenceState,
} from "../assets/js/phoenix_durable_channel/presence"
import { answer, join, openSocket, waitFor } from "./support/helpers"

const entry = (...refs: Array<string>): PresenceEntry => ({
  metas: refs.map((phx_ref) => ({ phx_ref })),
})

test("merges presence state and diffs, and reports joins and leaves", () => {
  const events: Array<string> = []
  const onJoin = (key: string, current: PresenceEntry | undefined, joined: PresenceEntry) =>
    events.push(`join ${key} ${current ? current.metas.length : "new"} +${joined.metas.length}`)
  const onLeave = (key: string, current: PresenceEntry, left: PresenceEntry) =>
    events.push(`leave ${key} ${current.metas.length} -${left.metas.length}`)

  const first = Presence.syncState({}, { alice: entry("1"), bob: entry("2") }, onJoin, onLeave)
  expect(Presence.list(first, (key) => key)).toEqual(["alice", "bob"])

  const second = Presence.syncState(
    first,
    { alice: entry("1", "3"), carol: entry("4") },
    onJoin,
    onLeave,
  )
  expect(Presence.list(second)).toEqual([entry("1", "3"), entry("4")])
  expect(Presence.list(first, (key) => key)).toEqual(["alice", "bob"])

  const third = Presence.syncState(
    second,
    { alice: entry("5", "3"), carol: entry("4") },
    onJoin,
    onLeave,
  )
  expect(third.alice).toEqual(entry("3", "5"))

  const fourth = Presence.syncDiff(third, {
    joins: { carol: entry("6") },
    leaves: { alice: entry("3", "5"), dave: entry("7") },
  })
  expect(Presence.list(fourth, (key, presence) => `${key}:${presence.metas.length}`)).toEqual([
    "carol:2",
  ])

  expect(events).toEqual([
    "join alice new +1",
    "join bob new +1",
    "join alice 1 +1",
    "join carol new +1",
    "leave bob 0 -1",
    "join alice 2 +1",
    "leave alice 2 -1",
  ])

  const awkward: PresenceState = JSON.parse(
    '{"toString": {"metas": [{"phx_ref": "1"}]}, "__proto__": {"metas": [{"phx_ref": "2"}]}}',
  )
  const synced = Presence.syncState({}, awkward)
  expect(Presence.list(synced, (key) => key).sort()).toEqual(["__proto__", "toString"])
  expect(Object.getPrototypeOf(synced)).toBe(null)
  const left = Presence.syncDiff(
    synced,
    JSON.parse('{"joins": {}, "leaves": {"__proto__": {"metas": [{"phx_ref": "2"}]}}}'),
  )
  expect(Presence.list(left, (key) => key)).toEqual(["toString"])
})

test("tracks who is on a channel of a real server", async () => {
  const socket = openSocket()
  socket.connect()
  const topic = `presence:${Math.random().toString(36).slice(2)}`
  const channel = socket.channel(topic, { name: "alice" })
  const presence = new Presence(channel)
  const joined: Array<string> = []
  const left: Array<string> = []
  let syncs = 0
  presence.onJoin((key) => joined.push(key))
  presence.onLeave((key) => left.push(key))
  presence.onSync(() => syncs++)
  expect(presence.inPendingSyncState()).toBe(true)
  await join(channel)
  await waitFor(() => presence.list().length === 1)
  expect(presence.inPendingSyncState()).toBe(false)

  const other = openSocket()
  other.connect()
  const otherChannel = other.channel(topic, { name: "bob" })
  await join(otherChannel)
  await waitFor(() => presence.list().length === 2)
  expect(presence.list((key) => key).sort()).toEqual(["alice", "bob"])

  expect((await answer(otherChannel.push("update", { status: "away" }))).status).toBe("ok")
  await waitFor(() => presence.state.bob?.metas[0]?.status === "away")

  await answer(otherChannel.leave())
  await waitFor(() => presence.list().length === 1)
  expect(joined).toContain("bob")
  expect(left).toContain("bob")
  expect(syncs).toBeGreaterThanOrEqual(4)
})

test("holds diffs that arrive before the state, and reads custom event names", () => {
  const socket = openSocket()
  const channel = socket.channel("presence:offline")
  const presence = new Presence(channel, {
    events: { state: "state", diff: "diff" },
  })
  channel.join()
  const trigger = (event: string, payload: unknown) =>
    channel.receiveMessage({
      topic: channel.topic,
      event,
      payload,
      ref: null,
      join_ref: null,
    })

  trigger("diff", { joins: { bob: entry("2") }, leaves: {} })
  expect(presence.list()).toEqual([])
  expect(presence.pendingDiffs).toHaveLength(1)

  trigger("state", { alice: entry("1") })
  expect(presence.list((key) => key)).toEqual(["alice", "bob"])
  expect(presence.pendingDiffs).toEqual([])

  trigger("state", { bob: entry("2") })
  expect(presence.list((key) => key)).toEqual(["bob"])
})
