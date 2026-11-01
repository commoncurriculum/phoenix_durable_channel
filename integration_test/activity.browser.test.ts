import { ActivityTracker } from "@supergrain/activity"
import { expect, onTestFinished, test } from "vitest"
import type { CloseDetails } from "../assets/js/phoenix_durable_channel/actors"
import { join, openSocket, topic, waitFor } from "./support/helpers"

function tracker(): ActivityTracker {
  const activity = new ActivityTracker()
  onTestFinished(activity.attachDOM(document))
  window.dispatchEvent(new Event("focus"))
  return activity
}

const away = () => window.dispatchEvent(new Event("blur"))
const back = () => window.dispatchEvent(new Event("focus"))

test("suspends while the user is away, and reconnects the moment they are back", async () => {
  const activity = tracker()
  const socket = openSocket("/socket", {
    activity,
    disconnectAfterInactiveMs: 100,
    rejoinAfterMs: () => 10,
  })
  const closes: Array<CloseDetails> = []
  socket.onClose((event) => closes.push(event))
  socket.connect()
  const channel = socket.channel(topic("activity"))
  await join(channel)

  const heartbeats: Array<string> = []
  socket.onMessage((message) => {
    if (message.topic === "phoenix") heartbeats.push(message.event)
  })
  away()
  back()
  await waitFor(() => heartbeats.length === 1)
  expect(closes).toEqual([])

  away()
  await waitFor(() => socket.connectionState() === "closed")
  expect(closes).toEqual([{ code: 1000, reason: "inactive", wasClean: true }])
  await waitFor(() => channel.state === "errored")

  back()
  await waitFor(() => channel.state === "joined")
})

test("a user coming back skips the wait before the next reconnect", async () => {
  const activity = tracker()
  const socket = openSocket("/socket", {
    activity,
    reconnectAfterMs: () => 60_000,
    rejoinAfterMs: () => 10,
  })
  socket.connect()
  const channel = socket.channel(topic("comeback"))
  await join(channel)
  channel.push("drain")
  await waitFor(() => channel.state === "errored")
  away()
  back()
  await waitFor(() => channel.state === "joined")
})

test("a socket that is not connected stays that way while the user is away", async () => {
  away()
  const activity = tracker()
  away()
  const socket = openSocket("/socket", {
    activity,
    disconnectAfterInactiveMs: 10,
  })
  await new Promise((resolve) => setTimeout(resolve, 50))
  back()
  expect(socket.connectionState()).toBe("closed")
  socket.connect()
  await waitFor(() => socket.isConnected())

  socket.disconnect()
  socket.connect()
  await waitFor(() => socket.isConnected())
  away()
  await waitFor(() => socket.connectionState() === "closed")
})
