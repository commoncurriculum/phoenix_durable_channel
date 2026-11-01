import { expect, test } from "vitest"
import type { Channel } from "../assets/js/phoenix_durable_channel/channel"
import { answer, join, openSocket, waitFor } from "./support/helpers"

const fast = { rejoinAfterMs: () => 10, reconnectAfterMs: () => 10 }

function updates(channel: Channel): Array<string> {
  const bodies: Array<string> = []
  channel.on("update", (payload) => bodies.push((payload as { body: string }).body))
  return bodies
}

test.each(["memory", "mongo", "s2", "durable-streams"])(
  "the %s store is the channel's ledger and its stream",
  async (store) => {
    const topic = `durable:store-${store}:${Math.random().toString(36).slice(2)}`
    const params = { ledger: store, stream: store }
    const socket = openSocket("/socket", fast)
    socket.connect()
    const channel = socket.channel(topic, params, { durable: true, after: 0 })
    const seen = updates(channel)
    await join(channel)
    expect((await answer(channel.push("publish", { body: "before" }))).status).toBe("ok")

    expect(await answer(channel.push("action", { body: "first" }, { durable: true }))).toEqual({
      status: "ok",
      response: { body: "first" },
    })

    // Handled and recorded, then the connection dies: the resend gets the recorded reply.
    const lost = channel.push("action", { body: "lose_reply" }, { durable: true })
    await waitFor(() => channel.state !== "joined")
    const waiting = channel.push("action", { body: "while away" }, { durable: true })
    expect(await answer(lost)).toEqual({ status: "ok", response: {} })
    expect(await answer(waiting)).toEqual({
      status: "ok",
      response: { body: "while away" },
    })
    const refused = channel.push("action", { body: "refuse_and_lose" }, { durable: true })
    expect(await answer(refused)).toEqual({
      status: "error",
      response: { reason: "refused" },
    })

    expect((await answer(channel.push("publish", { body: "after" }))).status).toBe("ok")
    await waitFor(() => seen.length === 2)
    expect(seen).toEqual(["before", "after"])

    // Another connection for the same client numbers on from what the store recorded.
    await new Promise<void>((resolve) => socket.disconnect(resolve))
    const again = openSocket("/socket", fast)
    again.connect()
    const resumed = again.channel(topic, params, { durable: true, after: 2 })
    await join(resumed)
    expect(await answer(resumed.push("action", { body: "number" }, { durable: true }))).toEqual({
      status: "ok",
      response: { number: 5 },
    })
  },
)
