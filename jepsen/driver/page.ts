// Runs in Chromium. Each client has its own Socket and its own durable channel,
// `jepsen:<client>`, and pushes durable `add` messages; the server appends each to the
// channel's stream as `added`. driver.mjs calls `window.jepsen`.

import { LongPoll, Socket, type Channel } from "../../src/index"

interface Client {
  socket: Socket
  channel: Channel
  // How many messages this client has pushed: the position of the last one.
  pushed: number
  unanswered: number
  // The position of every `added` the channel delivered, in delivery order.
  delivered: Array<number>
}

const clients = new Map<string, Client>()

function open(
  client: string,
  endPoint: string,
  heartbeatIntervalMs: number,
  transport: "websocket" | "longpoll",
) {
  if (clients.has(client)) return
  const socket = new Socket(endPoint, {
    heartbeatIntervalMs,
    ...(transport === "longpoll" ? { transport: LongPoll } : {}),
    reconnectAfterMs: (tries) => [50, 200, 500, 1000][tries - 1] ?? 2000,
    rejoinAfterMs: (tries) => [50, 200, 500, 1000][tries - 1] ?? 2000,
  })
  socket.connect()
  const channel = socket.channel(`jepsen:${client}`, {}, { durable: true, after: 0 })
  const state: Client = {
    socket,
    channel,
    pushed: 0,
    unanswered: 0,
    delivered: [],
  }
  channel.on("added", (payload: { value: number; position: number }) =>
    state.delivered.push(payload.position),
  )
  channel.join()
  clients.set(client, state)
}

/** Pushes `{value, position}` durable; position is its place among this client's pushes. */
function push(
  client: string,
  value: number,
): Promise<{ status: string; position: number; response: unknown }> {
  const state = clients.get(client)!
  const position = ++state.pushed
  state.unanswered++
  return new Promise((resolve) => {
    const answer = (status: string) => (response: unknown) => {
      state.unanswered--
      resolve({ status, position, response })
    }
    state.channel
      .push("add", { value, position }, { durable: true })
      .receive("ok", answer("ok"))
      .receive("error", answer("error"))
      .receive("timeout", answer("timeout"))
  })
}

function unanswered(): number {
  return [...clients.values()].reduce((sum, state) => sum + state.unanswered, 0)
}

function delivered(): Record<string, Array<number>> {
  return Object.fromEntries([...clients].map(([client, state]) => [client, state.delivered]))
}

function close() {
  for (const state of clients.values()) state.socket.disconnect()
  clients.clear()
}

declare global {
  interface Window {
    jepsen: {
      open: typeof open
      push: typeof push
      unanswered: typeof unanswered
      delivered: typeof delivered
      close: typeof close
    }
  }
}

window.jepsen = { open, push, unanswered, delivered, close }
