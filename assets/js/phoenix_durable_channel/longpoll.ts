import type { Actor } from "xstate"
import { ignore, startMachine, type TransportConnection } from "./actors"
import { SOCKET_STATES, TRANSPORTS } from "./constants"
import { longPollMachine } from "./machines/longpoll"

const DEFAULT_LONGPOLL_TIMEOUT = 20_000

export class LongPoll implements TransportConnection {
  timeout = DEFAULT_LONGPOLL_TIMEOUT
  binaryType: BinaryType = "arraybuffer"
  onopen: TransportConnection["onopen"] = ignore
  onerror: TransportConnection["onerror"] = ignore
  onmessage: TransportConnection["onmessage"] = ignore
  onclose: TransportConnection["onclose"] = ignore
  readonly actor: Actor<typeof longPollMachine>

  constructor(endPoint: string) {
    this.actor = startMachine(longPollMachine, (later) => ({
      hooks: {
        endpoint: endPoint
          .replace("ws://", "http://")
          .replace("wss://", "https://")
          .replace(new RegExp(`(.*)/${TRANSPORTS.websocket}`), `$1/${TRANSPORTS.longpoll}`),
        timeout: () => this.timeout,
        opened: () => later(() => this.onopen({})),
        message: (data) => later(() => this.onmessage({ data })),
        errored: (error) => later(() => this.onerror(error)),
        closed: (details) => later(() => this.onclose(details)),
      },
    }))
  }

  get readyState(): number {
    const snapshot = this.actor.getSnapshot()
    if (snapshot.status === "done") return SOCKET_STATES.closed
    if (snapshot.matches({ running: { polling: { open: "polling" } } })) return SOCKET_STATES.open
    return SOCKET_STATES.connecting
  }

  send(body: string | ArrayBuffer): void {
    this.actor.send({
      type: "SEND",
      frame: typeof body === "string" ? body : arrayBufferToBase64(body),
    })
  }

  close(code = 1000, reason = "", wasClean = true): void {
    this.actor.send({ type: "CLOSE", code, reason, wasClean })
  }
}

function arrayBufferToBase64(buffer: ArrayBuffer): string {
  let binary = ""
  for (const byte of new Uint8Array(buffer)) binary += String.fromCharCode(byte)
  return btoa(binary)
}
