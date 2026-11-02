import type { Actor } from "xstate"
import { ignore, startMachine, tagged, type TransportConnection } from "./actors"
import { AUTH_TOKEN_PREFIX, SOCKET_STATES, TRANSPORTS, decodeConnectionState } from "./constants"
import { longPollMachine, type LongPollEmitted } from "./machines/longpoll"

const DEFAULT_LONGPOLL_TIMEOUT = 20_000

export class LongPoll implements TransportConnection {
  binaryType: BinaryType = "arraybuffer"
  onopen: TransportConnection["onopen"] = ignore
  onerror: TransportConnection["onerror"] = ignore
  onmessage: TransportConnection["onmessage"] = ignore
  onclose: TransportConnection["onclose"] = ignore
  readonly actor: Actor<typeof longPollMachine>

  constructor(endPoint: string, protocols?: Array<string>) {
    const [, bearer] = protocols ?? []
    const authToken = bearer === undefined ? null : atob(bearer.slice(AUTH_TOKEN_PREFIX.length))
    this.actor = startMachine(
      longPollMachine,
      {
        authToken,
        endpoint: endPoint
          .replace("ws://", "http://")
          .replace("wss://", "https://")
          .replace(new RegExp(`(.*)/${TRANSPORTS.websocket}`), `$1/${TRANSPORTS.longpoll}`),
        timeout: DEFAULT_LONGPOLL_TIMEOUT,
      },
      ignore,
      (event) => this.handle(event),
    )
  }

  get timeout(): number {
    return this.actor.getSnapshot().context.timeout
  }

  set timeout(timeout: number) {
    this.actor.send({ type: "TIMEOUT", timeout })
  }

  get readyState(): number {
    return SOCKET_STATES[tagged(this.actor.getSnapshot(), decodeConnectionState)]
  }

  send(body: string | ArrayBuffer): void {
    this.actor.send({
      type: "SEND",
      frame: typeof body === "string" ? body : arrayBufferToBase64(body),
    })
  }

  close(code = 1000, reason = "", wasClean = true): void {
    if (this.actor.getSnapshot().status === "active")
      this.actor.send({ type: "CLOSE", code, reason, wasClean })
  }

  private handle(event: LongPollEmitted): void {
    switch (event.type) {
      case "opened":
        return this.onopen({})
      case "message":
        return this.onmessage({ data: event.data })
      case "errored":
        return this.onerror(event.error)
      case "closed":
        return this.onclose({ code: event.code, reason: event.reason, wasClean: event.wasClean })
    }
  }
}

function arrayBufferToBase64(buffer: ArrayBuffer): string {
  let binary = ""
  for (const byte of new Uint8Array(buffer)) binary += String.fromCharCode(byte)
  return btoa(binary)
}
