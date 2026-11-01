import { createReactive } from "@supergrain/kernel"
import type { Actor, SnapshotFrom } from "xstate"
import { startMachine } from "./actors"
import { pushMachine } from "./machines/push"
import { Reply } from "./reply"

export type ReplyStatus = "ok" | "error" | "timeout"
export type PushState = "buffered" | "sent" | ReplyStatus

function pushState(snapshot: SnapshotFrom<typeof pushMachine>): PushState {
  if (snapshot.matches({ pending: "sent" })) return "sent"
  if (snapshot.matches("pending")) return "buffered"
  if (snapshot.matches("ok")) return "ok"
  if (snapshot.matches("error")) return "error"
  return "timeout"
}

export class Push {
  readonly status: { state: PushState }
  readonly actor: Actor<typeof pushMachine>
  private readonly reply = new Reply()

  constructor(
    readonly event: string,
    readonly payload: () => unknown,
    timeout: number,
    onFinish: (push: Push) => void,
  ) {
    this.status = createReactive<{ state: PushState }>({ state: "buffered" })
    this.actor = startMachine(
      pushMachine,
      () => ({ timeout }),
      (snapshot) => {
        const state = pushState(snapshot)
        if (state === this.status.state) return
        this.status.state = state
        if (state === "buffered" || state === "sent") return
        onFinish(this)
        this.reply.answer(state, snapshot.context.response)
      },
    )
  }

  get ref(): string | null {
    return this.actor.getSnapshot().context.ref
  }

  receive(status: ReplyStatus, callback: (response: unknown) => void): this {
    this.reply.receive(status, callback)
    return this
  }
}
