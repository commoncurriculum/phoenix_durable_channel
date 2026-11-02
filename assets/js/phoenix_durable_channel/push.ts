import { createReactive } from "@supergrain/kernel"
import type { Actor } from "xstate"
import { startMachine, tagged } from "./actors"
import { decodePushState } from "./constants"
import { pushMachine, type PushState, type ReplyStatus } from "./machines/push"
import { Reply } from "./reply"

export type { PushState, ReplyStatus }

export class Push {
  readonly status: { state: PushState }
  readonly actor: Actor<typeof pushMachine>
  private readonly reply = new Reply()

  constructor(
    readonly event: string,
    readonly payload: unknown,
    timeout: number,
    onFinish: (push: Push) => void,
  ) {
    this.status = createReactive<{ state: PushState }>({ state: "buffered" })
    this.actor = startMachine(
      pushMachine,
      { timeout },
      (snapshot) => {
        this.status.state = tagged(snapshot, decodePushState)
      },
      ({ status, response }) => {
        onFinish(this)
        this.reply.answer(status, response)
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
