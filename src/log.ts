import { createReactive } from "@supergrain/kernel"
import type { Actor } from "xstate"
import { startMachine } from "./actors"
import { logMachine, type LogHooks } from "./machines/log"
import type { Reply } from "./reply"

/** A channel's durable messages to the server; see `logMachine`. `status.pending` counts the unanswered. */
export class Log {
  readonly status: { pending: number }
  readonly actor: Actor<typeof logMachine>

  constructor(channel: Omit<LogHooks, "answer">) {
    this.status = createReactive({ pending: 0 })
    this.actor = startMachine(
      logMachine,
      (later) => ({
        hooks: {
          ...channel,
          sync: () => later(() => channel.sync()),
          send: (entry) => later(() => channel.send(entry)),
          answer: (entry, status, response) => later(() => entry.reply.answer(status, response)),
        },
      }),
      (snapshot) => {
        this.status.pending = snapshot.context.queue.length
      },
    )
  }

  push(event: string, payload: unknown, reply: Reply): void {
    this.actor.send({
      type: "PUSH",
      entry: { event, payload, reply, number: null, ref: null },
    })
  }
}
