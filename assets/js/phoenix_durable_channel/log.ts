import { createReactive } from "@supergrain/kernel"
import type { Actor } from "xstate"
import { startMachine } from "./actors"
import { logMachine, type LogSettings, type SentEntry } from "./machines/log"
import type { Reply } from "./reply"

export interface LogChannel {
  sync(): void
  send(entry: SentEntry): void
}

export class Log {
  readonly status: { pending: number }
  readonly actor: Actor<typeof logMachine>

  constructor(settings: LogSettings, channel: LogChannel) {
    this.status = createReactive({ pending: 0 })
    this.actor = startMachine(
      logMachine,
      { settings },
      (snapshot) => {
        this.status.pending = snapshot.context.queue.length
      },
      (event) => {
        switch (event.type) {
          case "sync":
            return channel.sync()
          case "send":
            return channel.send(event.entry)
          case "answer":
            return event.entry.reply.answer(event.status, event.response)
        }
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
