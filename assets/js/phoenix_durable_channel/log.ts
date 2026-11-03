import { createReactive } from "@supergrain/kernel"
import type { Actor, AnyEventObject } from "xstate"
import { backoff, startMachine } from "./actors"
import { logMachine, type LogEmitted } from "./machines/log"

export class Log {
  readonly status: { pending: number }
  readonly actor: Actor<typeof logMachine>

  constructor(
    backoffMs: (tries: number) => number,
    handle: (event: LogEmitted) => void,
    took: (event: AnyEventObject) => void,
  ) {
    this.status = createReactive({ pending: 0 })
    this.actor = startMachine(
      logMachine.provide({ actors: { waitToSync: backoff(backoffMs) } }),
      undefined,
      (snapshot) => {
        this.status.pending = snapshot.context.queue.length
      },
      handle,
      took,
    )
  }
}
