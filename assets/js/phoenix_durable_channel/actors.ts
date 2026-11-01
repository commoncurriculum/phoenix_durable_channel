import { Effect, Fiber } from "effect"
import {
  createActor,
  fromCallback,
  fromPromise,
  type Actor,
  type AnyActorLogic,
  type InputFrom,
  type SnapshotFrom,
} from "xstate"
import type { EncodedMessage } from "./serializer"

export const sleep = fromPromise<void, { ms: number | null }>(({ input, signal }) =>
  Effect.runPromise(input.ms === null ? Effect.never : Effect.sleep(input.ms), { signal }),
)

export type Later = (effect: () => void) => void

export function startMachine<Logic extends AnyActorLogic>(
  logic: Logic,
  input: (later: Later) => InputFrom<Logic>,
  observe: (snapshot: SnapshotFrom<Logic>) => void = ignore,
): Actor<Logic> {
  const queue: Array<() => void> = []
  const actor = createActor(logic, {
    input: input((effect) => queue.push(effect)),
  })
  actor.subscribe((snapshot) => {
    observe(snapshot)
    for (const effect of queue.splice(0)) effect()
  })
  return actor.start()
}

export interface CloseDetails {
  code: number
  reason: string
  wasClean: boolean
}

export interface TransportConnection {
  binaryType?: BinaryType
  timeout?: number
  onopen: (event?: unknown) => void
  onerror: (error: unknown) => void
  onmessage: (event: { data: EncodedMessage }) => void
  onclose: (event: CloseDetails) => void
  send(data: string | ArrayBuffer): void
  close(code?: number, reason?: string): void
}

export const ignore = () => {}

export type TransportConstructor = new (
  endPoint: string,
  protocols?: Array<string>,
) => TransportConnection | WebSocket

export type TransportEvent =
  | { type: "TRANSPORT_OPEN" }
  | { type: "TRANSPORT_ERROR"; error: unknown }
  | { type: "TRANSPORT_MESSAGE"; data: EncodedMessage }
  | ({ type: "TRANSPORT_CLOSE" } & CloseDetails)

export type TransportCommand =
  { type: "SEND"; data: string | ArrayBuffer } | { type: "CLOSE"; code: number; reason: string }

export interface TransportInput {
  transport: TransportConstructor
  url: string
  protocols: Array<string> | undefined
  binaryType: BinaryType
  timeout: number
}

export const transport = fromCallback<TransportCommand, TransportInput, TransportEvent>(
  ({ input, sendBack, receive }) => {
    const connection = Effect.acquireRelease(
      Effect.sync(() => {
        const connection = new input.transport(input.url, input.protocols)
        connection.binaryType = input.binaryType
        if ("timeout" in connection) connection.timeout = input.timeout
        connection.onopen = () => sendBack({ type: "TRANSPORT_OPEN" })
        connection.onerror = (error: unknown) => sendBack({ type: "TRANSPORT_ERROR", error })
        connection.onmessage = (event: { data: EncodedMessage }) =>
          sendBack({ type: "TRANSPORT_MESSAGE", data: event.data })
        connection.onclose = (event: CloseDetails) =>
          sendBack({
            type: "TRANSPORT_CLOSE",
            code: event.code,
            reason: event.reason,
            wasClean: event.wasClean,
          })
        receive((command) => {
          if (command.type === "SEND") connection.send(command.data)
          if (command.type === "CLOSE") connection.close(command.code, command.reason)
        })
        return connection
      }),
      (connection) =>
        Effect.sync(() => {
          connection.onopen = ignore
          connection.onerror = ignore
          connection.onmessage = ignore
          connection.onclose = ignore
          connection.close(1000, "client closed")
        }),
    )
    const fiber = Effect.runFork(Effect.scoped(Effect.andThen(connection, Effect.never)))
    return () => {
      Effect.runFork(Fiber.interrupt(fiber))
    }
  },
)
