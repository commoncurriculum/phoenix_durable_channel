import { Effect, Fiber } from "effect"
import {
  createActor,
  fromCallback,
  fromPromise,
  type Actor,
  type AnyActorLogic,
  type EmittedFrom,
  type InputFrom,
  type SnapshotFrom,
} from "xstate"
import type { EncodedMessage } from "./serializer"

export const sleep = fromPromise<void, { ms: number | null }>(({ input, signal }) =>
  Effect.runPromise(input.ms === null ? Effect.never : Effect.sleep(input.ms), { signal }),
)

export const backoff = (delay: (tries: number) => number) =>
  fromPromise<void, { tries: number }>(({ input, signal }) =>
    Effect.runPromise(Effect.sleep(delay(input.tries)), { signal }),
  )

export type Data<T> = {
  [K in keyof T]: T[K] extends (...args: never[]) => unknown ? never : T[K]
}

export function startMachine<Logic extends AnyActorLogic>(
  logic: Logic,
  input: InputFrom<Logic>,
  observe: (snapshot: SnapshotFrom<Logic>) => void,
  handle: (event: EmittedFrom<Logic>) => void,
): Actor<Logic> {
  const actor = createActor(logic, { input })
  actor.subscribe(observe)
  actor.on("*", (event) => {
    observe(actor.getSnapshot())
    handle(event)
  })
  return actor.start()
}

export function tagged<Tag>(snapshot: { tags: Set<string> }, decode: (tag: unknown) => Tag): Tag {
  const [tag] = snapshot.tags
  return decode(tag)
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

export type TransportKind = "websocket" | "longpoll"

export interface TransportInput {
  transport: TransportConstructor
  url: string
  protocols: Array<string> | undefined
  binaryType: BinaryType
  timeout: number
}

export const transport = (connect: (kind: TransportKind) => TransportInput) =>
  fromCallback<TransportCommand, { kind: TransportKind }, TransportEvent>(
    ({ input: { kind }, sendBack, receive }) => {
      const input = connect(kind)
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
