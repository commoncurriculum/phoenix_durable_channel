import { CHANNEL_EVENTS } from "./constants"
import { decodeFrame } from "./wire"

export type Payload = unknown

export interface Message {
  join_ref: string | null
  ref: string | null
  topic: string
  event: string
  payload: Payload
  /** A durable message's number. Fire-and-forget messages have none. */
  number?: number
}

export type EncodedMessage = string | ArrayBuffer

const HEADER_LENGTH = 1
const META_LENGTH = 4
const KINDS = { push: 0, reply: 1, broadcast: 2 } as const

/**
 * Phoenix's V2 wire format: a JSON array `[join_ref, ref, topic, event, payload]`, or the
 * binary frame Phoenix uses when the payload is an ArrayBuffer. A durable message adds its
 * number as a sixth element, which the server reads with `PhoenixDurableChannel.Serializer`.
 */
export const Serializer = {
  encode<T>(message: Message, callback: (encoded: EncodedMessage) => T): T {
    if (message.payload instanceof ArrayBuffer)
      return callback(binaryEncode(message, message.payload))
    const { join_ref, ref, topic, event, payload, number } = message
    const frame = [join_ref, ref, topic, event, payload]
    return callback(JSON.stringify(number === undefined ? frame : [...frame, number]))
  },

  decode<T>(raw: EncodedMessage, callback: (message: Message) => T): T {
    if (raw instanceof ArrayBuffer) return callback(binaryDecode(raw))
    const [join_ref, ref, topic, event, payload, number] = decodeFrame(raw)
    const message = { join_ref, ref, topic, event, payload }
    return callback(number === undefined ? message : { ...message, number })
  },
}

function binaryEncode(message: Message, payload: ArrayBuffer): ArrayBuffer {
  const joinRef = message.join_ref ?? ""
  const ref = message.ref ?? ""
  const { topic, event } = message
  const metaLength = META_LENGTH + joinRef.length + ref.length + topic.length + event.length
  const header = new ArrayBuffer(HEADER_LENGTH + metaLength)
  const view = new DataView(header)
  let offset = 0
  view.setUint8(offset++, KINDS.push)
  view.setUint8(offset++, joinRef.length)
  view.setUint8(offset++, ref.length)
  view.setUint8(offset++, topic.length)
  view.setUint8(offset++, event.length)
  for (const text of [joinRef, ref, topic, event]) {
    for (const character of text) view.setUint8(offset++, character.charCodeAt(0))
  }
  const combined = new Uint8Array(header.byteLength + payload.byteLength)
  combined.set(new Uint8Array(header), 0)
  combined.set(new Uint8Array(payload), header.byteLength)
  return combined.buffer
}

function binaryDecode(buffer: ArrayBuffer): Message {
  const view = new DataView(buffer)
  const decoder = new TextDecoder()
  const read = (start: number, size: number) => decoder.decode(buffer.slice(start, start + size))
  const kind = view.getUint8(0)

  if (kind === KINDS.push) {
    const joinRefSize = view.getUint8(1)
    const topicSize = view.getUint8(2)
    const eventSize = view.getUint8(3)
    // A push has no ref, so its header is one byte shorter.
    const start = HEADER_LENGTH + META_LENGTH - 1
    const joinRef = read(start, joinRefSize)
    const topic = read(start + joinRefSize, topicSize)
    const event = read(start + joinRefSize + topicSize, eventSize)
    const payload = buffer.slice(start + joinRefSize + topicSize + eventSize)
    return { join_ref: joinRef, ref: null, topic, event, payload }
  }

  if (kind === KINDS.reply) {
    const joinRefSize = view.getUint8(1)
    const refSize = view.getUint8(2)
    const topicSize = view.getUint8(3)
    const eventSize = view.getUint8(4)
    const start = HEADER_LENGTH + META_LENGTH
    const joinRef = read(start, joinRefSize)
    const ref = read(start + joinRefSize, refSize)
    const topic = read(start + joinRefSize + refSize, topicSize)
    const status = read(start + joinRefSize + refSize + topicSize, eventSize)
    const response = buffer.slice(start + joinRefSize + refSize + topicSize + eventSize)
    return {
      join_ref: joinRef,
      ref,
      topic,
      event: CHANNEL_EVENTS.reply,
      payload: { status, response },
    }
  }

  if (kind === KINDS.broadcast) {
    const topicSize = view.getUint8(1)
    const eventSize = view.getUint8(2)
    const start = HEADER_LENGTH + 2
    const topic = read(start, topicSize)
    const event = read(start + topicSize, eventSize)
    const payload = buffer.slice(start + topicSize + eventSize)
    return { join_ref: null, ref: null, topic, event, payload }
  }

  throw new Error(`Unknown binary message kind ${kind}`)
}
