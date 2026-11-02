import { CHANNEL_EVENTS } from "./constants"
import { decodeFrame } from "./wire"

export type Payload = unknown

export interface Message {
  join_ref: string | null
  ref: string | null
  topic: string
  event: string
  payload: Payload
  number?: number
  epoch?: string | undefined
}

export type EncodedMessage = string | ArrayBuffer

const HEADER_LENGTH = 1
const META_LENGTH = 4
const KINDS = { push: 0, reply: 1, broadcast: 2 } as const

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
    const [join_ref, ref, topic, event, payload, number, epoch] = decodeFrame(raw)
    const message = { join_ref, ref, topic, event, payload }
    return callback(number === undefined ? message : { ...message, number, epoch })
  },
}

function binaryEncode(message: Message, payload: ArrayBuffer): ArrayBuffer {
  const encoder = new TextEncoder()
  const fields = {
    join_ref: encoder.encode(message.join_ref ?? ""),
    ref: encoder.encode(message.ref ?? ""),
    topic: encoder.encode(message.topic),
    event: encoder.encode(message.event),
  }
  const sizes = Object.entries(fields).map(([name, bytes]) => {
    if (bytes.byteLength > 255)
      throw new Error(
        `unable to convert ${name} to binary: must be less than or equal to 255 bytes, but is ${bytes.byteLength} bytes`,
      )
    return bytes.byteLength
  })
  const meta = Object.values(fields)
  const header = HEADER_LENGTH + META_LENGTH + sizes.reduce((total, size) => total + size, 0)
  const combined = new Uint8Array(header + payload.byteLength)
  combined.set([KINDS.push, ...sizes], 0)
  meta.reduce((offset, bytes) => {
    combined.set(bytes, offset)
    return offset + bytes.byteLength
  }, HEADER_LENGTH + META_LENGTH)
  combined.set(new Uint8Array(payload), header)
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
