import { expect, test } from "vitest"
import {
  Serializer,
  type EncodedMessage,
  type Message,
} from "../assets/js/phoenix_durable_channel/serializer"
import { appendParams, serialize } from "../assets/js/phoenix_durable_channel/url"

const encode = (message: Message) => Serializer.encode(message, (encoded) => encoded)
const decode = (raw: EncodedMessage) => Serializer.decode(raw, (message) => message)
const bytes = (...values: Array<number>) => new Uint8Array(values).buffer
const text = (value: string) => [...value].map((character) => character.charCodeAt(0))

test("encodes and decodes Phoenix's JSON and binary frames", () => {
  const message = {
    join_ref: "1",
    ref: "2",
    topic: "room:a",
    event: "echo",
    payload: { a: 1 },
  }
  expect(encode(message)).toBe('["1","2","room:a","echo",{"a":1}]')
  expect(decode('["1","2","room:a","echo",{"a":1}]')).toEqual(message)
  expect(decode('[null,null,"room:a","news",{}]')).toEqual({
    join_ref: null,
    ref: null,
    topic: "room:a",
    event: "news",
    payload: {},
  })

  const binaryPush = encode({
    join_ref: "1",
    ref: "2",
    topic: "t",
    event: "e",
    payload: bytes(9, 8),
  })
  expect([...new Uint8Array(binaryPush as ArrayBuffer)]).toEqual([
    0,
    1,
    1,
    1,
    1,
    ...text("12te"),
    9,
    8,
  ])
  const withoutRefs = encode({
    join_ref: null,
    ref: null,
    topic: "t",
    event: "e",
    payload: bytes(7),
  })
  expect([...new Uint8Array(withoutRefs as ArrayBuffer)]).toEqual([0, 0, 0, 1, 1, ...text("te"), 7])

  const push = decode(bytes(0, 1, 1, 1, ...text("1te"), 5, 6))
  expect(push).toMatchObject({
    join_ref: "1",
    ref: null,
    topic: "t",
    event: "e",
  })
  expect([...new Uint8Array(push.payload as ArrayBuffer)]).toEqual([5, 6])

  const reply = decode(bytes(1, 1, 1, 1, 2, ...text("12tok"), 3))
  expect(reply).toMatchObject({
    join_ref: "1",
    ref: "2",
    topic: "t",
    event: "phx_reply",
  })
  const { status, response } = reply.payload as {
    status: string
    response: ArrayBuffer
  }
  expect(status).toBe("ok")
  expect([...new Uint8Array(response)]).toEqual([3])

  const broadcast = decode(bytes(2, 1, 1, ...text("te"), 4))
  expect(broadcast).toMatchObject({
    join_ref: null,
    ref: null,
    topic: "t",
    event: "e",
  })
  expect([...new Uint8Array(broadcast.payload as ArrayBuffer)]).toEqual([4])

  expect(() => decode(bytes(7))).toThrow("Unknown binary message kind 7")
})

test("builds query strings the way Phoenix reads them", () => {
  expect(serialize({ a: 1, b: { c: "x y", d: { e: true } }, f: null })).toBe(
    "a=1&b%5Bc%5D=x%20y&b%5Bd%5D%5Be%5D=true&f=null",
  )
  expect(appendParams("/socket", {})).toBe("/socket")
  expect(appendParams("/socket", { token: "t" })).toBe("/socket?token=t")
  expect(appendParams("/socket?vsn=2", { token: "t" })).toBe("/socket?vsn=2&token=t")
})

test("measures binary metadata in UTF-8 bytes, up to 255 each", () => {
  const utf8 = (value: string) => [...new TextEncoder().encode(value)]
  const encoded = encode({
    join_ref: "🚀",
    ref: "1",
    topic: "room:café",
    event: "é",
    payload: bytes(1),
  })
  expect([...new Uint8Array(encoded as ArrayBuffer)]).toEqual([
    0,
    4,
    1,
    10,
    2,
    ...utf8("🚀1room:caféé"),
    1,
  ])

  const message = { join_ref: "1", ref: "1", topic: "t", event: "e", payload: bytes(1) }
  expect(encode({ ...message, topic: "é".repeat(127) })).toBeInstanceOf(ArrayBuffer)
  expect(() => encode({ ...message, topic: "é".repeat(128) })).toThrow(
    "unable to convert topic to binary: must be less than or equal to 255 bytes, but is 256 bytes",
  )
})
