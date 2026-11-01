import { expect, test } from "vitest"
import { Serializer, type EncodedMessage, type Message } from "../src/serializer"
import { appendParams, serialize } from "../src/url"

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

  // A binary push: kind, then the sizes of join_ref, ref, topic and event, then each of them.
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

  // What a server sends: a push has no ref, a reply carries its status, a broadcast has neither ref.
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
