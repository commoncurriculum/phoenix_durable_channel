const { TextEncoder, TextDecoder } = require("node:util")
const v8 = require("node:v8")
Object.assign(globalThis, {
  TextEncoder,
  TextDecoder,
  structuredClone: (value) => v8.deserialize(v8.serialize(value)),
})
