/// <reference types="node" />
import { execFileSync, spawn, type ChildProcess } from "node:child_process"
import { mkdtempSync } from "node:fs"
import { connect } from "node:net"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import type { TestProject } from "vitest/node"

const PORT = 4400
const serverDirectory = fileURLToPath(new URL("../server", import.meta.url))
const environment = {
  ...process.env,
  PORT: `${PORT}`,
  MIX_ENV: "dev",
  ELIXIR_ERL_OPTIONS: "+fnu",
}

let server: ChildProcess | null = null

function answers(url: string): Promise<boolean> {
  return fetch(url).then(
    () => true,
    () => false,
  )
}

function healthy(): Promise<boolean> {
  return fetch(`http://127.0.0.1:${PORT}/health`).then(
    (response) => response.ok,
    () => false,
  )
}

async function waitUntil(ready: () => Promise<boolean>, what: string, ms: number) {
  const deadline = Date.now() + ms
  while (!(await ready())) {
    if (Date.now() > deadline) throw new Error(`${what} did not start`)
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

const S2_PORT = 4401
let s2: ChildProcess | null = null

// S2 Lite, the in-memory S2 server from the `s2` CLI that mise installs.
async function startS2() {
  const basins = `http://127.0.0.1:${S2_PORT}/v1/basins`
  if (!(await answers(basins))) {
    s2 = spawn("s2", ["lite", "--port", `${S2_PORT}`], {
      stdio: "ignore",
      detached: true,
    })
    await waitUntil(() => answers(basins), "S2 Lite", 30_000)
  }
  await fetch(basins, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      basin: "phoenix-durable-channel",
      config: { create_stream_on_append: true, create_stream_on_read: true },
    }),
  })
}

const DURABLE_STREAMS_PORT = 4402
let durableStreams: ChildProcess | null = null

// The `durable-streams-cloudflare` worker on wrangler, with a project whose tokens the test
// server signs (test/server/config/config.exs).
async function startDurableStreams() {
  const url = `http://127.0.0.1:${DURABLE_STREAMS_PORT}/`
  if (await answers(url)) return
  durableStreams = spawn(
    fileURLToPath(new URL("../durable-streams/serve.sh", import.meta.url)),
    [
      `${DURABLE_STREAMS_PORT}`,
      "phoenix-durable-channel",
      "phoenix-durable-channel-signing-secret",
    ],
    { stdio: "ignore", detached: true },
  )
  await waitUntil(() => answers(url), "the Durable Streams worker", 60_000)
}

const MONGO_PORT = 4403
let mongo: ChildProcess | null = null

function listening(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = connect(port, "127.0.0.1")
    socket.once("connect", () => {
      socket.end()
      resolve(true)
    })
    socket.once("error", () => resolve(false))
  })
}

// MongoDB from mise, on fresh data, unless one is already on the port (CI's service).
async function startMongo() {
  if (await listening(MONGO_PORT)) return
  const directory = mkdtempSync(join(tmpdir(), "phoenix-durable-channel-mongo-"))
  mongo = spawn(
    "mongod",
    ["--port", `${MONGO_PORT}`, "--bind_ip", "127.0.0.1", "--dbpath", directory],
    { stdio: "ignore", detached: true },
  )
  await waitUntil(() => listening(MONGO_PORT), "MongoDB", 30_000)
}

export async function setup(project: TestProject) {
  project.provide("phoenixUrl", `ws://127.0.0.1:${PORT}`)
  project.provide("s2Url", `http://127.0.0.1:${S2_PORT}`)
  project.provide("durableStreamsUrl", `http://127.0.0.1:${DURABLE_STREAMS_PORT}`)
  await Promise.all([startS2(), startDurableStreams(), startMongo()])
  // Vitest can call this once per project; the first call starts the server for all of them.
  if (await healthy()) return
  execFileSync("mix", ["do", "deps.get,", "compile"], {
    cwd: serverDirectory,
    env: environment,
    stdio: "inherit",
  })
  // Its own process group, so teardown stops the BEAM that `mix` starts, not just `mix`.
  server = spawn("mix", ["run", "--no-halt"], {
    cwd: serverDirectory,
    env: environment,
    stdio: "inherit",
    detached: true,
  })
  await waitUntil(healthy, "the Phoenix test server", 60_000)
}

export function teardown() {
  for (const child of [server, s2, durableStreams, mongo]) if (child?.pid) process.kill(-child.pid)
  server = null
  s2 = null
  durableStreams = null
  mongo = null
}

declare module "vitest" {
  export interface ProvidedContext {
    phoenixUrl: string
    s2Url: string
    durableStreamsUrl: string
  }
}
