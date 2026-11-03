/// <reference types="node" />
import { execFile } from "node:child_process"
import {
  rmSync,
  copyFileSync,
  existsSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs"
import { homedir, tmpdir } from "node:os"
import { join } from "node:path"
import { promisify } from "node:util"

type Value = string | number
type Event = Record<string, Value>
type Recorded = {
  test: string
  topic: string
  client: Array<Record<string, unknown>>
  server: Array<Record<string, unknown>>
}

const directory = "verification/traces"
const jar = process.env.TLA2TOOLS ?? join(homedir(), ".cache/tla2tools-1.7.4.jar")
const run = promisify(execFile)

function ordinals(): (key: string) => number {
  const seen = new Map<string, number>()
  return (key) => {
    if (!seen.has(key)) seen.set(key, seen.size + 1)
    return seen.get(key) ?? 0
  }
}

function notTaken(response: unknown): "notTaken" | "unmatched" | null {
  if (typeof response !== "object" || response === null) return null
  if ("phx_not_taken" in response) return "notTaken"
  if ("reason" in response && response.reason === "unmatched topic") return "unmatched"
  return null
}

function clientEvents(recorded: Recorded["client"]): Array<Event> {
  const push = ordinals()
  return recorded.flatMap((event): Array<Event> => {
    const type = String(event.type)
    if (type === "xstate.init") return [{ type: "LOG_STARTED" }]
    if (type.startsWith("xstate.done.actor")) return [{ type: "BACKOFF_DONE" }]
    switch (type) {
      case "PUSH":
        return [{ type, id: push(String(event.id)) }]
      case "ANSWERED":
        return [
          { type, number: Number(event.number), status: event.status === "ok" ? "ok" : "error" },
        ]
      case "READ":
        return [{ type, after: Number(event.after) }]
      case "SYNCED":
        return [{ type, received: Number(event.received), last: Number(event.last) }]
      case "REPLY": {
        const ref = /^d(\d+)\.(\d+)$/.exec(String(event.ref))
        if (ref === null) return []
        const status = notTaken(event.response) ?? (event.status === "ok" ? "ok" : "error")
        return [{ type, num: Number(ref[2]), syncs: Number(ref[1]), status }]
      }
      default:
        return [{ type }]
    }
  })
}

function serverEvents(recorded: Recorded["server"]): Array<Event> {
  const processOf = ordinals()
  const mapped = recorded.flatMap((event): Array<Event> => {
    const type = String(event.type)
    const at = { type, process: processOf(String(event.join_ref)) }
    switch (type) {
      case "reader":
        return [{ ...at, last: Number(event.last) }]
      case "received":
        return [{ ...at, received: Number(event.received) }]
      case "sync":
        return [{ ...at, received: Number(event.received), last: Number(event.last) }]
      case "push": {
        const outcome = String(event.outcome)
        return [
          {
            ...at,
            number: Number(event.number),
            received: Number(event.received),
            outcome: ["old", "ahead", "retry", "ok"].includes(outcome) ? outcome : "error",
          },
        ]
      }
      case "append":
        if (event.received === null)
          return [{ type: "other", process: 0, number: Number(event.number) }]
        return [{ ...at, number: Number(event.number), received: Number(event.received) }]
      case "count":
        return [{ ...at, number: Number(event.number), received: Number(event.received) }]
      default:
        return [at]
    }
  })
  return mapped.reduce<Array<Event>>((ordered, event) => {
    if (event.type !== "other") return [...ordered, event]
    const number = Number(event.number)
    const first = ordered.findIndex(
      (earlier) =>
        Number(earlier.last ?? 0) >= number ||
        (["append", "count"].includes(String(earlier.type)) && Number(earlier.number) > number),
    )
    const observed =
      ordered[first]?.type === "count" && ordered[first - 1]?.type === "received"
        ? first - 1
        : first
    return observed === -1
      ? [...ordered, event]
      : [...ordered.slice(0, observed), event, ...ordered.slice(observed)]
  }, [])
}

const tla = (events: Array<Event>): string =>
  `<<${events
    .map(
      (event) =>
        `[${Object.entries(event)
          .map(
            ([key, value]) =>
              `${key} |-> ${typeof value === "string" ? JSON.stringify(value) : value}`,
          )
          .join(", ")}]`,
    )
    .join(",\n  ")}>>`

async function check(name: string, recorded: Recorded): Promise<string | null> {
  const client = clientEvents(recorded.client)
  const server = serverEvents(recorded.server)
  const pushes = client.filter((event) => event.type === "PUSH").length
  const ran = server.filter(
    (event) => event.type === "push" && ["ok", "error"].includes(String(event.outcome)),
  ).length
  const appends = server.filter((event) => event.type === "append").length
  const others = server.filter((event) => event.type === "other").length
  const highest = Math.max(
    0,
    ...[...client, ...server].flatMap((event) =>
      ["number", "last", "after"].map((key) => Number(event[key] ?? 0)),
    ),
  )
  const stops = client.filter((event) =>
    ["CHANNEL_CLOSED", "CHANNEL_LEFT"].includes(String(event.type)),
  ).length
  const work = mkdtempSync(join(tmpdir(), "trace-"))
  for (const module of ["DurableMessages", "DurableMessagesTrace"])
    copyFileSync(`verification/${module}.tla`, join(work, `${module}.tla`))
  writeFileSync(
    join(work, "TraceCheck.tla"),
    `---- MODULE TraceCheck ----\nEXTENDS DurableMessagesTrace\nClientValue == ${tla(client)}\nServerValue == ${tla(server)}\n====\n`,
  )
  const attempts: Array<{ faults: number; clientAt: number; serverAt: number; output: string }> = []
  const losses = client.filter((event) =>
    ["CHANNEL_LEFT", "SYNC_FAILED"].includes(String(event.type)),
  ).length
  for (const faults of Array.from({ length: Math.max(4, losses + 3) }, (_, faults) => faults)) {
    writeFileSync(
      join(work, "TraceCheck.cfg"),
      [
        "SPECIFICATION TraceSpec",
        "CONSTANTS",
        `  MaxPush = ${pushes}`,
        `  MaxPublish = ${Math.max(appends - ran, 0)}`,
        `  MaxOthers = ${others}`,
        `  Afters = {${Array.from({ length: highest + 2 }, (_, number) => number).join(", ")}}`,
        `  MaxFaults = ${faults}`,
        `  MaxStops = ${stops}`,
        '  Reads = "channel"',
        '  Broken = "none"',
        "  ClientTrace <- ClientValue",
        "  ServerTrace <- ServerValue",
        "VIEW TraceView",
        "CHECK_DEADLOCK FALSE",
        "INVARIANT NotAccepted",
        "",
      ].join("\n"),
    )
    const output = await run(
      "java",
      [
        "-XX:+UseParallelGC",
        `-Djava.io.tmpdir=${work}`,
        "-cp",
        jar,
        "tlc2.TLC",
        "-workers",
        "1",
        "TraceCheck.tla",
        "-config",
        "TraceCheck.cfg",
      ],
      { cwd: work, maxBuffer: 1 << 28, timeout: faults === 0 ? 120_000 : 600_000 },
    ).then(
      ({ stdout }) => stdout,
      (error: { stdout?: string }) => `${error.stdout ?? ""}\ngave up after the time limit`,
    )
    rmSync(join(work, "states"), { recursive: true, force: true })
    if (output.includes("Invariant NotAccepted is violated")) {
      rmSync(work, { recursive: true, force: true })
      console.log(`${name}: a behaviour of the model with ${faults} faults (${recorded.test})`)
      return null
    }
    const matched = [...output.matchAll(/<<"matched", (\d+), (\d+)>>/g)].at(-1)
    attempts.push({
      faults,
      clientAt: Number(matched?.[1] ?? 0),
      serverAt: Number(matched?.[2] ?? 0),
      output,
    })
  }
  const best = attempts.reduce((a, b) =>
    b.clientAt + b.serverAt > a.clientAt + a.serverAt ? b : a,
  )
  return [
    `${name}: ${recorded.test} (${recorded.topic}) is not a behaviour of the model; at most it matched ${best.clientAt} of ${client.length} client events and ${best.serverAt} of ${server.length} server events, with ${best.faults} faults.`,
    `  next client events: ${JSON.stringify(client.slice(best.clientAt, best.clientAt + 3))}`,
    `  next server events: ${JSON.stringify(server.slice(best.serverAt, best.serverAt + 3))}`,
    ...attempts.map(
      ({ faults, output }) =>
        `  with ${faults} faults: ${
          output.includes("No error has been found")
            ? "no behaviour matches"
            : output
                .split("\n")
                .filter((line) => /Error|Exception|gave up|Finished/.test(line))
                .join(" ")
        }`,
    ),
    `  model and trace kept in ${work}`,
  ].join("\n")
}

const files = existsSync(directory)
  ? readdirSync(directory).filter((file) => file.endsWith(".json"))
  : []
const byTopic = new Map<string, Array<[string, Recorded]>>()
for (const file of files) {
  const recorded: Recorded = JSON.parse(readFileSync(join(directory, file), "utf8"))
  byTopic.set(recorded.topic, [...(byTopic.get(recorded.topic) ?? []), [file, recorded]])
}

const longest = 400
const alone = [...byTopic.values()].flatMap((group) => (group.length === 1 ? group : []))
const single = alone.filter(
  ([, recorded]) => recorded.client.length + recorded.server.length <= longest,
)
const long = alone.filter(
  ([, recorded]) => recorded.client.length + recorded.server.length > longest,
)
const several = [...byTopic.values()].filter((group) => group.length > 1)
const rejected: Array<string> = []
const queue = [...single]
await Promise.all(
  Array.from({ length: 4 }, async () => {
    for (let entry = queue.shift(); entry !== undefined; entry = queue.shift()) {
      const failure = await check(entry[0], entry[1])
      if (failure !== null) {
        rejected.push(failure)
        console.log(failure)
      }
    }
  }),
)

console.log(
  `${single.length - rejected.length} of ${single.length} traces are behaviours of the model; ${several.length} topics had more than one client and were not checked; ${long.length} traces had more than ${longest} events and were not checked (${long.map(([, recorded]) => recorded.test).join("; ")}).`,
)
if (rejected.length > 0) {
  process.exit(1)
}
