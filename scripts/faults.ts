/// <reference types="node" />
import { execFileSync } from "node:child_process"
import { readFileSync } from "node:fs"

type Fault = {
  fault: string
  what: string
  model: Array<string>
  jepsen: Array<string>
  tests: Array<string>
}

const faults: Array<Fault> = JSON.parse(readFileSync("verification/faults.json", "utf8"))

const tracked = (pattern: string): Array<string> =>
  execFileSync("git", ["ls-files", "--cached", "--others", "--exclude-standard", pattern], {
    encoding: "utf8",
  })
    .split("\n")
    .filter((file) => file !== "")

const actions = new Set(
  tracked("verification/*.tla").flatMap((file) => {
    const spec = file.replace(/^verification\/|\.tla$/g, "")
    return [...readFileSync(file, "utf8").matchAll(/^(\w+)(?:\([^)]*\))?\s*==/gm)].map(
      (match) => `${spec}.${match[1]}`,
    )
  }),
)

const tests = new Set(
  [
    ...tracked("integration_test/*.ts"),
    ...tracked("test/*.exs"),
    ...tracked("integration_test/*.exs"),
  ].flatMap((file) =>
    [...readFileSync(file, "utf8").matchAll(/\btest\(?\s*"((?:[^"\\]|\\.)*)"/g)].map(
      (match) => match[1],
    ),
  ),
)

const jepsenFaults = new Set(
  (
    readFileSync("jepsen/src/phoenix_durable_channel_jepsen/core.clj", "utf8").match(
      /\(def faults #\{([^}]*)\}\)/,
    )?.[1] ?? ""
  )
    .split(/\s+/)
    .filter((name) => name !== "")
    .map((name) => name.replace(/^:/, "")),
)

const unknown = faults.flatMap((fault) => [
  ...fault.model
    .filter((name) => !actions.has(name))
    .map((name) => `${fault.fault}: no TLA+ action ${name}`),
  ...fault.jepsen
    .filter((name) => !jepsenFaults.has(name))
    .map((name) => `${fault.fault}: no Jepsen fault ${name}`),
  ...fault.tests
    .filter((name) => !tests.has(name))
    .map((name) => `${fault.fault}: no test "${name}"`),
])

const uncovered = faults.flatMap((fault) =>
  (["model", "jepsen", "tests"] as const)
    .filter((where) => fault[where].length === 0)
    .map((where) => `${fault.fault}: nothing in ${where}`),
)

if (unknown.length > 0)
  console.error(`Names in verification/faults.json that do not exist:\n${unknown.join("\n")}`)
if (uncovered.length > 0) console.error(`Faults not covered:\n${uncovered.join("\n")}`)
if (unknown.length > 0 || uncovered.length > 0) process.exit(1)
