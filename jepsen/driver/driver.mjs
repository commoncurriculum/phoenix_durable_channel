// The Jepsen test's clients: real phoenix-durable-channel sockets in Chromium (page.ts), driven over
// HTTP. Each WebSocket goes through a proxy that sends every new connection to a random node,
// as a load balancer would, so a session that reconnects can land on another node.
//
// Usage: node driver.mjs <http port> <proxy port> <node host:port,...>

import { createServer as createHttpServer } from "node:http"
import { createRequire } from "node:module"
import { connect, createServer as createTcpServer } from "node:net"
import { fileURLToPath } from "node:url"
import { chromium } from "playwright"

const [httpPort, proxyPort, nodeList] = process.argv.slice(2)
const nodes = nodeList.split(",").map((node) => {
  const [host, port] = node.split(":")
  return { host, port: Number(port) }
})

createTcpServer((client) => {
  const node = nodes[Math.floor(Math.random() * nodes.length)]
  const upstream = connect(node.port, node.host)
  client.pipe(upstream).pipe(client)
  client.on("error", () => upstream.destroy())
  upstream.on("error", () => client.destroy())
  client.on("close", () => upstream.destroy())
  upstream.on("close", () => client.destroy())
}).listen(Number(proxyPort), "127.0.0.1")

// Vitest's own Vite bundles the page, so the driver needs no dependency of its own.
const require = createRequire(createRequire(import.meta.url).resolve("vitest/package.json"))
const { build } = await import(require.resolve("vite"))
const bundle = await build({
  configFile: false,
  logLevel: "warn",
  build: {
    write: false,
    minify: false,
    lib: {
      entry: fileURLToPath(new URL("./page.ts", import.meta.url)),
      formats: ["iife"],
      name: "jepsenPage",
    },
  },
})
const script = bundle[0].output[0].code

const browser = await chromium.launch()
process.on("SIGTERM", () => browser.close().finally(() => process.exit(0)))
const page = await browser.newPage()
page.on("console", (message) => console.log(`page: ${message.text()}`))
await page.setContent("<!doctype html><title>phoenix-durable-channel jepsen</title>")
await page.addScriptTag({ content: script })

const routes = {
  "/open": ({ client, heartbeatIntervalMs, transport }) =>
    page.evaluate(
      ([client, endPoint, heartbeatIntervalMs, transport]) =>
        window.jepsen.open(client, endPoint, heartbeatIntervalMs, transport),
      [client, `ws://127.0.0.1:${proxyPort}/socket`, heartbeatIntervalMs, transport],
    ),
  "/push": ({ client, value }) =>
    page.evaluate(([client, value]) => window.jepsen.push(client, value), [client, value]),
  "/unanswered": () => page.evaluate(() => window.jepsen.unanswered()),
  "/delivered": () => page.evaluate(() => window.jepsen.delivered()),
  "/close": () => page.evaluate(() => window.jepsen.close()),
}

createHttpServer(async (request, response) => {
  let body = ""
  for await (const chunk of request) body += chunk
  try {
    const result = await routes[request.url](body === "" ? {} : JSON.parse(body))
    response.setHeader("content-type", "application/json")
    response.end(JSON.stringify(result ?? null))
  } catch (error) {
    response.statusCode = 500
    response.end(String(error))
  }
}).listen(Number(httpPort), "127.0.0.1", () => console.log("ready"))
