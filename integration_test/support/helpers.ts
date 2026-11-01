import { inject, onTestFinished } from "vitest"
import type { Channel } from "../../assets/js/phoenix_durable_channel/channel"
import type { Push, ReplyStatus } from "../../assets/js/phoenix_durable_channel/push"
import type { Reply } from "../../assets/js/phoenix_durable_channel/reply"
import { Socket, type SocketOptions } from "../../assets/js/phoenix_durable_channel/socket"

export const phoenixUrl = inject("phoenixUrl")

export function openSocket(path = "/socket", options: SocketOptions = {}): Socket {
  const socket = new Socket(`${phoenixUrl}${path}`, options)
  onTestFinished(() => new Promise<void>((resolve) => socket.disconnect(resolve)))
  return socket
}

export async function waitFor(condition: () => boolean, timeout = 5000): Promise<void> {
  const deadline = Date.now() + timeout
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${condition.toString()}`)
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}

export function answer(push: Push | Reply): Promise<{ status: ReplyStatus; response: unknown }> {
  return new Promise((resolve) => {
    for (const status of ["ok", "error", "timeout"] as const)
      push.receive(status, (response) => resolve({ status, response }))
  })
}

export async function join(channel: Channel): Promise<unknown> {
  const { status, response } = await answer(channel.join())
  if (status !== "ok") throw new Error(`join ${status}: ${JSON.stringify(response)}`)
  return response
}

export function nextEvent(channel: Channel, event: string): Promise<unknown> {
  return new Promise((resolve) => {
    const ref = channel.on(event, (payload) => {
      channel.off(event, ref)
      resolve(payload)
    })
  })
}

export function topic(name: string): string {
  return `room:${name}:${Math.random().toString(36).slice(2)}`
}
