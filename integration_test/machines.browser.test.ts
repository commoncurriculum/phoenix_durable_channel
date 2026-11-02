import { expect, test } from "vitest"
import type { AnyStateNode } from "xstate"
import { channelMachine } from "../assets/js/phoenix_durable_channel/machines/channel"
import { longPollMachine } from "../assets/js/phoenix_durable_channel/machines/longpoll"
import { pushMachine } from "../assets/js/phoenix_durable_channel/machines/push"
import { socketMachine } from "../assets/js/phoenix_durable_channel/machines/socket"

function leafTags(node: AnyStateNode, above: Array<string> = []): Array<[string, Array<string>]> {
  const tags = [...above, ...node.tags]
  const children = Object.values(node.states)
  if (children.length === 0) return [[node.id, tags]]
  return children.flatMap((child) => leafTags(child, tags))
}

test("every state a machine can be in has exactly one public state", () => {
  const regions = [
    channelMachine.root,
    pushMachine.root,
    socketMachine.getStateNodeById("socket.connection"),
    longPollMachine.getStateNodeById("longpoll.starting"),
    longPollMachine.getStateNodeById("longpoll.running.polling"),
    longPollMachine.getStateNodeById("longpoll.closed"),
  ]
  for (const region of regions)
    for (const [id, tags] of leafTags(region))
      expect({ id, tags: tags.length }).toEqual({ id, tags: 1 })
})
