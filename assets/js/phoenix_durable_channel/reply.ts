import type { ReplyStatus } from "./push"

type ReceiveHook = (response: unknown) => void

export class Reply {
  private last: { status: ReplyStatus; response: unknown } | null = null
  private readonly hooks: Array<{
    status: ReplyStatus
    callback: ReceiveHook
  }> = []

  receive(status: ReplyStatus, callback: ReceiveHook): this {
    if (this.last?.status === status) callback(this.last.response)
    this.hooks.push({ status, callback })
    return this
  }

  answer(status: ReplyStatus, response: unknown): void {
    this.last = { status, response }
    for (const hook of this.hooks) if (hook.status === status) hook.callback(response)
  }
}
