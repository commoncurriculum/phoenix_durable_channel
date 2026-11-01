type Vars = { conn: string; chan: string }

const connection: Record<string, string> = {
  disconnected: "Disconnected",
  connecting: "Connecting",
  open: "Connected",
  waiting: "Waiting to reconnect",
}

const channel: Record<string, string> = {
  unjoined: "Not joined",
  waitingForSocket: "Waiting for the connection",
  joining: "Joining",
  joined: "Joined",
  errored: "Rejoining soon",
  erroredOffline: "Waiting for the connection",
  leaving: "Leaving",
  closed: "Closed",
}

export const title = "Phoenix client: the socket and one channel"

export const state = (v: Vars) => ({
  Connection: connection[v.conn],
  Channel: channel[v.chan],
})

const steps: Record<string, string | null> = {
  YouConnect: "the app connects",
  YouDisconnect: "the app disconnects",
  ConnectionOpens: "the connection opens",
  ConnectionDrops: "the connection drops",
  ReconnectTimerFires: "the reconnect wait ends",
  YouJoin: "the app joins",
  JoinTimesOut: "the join times out",
  RejoinTimerFires: "the rejoin wait ends",
  YouLeave: "the app leaves",
  LeaveTimesOut: "the leave times out",
  ServerJoins: "the server joins the channel",
  ServerDeniesJoin: "the server refuses the join",
  ServerLeaves: "the server leaves the channel",
  ServerAnswersPush: "the server answers the push",
  ServerChannelCrashes: "the server's channel crashes",
  OldRuns: "the dropped connection's channel runs a push it had",
  OldStops: "the dropped connection's channel stops",
  YouPush: "the app pushes",
  PushTimesOut: "the push times out",
  JoinReplyArrives: null,
  LeaveReplyArrives: "the leave is answered",
  CloseArrives: "the server closes the channel",
  ErrorArrives: "the app hears of the crash",
  PushReplyArrives: "the push's reply arrives",
  Done: null,
}

export const step = (action: string, _from: Vars, to: Vars) => {
  if (action === "JoinReplyArrives")
    return to.chan === "joined" ? "the server accepts the join" : "the server refuses the join"
  return steps[action] ?? null
}
