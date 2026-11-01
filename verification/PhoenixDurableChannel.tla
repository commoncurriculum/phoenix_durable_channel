---------------------------- MODULE PhoenixDurableChannel ----------------------------
(***************************************************************************)
(* One Socket, one Channel on it, and the app's pushes on that channel,   *)
(* against one Phoenix server, in every order the network, the server, the *)
(* timers and the user can produce. Each action names the code it stands  *)
(* for; SKILL.md in .agents/skills/verify-phoenix-durable-channel maps them all.   *)
(*                                                                         *)
(* A server channel process outlives a connection that drops: it runs the  *)
(* frames it had already received, with its replies lost, until it sees    *)
(* the socket is gone (a heartbeat timeout, or a long-poll window ending). *)
(*                                                                         *)
(* Broken names a change that removes one protection, so each rule can   *)
(* be seen to fail. "none" is the code as it is.                          *)
(***************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS MaxFaults, MaxUserSteps, MaxPush, Broken

Pushes == 1..MaxPush

VARIABLES
    wants,          \* the app called connect() and not disconnect()
    conn,           \* socket.ts connection region
    chan,           \* channel.ts channelMachine
    wantsChannel,   \* the app has not called channel.leave()
    joinRef,        \* channel.joinRef(): the ref of the latest join sent
    nextRef,        \* socket.makeRef()
    pushed,         \* how many times the app called channel.push()
    push,           \* push.ts pushMachine of each push, "none" before channel.push()
    pushJoinRef,    \* the join each push went out on
    answers,        \* how many times each push has been answered
    srv,            \* the server's channel process for this topic on this connection
    srvJoinRef,     \* the join that process belongs to
    old,            \* the process of a connection that dropped, still running
    oldJoinRef,     \* the join that process belongs to
    oldBox,         \* the frames it had received and not yet run
    handled,        \* the pushes the server ran: <<[id, joinRef]>>
    toServer,       \* frames in flight to the server, in order
    toClient,       \* frames in flight to the client, in order
    faults,         \* drops, crashes, denials and timeouts so far
    userSteps       \* connect / disconnect calls so far

vars == <<wants, conn, chan, wantsChannel, joinRef, nextRef, pushed, push, pushJoinRef,
          answers, srv, srvJoinRef, old, oldJoinRef, oldBox, handled, toServer, toClient,
          faults, userSteps>>
oldServer == <<old, oldJoinRef, oldBox>>
pushes == <<pushed, push, pushJoinRef, answers>>

Init ==
    /\ wants = FALSE
    /\ conn = "disconnected"
    /\ chan = "unjoined"
    /\ wantsChannel = TRUE
    /\ joinRef = 0
    /\ nextRef = 1
    /\ pushed = 0
    /\ push = [i \in Pushes |-> "none"]
    /\ pushJoinRef = [i \in Pushes |-> 0]
    /\ answers = [i \in Pushes |-> 0]
    /\ srv = "none"
    /\ srvJoinRef = 0
    /\ old = "none"
    /\ oldJoinRef = 0
    /\ oldBox = <<>>
    /\ handled = <<>>
    /\ toServer = <<>>
    /\ toClient = <<>>
    /\ faults = 0
    /\ userSteps = 0

Pending == {"buffered", "sent"}

(* channel.ts hooks.sendJoin, from joining.awaitingReply's entry *)
SendJoin ==
    /\ joinRef' = nextRef
    /\ nextRef' = nextRef + 1
    /\ toServer' = Append(toServer, [kind |-> "join", ref |-> nextRef])

(* The pushes in `which` are answered with `outcome`. *)
AnswerAll(which, outcome) ==
    /\ push' = [i \in Pushes |-> IF i \in which THEN outcome ELSE push[i]]
    /\ answers' = [i \in Pushes |-> IF i \in which THEN answers[i] + 1 ELSE answers[i]]

(* channel.ts `lose` (entering errored or leaving): a sent push can no longer *)
(* be answered on this join, so it times out now. With Broken =              *)
(* "rebufferOnLoss" it is buffered to go out again on the next join.         *)
LoseSent ==
    LET sent == {i \in Pushes : push[i] = "sent"}
    IN CASE Broken = "pushesWaitOnLoss" -> UNCHANGED <<push, answers>>
         [] Broken = "rebufferOnLoss" ->
              /\ push' = [i \in Pushes |-> IF i \in sent THEN "buffered" ELSE push[i]]
              /\ UNCHANGED answers
         [] OTHER -> AnswerAll(sent, "timeout")

(* channel.ts hooks.closed: every pending push gets CHANNEL_CLOSED. *)
ClosePending == AnswerAll({i \in Pushes : push[i] \in Pending}, "timeout")

PushFrame(i, ref) == [kind |-> "push", id |-> i, joinRef |-> ref]

Ids == [i \in Pushes |-> i]
IsBuffered(i) == push[i] = "buffered"
Reverse(s) == [k \in 1..Len(s) |-> s[Len(s) + 1 - k]]

(* channel.ts hooks.joined: buffered pushes go out on the new join, in the   *)
(* order the app pushed them.                                                *)
Flush(ref) ==
    LET waiting == SelectSeq(Ids, IsBuffered)
        order == IF Broken = "flushReversed" THEN Reverse(waiting) ELSE waiting
    IN /\ push' = [i \in Pushes |-> IF push[i] = "buffered" THEN "sent" ELSE push[i]]
       /\ pushJoinRef' = [i \in Pushes |-> IF push[i] = "buffered" THEN ref ELSE pushJoinRef[i]]
       /\ toServer' = toServer \o [k \in 1..Len(order) |-> PushFrame(order[k], ref)]
       /\ UNCHANGED answers

(* What the channel does when the socket's connection goes away: socket.ts *)
(* hooks.connectionLost sends SOCKET_CLOSED to each channel.               *)
ChannelLosesConnection ==
    CASE chan \in {"joined", "joining"} ->
            /\ chan' = "erroredOffline"
            /\ LoseSent
      [] chan = "errored" ->
            /\ chan' = "erroredOffline"
            /\ UNCHANGED <<push, answers>>
      [] chan = "leaving" ->
            /\ chan' = "closed"
            /\ ClosePending
      [] OTHER ->
            /\ UNCHANGED chan
            /\ UNCHANGED <<push, answers>>

(* The connection is gone, and frames in flight with it, except those the    *)
(* server had received: its channel process, or a join it had not run yet,   *)
(* lives on with them.                                                       *)
ConnectionGone ==
    /\ toServer' = <<>>
    /\ toClient' = <<>>
    /\ srv' = "none"
    /\ IF srv = "joined" \/ \E i \in 1..Len(toServer) : toServer[i].kind = "join"
       THEN /\ old' = srv
            /\ oldJoinRef' = srvJoinRef
            /\ \E k \in 0..Len(toServer) : oldBox' = SubSeq(toServer, 1, k)
       ELSE UNCHANGED oldServer

---------------------------------------------------------------------------
(* The socket *)

(* socket.connect(): CONNECT -> live.connecting *)
YouConnect ==
    /\ userSteps < MaxUserSteps
    /\ conn = "disconnected"
    /\ wants' = TRUE
    /\ conn' = "connecting"
    /\ userSteps' = userSteps + 1
    /\ UNCHANGED <<chan, wantsChannel, joinRef, nextRef, pushes, srv, srvJoinRef, oldServer,
                   handled, toServer, toClient, faults>>

(* socket.disconnect(): DISCONNECT -> closing -> disconnected; leaving `open` *)
(* runs hooks.connectionLost.                                              *)
YouDisconnect ==
    /\ userSteps < MaxUserSteps
    /\ wants
    /\ wants' = FALSE
    /\ conn' = "disconnected"
    /\ userSteps' = userSteps + 1
    /\ IF conn = "open"
       THEN ChannelLosesConnection
       ELSE UNCHANGED <<chan, push, answers>>
    /\ ConnectionGone
    /\ UNCHANGED <<wantsChannel, joinRef, nextRef, pushed, pushJoinRef, srvJoinRef, handled,
                   faults>>

(* TRANSPORT_OPEN: live.connecting -> live.open; hooks.opened sends SOCKET_OPEN, *)
(* which takes joining.awaitingSocket and errored to joining.awaitingReply.    *)
ConnectionOpens ==
    /\ conn = "connecting"
    /\ conn' = "open"
    /\ IF chan = "waitingForSocket" \/ (chan = "erroredOffline" /\ Broken # "noRejoinOnOpen")
       THEN /\ chan' = "joining"
            /\ SendJoin
       ELSE UNCHANGED <<chan, joinRef, nextRef, toServer>>
    /\ UNCHANGED <<wants, wantsChannel, pushes, srv, srvJoinRef, oldServer, handled,
                   toClient, faults, userSteps>>

(* TRANSPORT_CLOSE with any code, or a heartbeat with no reply: live ->       *)
(* waitingToReconnect. phoenix.js 1.7 stops for good on code 1000 instead.    *)
ConnectionDrops ==
    /\ faults < MaxFaults
    /\ conn \in {"connecting", "open"}
    /\ faults' = faults + 1
    /\ conn' = IF Broken = "closeIsFinal" THEN "disconnected" ELSE "waiting"
    /\ IF conn = "open"
       THEN ChannelLosesConnection
       ELSE UNCHANGED <<chan, push, answers>>
    /\ ConnectionGone
    /\ UNCHANGED <<wants, wantsChannel, joinRef, nextRef, pushed, pushJoinRef, srvJoinRef,
                   handled, userSteps>>

(* waitingToReconnect's sleep ends: -> live *)
ReconnectTimerFires ==
    /\ conn = "waiting"
    /\ conn' = "connecting"
    /\ UNCHANGED <<wants, chan, wantsChannel, joinRef, nextRef, pushes, srv, srvJoinRef,
                   oldServer, handled, toServer, toClient, faults, userSteps>>

---------------------------------------------------------------------------
(* The channel *)

(* channel.join(): unjoined -> joining, which sends the join if the socket is open *)
YouJoin ==
    /\ chan = "unjoined"
    /\ IF conn = "open"
       THEN /\ chan' = "joining"
            /\ SendJoin
       ELSE /\ chan' = "waitingForSocket"
            /\ UNCHANGED <<joinRef, nextRef, toServer>>
    /\ UNCHANGED <<wants, conn, wantsChannel, pushes, srv, srvJoinRef, oldServer, handled,
                   toClient, faults, userSteps>>

(* joining.awaitingReply's sleep ends: answer "timeout", send a leave for that *)
(* join, -> errored. phoenix.js 1.7 sends no leave.                           *)
JoinTimesOut ==
    /\ faults < MaxFaults
    /\ chan = "joining"
    /\ faults' = faults + 1
    /\ chan' = "errored"
    /\ LoseSent
    /\ toServer' = IF Broken = "noLeaveOnJoinTimeout"
                   THEN toServer
                   ELSE Append(toServer, [kind |-> "leave", joinRef |-> joinRef])
    /\ UNCHANGED <<wants, conn, wantsChannel, joinRef, nextRef, pushed, pushJoinRef, srv,
                   srvJoinRef, oldServer, handled, toClient, userSteps>>

(* errored.awaitingRejoin's sleep ends: -> joining *)
RejoinTimerFires ==
    /\ chan = "errored"
    /\ conn = "open"
    /\ chan' = "joining"
    /\ SendJoin
    /\ UNCHANGED <<wants, conn, wantsChannel, pushes, srv, srvJoinRef, oldServer, handled,
                   toClient, faults, userSteps>>

(* channel.leave(): unjoined and errored close at once; joining and joined go *)
(* to leaving, which sends the leave if the socket is open.                   *)
YouLeave ==
    /\ wantsChannel
    /\ wantsChannel' = FALSE
    /\ IF chan \in {"joining", "joined"} /\ conn = "open"
       THEN /\ chan' = "leaving"
            /\ LoseSent
            /\ toServer' = Append(toServer, [kind |-> "leave", joinRef |-> joinRef])
       ELSE /\ chan' = "closed"
            /\ ClosePending
            /\ UNCHANGED toServer
    /\ UNCHANGED <<wants, conn, joinRef, nextRef, pushed, pushJoinRef, srv, srvJoinRef,
                   oldServer, handled, toClient, faults, userSteps>>

(* leaving.awaitingReply's sleep ends: answer "timeout", -> closed *)
LeaveTimesOut ==
    /\ chan = "leaving"
    /\ chan' = "closed"
    /\ ClosePending
    /\ UNCHANGED <<wants, conn, wantsChannel, joinRef, nextRef, pushed, pushJoinRef, srv,
                   srvJoinRef, oldServer, handled, toServer, toClient, faults, userSteps>>

(* channel.push(): sent at once if the channel is joined, else buffered *)
YouPush ==
    /\ pushed < MaxPush
    /\ chan # "unjoined"
    /\ LET i == pushed + 1
       IN /\ pushed' = i
          /\ IF chan = "joined"
             THEN /\ push' = [push EXCEPT ![i] = "sent"]
                  /\ pushJoinRef' = [pushJoinRef EXCEPT ![i] = joinRef]
                  /\ toServer' = Append(toServer, PushFrame(i, joinRef))
             ELSE /\ push' = [push EXCEPT ![i] = "buffered"]
                  /\ UNCHANGED <<pushJoinRef, toServer>>
    /\ UNCHANGED <<wants, conn, chan, wantsChannel, joinRef, nextRef, answers, srv, srvJoinRef,
                   oldServer, handled, toClient, faults, userSteps>>

(* pushMachine's sleep ends: -> timeout *)
PushTimesOut ==
    /\ Broken # "noPushTimer"
    /\ \E i \in Pushes :
        /\ push[i] \in Pending
        /\ AnswerAll({i}, "timeout")
    /\ UNCHANGED <<wants, conn, chan, wantsChannel, joinRef, nextRef, pushed, pushJoinRef, srv,
                   srvJoinRef, oldServer, handled, toServer, toClient, faults, userSteps>>


---------------------------------------------------------------------------
(* The server, frame by frame in order *)

(* A join: a duplicate closes the old channel process first (phx_close for  *)
(* its join_ref), then the new one joins.                                  *)
ServerJoins ==
    /\ toServer # <<>>
    /\ Head(toServer).kind = "join"
    /\ toServer' = Tail(toServer)
    /\ srv' = "joined"
    /\ srvJoinRef' = Head(toServer).ref
    /\ toClient' = toClient
                   \o (IF srv = "joined" THEN <<[kind |-> "close", joinRef |-> srvJoinRef]>> ELSE <<>>)
                   \o <<[kind |-> "joinReply", ref |-> Head(toServer).ref, ok |-> TRUE]>>
    /\ UNCHANGED <<wants, conn, chan, wantsChannel, joinRef, nextRef, pushes, oldServer,
                   handled, faults, userSteps>>

(* The channel's join/3 returns an error. *)
ServerDeniesJoin ==
    /\ faults < MaxFaults
    /\ toServer # <<>>
    /\ Head(toServer).kind = "join"
    /\ srv = "none"
    /\ faults' = faults + 1
    /\ toServer' = Tail(toServer)
    /\ toClient' = Append(toClient, [kind |-> "joinReply", ref |-> Head(toServer).ref, ok |-> FALSE])
    /\ UNCHANGED <<wants, conn, chan, wantsChannel, joinRef, nextRef, pushes, srv, srvJoinRef,
                   oldServer, handled, userSteps>>

(* A leave: the process for that join replies, stops, and Phoenix sends     *)
(* phx_close for it. A leave for no live join is still answered.           *)
ServerLeaves ==
    /\ toServer # <<>>
    /\ Head(toServer).kind = "leave"
    /\ toServer' = Tail(toServer)
    /\ IF srv = "joined" /\ srvJoinRef = Head(toServer).joinRef
       THEN /\ srv' = "none"
            /\ toClient' = toClient \o <<[kind |-> "leaveReply", joinRef |-> Head(toServer).joinRef],
                                         [kind |-> "close", joinRef |-> Head(toServer).joinRef]>>
       ELSE /\ UNCHANGED srv
            /\ toClient' = Append(toClient, [kind |-> "leaveReply", joinRef |-> Head(toServer).joinRef])
    /\ UNCHANGED <<wants, conn, chan, wantsChannel, joinRef, nextRef, pushes, srvJoinRef,
                   oldServer, handled, faults, userSteps>>

(* A push: run and answered by the process of the join it was sent on, or    *)
(* answered with "unmatched topic" and no join_ref when that process is gone. *)
ServerAnswersPush ==
    /\ toServer # <<>>
    /\ Head(toServer).kind = "push"
    /\ toServer' = Tail(toServer)
    /\ LET m == Head(toServer)
           ours == srv = "joined" /\ srvJoinRef = m.joinRef
       IN /\ handled' = IF ours THEN Append(handled, [id |-> m.id, joinRef |-> m.joinRef])
                        ELSE handled
          /\ toClient' = Append(toClient,
                                IF ours
                                THEN [kind |-> "reply", id |-> m.id, joinRef |-> m.joinRef, ok |-> TRUE]
                                ELSE [kind |-> "reply", id |-> m.id, joinRef |-> 0, ok |-> FALSE])
    /\ UNCHANGED <<wants, conn, chan, wantsChannel, joinRef, nextRef, pushes, srv, srvJoinRef,
                   oldServer, faults, userSteps>>

(* The channel process crashes: phx_error for its join. *)
ServerChannelCrashes ==
    /\ faults < MaxFaults
    /\ srv = "joined"
    /\ faults' = faults + 1
    /\ srv' = "none"
    /\ toClient' = Append(toClient, [kind |-> "error", joinRef |-> srvJoinRef])
    /\ UNCHANGED <<wants, conn, chan, wantsChannel, joinRef, nextRef, pushes, srvJoinRef,
                   oldServer, handled, toServer, userSteps>>

(* A dropped connection runs the next frame it had received; its replies go  *)
(* nowhere. A leave for its join stops its process. A join there is late: it *)
(* is a durable join, so replace/1 stops the current connection's process,    *)
(* whose client is sent phx_error and then phx_close (replaced/1).           *)
OldRuns ==
    /\ oldBox # <<>>
    /\ oldBox' = Tail(oldBox)
    /\ LET m == Head(oldBox)
           late == m.kind = "join"
           replaces == late /\ srv = "joined"
       IN /\ handled' = IF m.kind = "push" /\ old = "joined" /\ m.joinRef = oldJoinRef
                        THEN Append(handled, [id |-> m.id, joinRef |-> m.joinRef])
                        ELSE handled
          /\ old' = IF late THEN "joined"
                    ELSE IF m.kind = "leave" /\ m.joinRef = oldJoinRef THEN "none"
                    ELSE old
          /\ oldJoinRef' = IF late THEN m.ref ELSE oldJoinRef
          /\ srv' = IF replaces THEN "none" ELSE srv
          /\ toClient' = IF replaces
                         THEN toClient
                              \o (IF Broken = "replacedCloses" THEN <<>>
                                  ELSE <<[kind |-> "error", joinRef |-> srvJoinRef]>>)
                              \o <<[kind |-> "close", joinRef |-> srvJoinRef]>>
                         ELSE toClient
    /\ UNCHANGED <<wants, conn, chan, wantsChannel, joinRef, nextRef, pushes, srvJoinRef,
                   toServer, faults, userSteps>>

(* It sees its socket is gone, and stops. *)
OldStops ==
    /\ old = "joined" \/ oldBox # <<>>
    /\ old' = "none"
    /\ oldBox' = <<>>
    /\ UNCHANGED <<wants, conn, chan, wantsChannel, joinRef, nextRef, pushes, srv, srvJoinRef,
                   oldJoinRef, handled, toServer, toClient, faults, userSteps>>

---------------------------------------------------------------------------
(* The client, frame by frame in order. channel.isMember drops a frame for an *)
(* older join; frames with no join_ref belong to every join.                 *)

Member(m) == m.joinRef = 0 \/ m.joinRef = joinRef \/ Broken = "noJoinRefCheck"

(* channel.receiveReply: the join's reply counts only in joining.awaitingReply. *)
JoinReplyArrives ==
    /\ toClient # <<>>
    /\ Head(toClient).kind = "joinReply"
    /\ toClient' = Tail(toClient)
    /\ LET m == Head(toClient)
           counts == m.ref = joinRef
                     /\ (chan = "joining" \/ (Broken = "staleJoinReply" /\ chan = "errored"))
       IN IF counts /\ m.ok
          THEN /\ chan' = "joined"
               /\ Flush(joinRef)
          ELSE IF counts
          THEN /\ chan' = "errored"
               /\ LoseSent
               /\ UNCHANGED <<pushJoinRef, toServer>>
          ELSE UNCHANGED <<chan, push, pushJoinRef, answers, toServer>>
    /\ UNCHANGED <<wants, conn, wantsChannel, joinRef, nextRef, pushed, srv, srvJoinRef,
                   oldServer, handled, faults, userSteps>>

LeaveReplyArrives ==
    /\ toClient # <<>>
    /\ Head(toClient).kind = "leaveReply"
    /\ toClient' = Tail(toClient)
    /\ IF chan = "leaving" /\ Member(Head(toClient))
       THEN /\ chan' = "closed"
            /\ ClosePending
       ELSE UNCHANGED <<chan, push, answers>>
    /\ UNCHANGED <<wants, conn, wantsChannel, joinRef, nextRef, pushed, pushJoinRef, srv,
                   srvJoinRef, oldServer, handled, toServer, faults, userSteps>>

(* phx_close: joining and joined -> closed, for good. *)
CloseArrives ==
    /\ toClient # <<>>
    /\ Head(toClient).kind = "close"
    /\ toClient' = Tail(toClient)
    /\ IF chan \in {"joining", "joined"} /\ Member(Head(toClient))
       THEN /\ chan' = "closed"
            /\ ClosePending
       ELSE UNCHANGED <<chan, push, answers>>
    /\ UNCHANGED <<wants, conn, wantsChannel, joinRef, nextRef, pushed, pushJoinRef, srv,
                   srvJoinRef, oldServer, handled, toServer, faults, userSteps>>

(* phx_error: joining and joined -> errored. *)
ErrorArrives ==
    /\ toClient # <<>>
    /\ Head(toClient).kind = "error"
    /\ toClient' = Tail(toClient)
    /\ IF chan \in {"joining", "joined"} /\ Member(Head(toClient))
          /\ ~(Broken = "joinedIgnoresError" /\ chan = "joined")
       THEN /\ chan' = "errored"
            /\ LoseSent
       ELSE UNCHANGED <<chan, push, answers>>
    /\ UNCHANGED <<wants, conn, wantsChannel, joinRef, nextRef, pushed, pushJoinRef, srv,
                   srvJoinRef, oldServer, handled, toServer, faults, userSteps>>

(* A push's reply, matched by its ref: pushMachine takes REPLY only in `sent`. *)
PushReplyArrives ==
    /\ toClient # <<>>
    /\ Head(toClient).kind = "reply"
    /\ toClient' = Tail(toClient)
    /\ LET m == Head(toClient)
       IN IF Member(m) /\ (push[m.id] = "sent" \/ (Broken = "replyAfterTimeout" /\ push[m.id] = "timeout"))
          THEN AnswerAll({m.id}, IF m.ok THEN "ok" ELSE "error")
          ELSE UNCHANGED <<push, answers>>
    /\ UNCHANGED <<wants, conn, chan, wantsChannel, joinRef, nextRef, pushed, pushJoinRef, srv,
                   srvJoinRef, oldServer, handled, toServer, faults, userSteps>>

---------------------------------------------------------------------------

(* Nothing left to happen: no frame in flight, no timer running. *)
Settled ==
    /\ toServer = <<>>
    /\ toClient = <<>>
    /\ \A i \in Pushes : push[i] \notin Pending
    /\ chan \notin {"joining", "leaving"}
    /\ ~(chan = "errored" /\ conn = "open")
    /\ conn \notin {"connecting", "waiting"}
    /\ old = "none"
    /\ oldBox = <<>>

Done == Settled /\ UNCHANGED vars

(* chart.cfg leaves the pushes out, so the chart fits on a screen. *)
NoPush == pushed = 0

Next ==
    \/ YouConnect \/ YouDisconnect \/ YouJoin \/ YouLeave \/ YouPush
    \/ ConnectionOpens \/ ConnectionDrops \/ ReconnectTimerFires
    \/ JoinTimesOut \/ RejoinTimerFires \/ LeaveTimesOut \/ PushTimesOut
    \/ ServerJoins \/ ServerDeniesJoin \/ ServerLeaves \/ ServerAnswersPush \/ ServerChannelCrashes
    \/ OldRuns \/ OldStops
    \/ JoinReplyArrives \/ LeaveReplyArrives \/ CloseArrives \/ ErrorArrives \/ PushReplyArrives
    \/ Done

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ WF_vars(ConnectionOpens) /\ WF_vars(ReconnectTimerFires) /\ WF_vars(RejoinTimerFires)
    /\ WF_vars(LeaveTimesOut) /\ WF_vars(PushTimesOut)
    /\ WF_vars(ServerJoins) /\ WF_vars(ServerLeaves) /\ WF_vars(ServerAnswersPush)
    /\ WF_vars(OldStops)
    /\ WF_vars(JoinReplyArrives) /\ WF_vars(LeaveReplyArrives) /\ WF_vars(CloseArrives)
    /\ WF_vars(ErrorArrives) /\ WF_vars(PushReplyArrives)

---------------------------------------------------------------------------
(* The rules *)

(* The socket stays disconnected only when the app asked for it. *)
ReconnectsUnlessAsked == conn = "disconnected" => ~wants

(* A push waiting for its reply is on the join and connection that can still *)
(* answer it.                                                                *)
WaitingPushCanBeAnswered ==
    \A i \in Pushes :
        push[i] = "sent" => (conn = "open" /\ chan = "joined" /\ pushJoinRef[i] = joinRef)

(* Each push is answered exactly once: never twice. *)
AnsweredOnce == \A i \in Pushes : answers[i] <= 1

(* The server runs each push at most once, whichever process runs it. *)
HandledOnce ==
    \A a, b \in DOMAIN handled : a # b => handled[a].id # handled[b].id

(* The pushes one join carries run in the order the app pushed them. Across  *)
(* joins there is no order: the old connection's process can run its pushes  *)
(* after the new one runs later ones. Durable pushes are for that.            *)
HandledInOrderOnAJoin ==
    \A a, b \in DOMAIN handled :
        (a < b /\ handled[a].joinRef = handled[b].joinRef) => handled[a].id < handled[b].id

(* A channel the app still wants is never closed for good. *)
ClosedOnlyWhenLeft == chan = "closed" => ~wantsChannel

(* Once a channel is closed and the server has read everything sent to it on *)
(* this connection, the server keeps no channel process for it there. (A     *)
(* dropped connection's process stops on its own: OldStops.)                 *)
ServerForgetsClosedChannel == (chan = "closed" /\ toServer = <<>>) => srv = "none"

(* A channel that says it is joined is joined on the server, or the frame that *)
(* says otherwise is on its way.                                              *)
JoinedIsReal ==
    chan = "joined" =>
        \/ srv = "joined" /\ srvJoinRef = joinRef
        \/ \E i \in 1..Len(toClient) : toClient[i].kind \in {"error", "close"}
                                       /\ toClient[i].joinRef = joinRef

(* Once the connection is open, no channel is still waiting for it. *)
NoChannelWaitsOnAnOpenConnection ==
    conn = "open" => chan \notin {"waitingForSocket", "erroredOffline"}

(* A channel the app wants, on a socket the app wants, ends up joined. *)
RejoinsWhenItCan ==
    (wants /\ wantsChannel /\ chan # "unjoined")
        ~> (chan = "joined" \/ ~wants \/ ~wantsChannel)

(* Every push is answered. *)
EveryPushAnswered ==
    \A i \in Pushes : (push[i] \in Pending) ~> (push[i] \in {"ok", "error", "timeout"})

=============================================================================
