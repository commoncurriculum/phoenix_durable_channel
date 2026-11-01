---------------------------- MODULE PhoenixDurableChannel ----------------------------
EXTENDS Naturals, Sequences

CONSTANTS MaxFaults, MaxUserSteps, MaxPush, Broken

Pushes == 1..MaxPush

VARIABLES
    wants,
    conn,
    chan,
    wantsChannel,
    joinRef,
    nextRef,
    pushed,
    push,
    pushJoinRef,
    answers,
    srv,
    srvJoinRef,
    old,
    oldJoinRef,
    oldBox,
    handled,
    toServer,
    toClient,
    faults,
    userSteps

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

SendJoin ==
    /\ joinRef' = nextRef
    /\ nextRef' = nextRef + 1
    /\ toServer' = Append(toServer, [kind |-> "join", ref |-> nextRef])

AnswerAll(which, outcome) ==
    /\ push' = [i \in Pushes |-> IF i \in which THEN outcome ELSE push[i]]
    /\ answers' = [i \in Pushes |-> IF i \in which THEN answers[i] + 1 ELSE answers[i]]

LoseSent ==
    LET sent == {i \in Pushes : push[i] = "sent"}
    IN CASE Broken = "pushesWaitOnLoss" -> UNCHANGED <<push, answers>>
         [] Broken = "rebufferOnLoss" ->
              /\ push' = [i \in Pushes |-> IF i \in sent THEN "buffered" ELSE push[i]]
              /\ UNCHANGED answers
         [] OTHER -> AnswerAll(sent, "timeout")

ClosePending == AnswerAll({i \in Pushes : push[i] \in Pending}, "timeout")

PushFrame(i, ref) == [kind |-> "push", id |-> i, joinRef |-> ref]

Ids == [i \in Pushes |-> i]
IsBuffered(i) == push[i] = "buffered"
Reverse(s) == [k \in 1..Len(s) |-> s[Len(s) + 1 - k]]

Flush(ref) ==
    LET waiting == SelectSeq(Ids, IsBuffered)
        order == IF Broken = "flushReversed" THEN Reverse(waiting) ELSE waiting
    IN /\ push' = [i \in Pushes |-> IF push[i] = "buffered" THEN "sent" ELSE push[i]]
       /\ pushJoinRef' = [i \in Pushes |-> IF push[i] = "buffered" THEN ref ELSE pushJoinRef[i]]
       /\ toServer' = toServer \o [k \in 1..Len(order) |-> PushFrame(order[k], ref)]
       /\ UNCHANGED answers

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

YouConnect ==
    /\ userSteps < MaxUserSteps
    /\ conn = "disconnected"
    /\ wants' = TRUE
    /\ conn' = "connecting"
    /\ userSteps' = userSteps + 1
    /\ UNCHANGED <<chan, wantsChannel, joinRef, nextRef, pushes, srv, srvJoinRef, oldServer,
                   handled, toServer, toClient, faults>>

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

ConnectionOpens ==
    /\ conn = "connecting"
    /\ conn' = "open"
    /\ IF chan = "waitingForSocket" \/ (chan = "erroredOffline" /\ Broken # "noRejoinOnOpen")
       THEN /\ chan' = "joining"
            /\ SendJoin
       ELSE UNCHANGED <<chan, joinRef, nextRef, toServer>>
    /\ UNCHANGED <<wants, wantsChannel, pushes, srv, srvJoinRef, oldServer, handled,
                   toClient, faults, userSteps>>

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

ReconnectTimerFires ==
    /\ conn = "waiting"
    /\ conn' = "connecting"
    /\ UNCHANGED <<wants, chan, wantsChannel, joinRef, nextRef, pushes, srv, srvJoinRef,
                   oldServer, handled, toServer, toClient, faults, userSteps>>

---------------------------------------------------------------------------

YouJoin ==
    /\ chan = "unjoined"
    /\ IF conn = "open"
       THEN /\ chan' = "joining"
            /\ SendJoin
       ELSE /\ chan' = "waitingForSocket"
            /\ UNCHANGED <<joinRef, nextRef, toServer>>
    /\ UNCHANGED <<wants, conn, wantsChannel, pushes, srv, srvJoinRef, oldServer, handled,
                   toClient, faults, userSteps>>

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

RejoinTimerFires ==
    /\ chan = "errored"
    /\ conn = "open"
    /\ chan' = "joining"
    /\ SendJoin
    /\ UNCHANGED <<wants, conn, wantsChannel, pushes, srv, srvJoinRef, oldServer, handled,
                   toClient, faults, userSteps>>

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

LeaveTimesOut ==
    /\ chan = "leaving"
    /\ chan' = "closed"
    /\ ClosePending
    /\ UNCHANGED <<wants, conn, wantsChannel, joinRef, nextRef, pushed, pushJoinRef, srv,
                   srvJoinRef, oldServer, handled, toServer, toClient, faults, userSteps>>

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

PushTimesOut ==
    /\ Broken # "noPushTimer"
    /\ \E i \in Pushes :
        /\ push[i] \in Pending
        /\ AnswerAll({i}, "timeout")
    /\ UNCHANGED <<wants, conn, chan, wantsChannel, joinRef, nextRef, pushed, pushJoinRef, srv,
                   srvJoinRef, oldServer, handled, toServer, toClient, faults, userSteps>>

---------------------------------------------------------------------------

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

ServerChannelCrashes ==
    /\ faults < MaxFaults
    /\ srv = "joined"
    /\ faults' = faults + 1
    /\ srv' = "none"
    /\ toClient' = Append(toClient, [kind |-> "error", joinRef |-> srvJoinRef])
    /\ UNCHANGED <<wants, conn, chan, wantsChannel, joinRef, nextRef, pushes, srvJoinRef,
                   oldServer, handled, toServer, userSteps>>

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

OldStops ==
    /\ old = "joined" \/ oldBox # <<>>
    /\ old' = "none"
    /\ oldBox' = <<>>
    /\ UNCHANGED <<wants, conn, chan, wantsChannel, joinRef, nextRef, pushes, srv, srvJoinRef,
                   oldJoinRef, handled, toServer, toClient, faults, userSteps>>

---------------------------------------------------------------------------

Member(m) == m.joinRef = 0 \/ m.joinRef = joinRef \/ Broken = "noJoinRefCheck"

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

ReconnectsUnlessAsked == conn = "disconnected" => ~wants

WaitingPushCanBeAnswered ==
    \A i \in Pushes :
        push[i] = "sent" => (conn = "open" /\ chan = "joined" /\ pushJoinRef[i] = joinRef)

AnsweredOnce == \A i \in Pushes : answers[i] <= 1

HandledOnce ==
    \A a, b \in DOMAIN handled : a # b => handled[a].id # handled[b].id

HandledInOrderOnAJoin ==
    \A a, b \in DOMAIN handled :
        (a < b /\ handled[a].joinRef = handled[b].joinRef) => handled[a].id < handled[b].id

ClosedOnlyWhenLeft == chan = "closed" => ~wantsChannel

ServerForgetsClosedChannel == (chan = "closed" /\ toServer = <<>>) => srv = "none"

JoinedIsReal ==
    chan = "joined" =>
        \/ srv = "joined" /\ srvJoinRef = joinRef
        \/ \E i \in 1..Len(toClient) : toClient[i].kind \in {"error", "close"}
                                       /\ toClient[i].joinRef = joinRef

NoChannelWaitsOnAnOpenConnection ==
    conn = "open" => chan \notin {"waitingForSocket", "erroredOffline"}

RejoinsWhenItCan ==
    (wants /\ wantsChannel /\ chan # "unjoined")
        ~> (chan = "joined" \/ ~wants \/ ~wantsChannel)

EveryPushAnswered ==
    \A i \in Pushes : (push[i] \in Pending) ~> (push[i] \in {"ok", "error", "timeout"})

=============================================================================
