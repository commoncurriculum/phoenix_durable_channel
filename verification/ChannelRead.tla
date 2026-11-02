----------------------------- MODULE ChannelRead -----------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS NodeCount, MaxPublish, MaxFaults, Page, Broken

Nodes == 1..NodeCount
Epochs == 1..(NodeCount + MaxFaults)
None == [id |-> -1, pos |-> -1]

VARIABLES
    at,
    started,
    from,
    left,
    read,
    held,
    next,
    delivered,
    nodeEpoch,
    newEpoch,
    sent,
    forgotten,
    serving,
    toServer,
    toClient,
    faults,
    startAt

vars == <<at, started, from, left, read, held, next, delivered, nodeEpoch, newEpoch,
          sent, forgotten, serving, toServer, toClient, faults, startAt>>
client == <<started, from, left, read, held, next, delivered, startAt>>
store == <<nodeEpoch, newEpoch, sent, forgotten>>

Init ==
    /\ at = 0
    /\ started = FALSE
    /\ from = [after |-> 0, e |-> 0]
    /\ left = [e \in Epochs |-> 0]
    /\ read = "idle"
    /\ held = {}
    /\ next = 0
    /\ delivered = <<>>
    /\ nodeEpoch = [n \in Nodes |-> n]
    /\ newEpoch = NodeCount + 1
    /\ sent = [e \in Epochs |-> 0]
    /\ forgotten = [e \in Epochs |-> 0]
    /\ serving = None
    /\ toServer = <<>>
    /\ toClient = <<>>
    /\ faults = 0
    /\ startAt = [after |-> 0, e |-> 0]

Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b
Range(s) == {s[i] : i \in DOMAIN s}

RECURSIVE Sorted(_)
Sorted(h) == IF h = {} THEN <<>>
             ELSE LET m == CHOOSE x \in h : \A y \in h : x <= y
                  IN <<m>> \o Sorted(h \ {m})

MaxOf(h) == CHOOSE x \in h : \A y \in h : x >= y
Push(e, n) == [k |-> "msg", e |-> e, n |-> n]
Here == nodeEpoch[at]

---------------------------------------------------------------------------

Joins ==
    /\ at = 0
    /\ \E n \in Nodes, appAfter \in BOOLEAN :
        /\ at' = n
        /\ IF started
           THEN UNCHANGED <<started, from, startAt>>
           ELSE LET f == [after |-> IF appAfter THEN 0 ELSE sent[nodeEpoch[n]],
                          e |-> nodeEpoch[n]]
                IN /\ started' = TRUE
                   /\ from' = f
                   /\ startAt' = f
    /\ UNCHANGED <<left, read, held, next, delivered, store, serving, toServer,
                   toClient, faults>>

Lost ==
    /\ at' = 0
    /\ serving' = None
    /\ toServer' = <<>>
    /\ toClient' = <<>>
    /\ read' = IF Broken = "noFailOnLoss" THEN read ELSE "idle"

Drops ==
    /\ faults < MaxFaults
    /\ at # 0
    /\ faults' = faults + 1
    /\ Lost
    /\ UNCHANGED <<started, from, left, held, next, delivered, store, startAt>>

---------------------------------------------------------------------------

StartsRead ==
    /\ at # 0 \/ Broken = "readWhileAway"
    /\ started
    /\ read = "idle"
    /\ toServer = <<>>
    /\ read' = "waiting"
    /\ held' = {}
    /\ toServer' = Append(toServer, [id |-> 1, after |-> from.after])
    /\ UNCHANGED <<at, started, from, left, next, delivered, store, serving, toClient,
                   faults, startAt>>

ReadTimesOut ==
    /\ Broken = "readTimeout"
    /\ read = "waiting"
    /\ serving.id = 1
    /\ read' = "idle"
    /\ serving' = [serving EXCEPT !.id = 0]
    /\ UNCHANGED <<at, started, from, left, held, next, delivered, store, toServer,
                   toClient, faults, startAt>>

Deliver(m) ==
    /\ delivered' = Append(delivered, [e |-> m.e, n |-> m.n])
    /\ from' = [from EXCEPT !.after = m.n]

MessageArrives ==
    /\ toClient # <<>>
    /\ Head(toClient).k = "msg"
    /\ toClient' = Tail(toClient)
    /\ LET m == Head(toClient)
       IN CASE read = "waiting" /\ Broken = "deliverOnArrival" ->
                  /\ Deliver(m)
                  /\ UNCHANGED <<read, held, next>>
            [] read = "waiting" ->
                  /\ held' = held \cup {m.n}
                  /\ UNCHANGED <<read, from, next, delivered>>
            [] read = "live" /\ (m.n = next \/ (Broken = "noGapCheck" /\ m.n > from.after)) ->
                  /\ Deliver(m)
                  /\ next' = m.n + 1
                  /\ UNCHANGED <<read, held>>
            [] read = "live" ->
                  /\ read' = "idle"
                  /\ UNCHANGED <<from, held, next, delivered>>
            [] OTHER -> UNCHANGED <<read, from, held, next, delivered>>
    /\ UNCHANGED <<at, started, left, store, serving, toServer, faults, startAt>>

ReplyArrives ==
    /\ toClient # <<>>
    /\ Head(toClient).k = "reply"
    /\ toClient' = Tail(toClient)
    /\ LET m == Head(toClient)
           current == read = "waiting" /\ m.id = 1
       IN CASE current /\ m.epoch # from.e /\ Broken # "noEpochCheck" ->
                  /\ read' = "idle"
                  /\ left' = [left EXCEPT ![from.e] = from.after]
                  /\ from' = [after |-> IF Broken = "fromStartOnReturn" THEN 0
                                        ELSE left[m.epoch],
                              e |-> m.epoch]
                  /\ UNCHANGED <<held, next, delivered>>
            [] current ->
                  /\ delivered' = delivered \o [i \in 1..Cardinality(held) |->
                                                  [e |-> m.epoch, n |-> Sorted(held)[i]]]
                  /\ from' = [from EXCEPT !.after =
                                 Max(IF held = {} THEN from.after ELSE MaxOf(held), m.sent)]
                  /\ held' = {}
                  /\ next' = m.sent + 1
                  /\ read' = "live"
                  /\ UNCHANGED left
            [] OTHER -> UNCHANGED <<from, left, read, held, next, delivered>>
    /\ UNCHANGED <<at, started, store, serving, toServer, faults, startAt>>

---------------------------------------------------------------------------

ServerPublishes ==
    /\ at # 0
    /\ serving = None
    /\ sent[Here] < MaxPublish
    /\ sent' = [sent EXCEPT ![Here] = @ + 1]
    /\ toClient' = Append(toClient, Push(Here, sent[Here] + 1))
    /\ UNCHANGED <<at, client, nodeEpoch, newEpoch, forgotten, serving, toServer, faults>>

ServerPublishesAfterAGap ==
    /\ at # 0
    /\ serving = None
    /\ sent[Here] + 1 < MaxPublish
    /\ sent' = [sent EXCEPT ![Here] = @ + 2]
    /\ toClient' = Append(toClient, Push(Here, sent[Here] + 2))
    /\ UNCHANGED <<at, client, nodeEpoch, newEpoch, forgotten, serving, toServer, faults>>

OtherAppends ==
    /\ \E n \in Nodes :
        /\ n # at \/ serving # None
        /\ sent[nodeEpoch[n]] < MaxPublish
        /\ sent' = [sent EXCEPT ![nodeEpoch[n]] = @ + 1]
    /\ UNCHANGED <<at, client, nodeEpoch, newEpoch, forgotten, serving, toServer, toClient,
                   faults>>

StoreForgets ==
    /\ faults < MaxFaults
    /\ \E n \in Nodes :
        /\ forgotten[nodeEpoch[n]] < sent[nodeEpoch[n]]
        /\ forgotten' = [forgotten EXCEPT ![nodeEpoch[n]] = sent[nodeEpoch[n]]]
    /\ faults' = faults + 1
    /\ UNCHANGED <<at, client, nodeEpoch, newEpoch, sent, serving, toServer, toClient>>

StoreStartsAgain ==
    /\ faults < MaxFaults
    /\ \E n \in Nodes :
        /\ nodeEpoch' = [nodeEpoch EXCEPT ![n] = newEpoch]
        /\ IF at = n
           THEN Lost
           ELSE UNCHANGED <<at, read, serving, toServer, toClient>>
    /\ newEpoch' = newEpoch + 1
    /\ faults' = faults + 1
    /\ UNCHANGED <<started, from, left, held, next, delivered, sent, forgotten,
                   startAt>>

ServerTakesRead ==
    /\ at # 0
    /\ serving = None
    /\ toServer # <<>>
    /\ toServer' = Tail(toServer)
    /\ serving' = [id |-> Head(toServer).id, pos |-> Head(toServer).after]
    /\ toClient' = IF Broken = "replyBeforeResend"
                   THEN Append(toClient, [k |-> "reply", id |-> Head(toServer).id,
                                          sent |-> sent[Here], epoch |-> Here])
                   ELSE toClient
    /\ UNCHANGED <<at, client, store, faults>>

ServerSendsPage ==
    /\ at # 0
    /\ serving # None
    /\ LET e == Here
           pageSent == Min(sent[e], serving.pos + Page)
           pushes == [i \in 1..Max(0, pageSent - Max(serving.pos, forgotten[e])) |->
                        Push(e, Max(serving.pos, forgotten[e]) + i)]
           more == pageSent < sent[e] /\ Broken # "replyOnFirstPage"
           reply == IF more \/ Broken = "replyBeforeResend" THEN <<>>
                    ELSE <<[k |-> "reply", id |-> serving.id, sent |-> pageSent, epoch |-> e]>>
       IN /\ toClient' = toClient \o pushes \o reply
          /\ serving' = IF more THEN [serving EXCEPT !.pos = pageSent] ELSE None
    /\ UNCHANGED <<at, client, store, toServer, faults>>

---------------------------------------------------------------------------

Progress == Joins \/ StartsRead \/ MessageArrives \/ ReplyArrives \/ ServerTakesRead
            \/ ServerSendsPage

Settled == at # 0 /\ toServer = <<>> /\ toClient = <<>> /\ serving = None
Done == Settled /\ UNCHANGED vars

Next ==
    \/ Progress
    \/ Drops \/ StoreForgets \/ StoreStartsAgain \/ ReadTimesOut
    \/ ServerPublishes \/ ServerPublishesAfterAGap \/ OtherAppends
    \/ Done

Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)

---------------------------------------------------------------------------

DeliveredOnceInOrder ==
    \A i, j \in DOMAIN delivered :
        (i < j /\ delivered[i].e = delivered[j].e) => delivered[i].n < delivered[j].n

Reached(e) == IF e = from.e THEN from.after ELSE left[e]

NothingLostButForgotten ==
    started =>
        \A e \in Epochs : \A n \in 1..Reached(e) :
            \/ [e |-> e, n |-> n] \in Range(delivered)
            \/ n <= forgotten[e]
            \/ (e = startAt.e /\ n <= startAt.after)

ReadOnlyWhileJoined == read # "idle" => at # 0

CatchesUp == <>[](at # 0 /\ read = "live" /\ from.e = Here /\ next = sent[Here] + 1)

==============================================================================
