----------------------------- MODULE ChannelRead -----------------------------
(***************************************************************************)
(* A channel reading its durable messages through Phoenix, the store       *)
(* `"phoenix"`. In assets/js/phoenix_durable_channel: channelReadMachine    *)
(* and ChannelStream in stream.ts, readerMachine around them, readStream   *)
(* in machines/channel.ts. In lib/phoenix_durable_channel/channel.ex:      *)
(* read/3 and push_durable/4, over PhoenixDurableChannel.Stream.Memory.    *)
(*                                                                         *)
(* Each node keeps its own store, an epoch of its own; a restart, or a     *)
(* topic forgotten while no channel watches it, starts a new epoch. Each    *)
(* join may reach any node. The first join's reply says where to start:     *)
(* the store's last number, or the app's `after` of 0. A read sends        *)
(* phx_read {after}; the server resends what it kept after `after`, a page  *)
(* at a time, then replies {sent, epoch}. A reply in another epoch moves    *)
(* the reader to that epoch, from where it left it or from the start. The   *)
(* read has no timeout. Frames travel in order and a dropped connection     *)
(* loses every frame in flight and the read the server was serving.         *)
(*                                                                         *)
(* Broken names a change that removes one protection; "none" is the code.  *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS NodeCount, MaxPublish, MaxFaults, Page, Broken

Nodes == 1..NodeCount
Epochs == 1..(NodeCount + MaxFaults)
None == [id |-> -1, pos |-> -1]

VARIABLES
    at,          \* the node the channel is joined on; 0 while it is not joined
    started,     \* the first join has said where to start
    from,        \* readerMachine context.from: [after, e]
    left,        \* readerMachine context.left: where it got to in each epoch it left
    read,        \* the read: "idle" (none open), "waiting" for its reply, or "live"
    held,        \* channelReadMachine context.held
    next,        \* channelReadMachine context.next
    delivered,   \* what the channel's bindings were given: <<[e, n]>>
    nodeEpoch,   \* the epoch each node's store is in
    newEpoch,    \* the epoch the next store to start again gets
    sent,        \* the last number each epoch appended
    forgotten,   \* each epoch no longer has messages 1..forgotten
    serving,     \* the read the server's channel process is paging through, or None
    toServer,
    toClient,
    faults,
    startAt      \* ghost: where the first join started: [after, e]

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
(* The connection *)

(* The join, on any node. The first join reply's reader says where to start: *)
(* the store's last, or the app's `after`.                                    *)
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

(* The connection drops: frames in flight are lost, and so is the read the   *)
(* server was serving; the channel's connectionLost tells the read, LOST.    *)
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
(* The client *)

(* readerMachine enters `reading`: ChannelStream listens and sends phx_read. *)
StartsRead ==
    /\ at # 0
    /\ started
    /\ read = "idle"
    /\ toServer = <<>>
    /\ read' = "waiting"
    /\ held' = {}
    /\ toServer' = Append(toServer, [id |-> 1, after |-> from.after])
    /\ UNCHANGED <<at, started, from, left, next, delivered, store, serving, toClient,
                   faults, startAt>>

(* With Broken = "readTimeout", the read gives up while the server is still  *)
(* paging through it. Its reply's ref then matches no read: id 0.            *)
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

(* A durable message: missed with no read listening, held while waiting,      *)
(* delivered when live and next; any other ends the read.                    *)
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

(* The read's reply. In another epoch, the read ends and the reader moves to *)
(* that epoch; otherwise what is held is delivered in order, and the read    *)
(* goes live. A reply to a read that has ended is dropped.                   *)
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
                  /\ from' = IF held = {} THEN from ELSE [from EXCEPT !.after = MaxOf(held)]
                  /\ held' = {}
                  /\ next' = m.sent + 1
                  /\ read' = "live"
                  /\ UNCHANGED left
            [] OTHER -> UNCHANGED <<from, left, read, held, next, delivered>>
    /\ UNCHANGED <<at, started, store, serving, toServer, faults, startAt>>

---------------------------------------------------------------------------
(* The server *)

(* push_durable/4: the store appends, then this channel process pushes. It    *)
(* does not while it serves a read.                                          *)
ServerPublishes ==
    /\ at # 0
    /\ serving = None
    /\ sent[Here] < MaxPublish
    /\ sent' = [sent EXCEPT ![Here] = @ + 1]
    /\ toClient' = Append(toClient, Push(Here, sent[Here] + 1))
    /\ UNCHANGED <<at, client, nodeEpoch, newEpoch, forgotten, serving, toServer, faults>>

(* Another channel process appends, so this one pushes nothing for that     *)
(* message, then this one appends and pushes the next: a gap.                *)
ServerPublishesAfterAGap ==
    /\ at # 0
    /\ serving = None
    /\ sent[Here] + 1 < MaxPublish
    /\ sent' = [sent EXCEPT ![Here] = @ + 2]
    /\ toClient' = Append(toClient, Push(Here, sent[Here] + 2))
    /\ UNCHANGED <<at, client, nodeEpoch, newEpoch, forgotten, serving, toServer, faults>>

(* Another channel process appends to a store this one is not on, or to this *)
(* one's while it pages through a read, which reads on until it has it. (A   *)
(* message appended elsewhere and never followed reaches a live client at    *)
(* its next read only.)                                                      *)
OtherAppends ==
    /\ \E n \in Nodes :
        /\ n # at \/ serving # None
        /\ sent[nodeEpoch[n]] < MaxPublish
        /\ sent' = [sent EXCEPT ![nodeEpoch[n]] = @ + 1]
    /\ UNCHANGED <<at, client, nodeEpoch, newEpoch, forgotten, serving, toServer, toClient,
                   faults>>

(* The store prunes what it has kept. *)
StoreForgets ==
    /\ faults < MaxFaults
    /\ \E n \in Nodes :
        /\ forgotten[nodeEpoch[n]] < sent[nodeEpoch[n]]
        /\ forgotten' = [forgotten EXCEPT ![nodeEpoch[n]] = sent[nodeEpoch[n]]]
    /\ faults' = faults + 1
    /\ UNCHANGED <<at, client, nodeEpoch, newEpoch, sent, serving, toServer, toClient>>

(* A node's store starts again in a new epoch: the node restarted, which     *)
(* drops a connection to it, or the topic was forgotten, which happens only  *)
(* while no channel process watches it.                                      *)
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

(* The channel process takes phx_read. With Broken = "replyBeforeResend" it  *)
(* replies first.                                                            *)
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

(* read/3: one page of what followed `pos` and is still kept; after the last *)
(* page, the reply.                                                          *)
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
(* The rules *)

(* The bindings see each message of an epoch once, in the store's order,     *)
(* also after the reader leaves that epoch and comes back.                   *)
DeliveredOnceInOrder ==
    \A i, j \in DOMAIN delivered :
        (i < j /\ delivered[i].e = delivered[j].e) => delivered[i].n < delivered[j].n

(* How far the client got in epoch e. *)
Reached(e) == IF e = from.e THEN from.after ELSE left[e]

(* In every epoch, up to where the client got, a message it passed over is   *)
(* one the store no longer had, or one from before the first join started.   *)
NothingLostButForgotten ==
    started =>
        \A e \in Epochs : \A n \in 1..Reached(e) :
            \/ [e |-> e, n |-> n] \in Range(delivered)
            \/ n <= forgotten[e]
            \/ (e = startAt.e /\ n <= startAt.after)

(* Once faults stop, the read is live in the epoch of the node it is on, and *)
(* the next message is that store's next.                                    *)
CatchesUp == <>[](at # 0 /\ read = "live" /\ from.e = Here /\ next = sent[Here] + 1)

==============================================================================
