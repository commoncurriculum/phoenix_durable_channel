---------------------------- MODULE DurableMessages ----------------------------
(***************************************************************************)
(* The client's durable messages on one channel: logMachine in            *)
(* assets/js/phoenix_durable_channel/machines/log.ts on the client, and    *)
(* join/3, replace/1, handle_durable/4 and sync/2 in                       *)
(* lib/phoenix_durable_channel/channel.ex with *)
(* a ledger on the server. The server's durable messages to the client are  *)
(* ChannelRead.tla.                                                        *)
(*                                                                         *)
(* The connection and the channel's join are one switch here: PhoenixDurableChannel *)
(* .tla checks that a channel rejoins; this module checks what the log     *)
(* does across those rejoins. Frames travel in order. A dropped connection *)
(* loses the frames on their way to the client, but not the server's       *)
(* channel process: it lives on (a half-open WebSocket, a long-poll        *)
(* session) and runs the frames it already received. A durable join waits   *)
(* for it to finish them and stop, or kills it after a timeout. The server *)
(* runs a message (handle_in) and records it (put_received) as two steps,  *)
(* and a process can die between them. A late join from a connection the  *)
(* client gave up on can replace the current process (Replaced).          *)
(*                                                                         *)
(* The ledger keeps the replies of the last Kept numbers, and the client   *)
(* has at most Window unanswered (64 and 32 in the code; the rule needs    *)
(* only Window <= Kept). A ledger is keyed by one client: a page that      *)
(* reloads is another client with another key, so nothing here reloads.   *)
(*                                                                         *)
(* Broken names a change that removes one protection; "none" is the code.  *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS MaxPush, MaxFaults, Window, Kept, Broken

VARIABLES
    up,          \* the connection is open and the channel joined
    phase,       \* logMachine: "away", "syncing" or "synced"
    queue,       \* context.queue: <<[id, num]>>, num 0 = waiting, not sent
    next,        \* context.next; 0 before the first sync
    syncId,      \* which sync the client waits for; a timed-out one's Push is gone
    answers,     \* every answer the app got: <<[id, status]>>
    pushes,      \* ids given to the app's durable pushes so far
    received,    \* the ledger's received/1
    replies,     \* the ledger's reply/2: the status recorded for each number
    handled,     \* every run of the app's handle_in: <<[id, status]>>
    toServer,    \* the current process's mailbox
    toClient,
    running,     \* the current process between handle_in and put_received: <<[id, num, status]>>
    oldBox,      \* the old connection's process: its mailbox ...
    oldRunning,  \* ... and what it is between handle_in and put_received
    gone,        \* a late join replaced the current process; the client has not seen phx_error
    crashes,     \* processes that died between handle_in and put_received
    forgotten,   \* a resend asked for a reply the ledger no longer kept
    faults

vars == <<up, phase, queue, next, syncId, answers, pushes, received, replies, handled,
          toServer, toClient, running, oldBox, oldRunning, gone, crashes, forgotten, faults>>

clientVars == <<phase, queue, next, syncId, answers, pushes>>
ledgerVars == <<received, replies, handled, forgotten>>
oldVars == <<oldBox, oldRunning, gone>>

Init ==
    /\ up = FALSE
    /\ phase = "away"
    /\ queue = <<>>
    /\ next = 0
    /\ syncId = 0
    /\ answers = <<>>
    /\ pushes = 0
    /\ received = 0
    /\ replies = <<>>
    /\ handled = <<>>
    /\ toServer = <<>>
    /\ toClient = <<>>
    /\ running = <<>>
    /\ oldBox = <<>>
    /\ oldRunning = <<>>
    /\ gone = FALSE
    /\ crashes = 0
    /\ forgotten = FALSE
    /\ faults = 0

Max(a, b) == IF a > b THEN a ELSE b
Range(s) == {s[i] : i \in DOMAIN s}
PushFrame(e) == [k |-> "push", id |-> e.id, num |-> e.num]
OldGone == oldBox = <<>> /\ oldRunning = <<>>
Limit == IF Broken = "noWindow" THEN MaxPush ELSE Window

(* sendWaiting: numbers and sends the waiting entries, in order, while fewer *)
(* than Window are unanswered, numbering on from `from`.                     *)
Sent(q, from) ==
    LET inFlight == Cardinality({i \in DOMAIN q : q[i].num # 0})
        waiting == {i \in DOMAIN q : q[i].num = 0}
        rank(i) == Cardinality({j \in waiting : j <= i})
        sends(i) == i \in waiting /\ inFlight + rank(i) <= Limit
    IN [i \in DOMAIN q |-> IF sends(i) THEN [id |-> q[i].id, num |-> from + rank(i) - 1]
                           ELSE q[i]]
Fresh(q, s) == SelectSeq(s, LAMBDA e : \E i \in DOMAIN q : q[i].id = e.id /\ q[i].num = 0 /\ e.num # 0)
SendCount(q, s) == Len(Fresh(q, s))

---------------------------------------------------------------------------
(* The connection *)

(* CHANNEL_JOINED: away -> syncing, which sends phx_sync. replace/1 has     *)
(* already waited for the old connection's process to stop.                 *)
Joins ==
    /\ ~up
    /\ OldGone \/ Broken = "noReplace"
    /\ up' = TRUE
    /\ phase' = "syncing"
    /\ syncId' = syncId + 1
    /\ toServer' = <<[k |-> "sync", sid |-> syncId + 1]>>
    /\ gone' = FALSE
    /\ UNCHANGED <<queue, next, answers, pushes, ledgerVars, toClient, running, oldBox,
                   oldRunning, crashes, faults>>

(* replace/1 waited too long: it kills the old process, whatever it was doing. *)
ReplaceKills ==
    /\ ~up
    /\ ~OldGone
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ crashes' = crashes + IF oldRunning # <<>> THEN 1 ELSE 0
    /\ oldBox' = <<>>
    /\ oldRunning' = <<>>
    /\ UNCHANGED <<up, clientVars, ledgerVars, toServer, toClient, running, gone>>

(* The connection drops: CHANNEL_LEFT -> away. Frames on their way to the    *)
(* client are lost; the server's process becomes the old one, and keeps its  *)
(* mailbox. Durable pushes in flight time out, and are ignored.               *)
Drops ==
    /\ faults < MaxFaults
    /\ up
    /\ OldGone
    /\ faults' = faults + 1
    /\ up' = FALSE
    /\ phase' = "away"
    /\ oldBox' = IF gone THEN <<>> ELSE toServer
    /\ oldRunning' = running
    /\ gone' = FALSE
    /\ toServer' = <<>>
    /\ running' = <<>>
    /\ toClient' = <<>>
    /\ UNCHANGED <<queue, next, syncId, answers, pushes, ledgerVars, crashes>>

(* The current process dies between handle_in and put_received; the          *)
(* connection goes with it.                                                  *)
ServerCrashes ==
    /\ faults < MaxFaults
    /\ running # <<>>
    /\ faults' = faults + 1
    /\ crashes' = crashes + 1
    /\ up' = FALSE
    /\ phase' = "away"
    /\ toServer' = <<>>
    /\ running' = <<>>
    /\ toClient' = <<>>
    /\ UNCHANGED <<queue, next, syncId, answers, pushes, ledgerVars, oldVars>>

---------------------------------------------------------------------------
(* The client *)

(* channel.push(event, payload, { durable: true }): PUSH, then sendWaiting    *)
(* while synced.                                                             *)
AppPushes ==
    /\ pushes < MaxPush
    /\ pushes' = pushes + 1
    /\ LET q == Append(queue, [id |-> pushes + 1, num |-> 0])
           s == Sent(q, next)
       IN IF phase = "synced"
          THEN /\ queue' = s
               /\ next' = next + SendCount(q, s)
               /\ toServer' = toServer \o [i \in DOMAIN Fresh(q, s) |-> PushFrame(Fresh(q, s)[i])]
          ELSE /\ queue' = q
               /\ UNCHANGED <<next, toServer>>
    /\ UNCHANGED <<up, phase, syncId, answers, ledgerVars, toClient, running, oldVars,
                   crashes, faults>>

(* phx_sync times out while joined: SYNC_FAILED -> backingOff -> syncing, a *)
(* new sync. The old one's reply may still come, to a Push that is gone.     *)
SyncTimesOut ==
    /\ faults < MaxFaults
    /\ up
    /\ phase = "syncing"
    /\ faults' = faults + 1
    /\ syncId' = syncId + 1
    /\ toServer' = Append(toServer, [k |-> "sync", sid |-> syncId + 1])
    /\ UNCHANGED <<up, phase, queue, next, answers, pushes, ledgerVars, toClient, running,
                   oldVars, crashes>>

(* SYNCED {received} (applySync): every entry waits again, and sendWaiting   *)
(* numbers them from the first one's number, or received + 1 if lower.       *)
SyncReplyArrives ==
    /\ toClient # <<>>
    /\ Head(toClient).k = "syncReply"
    /\ toClient' = Tail(toClient)
    /\ LET m == Head(toClient)
           counts == phase = "syncing" /\ m.sid = syncId
           first == IF queue # <<>> /\ queue[1].num # 0 THEN queue[1].num ELSE m.received + 1
           base == IF first < m.received + 1 THEN first ELSE m.received + 1
           waiting == [i \in DOMAIN queue |-> [id |-> queue[i].id, num |-> 0]]
           s == Sent(waiting, base)
           wasNumbered == {queue[i].id : i \in {j \in DOMAIN queue : queue[j].num # 0}}
           fresh == Fresh(waiting, s)
           resend == IF Broken = "noResend"
                     THEN SelectSeq(fresh, LAMBDA e : e.id \notin wasNumbered)
                     ELSE fresh
       IN IF counts
          THEN /\ next' = base + Len(fresh)
               /\ queue' = s
               /\ phase' = "synced"
               /\ toServer' = toServer \o [i \in DOMAIN resend |-> PushFrame(resend[i])]
          ELSE UNCHANGED <<next, queue, phase, toServer>>
    /\ UNCHANGED <<up, syncId, answers, pushes, ledgerVars, running, oldVars, crashes, faults>>

(* REPLY: the app's Reply, then the entry leaves the queue, and sendWaiting  *)
(* while synced. "Not taken" is no answer: while synced, -> backingOff ->    *)
(* syncing; elsewhere the log is already syncing again and ignores it.       *)
AnswerArrives ==
    /\ toClient # <<>>
    /\ Head(toClient).k = "answer"
    /\ toClient' = Tail(toClient)
    /\ LET m == Head(toClient)
           q == SelectSeq(queue, LAMBDA e : e.id # m.id)
           s == Sent(q, next)
       IN IF \/ m.status = "notTaken" /\ Broken # "retryIsAnswer"
             \/ m.status = "unmatched" /\ Broken # "unmatchedIsAnswer"
          THEN /\ IF phase = "synced"
                  THEN /\ phase' = "syncing"
                       /\ syncId' = syncId + 1
                       /\ toServer' = Append(toServer, [k |-> "sync", sid |-> syncId + 1])
                  ELSE UNCHANGED <<phase, syncId, toServer>>
               /\ UNCHANGED <<answers, queue, next>>
          ELSE /\ answers' = Append(answers, [id |-> m.id, status |-> m.status])
               /\ IF phase = "synced"
                  THEN /\ queue' = s
                       /\ next' = next + SendCount(q, s)
                       /\ toServer' = toServer \o [i \in DOMAIN Fresh(q, s) |-> PushFrame(Fresh(q, s)[i])]
                  ELSE /\ queue' = q
                       /\ UNCHANGED <<next, toServer>>
               /\ UNCHANGED <<phase, syncId>>
    /\ UNCHANGED <<up, pushes, ledgerVars, running, oldVars, crashes, faults>>

---------------------------------------------------------------------------
(* The server: one step of handle_durable/4 for a frame `m`, by the current  *)
(* process (`answer` reaches the client) or the old one (it does not).       *)

(* The answer to a number the ledger already recorded: its reply, if kept. *)
Historic(num) == IF Broken = "historicIsOk" THEN "ok" ELSE replies[num]

(* received/1, then handle_in. The count moves only with put_received/3. *)
Begins(m, status) ==
    /\ handled' = Append(handled, [id |-> m.id, status |-> status])
    /\ received' = IF Broken = "countBeforeRecord" THEN m.num ELSE received

(* put_received/3: the reply and the count, which never goes back. *)
Records(r) ==
    /\ received' = Max(received, r.num)
    /\ replies' = IF r.num > Len(replies) THEN Append(replies, r.status)
                  ELSE [replies EXCEPT ![r.num] = r.status]

(* The current process takes the next frame of its mailbox. *)
ServerTakes ==
    /\ ~gone
    /\ running = <<>>
    /\ toServer # <<>>
    /\ toServer' = Tail(toServer)
    /\ LET m == Head(toServer)
       IN IF m.k = "sync"
          THEN /\ toClient' = Append(toClient,
                                     [k |-> "syncReply", received |-> received, sid |-> m.sid])
               /\ UNCHANGED <<ledgerVars, running>>
          ELSE IF m.num = received + 1 \/ (Broken = "noDedupe" /\ m.num <= received)
          THEN \E status \in {"ok", "error"} :
                  /\ Begins(m, status)
                  /\ running' = <<[id |-> m.id, num |-> m.num, status |-> status]>>
                  /\ UNCHANGED <<replies, forgotten, toClient>>
          ELSE IF m.num > received
          THEN /\ toClient' = Append(toClient, [k |-> "answer", id |-> m.id, status |-> "notTaken"])
               /\ UNCHANGED <<ledgerVars, running>>
          ELSE IF m.num <= received - Kept \/ m.num > Len(replies)
          THEN /\ forgotten' = TRUE
               /\ toClient' = Append(toClient, [k |-> "answer", id |-> m.id, status |-> "ok"])
               /\ UNCHANGED <<received, replies, handled, running>>
          ELSE /\ toClient' = Append(toClient,
                                     [k |-> "answer", id |-> m.id, status |-> Historic(m.num)])
               /\ UNCHANGED <<ledgerVars, running>>
    /\ UNCHANGED <<up, clientVars, oldVars, crashes, faults>>

(* The current process records what it ran, and answers. *)
ServerRecords ==
    /\ running # <<>>
    /\ Records(running[1])
    /\ running' = <<>>
    /\ toClient' = Append(toClient, [k |-> "answer", id |-> running[1].id,
                                     status |-> running[1].status])
    /\ UNCHANGED <<up, clientVars, handled, forgotten, toServer, oldVars, crashes, faults>>

(* handle_in/3 returns {:retry, reason, socket}: not counted, refused as not taken. *)
ServerCannotTake ==
    /\ faults < MaxFaults
    /\ ~gone
    /\ running = <<>>
    /\ toServer # <<>>
    /\ Head(toServer).k = "push"
    /\ Head(toServer).num = received + 1
    /\ faults' = faults + 1
    /\ toServer' = Tail(toServer)
    /\ toClient' = Append(toClient, [k |-> "answer", id |-> Head(toServer).id, status |-> "notTaken"])
    /\ UNCHANGED <<up, clientVars, ledgerVars, running, oldVars, crashes>>

(* The old process takes the next frame of its mailbox; its answers go to a *)
(* connection that is gone.                                                  *)
OldTakes ==
    /\ oldRunning = <<>>
    /\ oldBox # <<>>
    /\ oldBox' = Tail(oldBox)
    /\ LET m == Head(oldBox)
       IN IF m.k = "push" /\ (m.num = received + 1 \/ (Broken = "noDedupe" /\ m.num <= received))
          THEN \E status \in {"ok", "error"} :
                  /\ Begins(m, status)
                  /\ oldRunning' = <<[id |-> m.id, num |-> m.num, status |-> status]>>
                  /\ UNCHANGED <<replies, forgotten>>
          ELSE UNCHANGED <<ledgerVars, oldRunning>>
    /\ UNCHANGED <<up, clientVars, toServer, toClient, running, gone, crashes, faults>>

OldRecords ==
    /\ oldRunning # <<>>
    /\ Records(oldRunning[1])
    /\ oldRunning' = <<>>
    /\ UNCHANGED <<up, clientVars, handled, forgotten, toServer, toClient, running, oldBox,
                   gone, crashes, faults>>

(* A late join from a connection the client gave up on replaces the current *)
(* process once it has run its mailbox (replace/1), and replaced/1 sends    *)
(* the client phx_error.                                                    *)
Replaced ==
    /\ faults < MaxFaults
    /\ up
    /\ ~gone
    /\ running = <<>>
    /\ toServer = <<>>
    /\ faults' = faults + 1
    /\ gone' = TRUE
    /\ toClient' = Append(toClient, [k |-> "error"])
    /\ UNCHANGED <<up, clientVars, ledgerVars, toServer, running, oldBox, oldRunning, crashes>>

(* Phoenix answers a frame for the stopped process "unmatched topic". *)
Unmatched ==
    /\ gone
    /\ toServer # <<>>
    /\ toServer' = Tail(toServer)
    /\ toClient' = IF Head(toServer).k = "push"
                   THEN Append(toClient, [k |-> "answer", id |-> Head(toServer).id,
                                          status |-> "unmatched"])
                   ELSE toClient
    /\ UNCHANGED <<up, clientVars, ledgerVars, running, oldVars, crashes, faults>>

(* phx_error: CHANNEL_LEFT -> away, and the channel rejoins (Joins). *)
ErrorArrives ==
    /\ toClient # <<>>
    /\ Head(toClient).k = "error"
    /\ toClient' = Tail(toClient)
    /\ up' = FALSE
    /\ phase' = "away"
    /\ UNCHANGED <<queue, next, syncId, answers, pushes, ledgerVars, toServer, running,
                   oldVars, crashes, faults>>

---------------------------------------------------------------------------

Progress ==
    \/ Joins
    \/ SyncReplyArrives \/ AnswerArrives \/ ErrorArrives \/ Unmatched
    \/ ServerTakes \/ ServerRecords
    \/ OldTakes \/ OldRecords

Settled == up /\ toServer = <<>> /\ toClient = <<>> /\ running = <<>> /\ OldGone
Done == Settled /\ UNCHANGED vars

Next ==
    \/ Progress
    \/ Drops \/ ServerCrashes \/ ReplaceKills \/ SyncTimesOut \/ ServerCannotTake \/ Replaced
    \/ AppPushes
    \/ Done

Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)

---------------------------------------------------------------------------
(* The rules *)

(* The app's handle_in runs the durable pushes in the order pushed. *)
HandledInOrder == \A i \in DOMAIN handled : i > 1 => handled[i - 1].id <= handled[i].id

(* Each durable push runs once, but for a process that died between running  *)
(* it and recording it: each extra run has a crash of its own.               *)
HandledOnceUnlessCrashed ==
    Len(handled) - Cardinality({handled[i].id : i \in DOMAIN handled}) <= crashes

(* Each durable push is answered at most once. *)
AnsweredOnce == \A i, j \in DOMAIN answers : i < j => answers[i].id # answers[j].id

(* Every answer is one the app's handle_in gave that push, resends included. *)
AnsweredAsHandled == \A i \in DOMAIN answers : answers[i] \in Range(handled)

(* A push the server did not take, or that reached no process, is sent again, *)
(* never answered as if it were refused.                                       *)
OnlyTakenAreAnswered == \A i \in DOMAIN answers : answers[i].status \notin {"notTaken", "unmatched"}

(* Every number the ledger counts has its reply recorded. *)
CountIsRecorded == received = Len(replies)

(* A resend is always of a number whose reply the ledger still keeps. *)
RepliesKept == ~forgotten

(* Once faults stop, every durable push is answered. *)
CatchesUp == <>[](queue = <<>> /\ phase = "synced")

==============================================================================
