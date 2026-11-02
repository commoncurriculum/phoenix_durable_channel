---------------------------- MODULE DurableMessages ----------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS MaxPush, MaxFaults, Window, Kept, Broken

VARIABLES
    up,
    phase,
    queue,
    next,
    syncId,
    answers,
    pushes,
    received,
    known,
    replies,
    handled,
    toServer,
    toClient,
    running,
    oldBox,
    oldRunning,
    gone,
    crashes,
    forgotten,
    faults,
    replacing,
    oldKnown,
    reset

vars == <<up, phase, queue, next, syncId, answers, pushes, received, known, replies, handled,
          toServer, toClient, running, oldBox, oldRunning, gone, crashes, forgotten, faults,
          replacing, oldKnown, reset>>

clientVars == <<phase, queue, next, syncId, answers, pushes>>
ledgerVars == <<received, known, replies, handled, forgotten>>
oldVars == <<oldBox, oldRunning, gone, oldKnown>>

Init ==
    /\ up = FALSE
    /\ phase = "away"
    /\ queue = <<>>
    /\ next = 0
    /\ syncId = 0
    /\ answers = <<>>
    /\ pushes = 0
    /\ received = 0
    /\ known = 0
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
    /\ replacing = FALSE
    /\ oldKnown = 0
    /\ reset = "no"

Max(a, b) == IF a > b THEN a ELSE b
Range(s) == {s[i] : i \in DOMAIN s}
PushFrame(e) == [k |-> "push", id |-> e.id, num |-> e.num]
OldGone == oldBox = <<>> /\ oldRunning = <<>>
Limit == IF Broken = "noWindow" THEN MaxPush ELSE Window

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

Joins ==
    /\ ~up
    /\ up' = TRUE
    /\ replacing' = TRUE
    /\ phase' = "syncing"
    /\ syncId' = syncId + 1
    /\ toServer' = <<[k |-> "sync", sid |-> syncId + 1]>>
    /\ gone' = FALSE
    /\ reset' = IF reset = "live" THEN "joined" ELSE reset
    /\ UNCHANGED <<queue, next, answers, pushes, ledgerVars, toClient, running, oldBox,
                   oldRunning, oldKnown, crashes, faults>>

ServerReplaces ==
    /\ replacing
    /\ OldGone \/ Broken = "noReplace"
    /\ replacing' = FALSE
    /\ known' = IF Broken = "staleCount" THEN known ELSE received
    /\ UNCHANGED <<up, clientVars, received, replies, handled, forgotten, toServer, toClient,
                   running, oldVars, crashes, faults, reset>>

ReplaceKills ==
    /\ replacing
    /\ ~OldGone
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ crashes' = crashes + IF oldRunning # <<>> THEN 1 ELSE 0
    /\ oldBox' = <<>>
    /\ oldRunning' = <<>>
    /\ UNCHANGED <<up, clientVars, ledgerVars, toServer, toClient, running, gone, oldKnown,
                   replacing, reset>>

Drops ==
    /\ faults < MaxFaults
    /\ up
    /\ OldGone
    /\ faults' = faults + 1
    /\ up' = FALSE
    /\ replacing' = FALSE
    /\ phase' = "away"
    /\ oldBox' = IF gone THEN <<>> ELSE toServer
    /\ oldRunning' = running
    /\ oldKnown' = IF replacing THEN received ELSE known
    /\ gone' = FALSE
    /\ toServer' = <<>>
    /\ running' = <<>>
    /\ toClient' = <<>>
    /\ UNCHANGED <<queue, next, syncId, answers, pushes, ledgerVars, crashes, reset>>

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
    /\ UNCHANGED <<queue, next, syncId, answers, pushes, ledgerVars, oldVars, replacing, reset>>

LedgerResets ==
    /\ faults < MaxFaults
    /\ up
    /\ ~replacing
    /\ faults' = faults + 1
    /\ reset' = "live"
    /\ received' = 0
    /\ replies' = <<>>
    /\ known' = IF Broken = "resetRereads" THEN 0 ELSE known
    /\ UNCHANGED <<up, clientVars, handled, forgotten, toServer, toClient, running, oldVars,
                   crashes, replacing>>

---------------------------------------------------------------------------

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
                   crashes, faults, replacing, reset>>

SyncTimesOut ==
    /\ faults < MaxFaults
    /\ up
    /\ phase = "syncing"
    /\ faults' = faults + 1
    /\ syncId' = syncId + 1
    /\ toServer' = Append(toServer, [k |-> "sync", sid |-> syncId + 1])
    /\ UNCHANGED <<up, phase, queue, next, answers, pushes, ledgerVars, toClient, running,
                   oldVars, crashes, replacing, reset>>

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
    /\ UNCHANGED <<up, syncId, answers, pushes, ledgerVars, running, oldVars, crashes, faults,
                   replacing, reset>>

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
    /\ UNCHANGED <<up, pushes, ledgerVars, running, oldVars, crashes, faults, replacing, reset>>

---------------------------------------------------------------------------

Historic(num) == IF Broken = "historicIsOk" THEN "ok" ELSE replies[num]

Begins(m, status) ==
    /\ handled' = Append(handled, [id |-> m.id, status |-> status])
    /\ received' = IF Broken = "countBeforeRecord" THEN m.num ELSE received
    /\ known' = known

Records(r) ==
    /\ received' = Max(received, r.num)
    /\ replies' = IF r.num > Len(replies)
                  THEN [i \in 1..r.num |-> IF i = r.num THEN r.status
                                           ELSE IF i <= Len(replies) THEN replies[i]
                                           ELSE "lost"]
                  ELSE [replies EXCEPT ![r.num] = r.status]

ServerTakes ==
    /\ ~gone
    /\ ~replacing \/ Broken = "syncBeforeReplace"
    /\ running = <<>>
    /\ toServer # <<>>
    /\ toServer' = Tail(toServer)
    /\ LET m == Head(toServer)
       IN IF m.k = "sync"
          THEN /\ toClient' = Append(toClient,
                                     [k |-> "syncReply", received |-> known, sid |-> m.sid])
               /\ UNCHANGED <<ledgerVars, running>>
          ELSE IF m.num = known + 1 \/ (Broken = "noDedupe" /\ m.num <= known)
          THEN \E status \in {"ok", "error"} :
                  /\ Begins(m, status)
                  /\ running' = <<[id |-> m.id, num |-> m.num, status |-> status]>>
                  /\ UNCHANGED <<replies, forgotten, toClient>>
          ELSE IF m.num > known
          THEN /\ toClient' = Append(toClient, [k |-> "answer", id |-> m.id, status |-> "notTaken"])
               /\ UNCHANGED <<ledgerVars, running>>
          ELSE IF m.num <= received - Kept \/ m.num > Len(replies) \/ replies[m.num] = "lost"
          THEN /\ forgotten' = TRUE
               /\ toClient' = Append(toClient, [k |-> "answer", id |-> m.id, status |-> "error"])
               /\ UNCHANGED <<received, known, replies, handled, running>>
          ELSE /\ toClient' = Append(toClient,
                                     [k |-> "answer", id |-> m.id, status |-> Historic(m.num)])
               /\ UNCHANGED <<ledgerVars, running>>
    /\ UNCHANGED <<up, clientVars, oldVars, crashes, faults, replacing, reset>>

ServerRecords ==
    /\ running # <<>>
    /\ Records(running[1])
    /\ known' = running[1].num
    /\ running' = <<>>
    /\ toClient' = Append(toClient, [k |-> "answer", id |-> running[1].id,
                                     status |-> running[1].status])
    /\ UNCHANGED <<up, clientVars, handled, forgotten, toServer, oldVars, crashes, faults,
                   replacing, reset>>

ServerCannotTake ==
    /\ faults < MaxFaults
    /\ ~gone
    /\ ~replacing
    /\ running = <<>>
    /\ toServer # <<>>
    /\ Head(toServer).k = "push"
    /\ Head(toServer).num = known + 1
    /\ faults' = faults + 1
    /\ toServer' = Tail(toServer)
    /\ toClient' = Append(toClient, [k |-> "answer", id |-> Head(toServer).id, status |-> "notTaken"])
    /\ UNCHANGED <<up, clientVars, ledgerVars, running, oldVars, crashes, replacing, reset>>

OldTakes ==
    /\ oldRunning = <<>>
    /\ oldBox # <<>>
    /\ oldBox' = Tail(oldBox)
    /\ LET m == Head(oldBox)
       IN IF m.k = "push" /\ (m.num = oldKnown + 1 \/ (Broken = "noDedupe" /\ m.num <= oldKnown))
          THEN \E status \in {"ok", "error"} :
                  /\ Begins(m, status)
                  /\ oldRunning' = <<[id |-> m.id, num |-> m.num, status |-> status]>>
                  /\ UNCHANGED <<replies, forgotten>>
          ELSE UNCHANGED <<ledgerVars, oldRunning>>
    /\ UNCHANGED <<up, clientVars, toServer, toClient, running, gone, oldKnown, crashes, faults,
                   replacing, reset>>

OldRecords ==
    /\ oldRunning # <<>>
    /\ Records(oldRunning[1])
    /\ known' = known
    /\ oldKnown' = oldRunning[1].num
    /\ oldRunning' = <<>>
    /\ UNCHANGED <<up, clientVars, handled, forgotten, toServer, toClient, running, oldBox,
                   gone, crashes, faults, replacing, reset>>

Replaced ==
    /\ faults < MaxFaults
    /\ up
    /\ ~replacing
    /\ ~gone
    /\ running = <<>>
    /\ toServer = <<>>
    /\ faults' = faults + 1
    /\ gone' = TRUE
    /\ toClient' = Append(toClient, [k |-> "error"])
    /\ UNCHANGED <<up, clientVars, ledgerVars, toServer, running, oldBox, oldRunning, oldKnown,
                   crashes, replacing, reset>>

Unmatched ==
    /\ gone
    /\ toServer # <<>>
    /\ toServer' = Tail(toServer)
    /\ toClient' = IF Head(toServer).k = "push"
                   THEN Append(toClient, [k |-> "answer", id |-> Head(toServer).id,
                                          status |-> "unmatched"])
                   ELSE toClient
    /\ UNCHANGED <<up, clientVars, ledgerVars, running, oldVars, crashes, faults, replacing,
                   reset>>

ErrorArrives ==
    /\ toClient # <<>>
    /\ Head(toClient).k = "error"
    /\ toClient' = Tail(toClient)
    /\ up' = FALSE
    /\ phase' = "away"
    /\ UNCHANGED <<queue, next, syncId, answers, pushes, ledgerVars, toServer, running,
                   oldVars, crashes, faults, replacing, reset>>

---------------------------------------------------------------------------

Progress ==
    \/ Joins \/ ServerReplaces
    \/ SyncReplyArrives \/ AnswerArrives \/ ErrorArrives \/ Unmatched
    \/ ServerTakes \/ ServerRecords
    \/ OldTakes \/ OldRecords

Settled == up /\ ~replacing /\ toServer = <<>> /\ toClient = <<>> /\ running = <<>> /\ OldGone
Done == Settled /\ UNCHANGED vars

Next ==
    \/ Progress
    \/ Drops \/ ServerCrashes \/ ReplaceKills \/ SyncTimesOut \/ ServerCannotTake \/ Replaced
    \/ LedgerResets
    \/ AppPushes
    \/ Done

Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)

---------------------------------------------------------------------------

HandledInOrder ==
    reset # "joined" => \A i \in DOMAIN handled : i > 1 => handled[i - 1].id <= handled[i].id

HandledOnceUnlessCrashed ==
    reset # "joined" =>
        Len(handled) - Cardinality({handled[i].id : i \in DOMAIN handled}) <= crashes

AnsweredOnce == \A i, j \in DOMAIN answers : i < j => answers[i].id # answers[j].id

AnsweredAsHandled == reset = "no" => \A i \in DOMAIN answers : answers[i] \in Range(handled)

OnlyTakenAreAnswered == \A i \in DOMAIN answers : answers[i].status \notin {"notTaken", "unmatched"}

CountIsRecorded == received = Len(replies)

RepliesKept == reset = "no" => ~forgotten

CatchesUp == <>[](queue = <<>> /\ phase = "synced")

==============================================================================
