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
    faults

vars == <<up, phase, queue, next, syncId, answers, pushes, received, known, replies, handled,
          toServer, toClient, running, oldBox, oldRunning, gone, crashes, forgotten, faults>>

clientVars == <<phase, queue, next, syncId, answers, pushes>>
ledgerVars == <<received, known, replies, handled, forgotten>>
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
    /\ OldGone \/ Broken = "noReplace"
    /\ up' = TRUE
    /\ phase' = "syncing"
    /\ syncId' = syncId + 1
    /\ toServer' = <<[k |-> "sync", sid |-> syncId + 1]>>
    /\ gone' = FALSE
    /\ known' = IF Broken = "staleCount" THEN known ELSE received
    /\ UNCHANGED <<queue, next, answers, pushes, received, replies, handled, forgotten,
                   toClient, running, oldBox,
                   oldRunning, crashes, faults>>

ReplaceKills ==
    /\ ~up
    /\ ~OldGone
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ crashes' = crashes + IF oldRunning # <<>> THEN 1 ELSE 0
    /\ oldBox' = <<>>
    /\ oldRunning' = <<>>
    /\ UNCHANGED <<up, clientVars, ledgerVars, toServer, toClient, running, gone>>

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

SyncTimesOut ==
    /\ faults < MaxFaults
    /\ up
    /\ phase = "syncing"
    /\ faults' = faults + 1
    /\ syncId' = syncId + 1
    /\ toServer' = Append(toServer, [k |-> "sync", sid |-> syncId + 1])
    /\ UNCHANGED <<up, phase, queue, next, answers, pushes, ledgerVars, toClient, running,
                   oldVars, crashes>>

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

Historic(num) == IF Broken = "historicIsOk" THEN "ok" ELSE replies[num]

Begins(m, status) ==
    /\ handled' = Append(handled, [id |-> m.id, status |-> status])
    /\ received' = IF Broken = "countBeforeRecord" THEN m.num ELSE received
    /\ known' = known

Records(r) ==
    /\ received' = Max(received, r.num)
    /\ replies' = IF r.num > Len(replies) THEN Append(replies, r.status)
                  ELSE [replies EXCEPT ![r.num] = r.status]

ServerTakes ==
    /\ ~gone
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
          ELSE IF m.num <= received - Kept \/ m.num > Len(replies)
          THEN /\ forgotten' = TRUE
               /\ toClient' = Append(toClient, [k |-> "answer", id |-> m.id, status |-> "ok"])
               /\ UNCHANGED <<received, known, replies, handled, running>>
          ELSE /\ toClient' = Append(toClient,
                                     [k |-> "answer", id |-> m.id, status |-> Historic(m.num)])
               /\ UNCHANGED <<ledgerVars, running>>
    /\ UNCHANGED <<up, clientVars, oldVars, crashes, faults>>

ServerRecords ==
    /\ running # <<>>
    /\ Records(running[1])
    /\ known' = running[1].num
    /\ running' = <<>>
    /\ toClient' = Append(toClient, [k |-> "answer", id |-> running[1].id,
                                     status |-> running[1].status])
    /\ UNCHANGED <<up, clientVars, handled, forgotten, toServer, oldVars, crashes, faults>>

ServerCannotTake ==
    /\ faults < MaxFaults
    /\ ~gone
    /\ running = <<>>
    /\ toServer # <<>>
    /\ Head(toServer).k = "push"
    /\ Head(toServer).num = known + 1
    /\ faults' = faults + 1
    /\ toServer' = Tail(toServer)
    /\ toClient' = Append(toClient, [k |-> "answer", id |-> Head(toServer).id, status |-> "notTaken"])
    /\ UNCHANGED <<up, clientVars, ledgerVars, running, oldVars, crashes>>

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
    /\ known' = known
    /\ oldRunning' = <<>>
    /\ UNCHANGED <<up, clientVars, handled, forgotten, toServer, toClient, running, oldBox,
                   gone, crashes, faults>>

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

Unmatched ==
    /\ gone
    /\ toServer # <<>>
    /\ toServer' = Tail(toServer)
    /\ toClient' = IF Head(toServer).k = "push"
                   THEN Append(toClient, [k |-> "answer", id |-> Head(toServer).id,
                                          status |-> "unmatched"])
                   ELSE toClient
    /\ UNCHANGED <<up, clientVars, ledgerVars, running, oldVars, crashes, faults>>

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

HandledInOrder == \A i \in DOMAIN handled : i > 1 => handled[i - 1].id <= handled[i].id

HandledOnceUnlessCrashed ==
    Len(handled) - Cardinality({handled[i].id : i \in DOMAIN handled}) <= crashes

AnsweredOnce == \A i, j \in DOMAIN answers : i < j => answers[i].id # answers[j].id

AnsweredAsHandled == \A i \in DOMAIN answers : answers[i] \in Range(handled)

OnlyTakenAreAnswered == \A i \in DOMAIN answers : answers[i].status \notin {"notTaken", "unmatched"}

CountIsRecorded == received = Len(replies)

RepliesKept == ~forgotten

CatchesUp == <>[](queue = <<>> /\ phase = "synced")

==============================================================================
