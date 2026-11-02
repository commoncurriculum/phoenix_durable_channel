---------------------------- MODULE DurableMessages ----------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS MaxPush, MaxFaults, Broken

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
    log,
    read,
    handled,
    toServer,
    toClient,
    running,
    oldBox,
    oldRunning,
    gone,
    crashes,
    faults,
    replacing,
    oldKnown,
    reset,
    answeredBy,
    forgot

vars == <<up, phase, queue, next, syncId, answers, pushes, received, known, log, read, handled,
          toServer, toClient, running, oldBox, oldRunning, gone, crashes, faults,
          replacing, oldKnown, reset, answeredBy, forgot>>

clientVars == <<phase, queue, next, syncId, answers, pushes, read, answeredBy>>
ledgerVars == <<received, known, log, handled, forgot>>
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
    /\ log = <<>>
    /\ read = 0
    /\ handled = <<>>
    /\ toServer = <<>>
    /\ toClient = <<>>
    /\ running = <<>>
    /\ oldBox = <<>>
    /\ oldRunning = <<>>
    /\ gone = FALSE
    /\ crashes = 0
    /\ faults = 0
    /\ replacing = FALSE
    /\ oldKnown = 0
    /\ reset = "no"
    /\ answeredBy = <<>>
    /\ forgot = FALSE

Max(a, b) == IF a > b THEN a ELSE b
Range(s) == {s[i] : i \in DOMAIN s}
PushFrame(e) == [k |-> "push", id |-> e.id, num |-> e.num]
OldGone == oldBox = <<>> /\ oldRunning = <<>>

Sent(q, from) ==
    LET waiting == {i \in DOMAIN q : q[i].num = 0}
        rank(i) == Cardinality({j \in waiting : j <= i})
    IN [i \in DOMAIN q |-> IF i \in waiting THEN [id |-> q[i].id, num |-> from + rank(i) - 1]
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
    /\ UNCHANGED <<queue, next, answers, pushes, read, answeredBy, ledgerVars, toClient, running, oldBox,
                   oldRunning, oldKnown, crashes, faults>>

ServerReplaces ==
    /\ replacing
    /\ OldGone \/ Broken = "noReplace"
    /\ replacing' = FALSE
    /\ known' = IF Broken = "staleCount" THEN known ELSE received
    /\ UNCHANGED <<up, clientVars, received, log, handled, forgot, toServer, toClient,
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
    /\ UNCHANGED <<queue, next, syncId, answers, pushes, read, answeredBy, ledgerVars, crashes, reset>>

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
    /\ UNCHANGED <<queue, next, syncId, answers, pushes, read, answeredBy, ledgerVars, oldVars, replacing,
                   reset>>

LedgerResets ==
    /\ faults < MaxFaults
    /\ up
    /\ ~replacing
    /\ faults' = faults + 1
    /\ reset' = "live"
    /\ received' = 0
    /\ log' = <<>>
    /\ read' = 0
    /\ answeredBy' = <<>>
    /\ known' = IF Broken = "resetRereads" THEN 0 ELSE known
    /\ UNCHANGED <<up, phase, queue, next, syncId, answers, pushes, handled, forgot, toServer,
                   toClient, running, oldVars, crashes, replacing>>

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
    /\ UNCHANGED <<up, phase, syncId, answers, read, answeredBy, ledgerVars, toClient, running, oldVars,
                   crashes, faults, replacing, reset>>

SyncTimesOut ==
    /\ faults < MaxFaults
    /\ up
    /\ phase = "syncing"
    /\ faults' = faults + 1
    /\ syncId' = syncId + 1
    /\ toServer' = Append(toServer, [k |-> "sync", sid |-> syncId + 1])
    /\ UNCHANGED <<up, phase, queue, next, answers, pushes, read, answeredBy, ledgerVars, toClient, running,
                   oldVars, crashes, replacing, reset>>

SyncReplyArrives ==
    /\ toClient # <<>>
    /\ Head(toClient).k = "syncReply"
    /\ toClient' = Tail(toClient)
    /\ LET m == Head(toClient)
           counts == phase = "syncing" /\ m.sid = syncId
           base == m.received + 1
           awaits(e) == e.num # 0 /\ e.num <= m.received /\ Broken # "renumberHandled"
           waiting == [i \in DOMAIN queue |-> IF awaits(queue[i]) THEN queue[i]
                                              ELSE [id |-> queue[i].id, num |-> 0]]
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
               /\ answeredBy' = <<[received |-> m.received,
                                    last |-> IF Broken = "staleLast" /\ answeredBy # <<>>
                                             THEN answeredBy[1].last ELSE m.last]>>
          ELSE UNCHANGED <<next, queue, phase, toServer, answeredBy>>
    /\ UNCHANGED <<up, syncId, answers, pushes, read, ledgerVars, running, oldVars, crashes,
                   faults, replacing, reset>>

AnswerArrives ==
    /\ toClient # <<>>
    /\ Head(toClient).k = "answer"
    /\ toClient' = Tail(toClient)
    /\ LET m == Head(toClient)
       IN IF Broken \in {"retryIsAnswer", "unmatchedIsAnswer"}
             /\ m.status = IF Broken = "retryIsAnswer" THEN "notTaken" ELSE "unmatched"
          THEN /\ answers' = Append(answers, [id |-> m.id, status |-> m.status])
               /\ queue' = SelectSeq(queue, LAMBDA e : e.id # m.id)
               /\ UNCHANGED <<phase, syncId, toServer>>
          ELSE /\ IF phase = "synced"
                  THEN /\ phase' = "syncing"
                       /\ syncId' = syncId + 1
                       /\ toServer' = Append(toServer, [k |-> "sync", sid |-> syncId + 1])
                  ELSE UNCHANGED <<phase, syncId, toServer>>
               /\ UNCHANGED <<answers, queue>>
    /\ UNCHANGED <<up, next, pushes, read, answeredBy, ledgerVars, running, oldVars, crashes, faults,
                   replacing, reset>>

AnswerRead ==
    /\ read < Len(log)
    /\ read' = read + 1
    /\ LET a == log[read + 1]
           matches == {i \in DOMAIN queue : queue[i].num = a.num /\ a.status # "forgotten"}
       IN /\ IF matches = {}
             THEN UNCHANGED <<answers, queue>>
             ELSE LET i == CHOOSE x \in matches : TRUE
                  IN /\ answers' = Append(answers, [id |-> queue[i].id, status |-> a.status])
                     /\ queue' = IF Broken = "keepAnswered" THEN queue
                                  ELSE [j \in 1..(Len(queue) - 1) |->
                                           IF j < i THEN queue[j] ELSE queue[j + 1]]
          /\ IF a.status = "forgotten" /\ phase = "synced" /\ Broken # "noSyncOnSkip"
             THEN /\ phase' = "syncing"
                  /\ syncId' = syncId + 1
                  /\ toServer' = Append(toServer, [k |-> "sync", sid |-> syncId + 1])
             ELSE UNCHANGED <<phase, syncId, toServer>>
    /\ UNCHANGED <<up, next, pushes, answeredBy, ledgerVars, toClient, running,
                   oldVars, crashes, faults, replacing, reset>>

LosesUnread ==
    /\ answeredBy # <<>>
    /\ read >= answeredBy[1].last \/ Broken = "loseAtSync"
    /\ Broken # "noLoseUnread"
    /\ LET handledBefore(e) == e.num # 0 /\ (e.num <= answeredBy[1].received \/ Broken = "loseUnhandled")
           lost == SelectSeq(queue, handledBefore)
       IN /\ answers' = answers \o [i \in DOMAIN lost |-> [id |-> lost[i].id, status |-> "lost"]]
          /\ queue' = SelectSeq(queue, LAMBDA e : ~handledBefore(e))
    /\ answeredBy' = <<>>
    /\ UNCHANGED <<up, phase, next, syncId, pushes, read, ledgerVars, toServer, toClient, running,
                   oldVars, crashes, faults, replacing, reset>>

Forgets ==
    /\ faults < MaxFaults
    /\ \E i \in (read + 1)..Len(log) :
          log' = [log EXCEPT ![i].status = "forgotten"]
    /\ faults' = faults + 1
    /\ forgot' = TRUE
    /\ UNCHANGED <<up, clientVars, received, known, handled, toServer, toClient, running, oldVars,
                   crashes, replacing, reset>>

---------------------------------------------------------------------------

Begins(m, status) ==
    /\ handled' = Append(handled, [id |-> m.id, status |-> status])
    /\ received' = IF Broken = "countBeforeRecord" THEN m.num ELSE received
    /\ known' = known
    /\ forgot' = forgot

Records(r) ==
    /\ log' = Append(log, [num |-> r.num, status |-> r.status])
    /\ received' = r.num
    /\ forgot' = forgot

ServerTakes ==
    /\ ~gone
    /\ ~replacing \/ Broken = "syncBeforeReplace"
    /\ running = <<>>
    /\ toServer # <<>>
    /\ toServer' = Tail(toServer)
    /\ LET m == Head(toServer)
       IN IF m.k = "sync"
          THEN /\ toClient' = Append(toClient,
                                     [k |-> "syncReply", received |-> known, last |-> Len(log), sid |-> m.sid])
               /\ UNCHANGED <<ledgerVars, running>>
          ELSE IF m.num = known + 1
          THEN \E status \in {"ok", "error"} :
                  /\ Begins(m, status)
                  /\ running' = <<[id |-> m.id, num |-> m.num, status |-> status]>>
                  /\ UNCHANGED <<log, toClient>>
          ELSE IF m.num > known
          THEN /\ toClient' = Append(toClient, [k |-> "answer", id |-> m.id, status |-> "notTaken"])
               /\ UNCHANGED <<ledgerVars, running>>
          ELSE UNCHANGED <<ledgerVars, running, toClient>>
    /\ UNCHANGED <<up, clientVars, oldVars, crashes, faults, replacing, reset>>

ServerRecords ==
    /\ running # <<>>
    /\ Records(running[1])
    /\ known' = running[1].num
    /\ running' = <<>>
    /\ UNCHANGED <<up, clientVars, handled, toServer, toClient, oldVars, crashes, faults,
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
       IN IF m.k = "push" /\ m.num = oldKnown + 1
          THEN \E status \in {"ok", "error"} :
                  /\ Begins(m, status)
                  /\ oldRunning' = <<[id |-> m.id, num |-> m.num, status |-> status]>>
                  /\ UNCHANGED log
          ELSE UNCHANGED <<ledgerVars, oldRunning>>
    /\ UNCHANGED <<up, clientVars, toServer, toClient, running, gone, oldKnown, crashes, faults,
                   replacing, reset>>

OldRecords ==
    /\ oldRunning # <<>>
    /\ Records(oldRunning[1])
    /\ known' = known
    /\ oldKnown' = oldRunning[1].num
    /\ oldRunning' = <<>>
    /\ UNCHANGED <<up, clientVars, handled, toServer, toClient, running, oldBox,
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
    /\ UNCHANGED <<queue, next, syncId, answers, pushes, read, answeredBy, ledgerVars, toServer, running,
                   oldVars, crashes, faults, replacing, reset>>

---------------------------------------------------------------------------

Progress ==
    \/ Joins \/ ServerReplaces
    \/ SyncReplyArrives \/ AnswerArrives \/ AnswerRead \/ LosesUnread \/ ErrorArrives \/ Unmatched
    \/ ServerTakes \/ ServerRecords
    \/ OldTakes \/ OldRecords

Settled == up /\ ~replacing /\ toServer = <<>> /\ toClient = <<>> /\ running = <<>> /\ OldGone
Done == Settled /\ UNCHANGED vars

Next ==
    \/ Progress
    \/ Drops \/ ServerCrashes \/ ReplaceKills \/ SyncTimesOut \/ ServerCannotTake \/ Replaced
    \/ LedgerResets \/ Forgets
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

AnsweredAsHandled ==
    reset = "no" => \A i \in DOMAIN answers : answers[i].status = "lost" \/ answers[i] \in Range(handled)

LostOnlyWhenForgotten ==
    reset = "no" /\ ~forgot => \A i \in DOMAIN answers : answers[i].status # "lost"

LostOnlyWhenHandled ==
    \A i \in DOMAIN answers :
        answers[i].status = "lost" => \E j \in DOMAIN handled : handled[j].id = answers[i].id

OnlyTakenAreAnswered == \A i \in DOMAIN answers : answers[i].status \notin {"notTaken", "unmatched"}

CountIsRecorded == reset = "no" => \A n \in 1..received : \E i \in DOMAIN log : log[i].num = n

CatchesUp == <>[](reset = "live" \/ (queue = <<>> /\ phase = "synced"))

==============================================================================
