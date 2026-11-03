---------------------------- MODULE DurableMessages ----------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS MaxPush, MaxPublish, MaxOthers, Afters, MaxFaults, MaxStops, Reads, Broken

MaxJoins == 1 + 2 * MaxFaults + MaxStops

MaxConnections == MaxFaults + 1
Joins == 1..MaxJoins
Connections == 1..MaxConnections

VARIABLES
    connection,
    open,
    gens,
    joinGen,
    channel,
    lg,
    sync,
    reader,
    answers,
    pushes,
    toServer,
    toClient,
    serverOpen,
    chan,
    proc,
    registered,
    lock,
    inflight,
    published,
    stream,
    epoch,
    trimmed,
    saved,
    handled,
    faults,
    crashes,
    forgot,
    reset,
    stops,
    logged

clientVars == <<connection, open, gens, joinGen, channel, lg, sync, reader, answers, pushes>>
serverVars == <<chan, proc, registered, lock, inflight, published>>
storeVars == <<stream, epoch, trimmed, saved>>
historyVars == <<handled, faults, crashes, forgot, reset, stops>>
vars == <<clientVars, toServer, toClient, serverOpen, serverVars, storeVars, historyVars, logged>>
View == <<clientVars, toServer, toClient, serverOpen, serverVars, storeVars, historyVars>>

NoProcess == [state |-> "none", conn |-> 0, received |-> 0, mailbox |-> <<>>, running |-> <<>>,
              counted |-> FALSE]
NoReader == [after |-> -1, epoch |-> 0, left |-> [e \in {} |-> 0]]
NoStore == [stream |-> <<>>, epoch |-> -1, trimmed |-> 0]
NoLog == [state |-> "away", queue |-> <<>>, next |-> 0, syncs |-> 0, answeredBy |-> <<>>]
Nothing == [client |-> <<>>, server |-> <<>>]

Init ==
    /\ connection = 1
    /\ open = TRUE
    /\ gens = 0
    /\ joinGen = 0
    /\ channel = "none"
    /\ lg = NoLog
    /\ sync = [id |-> 0, live |-> FALSE]
    /\ reader = NoReader
    /\ answers = <<>>
    /\ pushes = 0
    /\ toServer = [c \in Connections |-> <<>>]
    /\ toClient = [c \in Connections |-> <<>>]
    /\ serverOpen = [c \in Connections |-> c = 1]
    /\ chan = [c \in Connections |-> 0]
    /\ proc = [g \in Joins |-> NoProcess]
    /\ registered = 0
    /\ lock = 0
    /\ inflight = <<>>
    /\ published = 0
    /\ stream = <<>>
    /\ epoch = 0
    /\ trimmed = 0
    /\ saved = NoStore
    /\ handled = <<>>
    /\ faults = 0
    /\ crashes = 0
    /\ forgot = "no"
    /\ reset = "no"
    /\ stops = 0
    /\ logged = Nothing

Min(s) == CHOOSE x \in s : \A y \in s : x <= y
Max(s) == CHOOSE x \in s : \A y \in s : x >= y
Range(s) == {s[i] : i \in DOMAIN s}

Counted(s, t) == {i \in DOMAIN s : i > t /\ s[i].kind \in {"answer", "message", "count"}}
Count(s, t) == IF Counted(s, t) = {} THEN 0 ELSE s[Max(Counted(s, t))].number

Silent == logged' = Nothing
ClientLogs(events) == logged' = [client |-> events, server |-> <<>>]
ServerLogs(events) == logged' = [client |-> <<>>, server |-> events]

---------------------------------------------------------------------------

Out(l) == [lg |-> l, sends |-> <<>>, answered |-> <<>>, sync |-> FALSE]

Then(r, s) ==
    [lg |-> s.lg, sends |-> r.sends \o s.sends, answered |-> r.answered \o s.answered,
     sync |-> r.sync \/ s.sync]

FirstUnsent(q) ==
    IF \E i \in DOMAIN q : q[i].num = 0 THEN Min({i \in DOMAIN q : q[i].num = 0}) ELSE Len(q) + 1

NumberWaiting(l) ==
    LET q == l.queue
        f == FirstUnsent(q)
        numbered == [i \in DOMAIN q |-> IF i >= f
                                         THEN [id |-> q[i].id, num |-> l.next + i - f, syncs |-> l.syncs]
                                         ELSE q[i]]
    IN [lg |-> [l EXCEPT !.queue = numbered, !.next = l.next + Len(q) - f + 1],
        sends |-> SubSeq(numbered, f, Len(q)), answered |-> <<>>, sync |-> FALSE]

Answer(l, number, status) ==
    LET q == l.queue
        matching == SelectSeq(q, LAMBDA e : e.num # 0 /\ e.num = number)
    IN [lg |-> IF Broken = "keepAnswered" THEN l
               ELSE [l EXCEPT !.queue = SelectSeq(q, LAMBDA e : ~(e.num # 0 /\ e.num = number))],
        sends |-> <<>>,
        answered |-> [i \in DOMAIN matching |-> [id |-> matching[i].id, status |-> status]],
        sync |-> FALSE]

Lose(l, below) ==
    LET q == l.queue
        earlier(e) == e.num # 0 /\ (e.num < below \/ Broken = "loseUnhandled")
        lost == SelectSeq(q, earlier)
    IN [lg |-> [l EXCEPT !.queue = SelectSeq(q, LAMBDA e : ~earlier(e))],
        sends |-> <<>>,
        answered |-> [i \in DOMAIN lost |-> [id |-> lost[i].id, status |-> "lost"]],
        sync |-> FALSE]

ToSyncing(l) == [lg |-> [l EXCEPT !.state = "syncing"], sends |-> <<>>, answered |-> <<>>, sync |-> TRUE]

NotTaken(status) ==
    \/ status = "notTaken" /\ Broken # "retryIsAnswer"
    \/ status = "unmatched" /\ Broken # "unmatchedIsAnswer"

Kept(q, received) ==
    LET waiting == {i \in DOMAIN q : q[i].num = 0 \/ q[i].num > received}
    IN IF Broken = "renumberHandled" THEN 0
       ELSE IF waiting = {} THEN Len(q) ELSE Min(waiting) - 1

ApplySync(l, received, last) ==
    LET k == Kept(l.queue, received)
        q == [i \in DOMAIN l.queue |-> IF i <= k THEN l.queue[i]
                                         ELSE [id |-> l.queue[i].id, num |-> 0, syncs |-> 0]]
        r == NumberWaiting([l EXCEPT !.queue = q, !.next = received + 1, !.syncs = l.syncs + 1,
                                     !.state = "synced",
                                     !.answeredBy =
                                         <<[received |-> received,
                                            last |-> IF Broken = "staleLast" /\ l.answeredBy # <<>>
                                                     THEN l.answeredBy[1].last ELSE last]>>])
        wasSent == {l.queue[i].id : i \in {j \in DOMAIN l.queue : j > k /\ l.queue[j].num # 0}}
    IN IF Broken = "noResend"
       THEN [r EXCEPT !.sends = SelectSeq(r.sends, LAMBDA e : e.id \notin wasSent)]
       ELSE r

Log(l, e) ==
    CASE e.type = "PUSH" ->
             IF l.state = "closed"
             THEN [lg |-> l, sends |-> <<>>, answered |-> <<[id |-> e.id, status |-> "timeout"]>>,
                   sync |-> FALSE]
             ELSE LET queued == [l EXCEPT !.queue = Append(l.queue, [id |-> e.id, num |-> 0, syncs |-> 0])]
                  IN IF l.state = "synced" THEN NumberWaiting(queued) ELSE Out(queued)
      [] e.type = "ANSWERED" -> Answer(l, e.number, e.status)
      [] e.type = "READ" ->
             IF /\ l.answeredBy # <<>>
                /\ e.after >= l.answeredBy[1].last \/ Broken = "loseAtSync"
                /\ Broken # "noLoseUnread"
             THEN LET r == Lose(l, l.answeredBy[1].received + 1)
                  IN [r EXCEPT !.lg.answeredBy = <<>>]
             ELSE Out(l)
      [] e.type = "REPLY" ->
             LET match == {i \in DOMAIN l.queue : l.queue[i].num = e.num /\ l.queue[i].num # 0
                                                   /\ l.queue[i].syncs = e.syncs}
             IN IF l.state = "synced" /\ NotTaken(e.status) /\ match # {}
                THEN Out([l EXCEPT !.state = "backingOff"])
                ELSE IF ~NotTaken(e.status) /\ match # {}
                THEN Answer(l, e.num, IF e.status = "ok" THEN "ok" ELSE e.status)
                ELSE Out(l)
      [] e.type = "CHANNEL_JOINED" -> IF l.state = "away" THEN ToSyncing(l) ELSE Out(l)
      [] e.type = "CHANNEL_LEFT" -> Out([l EXCEPT !.state = "away"])
      [] e.type = "CHANNEL_CLOSED" ->
             [lg |-> [l EXCEPT !.state = "closed", !.queue = <<>>], sends |-> <<>>,
              answered |-> [i \in DOMAIN l.queue |-> [id |-> l.queue[i].id, status |-> "timeout"]],
              sync |-> FALSE]
      [] e.type = "SYNCED" ->
             IF l.state = "syncing" THEN ApplySync(l, e.received, e.last) ELSE Out(l)
      [] e.type = "SKIPPED" ->
             IF l.state = "synced" /\ Broken # "noSyncOnSkip" THEN ToSyncing(l) ELSE Out(l)
      [] e.type = "SYNC_FAILED" ->
             IF l.state = "syncing" THEN Out([l EXCEPT !.state = "backingOff"]) ELSE Out(l)
      [] e.type = "BACKOFF_DONE" ->
             IF l.state = "backingOff" THEN ToSyncing(l) ELSE Out(l)

RECURSIVE Feed(_, _)
Feed(r, events) ==
    IF events = <<>> THEN r ELSE Feed(Then(r, Log(r.lg, Head(events))), Tail(events))

PushFrame(e) == [k |-> "push", g |-> joinGen, id |-> e.id, num |-> e.num, syncs |-> e.syncs]

RunKilling(events, extraFrames, syncDies) ==
    LET r == Feed(Out(lg), events)
        frames == [i \in DOMAIN r.sends |-> PushFrame(r.sends[i])]
        syncFrame == IF r.sync THEN <<[k |-> "sync", g |-> joinGen, sid |-> sync.id + 1]>> ELSE <<>>
    IN /\ lg' = r.lg
       /\ answers' = answers \o r.answered
       /\ sync' = IF r.sync THEN [id |-> sync.id + 1, live |-> TRUE]
                  ELSE IF syncDies THEN [sync EXCEPT !.live = FALSE] ELSE sync
       /\ toServer' = [toServer EXCEPT ![connection] = @ \o extraFrames \o frames \o syncFrame]
       /\ ClientLogs(events)

Run(events, extraFrames) == RunKilling(events, extraFrames, FALSE)

SyncFails == IF sync.live THEN <<[type |-> "SYNC_FAILED"]>> ELSE <<>>

---------------------------------------------------------------------------

ClientConnects ==
    /\ ~open
    /\ connection < MaxConnections
    /\ connection' = connection + 1
    /\ open' = TRUE
    /\ serverOpen' = [serverOpen EXCEPT ![connection + 1] = TRUE]
    /\ Silent
    /\ UNCHANGED <<gens, joinGen, channel, lg, sync, reader, answers, pushes, toServer, toClient,
                   serverVars, storeVars, historyVars>>

ClientOpensChannel ==
    /\ channel \in {"none", "closed"}
    /\ gens < MaxJoins
    /\ channel' = "new"
    /\ joinGen' = 0
    /\ lg' = NoLog
    /\ sync' = [sync EXCEPT !.live = FALSE]
    /\ reader' = NoReader
    /\ ClientLogs(<<[type |-> "LOG_STARTED"]>>)
    /\ UNCHANGED <<connection, open, gens, answers, pushes, toServer, toClient, serverOpen, serverVars,
                   storeVars, historyVars>>

ClientJoins ==
    /\ open
    /\ channel \in {"new", "errored"}
    /\ gens < MaxJoins
    /\ gens' = gens + 1
    /\ joinGen' = gens + 1
    /\ channel' = "joining"
    /\ toServer' = [toServer EXCEPT ![connection] = Append(@, [k |-> "join", g |-> gens + 1])]
    /\ Silent
    /\ UNCHANGED <<connection, open, lg, sync, reader, answers, pushes, toClient, serverOpen,
                   serverVars, storeVars, historyVars>>

Published(frame, start) ==
    IF reader.after = -1 THEN [reader EXCEPT !.after = start, !.epoch = frame.epoch] ELSE reader

Starts(frame) == {frame.last} \cup Afters

ClientReceives ==
    /\ open
    /\ toClient[connection] # <<>>
    /\ LET m == Head(toClient[connection])
           rest == [toClient EXCEPT ![connection] = Tail(@)]
       IN /\ toClient' = rest
          /\ IF m.g # joinGen
             THEN /\ Silent
                  /\ UNCHANGED <<channel, lg, sync, reader, answers, toServer>>
             ELSE CASE m.k = "joinReply" ->
                        IF channel = "joining"
                        THEN /\ channel' = "joined"
                             /\ Run(<<[type |-> "CHANNEL_JOINED"]>>, <<>>)
                             /\ UNCHANGED reader
                        ELSE /\ Silent
                             /\ UNCHANGED <<channel, lg, sync, reader, answers, toServer>>
                    [] m.k = "joinRefused" ->
                        IF channel = "joining"
                        THEN /\ channel' = "errored"
                             /\ Run(<<[type |-> "CHANNEL_LEFT"]>>, <<>>)
                             /\ UNCHANGED reader
                        ELSE /\ Silent
                             /\ UNCHANGED <<channel, lg, sync, reader, answers, toServer>>
                    [] m.k = "reader" ->
                        /\ \E start \in Starts(m) : reader' = Published(m, start)
                        /\ Silent
                        /\ UNCHANGED <<channel, lg, sync, answers, toServer>>
                    [] m.k = "syncReply" ->
                        /\ IF sync.live /\ m.sid = sync.id
                           THEN \E start \in Starts(m) :
                                LET r == Published(m, start)
                                IN /\ reader' = r
                                   /\ RunKilling(<<[type |-> "SYNCED", received |-> m.received, last |-> m.last],
                                                   [type |-> "READ", after |-> r.after]>>, <<>>, TRUE)
                           ELSE /\ Silent
                                /\ UNCHANGED <<lg, sync, reader, answers, toServer>>
                        /\ UNCHANGED channel
                    [] m.k = "syncRefused" ->
                        /\ sync' = IF m.sid = sync.id THEN [sync EXCEPT !.live = FALSE] ELSE sync
                        /\ Silent
                        /\ UNCHANGED <<channel, lg, reader, answers, toServer>>
                    [] m.k = "reply" ->
                        /\ Run(<<[type |-> "REPLY", num |-> m.num, syncs |-> m.syncs, status |-> m.status]>>, <<>>)
                        /\ UNCHANGED <<channel, reader>>
                    [] m.k = "error" ->
                        /\ IF channel \in {"joining", "joined"}
                           THEN /\ channel' = "errored"
                                /\ RunKilling(<<[type |-> "CHANNEL_LEFT"]>> \o SyncFails, <<>>, TRUE)
                           ELSE /\ Silent
                                /\ UNCHANGED <<channel, lg, sync, answers, toServer>>
                        /\ UNCHANGED reader
                    [] m.k = "close" ->
                        /\ IF channel \in {"joining", "joined", "leaving"}
                           THEN /\ channel' = "closed"
                                /\ RunKilling(SyncFails \o <<[type |-> "CHANNEL_CLOSED"]>>, <<>>, TRUE)
                           ELSE /\ Silent
                                /\ UNCHANGED <<channel, lg, sync, answers, toServer>>
                        /\ UNCHANGED reader
    /\ UNCHANGED <<connection, open, gens, joinGen, pushes, serverOpen, serverVars, storeVars,
                   historyVars>>

AppPushes ==
    /\ channel # "none"
    /\ pushes < MaxPush
    /\ pushes' = pushes + 1
    /\ Run(<<[type |-> "PUSH", id |-> pushes + 1]>>, <<>>)
    /\ UNCHANGED <<connection, open, gens, joinGen, channel, reader, toClient, serverOpen,
                   serverVars, storeVars, historyVars>>

AppLeaves ==
    /\ channel \in {"new", "joining", "joined", "errored"}
    /\ stops < MaxStops
    /\ stops' = stops + 1
    /\ IF channel \in {"joining", "joined"}
       THEN /\ channel' = "leaving"
            /\ RunKilling(<<[type |-> "CHANNEL_LEFT"]>> \o SyncFails, <<[k |-> "leave", g |-> joinGen]>>, TRUE)
       ELSE /\ channel' = "closed"
            /\ RunKilling(<<[type |-> "CHANNEL_CLOSED"]>>, <<>>, TRUE)
    /\ UNCHANGED <<connection, open, gens, joinGen, reader, pushes, toClient, serverOpen,
                   serverVars, storeVars, handled, faults, crashes, forgot, reset>>

LeaveTimesOut ==
    /\ channel = "leaving"
    /\ channel' = "closed"
    /\ RunKilling(<<[type |-> "CHANNEL_CLOSED"]>>, <<>>, TRUE)
    /\ UNCHANGED <<connection, open, gens, joinGen, reader, pushes, toClient, serverOpen,
                   serverVars, storeVars, historyVars>>

BackoffEnds ==
    /\ lg.state = "backingOff"
    /\ Run(<<[type |-> "BACKOFF_DONE"]>>, <<>>)
    /\ UNCHANGED <<connection, open, gens, joinGen, channel, reader, pushes, toClient, serverOpen,
                   serverVars, storeVars, historyVars>>

Unread(after) == {i \in DOMAIN stream : i > after /\ i > trimmed}

ReaderAdvances ==
    /\ reader.after >= 0
    /\ reader.epoch = epoch
    /\ reader.after < Len(stream)
    /\ Unread(reader.after) # {} \/ Reads = "channel"
    /\ LET next == IF Unread(reader.after) = {} THEN Len(stream) + 1 ELSE Min(Unread(reader.after))
           after == IF next > Len(stream) THEN Len(stream) ELSE next
           skipped == IF next > reader.after + 1 THEN <<[type |-> "SKIPPED"]>> ELSE <<>>
           delivered == IF next <= Len(stream) /\ stream[next].kind = "answer"
                        THEN <<[type |-> "ANSWERED", number |-> stream[next].number,
                                status |-> stream[next].status]>>
                        ELSE <<>>
       IN /\ reader' = [reader EXCEPT !.after = after]
          /\ Run(skipped \o delivered \o <<[type |-> "READ", after |-> after]>>, <<>>)
    /\ UNCHANGED <<connection, open, gens, joinGen, channel, pushes, toClient, serverOpen,
                   serverVars, storeVars, historyVars>>

ReaderReports ==
    /\ reader.after >= 0
    /\ Run(<<[type |-> "READ", after |-> reader.after]>>, <<>>)
    /\ UNCHANGED <<connection, open, gens, joinGen, channel, reader, pushes, toClient, serverOpen,
                   serverVars, storeVars, historyVars>>

ReaderSeesEpoch ==
    /\ reader.after >= 0
    /\ reader.epoch # epoch
    /\ reader' = [after |-> IF epoch \in DOMAIN reader.left THEN reader.left[epoch] ELSE 0,
                   epoch |-> epoch,
                   left |-> [e \in DOMAIN reader.left \cup {reader.epoch} |->
                                IF e = reader.epoch THEN reader.after ELSE reader.left[e]]]
    /\ Silent
    /\ UNCHANGED <<connection, open, gens, joinGen, channel, lg, sync, answers, pushes, toServer,
                   toClient, serverOpen, serverVars, storeVars, historyVars>>

---------------------------------------------------------------------------

Live(g) == g # 0 /\ proc[g].state \notin {"none", "stopped"}

Killed(g) ==
    /\ inflight' = IF proc[g].running # <<>>
                   THEN Append(inflight, [record |-> [kind |-> "answer", number |-> proc[g].running[1].num,
                                                      status |-> proc[g].running[1].status],
                                          g |-> g])
                   ELSE inflight
    /\ crashes' = crashes + IF proc[g].running # <<>> THEN 1 ELSE 0

Notices(g, c, kinds) ==
    IF serverOpen[c] /\ chan[c] = g THEN [i \in DOMAIN kinds |-> [k |-> kinds[i], g |-> g]] ELSE <<>>

Stopped(p, g) == [p EXCEPT ![g].state = "stopped", ![g].mailbox = <<>>, ![g].running = <<>>]

Unchannelled(g, c) == IF chan[c] = g THEN [chan EXCEPT ![c] = 0] ELSE chan

ServerReceivesJoin(c) ==
    /\ serverOpen[c]
    /\ toServer[c] # <<>>
    /\ Head(toServer[c]).k = "join"
    /\ LET g == Head(toServer[c]).g
           d == chan[c]
           started == [NoProcess EXCEPT !.state = "replacing", !.conn = c,
                                        !.received = IF Broken = "staleCount" THEN Count(stream, trimmed) ELSE 0]
       IN /\ chan' = [chan EXCEPT ![c] = g]
          /\ toServer' = [toServer EXCEPT ![c] = Tail(@)]
          /\ ServerLogs(<<[type |-> "join", process |-> g]>>)
          /\ IF d = 0
             THEN /\ proc' = [proc EXCEPT ![g] = started]
                  /\ toClient' = [toClient EXCEPT ![c] = Append(@, [k |-> "joinReply", g |-> g])]
                  /\ UNCHANGED <<lock, inflight, crashes>>
             ELSE /\ Killed(d)
                  /\ proc' = [Stopped(proc, d) EXCEPT ![g] = started]
                  /\ toClient' = [toClient EXCEPT ![c] = @ \o <<[k |-> "close", g |-> d], [k |-> "joinReply", g |-> g]>>]
                  /\ lock' = IF lock = d THEN 0 ELSE lock
    /\ UNCHANGED <<clientVars, serverOpen, registered, published, storeVars, handled, faults, forgot, reset, stops>>

ServerRefusesJoin(c) ==
    /\ stops < MaxStops
    /\ serverOpen[c]
    /\ toServer[c] # <<>>
    /\ Head(toServer[c]).k = "join"
    /\ stops' = stops + 1
    /\ LET g == Head(toServer[c]).g
           d == chan[c]
       IN /\ chan' = [chan EXCEPT ![c] = 0]
          /\ toServer' = [toServer EXCEPT ![c] = Tail(@)]
          /\ IF d = 0
             THEN /\ toClient' = [toClient EXCEPT ![c] = Append(@, [k |-> "joinRefused", g |-> g])]
                  /\ UNCHANGED <<proc, lock, inflight, crashes>>
             ELSE /\ Killed(d)
                  /\ proc' = Stopped(proc, d)
                  /\ toClient' = [toClient EXCEPT ![c] = @ \o <<[k |-> "close", g |-> d], [k |-> "joinRefused", g |-> g]>>]
                  /\ lock' = IF lock = d THEN 0 ELSE lock
    /\ Silent
    /\ UNCHANGED <<clientVars, serverOpen, registered, published, storeVars, handled, forgot, reset, faults>>

ServerRoutes(c) ==
    /\ serverOpen[c]
    /\ toServer[c] # <<>>
    /\ Head(toServer[c]).k # "join"
    /\ LET m == Head(toServer[c])
       IN /\ toServer' = [toServer EXCEPT ![c] = Tail(@)]
          /\ IF chan[c] = m.g
             THEN /\ proc' = [proc EXCEPT ![m.g].mailbox = Append(@, m)]
                  /\ UNCHANGED toClient
             ELSE IF chan[c] = 0 /\ m.k = "push"
             THEN /\ toClient' = [toClient EXCEPT ![c] = Append(@, [k |-> "reply", g |-> m.g, num |-> m.num,
                                                                   syncs |-> m.syncs, status |-> "unmatched"])]
                  /\ UNCHANGED proc
             ELSE IF chan[c] = 0 /\ m.k = "leave"
             THEN /\ toClient' = [toClient EXCEPT ![c] = Append(@, [k |-> "close", g |-> m.g])]
                  /\ UNCHANGED proc
             ELSE IF chan[c] = 0 /\ m.k = "sync"
             THEN /\ toClient' = [toClient EXCEPT ![c] = Append(@, [k |-> "syncRefused", g |-> m.g, sid |-> m.sid])]
                  /\ UNCHANGED proc
             ELSE UNCHANGED <<proc, toClient>>
    /\ Silent
    /\ UNCHANGED <<clientVars, serverOpen, chan, registered, lock, inflight, published, storeVars, historyVars>>

ServerTellsReplaced(g) ==
    /\ proc[g].state = "replacing"
    /\ lock = 0
    /\ registered # g
    /\ Live(registered)
    /\ Broken # "noReplace"
    /\ lock' = g
    /\ proc' = [proc EXCEPT ![registered].mailbox = Append(@, [k |-> "replaced"])]
    /\ ServerLogs(<<[type |-> "replace", process |-> g]>>)
    /\ UNCHANGED <<clientVars, toServer, toClient, serverOpen, chan, registered, inflight, published,
                   storeVars, historyVars>>

ServerRegisters(g) ==
    /\ proc[g].state = "replacing"
    /\ lock \in {0, g}
    /\ registered = g \/ ~Live(registered) \/ Broken = "noReplace"
    /\ lock' = 0
    /\ registered' = g
    /\ proc' = [proc EXCEPT ![g].state = "reading"]
    /\ ServerLogs(<<[type |-> "register", process |-> g]>>)
    /\ UNCHANGED <<clientVars, toServer, toClient, serverOpen, chan, inflight, published, storeVars,
                   historyVars>>

ReplaceKills(g) ==
    /\ faults < MaxFaults
    /\ proc[g].state = "replacing"
    /\ lock = g
    /\ Live(registered)
    /\ faults' = faults + 1
    /\ Killed(registered)
    /\ LET r == registered
           c == proc[r].conn
       IN /\ proc' = Stopped(proc, r)
          /\ toClient' = [toClient EXCEPT ![c] = @ \o Notices(r, c, <<"error">>)]
          /\ chan' = Unchannelled(r, c)
    /\ ServerLogs(<<[type |-> "kill", process |-> g]>>)
    /\ UNCHANGED <<clientVars, toServer, serverOpen, registered, lock, published, storeVars, handled,
                   forgot, reset, stops>>

ServerReadsReader(g) ==
    /\ proc[g].state = "reading"
    /\ proc' = [proc EXCEPT ![g].state = "counting"]
    /\ LET c == proc[g].conn
       IN toClient' = IF serverOpen[c]
                      THEN [toClient EXCEPT ![c] = Append(@, [k |-> "reader", g |-> g, last |-> Len(stream),
                                                             epoch |-> epoch])]
                      ELSE toClient
    /\ ServerLogs(<<[type |-> "reader", process |-> g, last |-> Len(stream)]>>)
    /\ UNCHANGED <<clientVars, toServer, serverOpen, chan, registered, lock, inflight, published,
                   storeVars, historyVars>>

ServerReadsCount(g) ==
    /\ proc[g].state = "counting"
    /\ LET received == IF Broken = "staleCount" THEN proc[g].received ELSE Count(stream, trimmed)
       IN /\ proc' = [proc EXCEPT ![g].state = "live", ![g].received = received, ![g].counted = TRUE]
          /\ IF Broken = "noCountRecord"
             THEN /\ stream' = stream
                  /\ ServerLogs(<<[type |-> "received", process |-> g, received |-> received]>>)
             ELSE /\ stream' = Append(stream, [kind |-> "count", number |-> received, status |-> "none"])
                  /\ ServerLogs(<<[type |-> "received", process |-> g, received |-> received],
                                  [type |-> "count", process |-> g, number |-> Len(stream) + 1,
                                   received |-> received]>>)
    /\ UNCHANGED <<clientVars, toServer, toClient, serverOpen, chan, registered, lock, inflight,
                   published, epoch, trimmed, saved, historyVars>>

Takes(g) == proc[g].state = "live" \/ (Broken = "syncBeforeReplace" /\ proc[g].state = "replacing")

PushLog(g, m, outcome) ==
    <<[type |-> "push", process |-> g, number |-> m.num, received |-> proc[g].received, outcome |-> outcome]>>

ServerTakes(g) ==
    /\ Takes(g)
    /\ proc[g].running = <<>>
    /\ proc[g].mailbox # <<>>
    /\ LET m == Head(proc[g].mailbox)
           c == proc[g].conn
           p == [proc EXCEPT ![g].mailbox = Tail(@)]
           received == proc[g].received
           send(frame) == IF serverOpen[c] THEN [toClient EXCEPT ![c] = Append(@, frame)] ELSE toClient
       IN CASE m.k = "replaced" ->
                 /\ proc' = Stopped(p, g)
                 /\ toClient' = [toClient EXCEPT ![c] = @ \o (IF serverOpen[c] THEN <<[k |-> "error", g |-> g]>>
                                                              ELSE <<>>)
                                                         \o Notices(g, c, <<"close">>)]
                 /\ chan' = Unchannelled(g, c)
                 /\ ServerLogs(<<[type |-> "replaced", process |-> g]>>)
                 /\ UNCHANGED <<handled, faults, stops>>
            [] m.k = "closed" ->
                 /\ proc' = Stopped(p, g)
                 /\ Silent
                 /\ UNCHANGED <<toClient, chan, handled, faults, stops>>
            [] m.k = "leave" ->
                 /\ proc' = Stopped(p, g)
                 /\ toClient' = [toClient EXCEPT ![c] = @ \o Notices(g, c, <<"close">>)]
                 /\ chan' = Unchannelled(g, c)
                 /\ Silent
                 /\ UNCHANGED <<handled, faults, stops>>
            [] m.k = "sync" ->
                 /\ proc' = p
                 /\ toClient' = send([k |-> "syncReply", g |-> g, sid |-> m.sid, received |-> received,
                                      last |-> Len(stream), epoch |-> epoch])
                 /\ ServerLogs(<<[type |-> "sync", process |-> g, received |-> received, last |-> Len(stream)]>>)
                 /\ UNCHANGED <<chan, handled, faults, stops>>
            [] m.k = "push" /\ m.num <= received ->
                 /\ proc' = p
                 /\ ServerLogs(PushLog(g, m, "old"))
                 /\ UNCHANGED <<toClient, chan, handled, faults, stops>>
            [] m.k = "push" /\ m.num > received + 1 ->
                 /\ proc' = p
                 /\ toClient' = send([k |-> "reply", g |-> g, num |-> m.num, syncs |-> m.syncs,
                                      status |-> "notTaken"])
                 /\ ServerLogs(PushLog(g, m, "ahead"))
                 /\ UNCHANGED <<chan, handled, faults, stops>>
            [] m.k = "push" ->
                 \/ \E status \in {"ok", "error"}, stop \in {"no", "close", "error"} :
                       /\ stops' = stops + IF stop = "no" THEN 0 ELSE 1
                       /\ stops' <= MaxStops
                       /\ handled' = Append(handled, [id |-> m.id, status |-> status])
                       /\ proc' = [p EXCEPT ![g].running = <<[id |-> m.id, num |-> m.num, syncs |-> m.syncs,
                                                             status |-> status, stop |-> stop]>>]
                       /\ ServerLogs(PushLog(g, m, status))
                       /\ UNCHANGED <<toClient, chan, faults>>
                 \/ /\ faults < MaxFaults
                    /\ faults' = faults + 1
                    /\ proc' = p
                    /\ toClient' = send([k |-> "reply", g |-> g, num |-> m.num, syncs |-> m.syncs,
                                         status |-> "notTaken"])
                    /\ ServerLogs(PushLog(g, m, "retry"))
                    /\ UNCHANGED <<chan, handled, stops>>
    /\ UNCHANGED <<clientVars, toServer, serverOpen, registered, lock, inflight, published, storeVars,
                   crashes, forgot, reset>>

ServerAppends(g) ==
    /\ proc[g].running # <<>>
    /\ LET r == proc[g].running[1]
           c == proc[g].conn
           reply == IF r.stop # "no" /\ serverOpen[c]
                    THEN <<[k |-> "reply", g |-> g, num |-> r.num, syncs |-> r.syncs, status |-> r.status]>>
                    ELSE <<>>
       IN /\ stream' = Append(stream, [kind |-> "answer", number |-> r.num, status |-> r.status])
          /\ ServerLogs(<<[type |-> "append", process |-> g, number |-> Len(stream) + 1, received |-> r.num]>>)
          /\ IF r.stop = "no"
             THEN /\ proc' = [proc EXCEPT ![g].running = <<>>, ![g].received = r.num]
                  /\ UNCHANGED <<toClient, chan>>
             ELSE /\ proc' = [Stopped(proc, g) EXCEPT ![g].received = r.num]
                  /\ toClient' = [toClient EXCEPT ![c] = @ \o reply \o Notices(g, c, <<r.stop>>)]
                  /\ chan' = Unchannelled(g, c)
    /\ UNCHANGED <<clientVars, toServer, serverOpen, registered, lock, inflight, published, epoch,
                   trimmed, saved, historyVars>>

AppPublishes(g) ==
    /\ proc[g].state = "live"
    /\ published < MaxPublish
    /\ published' = published + 1
    /\ stream' = Append(stream, [kind |-> "message", number |-> proc[g].received, status |-> "none"])
    /\ ServerLogs(<<[type |-> "append", process |-> g, number |-> Len(stream) + 1, received |-> proc[g].received]>>)
    /\ UNCHANGED <<clientVars, toServer, toClient, serverOpen, chan, proc, registered, lock, inflight,
                   epoch, trimmed, saved, historyVars>>

ServerNoticesClose(c) ==
    /\ serverOpen[c]
    /\ c # connection \/ ~open
    /\ serverOpen' = [serverOpen EXCEPT ![c] = FALSE]
    /\ toServer' = [toServer EXCEPT ![c] = <<>>]
    /\ proc' = IF chan[c] # 0 THEN [proc EXCEPT ![chan[c]].mailbox = Append(@, [k |-> "closed"])] ELSE proc
    /\ chan' = [chan EXCEPT ![c] = 0]
    /\ Silent
    /\ UNCHANGED <<clientVars, toClient, registered, lock, inflight, published, storeVars, historyVars>>

LateAppendRefused ==
    /\ inflight # <<>>
    /\ Broken # "noCountRecord"
    /\ \E g \in Joins : g > Head(inflight).g /\ proc[g].counted
    /\ inflight' = Tail(inflight)
    /\ Silent
    /\ UNCHANGED <<clientVars, toServer, toClient, serverOpen, chan, proc, registered, lock, published,
                   storeVars, historyVars>>

LateAppendLands ==
    /\ inflight # <<>>
    /\ Broken = "noCountRecord" \/ ~\E g \in Joins : g > Head(inflight).g /\ proc[g].counted
    /\ stream' = Append(stream, Head(inflight).record)
    /\ inflight' = Tail(inflight)
    /\ Silent
    /\ UNCHANGED <<clientVars, toServer, toClient, serverOpen, chan, proc, registered, lock, published,
                   epoch, trimmed, saved, historyVars>>

---------------------------------------------------------------------------

ClientDisconnects ==
    /\ faults < MaxFaults
    /\ open
    /\ faults' = faults + 1
    /\ open' = FALSE
    /\ toClient' = [toClient EXCEPT ![connection] = <<>>]
    /\ IF channel \in {"joining", "joined"}
       THEN /\ channel' = "errored"
            /\ LET events == <<[type |-> "CHANNEL_LEFT"]>> \o SyncFails
                   r == Feed(Out(lg), events)
               IN /\ lg' = r.lg
                  /\ answers' = answers \o r.answered
                  /\ ClientLogs(events)
            /\ sync' = [sync EXCEPT !.live = FALSE]
       ELSE IF channel = "leaving"
       THEN /\ channel' = "closed"
            /\ LET events == <<[type |-> "CHANNEL_CLOSED"]>>
                   r == Feed(Out(lg), events)
               IN /\ lg' = r.lg
                  /\ answers' = answers \o r.answered
                  /\ ClientLogs(events)
            /\ sync' = [sync EXCEPT !.live = FALSE]
       ELSE /\ Silent
            /\ UNCHANGED <<channel, lg, answers, sync>>
    /\ UNCHANGED <<connection, gens, joinGen, reader, pushes, toServer, serverOpen, serverVars,
                   storeVars, crashes, handled, forgot, reset, stops>>

JoinTimesOut ==
    /\ faults < MaxFaults
    /\ channel = "joining"
    /\ faults' = faults + 1
    /\ channel' = "errored"
    /\ Run(<<[type |-> "CHANNEL_LEFT"]>>, <<[k |-> "leave", g |-> joinGen]>>)
    /\ UNCHANGED <<connection, open, gens, joinGen, reader, pushes, toClient, serverOpen, serverVars,
                   storeVars, crashes, handled, forgot, reset, stops>>

SyncTimesOut ==
    /\ faults < MaxFaults
    /\ sync.live
    /\ faults' = faults + 1
    /\ LET r == Feed(Out(lg), <<[type |-> "SYNC_FAILED"]>>)
       IN /\ lg' = r.lg
          /\ sync' = [sync EXCEPT !.live = FALSE]
    /\ ClientLogs(<<[type |-> "SYNC_FAILED"]>>)
    /\ UNCHANGED <<connection, open, gens, joinGen, channel, reader, answers, pushes, toServer,
                   toClient, serverOpen, serverVars, storeVars, crashes, handled, forgot, reset, stops>>

ProcessCrashes(g) ==
    /\ faults < MaxFaults
    /\ Live(g)
    /\ faults' = faults + 1
    /\ Killed(g)
    /\ LET c == proc[g].conn
       IN /\ proc' = Stopped(proc, g)
          /\ toClient' = [toClient EXCEPT ![c] = @ \o Notices(g, c, <<"error">>)]
          /\ chan' = Unchannelled(g, c)
          /\ lock' = IF lock = g THEN 0 ELSE lock
    /\ Silent
    /\ UNCHANGED <<clientVars, toServer, serverOpen, registered, published, storeVars, handled, forgot,
                   reset, stops>>

StoreForgets ==
    /\ faults < MaxFaults
    /\ trimmed < Len(stream)
    /\ faults' = faults + 1
    /\ \E t \in (trimmed + 1)..Len(stream) :
           /\ trimmed' = t
           /\ forgot' = IF forgot = "count" \/ Count(stream, t) # Count(stream, trimmed)
                        THEN "count" ELSE "answers"
    /\ Silent
    /\ UNCHANGED <<clientVars, toServer, toClient, serverOpen, serverVars, stream, epoch, saved,
                   crashes, handled, reset, stops>>

StoreStartsAgain ==
    /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ stream' = <<>>
    /\ trimmed' = 0
    /\ epoch' = Max({epoch, saved.epoch}) + 1
    /\ saved' = IF faults' < MaxFaults
                THEN [stream |-> stream, epoch |-> epoch, trimmed |-> trimmed]
                ELSE [NoStore EXCEPT !.epoch = epoch]
    /\ reset' = "yes"
    /\ Silent
    /\ UNCHANGED <<clientVars, toServer, toClient, serverOpen, serverVars, crashes, handled, forgot, stops>>

StoreGoesBack ==
    /\ faults < MaxFaults
    /\ saved # NoStore
    /\ faults' = faults + 1
    /\ stream' = saved.stream
    /\ trimmed' = saved.trimmed
    /\ epoch' = saved.epoch
    /\ saved' = [stream |-> stream, epoch |-> epoch, trimmed |-> trimmed]
    /\ reset' = "yes"
    /\ Silent
    /\ UNCHANGED <<clientVars, toServer, toClient, serverOpen, serverVars, crashes, handled, forgot, stops>>

OtherAppends ==
    /\ Cardinality({i \in DOMAIN stream : stream[i].kind = "other"}) < MaxOthers
    /\ stream' = Append(stream, [kind |-> "other", number |-> 0, status |-> "none"])
    /\ ServerLogs(<<[type |-> "other", process |-> 0, number |-> Len(stream) + 1]>>)
    /\ UNCHANGED <<clientVars, toServer, toClient, serverOpen, serverVars, epoch, trimmed, saved,
                   historyVars>>

---------------------------------------------------------------------------

Progress ==
    \/ ClientOpensChannel \/ ClientConnects \/ ClientJoins \/ ClientReceives \/ BackoffEnds \/ LeaveTimesOut
    \/ ReaderAdvances \/ ReaderSeesEpoch
    \/ \E c \in Connections : ServerReceivesJoin(c) \/ ServerRoutes(c) \/ ServerNoticesClose(c)
    \/ \E g \in Joins : \/ ServerTellsReplaced(g) \/ ServerRegisters(g) \/ ServerReadsReader(g)
                        \/ ServerReadsCount(g) \/ ServerTakes(g) \/ ServerAppends(g)

Faults ==
    \/ ClientDisconnects \/ JoinTimesOut \/ SyncTimesOut
    \/ \E g \in Joins : ProcessCrashes(g) \/ ReplaceKills(g)
    \/ StoreForgets \/ StoreStartsAgain \/ StoreGoesBack \/ LateAppendLands \/ LateAppendRefused

Next == Progress \/ Faults \/ AppPushes \/ AppLeaves \/ ReaderReports \/ OtherAppends
    \/ \E c \in Connections : ServerRefusesJoin(c) \/ \E g \in Joins : AppPublishes(g)

Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)

---------------------------------------------------------------------------

HandledInOrder ==
    reset = "no" /\ forgot # "count" =>
        \A i \in DOMAIN handled : i > 1 => handled[i - 1].id <= handled[i].id

Duplicates == Len(handled) - Cardinality({handled[i].id : i \in DOMAIN handled})

HandledOnceUnlessCrashed == reset = "no" /\ forgot # "count" => Duplicates <= crashes

AnsweredOnce == \A i, j \in DOMAIN answers : i < j => answers[i].id # answers[j].id

AnsweredAsHandled ==
    reset = "no" =>
        \A i \in DOMAIN answers :
            answers[i].status \in {"lost", "timeout"} \/ answers[i] \in Range(handled)

LostOnlyWhenForgotten ==
    reset = "no" /\ forgot = "no" => \A i \in DOMAIN answers : answers[i].status # "lost"

LostOnlyWhenHandled ==
    \A i \in DOMAIN answers :
        answers[i].status = "lost" => \E j \in DOMAIN handled : handled[j].id = answers[i].id

OnlyTakenAreAnswered == \A i \in DOMAIN answers : answers[i].status \notin {"notTaken", "unmatched"}

CountIsRecorded ==
    reset = "no" /\ forgot = "no" =>
        \A g \in Joins : proc[g].state = "live" =>
            \A n \in 1..proc[g].received : \E i \in DOMAIN stream : stream[i].kind = "answer" /\ stream[i].number = n

CatchesUp == <>[](reset # "no" \/ (lg.queue = <<>> /\ lg.state \in {"synced", "closed"}))

==============================================================================
