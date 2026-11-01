---------------------------- MODULE SocketBuffer ----------------------------
EXTENDS Integers, Sequences

CONSTANTS MaxPush, MaxFaults, MaxUserSteps, Broken

VARIABLES
    conn,
    wanted,
    buffer,
    inFlight,
    written,
    fate,
    pushes, faults, userSteps

vars == <<conn, wanted, buffer, inFlight, written, fate, pushes, faults, userSteps>>

Init ==
    /\ conn = "disconnected"
    /\ wanted = FALSE
    /\ buffer = <<>>
    /\ inFlight = <<>>
    /\ written = <<>>
    /\ fate = [p \in 1..MaxPush |-> "pending"]
    /\ pushes = 0
    /\ faults = 0
    /\ userSteps = 0

Range(s) == {s[i] : i \in DOMAIN s}
Mark(f, s, v) == [p \in DOMAIN f |-> IF p \in Range(s) THEN v ELSE f[p]]

AppPushes ==
    /\ pushes < MaxPush
    /\ pushes' = pushes + 1
    /\ LET p == pushes + 1 IN
       IF conn = "open"
       THEN /\ inFlight' = Append(inFlight, p)
            /\ UNCHANGED <<buffer, fate>>
       ELSE IF conn = "connecting" /\ Broken = "sendWhileConnecting"
            THEN /\ fate' = [fate EXCEPT ![p] = "vanished"]
                 /\ UNCHANGED <<buffer, inFlight>>
            ELSE /\ buffer' = Append(buffer, p)
                 /\ UNCHANGED <<inFlight, fate>>
    /\ UNCHANGED <<conn, wanted, written, faults, userSteps>>

AppConnects ==
    /\ userSteps < MaxUserSteps
    /\ conn = "disconnected"
    /\ userSteps' = userSteps + 1
    /\ conn' = "connecting"
    /\ wanted' = TRUE
    /\ UNCHANGED <<buffer, inFlight, written, fate, pushes, faults>>

AppDisconnects ==
    /\ userSteps < MaxUserSteps
    /\ wanted
    /\ userSteps' = userSteps + 1
    /\ conn' = "disconnected"
    /\ wanted' = FALSE
    /\ buffer' = <<>>
    /\ fate' = Mark(fate, buffer, "dropped")
    /\ UNCHANGED <<inFlight, written, pushes, faults>>

Opens ==
    /\ conn = "connecting"
    /\ conn' = "open"
    /\ IF Broken = "noFlush"
       THEN UNCHANGED <<buffer, inFlight>>
       ELSE IF Broken = "lateFlush"
       THEN UNCHANGED <<buffer, inFlight>>
       ELSE /\ inFlight' = inFlight \o buffer
            /\ buffer' = IF Broken = "keepBuffer" THEN buffer ELSE <<>>
    /\ UNCHANGED <<wanted, written, fate, pushes, faults, userSteps>>

LateFlush ==
    /\ Broken = "lateFlush"
    /\ conn = "open"
    /\ buffer # <<>>
    /\ inFlight' = inFlight \o buffer
    /\ buffer' = <<>>
    /\ UNCHANGED <<conn, wanted, written, fate, pushes, faults, userSteps>>

ServerReads ==
    /\ inFlight # <<>>
    /\ written' = Append(written, Head(inFlight))
    /\ fate' = [fate EXCEPT ![Head(inFlight)] = "written"]
    /\ inFlight' = Tail(inFlight)
    /\ UNCHANGED <<conn, wanted, buffer, pushes, faults, userSteps>>

Drops ==
    /\ faults < MaxFaults
    /\ conn \in {"connecting", "open"}
    /\ faults' = faults + 1
    /\ conn' = "connecting"
    /\ inFlight' = <<>>
    /\ fate' = Mark(fate, inFlight, "lost")
    /\ UNCHANGED <<wanted, buffer, written, pushes, userSteps>>

Done == UNCHANGED vars

Next ==
    \/ AppPushes \/ AppConnects \/ AppDisconnects
    \/ Opens \/ LateFlush \/ ServerReads \/ Drops
    \/ Done

Spec == Init /\ [][Next]_vars /\ WF_vars(Opens) /\ WF_vars(LateFlush) /\ WF_vars(ServerReads)

---------------------------------------------------------------------------

WrittenOnceInOrder == \A i \in DOMAIN written : i > 1 => written[i - 1] < written[i]

NothingVanishes == \A p \in DOMAIN fate : fate[p] # "vanished"

EveryPushGoesOut ==
    <>[](wanted => \A p \in 1..pushes : fate[p] # "pending")

==============================================================================
