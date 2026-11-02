---------------------------- MODULE StreamReader ----------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS MaxAppend, MaxFaults, Broken

VARIABLES
    appended,
    after,
    phase,
    last,
    reads,
    floors,
    delivered,
    token,
    forgotten,
    faults

vars == <<appended, after, phase, last, reads, floors, delivered, token, forgotten, faults>>

Init ==
    /\ appended \in 0..1
    /\ after \in 0..appended
    /\ phase = "reading"
    /\ last = after
    /\ reads \in {<<after>>, <<0>>}
    /\ floors = <<after>>
    /\ delivered = <<>>
    /\ token = "good"
    /\ forgotten = 0
    /\ faults = 0

Current == Len(reads)
Max(a, b) == IF a > b THEN a ELSE b
Range(s) == {s[i] : i \in DOMAIN s}

ServerAppends ==
    /\ appended < MaxAppend
    /\ appended' = appended + 1
    /\ UNCHANGED <<after, phase, last, reads, floors, delivered, token, forgotten, faults>>

StoreForgets ==
    /\ faults < MaxFaults
    /\ forgotten < appended
    /\ faults' = faults + 1
    /\ forgotten' = appended
    /\ UNCHANGED <<appended, after, phase, last, reads, floors, delivered, token>>

ReadDelivers(r) ==
    /\ \/ r = Current /\ phase = "reading"
       \/ Broken = "staleRead" /\ r < Current
    /\ \E n \in {Max(reads[r], forgotten) + 1, Max(reads[r], forgotten) + 2} :
        /\ n = Max(reads[r], forgotten) + 1 \/ Broken = "skipsKept"
        /\ n <= appended
        /\ reads' = [reads EXCEPT ![r] = n]
        /\ IF n > floors[r] \/ Broken = "noSkip"
           THEN /\ delivered' = Append(delivered, n)
                /\ last' = n
                /\ floors' = [floors EXCEPT ![r] = n]
           ELSE UNCHANGED <<delivered, last, floors>>
    /\ UNCHANGED <<appended, after, phase, token, forgotten, faults>>

ReadEnds ==
    /\ faults < MaxFaults
    /\ phase = "reading"
    /\ faults' = faults + 1
    /\ phase' = IF Broken = "noRetry" THEN "stopped" ELSE "backingOff"
    /\ UNCHANGED <<appended, after, last, reads, floors, delivered, token, forgotten>>

TokenExpires ==
    /\ faults < MaxFaults
    /\ token = "good"
    /\ faults' = faults + 1
    /\ token' = "expired"
    /\ UNCHANGED <<appended, after, phase, last, reads, floors, delivered, forgotten>>

ReadRefused ==
    /\ phase = "refused"
    /\ phase' = "backingOff"
    /\ token' = IF Broken = "noRefresh" THEN "expired" ELSE "good"
    /\ UNCHANGED <<appended, after, last, reads, floors, delivered, forgotten, faults>>

ReadsAgain ==
    /\ phase = "backingOff"
    /\ IF token = "good"
       THEN /\ phase' = "reading"
            /\ \E start \in {last, 0} : reads' = Append(reads, start)
            /\ floors' = Append(floors, IF Broken = "fromStart" THEN after ELSE last)
       ELSE /\ phase' = "refused"
            /\ UNCHANGED <<reads, floors>>
    /\ UNCHANGED <<appended, after, last, delivered, token, forgotten, faults>>

Done == UNCHANGED vars

Next ==
    \/ ServerAppends \/ StoreForgets
    \/ \E r \in 1..Current : ReadDelivers(r)
    \/ ReadEnds \/ TokenExpires \/ ReadRefused \/ ReadsAgain
    \/ Done

Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(ReadDelivers(Current)) /\ WF_vars(ReadsAgain) /\ WF_vars(ReadRefused)

---------------------------------------------------------------------------

InOrder ==
    \A i \in DOMAIN delivered :
        delivered[i] > IF i = 1 THEN after ELSE delivered[i - 1]

GapsOnlyWhereForgotten ==
    \A n \in (after + 1)..last : n \in Range(delivered) \/ n <= forgotten

CatchesUp == <>[](Max(last, forgotten) = appended)

==============================================================================
