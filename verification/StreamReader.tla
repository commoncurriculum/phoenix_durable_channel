---------------------------- MODULE StreamReader ----------------------------
EXTENDS Integers, Sequences

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
    faults

vars == <<appended, after, phase, last, reads, floors, delivered, token, faults>>

Init ==
    /\ appended \in 0..1
    /\ after \in 0..appended
    /\ phase = "reading"
    /\ last = after
    /\ reads \in {<<after>>, <<0>>}
    /\ floors = <<after>>
    /\ delivered = <<>>
    /\ token = "good"
    /\ faults = 0

Current == Len(reads)

ServerAppends ==
    /\ appended < MaxAppend
    /\ appended' = appended + 1
    /\ UNCHANGED <<after, phase, last, reads, floors, delivered, token, faults>>

ReadDelivers(r) ==
    /\ \/ r = Current /\ phase = "reading"
       \/ Broken = "staleRead" /\ r < Current
    /\ reads[r] < appended
    /\ reads' = [reads EXCEPT ![r] = reads[r] + 1]
    /\ IF reads[r] + 1 > floors[r] \/ Broken = "noSkip"
       THEN /\ delivered' = Append(delivered, reads[r] + 1)
            /\ last' = reads[r] + 1
            /\ floors' = [floors EXCEPT ![r] = reads[r] + 1]
       ELSE UNCHANGED <<delivered, last, floors>>
    /\ UNCHANGED <<appended, after, phase, token, faults>>

ReadEnds ==
    /\ faults < MaxFaults
    /\ phase = "reading"
    /\ faults' = faults + 1
    /\ phase' = IF Broken = "noRetry" THEN "stopped" ELSE "backingOff"
    /\ UNCHANGED <<appended, after, last, reads, floors, delivered, token>>

TokenExpires ==
    /\ faults < MaxFaults
    /\ token = "good"
    /\ faults' = faults + 1
    /\ token' = "expired"
    /\ UNCHANGED <<appended, after, phase, last, reads, floors, delivered>>

ReadRefused ==
    /\ phase = "refused"
    /\ phase' = "backingOff"
    /\ token' = IF Broken = "noRefresh" THEN "expired" ELSE "good"
    /\ UNCHANGED <<appended, after, last, reads, floors, delivered, faults>>

ReadsAgain ==
    /\ phase = "backingOff"
    /\ IF token = "good"
       THEN /\ phase' = "reading"
            /\ \E start \in {last, 0} : reads' = Append(reads, start)
            /\ floors' = Append(floors, IF Broken = "fromStart" THEN after ELSE last)
       ELSE /\ phase' = "refused"
            /\ UNCHANGED <<reads, floors>>
    /\ UNCHANGED <<appended, after, last, delivered, token, faults>>

Done == UNCHANGED vars

Next ==
    \/ ServerAppends
    \/ \E r \in 1..Current : ReadDelivers(r)
    \/ ReadEnds \/ TokenExpires \/ ReadRefused \/ ReadsAgain
    \/ Done

Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(ReadDelivers(Current)) /\ WF_vars(ReadsAgain) /\ WF_vars(ReadRefused)

---------------------------------------------------------------------------

DeliveredInOrder == \A i \in DOMAIN delivered : delivered[i] = after + i

CatchesUp == <>[](last = appended)

==============================================================================
