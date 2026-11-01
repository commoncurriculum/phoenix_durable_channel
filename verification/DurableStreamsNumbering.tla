------------------------ MODULE DurableStreamsNumbering ------------------------
EXTENDS Integers, Sequences

CONSTANTS Nodes, Workers, MaxAppend, Broken

Processes == Nodes \X (1..Workers)

VARIABLES
    stream,
    read,
    given,
    posting

vars == <<stream, read, given, posting>>

Last == IF stream = <<>> THEN 0 ELSE stream[Len(stream)]

Init ==
    /\ stream = <<>>
    /\ read = [n \in Nodes |-> FALSE]
    /\ given = [n \in Nodes |-> 0]
    /\ posting = [p \in Processes |-> 0]

Reads(n) ==
    /\ ~read[n]
    /\ read' = [read EXCEPT ![n] = TRUE]
    /\ given' = [given EXCEPT ![n] = Last]
    /\ UNCHANGED <<stream, posting>>

Takes(p) ==
    /\ posting[p] = 0
    /\ read[p[1]]
    /\ Len(stream) < MaxAppend
    /\ given[p[1]] < MaxAppend
    /\ posting' = [posting EXCEPT ![p] = given[p[1]] + 1]
    /\ given' = IF Broken = "givenBeforePost"
                THEN [given EXCEPT ![p[1]] = given[p[1]] + 1]
                ELSE given
    /\ UNCHANGED <<stream, read>>

Posts(p) ==
    /\ posting[p] # 0
    /\ posting' = [posting EXCEPT ![p] = 0]
    /\ IF posting[p] > Last \/ Broken = "noStreamSeq"
       THEN /\ stream' = Append(stream, posting[p])
            /\ given' = IF Broken = "givenBeforePost" THEN given
                        ELSE [given EXCEPT ![p[1]] = posting[p]]
       ELSE /\ given' = [given EXCEPT ![p[1]] = Last]
            /\ UNCHANGED stream
    /\ UNCHANGED read

PostFails(p) ==
    /\ posting[p] # 0
    /\ posting' = [posting EXCEPT ![p] = 0]
    /\ \/ UNCHANGED stream
       \/ posting[p] > Last /\ stream' = Append(stream, posting[p])
    /\ UNCHANGED <<read, given>>

Done == UNCHANGED vars

Next == \E n \in Nodes : Reads(n) \/ \E p \in Processes : Takes(p) \/ Posts(p) \/ PostFails(p) \/ Done

Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------------

NumberedInOrder == \A i \in DOMAIN stream : stream[i] = i

==============================================================================
