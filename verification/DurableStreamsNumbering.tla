------------------------ MODULE DurableStreamsNumbering ------------------------
(***************************************************************************)
(* Channel processes numbering one topic's messages on a Durable Streams   *)
(* store: append/3 and read/1 in PhoenixDurableChannel.Stream.DurableStreams. Each  *)
(* node keeps one row per topic, shared by its processes: the last number   *)
(* given out. A process reads the row, posts the next number as the         *)
(* append's Stream-Seq, and on success writes the number back to the row:   *)
(* three steps, so two processes can post the same number. The store        *)
(* refuses a Stream-Seq that is not past its last (409); the process then   *)
(* reads the stream on to its end, which sets the row to the store's last,  *)
(* and appends again. A post can fail with the store having taken it or     *)
(* not; the process then raises and the row stays as it was.                *)
(*                                                                         *)
(* Broken names a change that removes one protection; "none" is the code.  *)
(***************************************************************************)
EXTENDS Integers, Sequences

CONSTANTS Nodes, Workers, MaxAppend, Broken

Processes == Nodes \X (1..Workers)

VARIABLES
    stream,     \* the numbers in the store, in order
    read,       \* whether each node has read the store for the topic
    given,      \* each node's row: the last number given out
    posting     \* the number each process is posting; 0 while it posts none

vars == <<stream, read, given, posting>>

Last == IF stream = <<>> THEN 0 ELSE stream[Len(stream)]

Init ==
    /\ stream = <<>>
    /\ read = [n \in Nodes |-> FALSE]
    /\ given = [n \in Nodes |-> 0]
    /\ posting = [p \in Processes |-> 0]

(* read/1: the row takes the store's last. *)
Reads(n) ==
    /\ ~read[n]
    /\ read' = [read EXCEPT ![n] = TRUE]
    /\ given' = [given EXCEPT ![n] = Last]
    /\ UNCHANGED <<stream, posting>>

(* append/3 reads the row and takes the next number. With Broken =           *)
(* "givenBeforePost" it writes the number to the row before posting.          *)
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

(* The store answers the post: taken, so the row gets the number; or 409, so *)
(* the process reads on to the store's last, and appends again.              *)
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

(* The post fails (a 5xx or the network), whether or not the store took it. *)
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
(* The rules *)

(* The store holds 1, 2, 3, ... : each number once, in order, none skipped. *)
NumberedInOrder == \A i \in DOMAIN stream : stream[i] = i

==============================================================================
