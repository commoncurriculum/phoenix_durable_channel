---------------------------- MODULE StreamReader ----------------------------
(***************************************************************************)
(* A channel reading its durable messages from the store its join reply     *)
(* names instead of from Phoenix: readerMachine in src/machines/reader.ts, *)
(* and Reader, S2Stream and DurableStream in src/stream.ts. The server     *)
(* appends; each read returns the store in order from where it starts. An  *)
(* S2 read starts right after `last`; a Durable Streams read starts there  *)
(* only when it knows where `last` ends (the read that delivered it, or    *)
(* the join reply), and otherwise at the beginning, skipping               *)
(* every number up to the last it delivered. A read can end at any moment, *)
(* and a read that has ended sends nothing more because its actor is       *)
(* stopped with the `reading` state.                                       *)
(*                                                                         *)
(* The read token expires. A store that refuses it gets the read refused:  *)
(* readEvents asks the server for a fresh reader (`phx_reader`) and the    *)
(* read fails, so the next one reads with the fresh token.                 *)
(*                                                                         *)
(* Broken names a change that removes one protection; "none" is the code.  *)
(***************************************************************************)
EXTENDS Integers, Sequences

CONSTANTS MaxAppend, MaxFaults, Broken

VARIABLES
    appended,   \* messages in the store, numbered 1..appended
    after,      \* the channel's `after`: where the first read starts
    phase,      \* readerMachine: "reading", "backingOff", or "refused" by the store
    last,       \* context.last: the last number delivered
    reads,      \* each read's position: the number it read last
    floors,     \* each read's fold: the last number it delivered or started after
    delivered,  \* what the channel's bindings were given, in order
    token,      \* the read token the next request carries: "good" or "expired"
    faults

vars == <<appended, after, phase, last, reads, floors, delivered, token, faults>>

(* `after` is the app's, or else the store's last number in the join reply: *)
(* what was appended before it is not for this client.                      *)
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

(* A read takes the next message. Past its floor, `reading` takes a MESSAGE: *)
(* deliver, and keep its number as `last`. Only the current read's actor is *)
(* alive, unless Broken = "staleRead". A read the store has accepted reads   *)
(* on after its token expires.                                               *)
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

(* The read ends (an error event, the network, the end of the response): FAILED. *)
ReadEnds ==
    /\ faults < MaxFaults
    /\ phase = "reading"
    /\ faults' = faults + 1
    /\ phase' = IF Broken = "noRetry" THEN "stopped" ELSE "backingOff"
    /\ UNCHANGED <<appended, after, last, reads, floors, delivered, token>>

(* The token expires (S2 and Durable Streams tokens last a day). *)
TokenExpires ==
    /\ faults < MaxFaults
    /\ token = "good"
    /\ faults' = faults + 1
    /\ token' = "expired"
    /\ UNCHANGED <<appended, after, phase, last, reads, floors, delivered>>

(* A read with an expired token is refused with a 401 or 403: readEvents runs *)
(* `refused`, which fetches a fresh reader into `latest`, then the read fails. *)
ReadRefused ==
    /\ phase = "refused"
    /\ phase' = "backingOff"
    /\ token' = IF Broken = "noRefresh" THEN "expired" ELSE "good"
    /\ UNCHANGED <<appended, after, last, reads, floors, delivered, faults>>

(* The backoff's sleep ends: `reading` again after `last`, at `last` or at *)
(* the beginning of the store, unless the store refuses the token.          *)
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
(* The rules *)

(* The bindings get each message after `after` once, in order, with none skipped. *)
DeliveredInOrder == \A i \in DOMAIN delivered : delivered[i] = after + i

(* Once reads stop failing, the client has every message after `after`. *)
CatchesUp == <>[](last = appended)

==============================================================================
