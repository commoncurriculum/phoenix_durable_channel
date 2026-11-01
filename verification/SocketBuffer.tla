---------------------------- MODULE SocketBuffer ----------------------------
(***************************************************************************)
(* The socket's own push buffer in src/socket.ts: frames pushed while the  *)
(* connection is not open wait in context.buffer and go out, in order, as  *)
(* it opens. A dropped connection loses the frames in flight; the channel  *)
(* and its pushes' timeouts answer those (PhoenixDurableChannel.tla).              *)
(*                                                                         *)
(* Broken names a change that removes one protection; "none" is the code.  *)
(***************************************************************************)
EXTENDS Integers, Sequences

CONSTANTS MaxPush, MaxFaults, MaxUserSteps, Broken

VARIABLES
    conn,       \* socketMachine connection: "disconnected", "connecting" or "open"
    wanted,     \* the app called connect() and not disconnect() since
    buffer,     \* context.buffer
    inFlight,   \* frames written to the transport, not yet read by the server
    written,    \* what the server read, in order, across every connection
    fate,       \* each push: "pending", "written", "lost" (in flight on a drop),
                \* "dropped" (buffered when the app disconnected) or "vanished"
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

(* Socket.push -> PUSH: `open` sends it; anywhere else it is buffered. *)
AppPushes ==
    /\ pushes < MaxPush
    /\ pushes' = pushes + 1
    /\ LET p == pushes + 1 IN
       IF conn = "open"
       THEN /\ inFlight' = Append(inFlight, p)
            /\ UNCHANGED <<buffer, fate>>
       ELSE IF conn = "connecting" /\ Broken = "sendWhileConnecting"
            \* WebSocket.send before the open event throws; the frame is gone.
            THEN /\ fate' = [fate EXCEPT ![p] = "vanished"]
                 /\ UNCHANGED <<buffer, inFlight>>
            ELSE /\ buffer' = Append(buffer, p)
                 /\ UNCHANGED <<inFlight, fate>>
    /\ UNCHANGED <<conn, wanted, written, faults, userSteps>>

(* Socket.connect -> CONNECT: disconnected -> engaged.live.connecting. *)
AppConnects ==
    /\ userSteps < MaxUserSteps
    /\ conn = "disconnected"
    /\ userSteps' = userSteps + 1
    /\ conn' = "connecting"
    /\ wanted' = TRUE
    /\ UNCHANGED <<buffer, inFlight, written, fate, pushes, faults>>

(* Socket.disconnect -> DISCONNECT, with dropBuffer. What is in flight still *)
(* reaches the server before the close frame.                               *)
AppDisconnects ==
    /\ userSteps < MaxUserSteps
    /\ wanted
    /\ userSteps' = userSteps + 1
    /\ conn' = "disconnected"
    /\ wanted' = FALSE
    /\ buffer' = <<>>
    /\ fate' = Mark(fate, buffer, "dropped")
    /\ UNCHANGED <<inFlight, written, pushes, faults>>

(* TRANSPORT_OPEN -> open. Its entry sends the buffer, then dropBuffer, in *)
(* one step: nothing else runs between them.                               *)
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

(* Only in Broken = "lateFlush": the buffer goes out in a later step. *)
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

(* TRANSPORT_CLOSE or a missed heartbeat: waitingToReconnect, then connecting *)
(* again; the reconnect timer is folded into this step.                       *)
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
(* The rules *)

(* The server reads each push once, in the order the app pushed. *)
WrittenOnceInOrder == \A i \in DOMAIN written : i > 1 => written[i - 1] < written[i]

(* No push disappears without the server reading it, a drop or a disconnect *)
(* accounting for it.                                                        *)
NothingVanishes == \A p \in DOMAIN fate : fate[p] # "vanished"

(* While the app wants the connection, every push is eventually read by the *)
(* server or lost with a dropped connection.                                 *)
EveryPushGoesOut ==
    <>[](wanted => \A p \in 1..pushes : fate[p] # "pending")

==============================================================================
