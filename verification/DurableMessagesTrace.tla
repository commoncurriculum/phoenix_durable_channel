--------------------------- MODULE DurableMessagesTrace ---------------------------
EXTENDS DurableMessages, TLC

CONSTANTS ClientTrace, ServerTrace

VARIABLES clientAt, serverAt, processOf

traceVars == <<vars, clientAt, serverAt, processOf>>
TraceView == <<View, clientAt, serverAt, processOf>>

TraceInit ==
    /\ Init
    /\ clientAt = 1
    /\ serverAt = 1
    /\ processOf = [label \in {} |-> 0]
    /\ TLCSet(1, 0)

ClientMatches ==
    LET events == logged'.client
    IN /\ clientAt + Len(events) - 1 <= Len(ClientTrace)
       /\ \A k \in DOMAIN events : events[k] = ClientTrace[clientAt + k - 1]
       /\ clientAt' = clientAt + Len(events)

ServerMatches ==
    LET events == logged'.server
    IN IF events = <<>>
       THEN UNCHANGED <<serverAt, processOf>>
       ELSE /\ serverAt + Len(events) - 1 <= Len(ServerTrace)
            /\ \A k \in DOMAIN events :
                  /\ [events[k] EXCEPT !.process = 0] = [ServerTrace[serverAt + k - 1] EXCEPT !.process = 0]
                  /\ ServerTrace[serverAt + k - 1].process = ServerTrace[serverAt].process
            /\ LET e == events[1]
                   t == ServerTrace[serverAt]
               IN IF t.process \in DOMAIN processOf
                  THEN /\ processOf[t.process] = e.process
                       /\ UNCHANGED processOf
                  ELSE /\ e.process \notin Range(processOf)
                       /\ processOf' = processOf @@ (t.process :> e.process)
            /\ serverAt' = serverAt + Len(events)

Furthest ==
    clientAt' + serverAt' > TLCGet(1) =>
        /\ TLCSet(1, clientAt' + serverAt')
        /\ PrintT(<<"matched", clientAt' - 1, serverAt' - 1>>)

TraceNext == Next /\ ClientMatches /\ ServerMatches /\ Furthest

TraceSpec == TraceInit /\ [][TraceNext]_traceVars

NotAccepted == ~(clientAt > Len(ClientTrace) /\ serverAt > Len(ServerTrace))

==============================================================================
