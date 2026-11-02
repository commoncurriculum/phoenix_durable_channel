#!/usr/bin/env bash
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
jar="${TLA2TOOLS:-$HOME/.cache/tla2tools-1.7.4.jar}"
if [ ! -f "$jar" ]; then
  mkdir -p "$(dirname "$jar")"
  curl -sSL -o "$jar" https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
  echo "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88  $jar" | sha256sum -c --quiet
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cp "$here"/*.tla "$work/"

tlc() {
  (cd "$work" && java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -cleanup "$1.tla" -config "$2" 2>&1)
}

failures=0

check() {
  local module="$1" constants="$2"
  echo "$module, the code as it is: every rule holds"
  cp "$here/$module.cfg" "$work/$module-as-is.cfg"
  if ! out="$(tlc "$module" "$module-as-is.cfg")" || ! grep -q "No error has been found" <<<"$out"; then
    echo "$out" | grep -v JAVA_TOOL_OPTIONS
    echo "FAILED: a rule breaks on the code as it is. Read the trace with the /statechart skill."
    exit 1
  fi
  grep "distinct states found" <<<"$out"
  while read -r broken rule kind; do
    printf 'SPECIFICATION Spec\nCONSTANTS\n%s\n  Broken = "%s"\nCHECK_DEADLOCK FALSE\n%s %s\n' \
      "$constants" "$broken" "$kind" "$rule" >"$work/$module-$broken-$rule.cfg"
    out="$(tlc "$module" "$module-$broken-$rule.cfg" || true)"
    if grep -qE "Invariant $rule is violated|Temporal properties were violated" <<<"$out"; then
      echo "  breaks as it should: $rule, when $broken"
    else
      echo "  DID NOT BREAK: $rule, when $broken"
      failures=$((failures + 1))
    fi
  done
}

check PhoenixDurableChannel "  MaxFaults = 3
  MaxUserSteps = 2
  MaxPush = 2" <<'EOF'
closeIsFinal ReconnectsUnlessAsked INVARIANT
pushesWaitOnLoss WaitingPushCanBeAnswered INVARIANT
replyAfterTimeout AnsweredOnce INVARIANT
staleJoinReply ClosedOnlyWhenLeft INVARIANT
noJoinRefCheck ClosedOnlyWhenLeft INVARIANT
noLeaveOnJoinTimeout ServerForgetsClosedChannel INVARIANT
joinedIgnoresError JoinedIsReal INVARIANT
noRejoinOnOpen NoChannelWaitsOnAnOpenConnection INVARIANT
noRejoinOnOpen RejoinsWhenItCan PROPERTY
noPushTimer EveryPushAnswered PROPERTY
rebufferOnLoss HandledOnce INVARIANT
flushReversed HandledInOrderOnAJoin INVARIANT
replacedCloses ClosedOnlyWhenLeft INVARIANT
openAfterCallback KnowsWhetherTheSocketIsOpen INVARIANT
leaveOnlyWhenOpen ServerForgetsClosedChannel INVARIANT
EOF

check DurableMessages "  MaxPush = 4
  MaxFaults = 2
  Window = 2
  Kept = 2" <<'EOF'
noReplace HandledOnceUnlessCrashed INVARIANT
noDedupe HandledOnceUnlessCrashed INVARIANT
noWindow RepliesKept INVARIANT
countBeforeRecord CountIsRecorded INVARIANT
noResend CatchesUp PROPERTY
historicIsOk AnsweredAsHandled INVARIANT
retryIsAnswer OnlyTakenAreAnswered INVARIANT
unmatchedIsAnswer OnlyTakenAreAnswered INVARIANT
staleCount HandledOnceUnlessCrashed INVARIANT
syncBeforeReplace HandledOnceUnlessCrashed INVARIANT
resetRereads HandledOnceUnlessCrashed INVARIANT
EOF

check ChannelRead "  NodeCount = 2
  MaxPublish = 2
  MaxFaults = 2
  Page = 1" <<'EOF'
deliverOnArrival DeliveredOnceInOrder INVARIANT
replyBeforeResend NothingLostButForgotten INVARIANT
noGapCheck NothingLostButForgotten INVARIANT
noFailOnLoss CatchesUp PROPERTY
noEpochCheck NothingLostButForgotten INVARIANT
fromStartOnReturn DeliveredOnceInOrder INVARIANT
replyOnFirstPage CatchesUp PROPERTY
readTimeout CatchesUp PROPERTY
EOF

check SocketBuffer "  MaxPush = 3
  MaxFaults = 2
  MaxUserSteps = 3" <<'EOF'
keepBuffer WrittenOnceInOrder INVARIANT
lateFlush WrittenOnceInOrder INVARIANT
sendWhileConnecting NothingVanishes INVARIANT
noFlush EveryPushGoesOut PROPERTY
EOF

check StreamReader "  MaxAppend = 3
  MaxFaults = 2" <<'EOF'
staleRead DeliveredInOrder INVARIANT
fromStart DeliveredInOrder INVARIANT
noSkip DeliveredInOrder INVARIANT
noRetry CatchesUp PROPERTY
noRefresh CatchesUp PROPERTY
EOF

check DurableStreamsNumbering '  Nodes = {"a", "b"}
  Workers = 2
  MaxAppend = 3' <<'EOF'
noStreamSeq NumberedInOrder INVARIANT
givenBeforePost NumberedInOrder INVARIANT
EOF

if [ "$failures" -gt 0 ]; then
  echo "FAILED: $failures rule(s) no longer break on the change they guard against."
  exit 1
fi
echo "OK"
