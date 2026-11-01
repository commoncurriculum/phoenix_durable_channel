FROM mirror.gcr.io/library/ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
      sudo iptables iproute2 socat procps curl ca-certificates libssl3t64 libncurses6 \
      libstdc++6 locales && rm -rf /var/lib/apt/lists/*
ENV LANG=C.UTF-8 ELIXIR_ERL_OPTIONS=+fnu
CMD ["sleep", "infinity"]
