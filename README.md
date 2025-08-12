Developed a Redis clone with support for key data structures (hashmap, streams), RDB persistence,
stream operations (XADD, XRANGE, XREAD), and atomic transactions using MULTI, EXEC, and
DISCARD commands. Implemented leader-follower replication via sockets and the PSYNC protocol,
with multi-threading, RESP parsing, and command propagation for distributed consistency.
