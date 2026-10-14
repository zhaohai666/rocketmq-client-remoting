# Unit tests: encode/decode round-trip + protocol-alignment guards
add_executable(rmq_test_codec test_codec.cpp)
target_link_libraries(rmq_test_codec PRIVATE rocketmq::remoting)
add_test(NAME codec COMMAND rmq_test_codec)

# clientId convention: IP@instanceName + changeInstanceNameToPID
add_executable(rmq_test_client_id test_client_id.cpp)
target_link_libraries(rmq_test_client_id PRIVATE rocketmq::remoting)
add_test(NAME client_id COMMAND rmq_test_client_id)

# Protocol-alignment guards
add_executable(rmq_test_protocol_alignment test_protocol_alignment.cpp)
target_link_libraries(rmq_test_protocol_alignment PRIVATE rocketmq::remoting)
add_test(NAME protocol_alignment COMMAND rmq_test_protocol_alignment)

# Route / heartbeat / subscription data: round-trip + protocol field-name guards
add_executable(rmq_test_route_heartbeat test_route_heartbeat.cpp)
target_link_libraries(rmq_test_route_heartbeat PRIVATE rocketmq::remoting)
add_test(NAME route_heartbeat COMMAND rmq_test_route_heartbeat)

# Transport layer: real local TCP send/recv (partial-frame reassembly / timeout / connect failure)
add_executable(rmq_test_transport test_transport.cpp)
target_link_libraries(rmq_test_transport PRIVATE rocketmq::remoting)
add_test(NAME transport COMMAND rmq_test_transport)

# SOCKS proxy: local fake SOCKS5/SOCKS4 servers pin the handshake bytes and the CONNECT target,
# the accept/reject paths of RFC 1929 username/password sub-negotiation, and one real remoting
# request sent over the tunnel. Failure-path assertions get a >=30s connect budget per loopback quirks.
add_executable(rmq_test_socks_proxy test_socks_proxy.cpp)
target_link_libraries(rmq_test_socks_proxy PRIVATE rocketmq::remoting)
add_test(NAME socks_proxy COMMAND rmq_test_socks_proxy)

# Disconnect fails in-flight requests immediately (failFast / in-flight request set requestFail): a real socket peer reads then closes;
# assert "death judged in milliseconds + reports SEND_REQUEST not a timeout + callback fires once + does not drag in another peer's requests"
add_executable(rmq_test_fail_fast test_fail_fast.cpp)
target_link_libraries(rmq_test_fail_fast PRIVATE rocketmq::remoting)
add_test(NAME fail_fast COMMAND rmq_test_fail_fast)

# Compression: zlib round-trip + cross-check against external fixtures + failure must raise explicitly
add_executable(rmq_test_compression test_compression.cpp)
target_link_libraries(rmq_test_compression PRIVATE rocketmq::remoting)
add_test(NAME compression COMMAND rmq_test_compression)

# Admin side: fastjson2 tolerance of malformed JSON + admin response-body DTO + TopicConfig/subscription-group field names and defaults
add_executable(rmq_test_admin test_admin.cpp)
target_link_libraries(rmq_test_admin PRIVATE rocketmq::remoting)
add_test(NAME admin COMMAND rmq_test_admin)

# Admin-originated CONSUME_MESSAGE_DIRECTLY(309): mock broker pins the two viewMessage msgId paths (direct offsetMsgId
# lookup + uniqKey search that replaces offsetMsgId), the 309 header carrying exactly four fields, the error semantics of non-SUCCESS / empty
# body, and "if the message cannot be decoded, not one 309 is sent". The consumer-side handler lives in rmq_test_orderly_reconsume.
add_executable(rmq_test_admin_consume_directly test_admin_consume_directly.cpp)
target_link_libraries(rmq_test_admin_consume_directly PRIVATE rocketmq::remoting)
add_test(NAME admin_consume_directly COMMAND rmq_test_admin_consume_directly)

# Client logging: line format (ms/pid/thread name/file:line) + size-based rotation + maxIndex cap + level filtering
add_executable(rmq_test_logging test_logging.cpp)
target_link_libraries(rmq_test_logging PRIVATE rocketmq::remoting)
add_test(NAME logging COMMAND rmq_test_logging)

# ACL authentication: SHA1/HMAC/Base64 standard vectors + signed content and signature value checked against fixed vectors
add_executable(rmq_test_acl test_acl.cpp)
target_link_libraries(rmq_test_acl PRIVATE rocketmq::remoting)
add_test(NAME acl COMMAND rmq_test_acl)

# NamespaceRpcHook (5.x namespaceV2 -> nsd/ns): chained-semantics comparison (empty namespace adds no field),
# chain order Namespace -> Stream -> ACL (the shape of composeRequestHooks and probe evidence),
# the ACL signature varying with namespaceV2 (including a broker-side algorithm replay and verification), and the online evidence for the five facades.
add_executable(rmq_test_namespace_hook test_namespace_hook.cpp)
target_link_libraries(rmq_test_namespace_hook PRIVATE rocketmq::remoting)
add_test(NAME namespace_hook COMMAND rmq_test_namespace_hook)

# Request-Reply (5.x): wait-slot semantics + createReplyMessage shape + 326 push-back entry (incl. late/bad packets)
add_executable(rmq_test_request_reply test_request_reply.cpp)
target_link_libraries(rmq_test_request_reply PRIVATE rocketmq::remoting)
add_test(NAME request_reply COMMAND rmq_test_request_reply)

# Send-latency fault tolerance: per-tier threshold-table mapping + isolate/recover-on-expiry + selection degradation chain (available->reachable->round-robin)
add_executable(rmq_test_latency test_latency.cpp)
target_link_libraries(rmq_test_latency PRIVATE rocketmq::remoting)
add_test(NAME latency COMMAND rmq_test_latency)

# Proactive fault-avoidance probing (startDetectorEnable/detectTimeout/detectInterval + anonymous
# ServiceDetector): detectByOneRound offline semantics (flip back to reachable / slot-taking rate limit / failure leaves the flag
# untouched / resolver removal) + periodic triggering on a fake broker, timeout marks unavailable, recovery lets it pass. Failure-path
# assertions get a >=8s budget per loopback quirks.
add_executable(rmq_test_fault_detector test_fault_detector.cpp)
target_link_libraries(rmq_test_fault_detector PRIVATE rocketmq::remoting)
add_test(NAME fault_detector COMMAND rmq_test_fault_detector)

# Send-retry chain: mock name server + broker, pin the retry-classification semantics of sendDefaultImpl
add_executable(rmq_test_send_retry test_send_retry.cpp)
target_link_libraries(rmq_test_send_retry PRIVATE rocketmq::remoting)
add_test(NAME send_retry COMMAND rmq_test_send_retry)

# Async-send chain: the AsyncSenderExecutor / NettyClientPublicExecutor pools + a retry chain classified
# by exception type (sendMessageAsync), and executor rejected on the bounded queue
add_executable(rmq_test_producer_async test_producer_async.cpp)
target_link_libraries(rmq_test_producer_async PRIVATE rocketmq::remoting)
add_test(NAME producer_async COMMAND rmq_test_producer_async)

# Async-send backpressure primitives: fairness (only the head can take) + in-place runtime capacity change (in-flight retained, waiters woken)
add_executable(rmq_test_backpressure test_backpressure.cpp)
target_link_libraries(rmq_test_backpressure PRIVATE rocketmq::remoting)
add_test(NAME backpressure COMMAND rmq_test_backpressure)

# POP mode: CK-string encode/decode + request-header field-name guards + POP_CK reconstruction
add_executable(rmq_test_pop test_pop.cpp)
target_link_libraries(rmq_test_pop PRIVATE rocketmq::remoting)
add_test(NAME pop COMMAND rmq_test_pop)

# POP **consumer side** (pop_mode consume loop) local unit tests
add_executable(rmq_test_pop_consumer test_pop_consumer.cpp)
target_link_libraries(rmq_test_pop_consumer PRIVATE rocketmq::remoting)
add_test(NAME pop_consumer COMMAND rmq_test_pop_consumer)

# POP + orderly listener (the POP variant of orderly consumption, an unfinished 5.5.0 skeleton):
# after dispatch into the skeleton the listener is not called and not acked; the request set is deduped by (pq, mq), removed only when pq is revoked
add_executable(rmq_test_pop_orderly test_pop_orderly.cpp)
target_link_libraries(rmq_test_pop_orderly PRIVATE rocketmq::remoting)
add_test(NAME pop_orderly COMMAND rmq_test_pop_orderly)

# ackIndex semantics of classic concurrent consumption (the consume-result dispatch of processConsumeResult): the tail of a partial ack is redelivered,
# a failed redelivery pins the offset, RECONSUME_LATER overrides a wide ackIndex, broadcast drops the tail
add_executable(rmq_test_consume_ack_index test_consume_ack_index.cpp)
target_link_libraries(rmq_test_consume_ack_index PRIVATE rocketmq::remoting)
add_test(NAME consume_ack_index COMMAND rmq_test_consume_ack_index)

# The redelivery gate for orderly consumption: the two meanings of -1,
# local +1, the fields of the message redelivered after exhaustion, a failed redelivery still suspends, and the %RETRY% attribute lifted into the send header
add_executable(rmq_test_orderly_reconsume test_orderly_reconsume.cpp)
target_link_libraries(rmq_test_orderly_reconsume PRIVATE rocketmq::remoting)
add_test(NAME orderly_reconsume COMMAND rmq_test_orderly_reconsume)

# Message trace: byte-for-byte comparison vectors + javaSplit semantics + regression for no-keys SubBefore / isolation of bad records
add_executable(rmq_test_trace test_trace.cpp)
target_link_libraries(rmq_test_trace PRIVATE rocketmq::remoting)
add_test(NAME trace COMMAND rmq_test_trace)

# Hooks: CheckForbiddenHook (does not swallow exceptions) and FilterMessageHook (swallows exceptions + pull/POP difference)
# and the comparison vectors for subscription semantics tagsSet/codeSet
add_executable(rmq_test_hook test_hook.cpp)
target_link_libraries(rmq_test_hook PRIVATE rocketmq::remoting)
add_test(NAME hook COMMAND rmq_test_hook)

# Consume-thread elasticity: ConsumeExecutor (core/max two tiers) + five guards for updateCorePoolSize +
# the upstream no-op semantics of adjustThreadPool + msgAccCnt
add_executable(rmq_test_consume_thread_pool test_consume_thread_pool.cpp)
target_link_libraries(rmq_test_consume_thread_pool PRIVATE rocketmq::remoting)
add_test(NAME consume_thread_pool COMMAND rmq_test_consume_thread_pool)

# Dynamic name server: DefaultTopAddressing URL rules / clearNewLine / mock address server
add_executable(rmq_test_top_addressing test_top_addressing.cpp)
target_link_libraries(rmq_test_top_addressing PRIVATE rocketmq::remoting)
add_test(NAME top_addressing COMMAND rmq_test_top_addressing)

add_executable(rmq_test_consumer_stats test_consumer_stats.cpp)
target_link_libraries(rmq_test_consumer_stats PRIVATE rocketmq::remoting)
add_test(NAME consumer_stats COMMAND rmq_test_consumer_stats)

# W3C traceparent: generation/validation/child span/injection does not overwrite/attribute extraction
add_executable(rmq_test_trace_context test_trace_context.cpp)
target_link_libraries(rmq_test_trace_context PRIVATE rocketmq::remoting)
add_test(NAME trace_context COMMAND rmq_test_trace_context)

# Cross-language interop (C++ <-> Python reference client). Depends on the python-side venv; skipped if absent.
set(RMQ_PY_VENV "${CMAKE_CURRENT_SOURCE_DIR}/../../python/.venv/bin/python")
if(EXISTS "${RMQ_PY_VENV}")
    add_test(NAME interop
             COMMAND ${RMQ_PY_VENV} ${CMAKE_CURRENT_SOURCE_DIR}/interop_check.py
                     $<TARGET_FILE:rmq_interop>)
endif()

# Lite-pull consumer (DefaultLitePullConsumer): pure state behavior before start + 221/309 body wire shape
add_executable(rmq_test_lite_pull test_lite_pull.cpp)
target_link_libraries(rmq_test_lite_pull PRIVATE rocketmq::remoting)
add_test(NAME lite_pull COMMAND rmq_test_lite_pull)

# Lite-pull cursor: every successful pull round follows nextBeginOffset,
# not only on FOUND; also pins that FIRST_OFFSET does not send GET_MIN_OFFSET (in-frame fake cluster, not a mock)
add_executable(rmq_test_lite_pull_cursor test_lite_pull_cursor.cpp)
target_link_libraries(rmq_test_lite_pull_cursor PRIVATE rocketmq::remoting)
add_test(NAME lite_pull_cursor COMMAND rmq_test_lite_pull_cursor)

# Queue allocation strategies (AVG / AVG_BY_CIRCLE / CONFIG / CONSISTENT_HASH / MACHINE_ROOM /
# MACHINE_ROOM_NEARBY): allocation-result comparison + guards + consumer replaceability
add_executable(rmq_test_allocate_strategy test_allocate_strategy.cpp)
target_link_libraries(rmq_test_allocate_strategy PRIVATE rocketmq::remoting)
add_test(NAME allocate_strategy COMMAND rmq_test_allocate_strategy)

# Consistent-hash ring (the basis of CONSISTENT_HASH): MD5 official vectors + landing/wrap-around/replica conventions
add_executable(rmq_test_consistent_hash test_consistent_hash.cpp)
target_link_libraries(rmq_test_consistent_hash PRIVATE rocketmq::remoting)
add_test(NAME consistent_hash COMMAND rmq_test_consistent_hash)

# Validators fast-fail: topic/group character tables and length + checkMessage wording/order/response code +
# start() group-name guard (illegal/reserved groups fail without touching the network)
add_executable(rmq_test_validators test_validators.cpp)
target_link_libraries(rmq_test_validators PRIVATE rocketmq::remoting)
add_test(NAME validators COMMAND rmq_test_validators)

# broker reverse push 40 NOTIFY_CONSUMER_IDS_CHANGED: instance-level handler + whole-group wake-up + counting
# (a real-machine case cannot inject a reverse request, so only offline dispatch can be pinned)
add_executable(rmq_test_broker_requests test_broker_requests.cpp)
target_link_libraries(rmq_test_broker_requests PRIVATE rocketmq::remoting)
add_test(NAME broker_requests COMMAND rmq_test_broker_requests)

# Scheduled-message recall (370): handle encode/decode compared against the wire vector + bname key name + local validation order (does not hit the network)
add_executable(rmq_test_recall_message test_recall_message.cpp)
target_link_libraries(rmq_test_recall_message PRIVATE rocketmq::remoting)
add_test(NAME recall_message COMMAND rmq_test_recall_message)

# CHECK_CLIENT_CONFIG(46): mock broker pins the protocol shape and branch semantics
# (when SQL92 is mistyped the broker silently lets all messages through; only this request can turn it into a startup error; for real-machine see rmq_sql92_live)
add_executable(rmq_test_check_client_config test_check_client_config.cpp)
target_link_libraries(rmq_test_check_client_config PRIVATE rocketmq::remoting)
add_test(NAME check_client_config COMMAND rmq_test_check_client_config)

# isPullExpired stall self-healing: pin the threshold arithmetic and the per-queue timetable (a wrong direction either silently stops consuming or invents redelivery).
# "can actually resume consumption after revocation" needs a real thread, so it can only be verified on a real machine (the stall scenario in examples/live_consumer.cpp).
add_executable(rmq_test_pull_expired test_pull_expired.cpp)
target_link_libraries(rmq_test_pull_expired PRIVATE rocketmq::remoting)
add_test(NAME pull_expired COMMAND rmq_test_pull_expired)

# The five pre-pull flow-control thresholds (ProcessQueue backlog watermarks): only the message-count one is hit first in a small-message scenario; the other four
# judge direction silently (a byte/span/topic-level gate failing => buffers pile up unbounded), so they must be pinned offline before going to a real machine.
add_executable(rmq_test_flow_control test_flow_control.cpp)
target_link_libraries(rmq_test_flow_control PRIVATE rocketmq::remoting)
add_test(NAME flow_control COMMAND rmq_test_flow_control)

# Shutdown unregister UNREGISTER_CLIENT(35): mock broker pins the header shape (a blank group slot is not sent online at all) and
# the "every broker incl. slaves" fan-out (the broker-side check is group != null, invisible on a real machine).
# The real-machine evidence is in examples/live_producer_unregister.cpp (frame capture + 204 before/after comparison).
add_executable(rmq_test_producer_unregister test_producer_unregister.cpp)
target_link_libraries(rmq_test_producer_unregister PRIVATE rocketmq::remoting)
add_test(NAME producer_unregister COMMAND rmq_test_producer_unregister)

# Push-consumer config numeric gates (checkConfig): the boundary values of thirteen ranges, out-of-range wording,
# the min/max relative relationship and "a bad config is rejected before MQClientInstance is built" (the address points at a port nobody
# listens on; the reported error must be the config wording not a connection failure). The real-machine side of "boundary values can actually start and consume + broker has no such group"
# is in S5 of examples/live_flow_control.cpp.
add_executable(rmq_test_consumer_check_config test_consumer_check_config.cpp)
target_link_libraries(rmq_test_consumer_check_config PRIVATE rocketmq::remoting)
add_test(NAME consumer_check_config COMMAND rmq_test_consumer_check_config)

# Subscribe post-hoc + immediate heartbeat (handling a subscribe while running):
# after start(), subscribe no longer throws already started and enters the live subscription table at once; unsubscribe only deletes the entry.
# The consumer starts against an unreachable name server (zero brokers => no heartbeat can be sent to any),
# the wire-level "the broker really received a heartbeat carrying the new subscription" is verified on a real machine by examples/live_subscribe.cpp.
add_executable(rmq_test_subscribe_after_start test_subscribe_after_start.cpp)
target_link_libraries(rmq_test_subscribe_after_start PRIVATE rocketmq::remoting)
add_test(NAME subscribe_after_start COMMAND rmq_test_subscribe_after_start)

# Broadcast local offset file: byte-for-byte comparison of the fastjson2 "object-as-key" format + .bak rolling +
# falling back to .bak when the main file is missing (the basis for clients to mutually recognize ~/.rocketmq_offsets)
add_executable(rmq_test_local_offsets test_local_offsets.cpp)
target_link_libraries(rmq_test_local_offsets PRIVATE rocketmq::remoting)
add_test(NAME local_offsets COMMAND rmq_test_local_offsets)

# Scheduled-task cadence (startScheduledTask of MQClientInstance): the default values and pass-through of
# pollNameServerInterval=30000 / persistConsumerOffsetInterval=5000 across the five facades +
# "the first hop lands on initialDelay (10ms) not initialDelay+period" + the period really following the passed-in value.
# The fake name server records the arrival times of GET_ROUTEINFO_BY_TOPIC -- these two deviations on a real machine
# show only as "slow" not "wrong", so only timing assertions can catch them.
add_executable(rmq_test_scheduled_intervals test_scheduled_intervals.cpp)
target_link_libraries(rmq_test_scheduled_intervals PRIVATE rocketmq::remoting)
add_test(NAME scheduled_intervals COMMAND rmq_test_scheduled_intervals)

# postSubscriptionWhenPull (push consumer default false) + pullFromWhichNode
# (recalculatePullFromWhichNode / updatePullFromWhichNode):
# the four branches of findBrokerAddressInSubscribe, the real-socket packets choosing master/slave by brokerId,
# clearing the COMMIT_OFFSET bit on slaves (:219-221), the subscription key not being sent online when the SUBSCRIPTION bit is off,
# and falling back to the master when the slave is absent.
add_executable(rmq_test_pull_post_subscription test_pull_post_subscription.cpp)
target_link_libraries(rmq_test_pull_post_subscription PRIVATE rocketmq::remoting)
add_test(NAME pull_post_subscription COMMAND rmq_test_pull_post_subscription)

# Producer auto-batching (ProduceAccumulator): the four-tuple merge key / the ID order of batch-body assembly /
# the conventions of four groups of batch-level attributes / the global byte gate / the guard thread cleaning empty batches / splitting batch replies /
# synchronous blocking semantics / async callback and permit return (incl. the missing-return path) / sender lifecycle.
add_executable(rmq_test_produce_accumulator test_produce_accumulator.cpp)
target_link_libraries(rmq_test_produce_accumulator PRIVATE rocketmq::remoting)
add_test(NAME produce_accumulator COMMAND rmq_test_produce_accumulator)

# correctTagsOffset:
# on an empty reply (NO_NEW_MSG / NO_MATCHED_MSG) with an empty ProcessQueue, raise the "consumed offset" to
# the reply's nextBeginOffset (increaseOnly); the gate = pending empty **and** in-flight batches 0
# (msgCount only decreases after the listener returns). Missing it leaves the offset permanently behind; loosening the gate silently skips messages on crash.
add_executable(rmq_test_correct_tags_offset test_correct_tags_offset.cpp)
target_link_libraries(rmq_test_correct_tags_offset PRIVATE rocketmq::remoting)
add_test(NAME correct_tags_offset COMMAND rmq_test_correct_tags_offset)

# OFFSET_ILLEGAL recovery: when the offset is corrected by the broker
# overwrite with the corrected value and freeze it (ControllableOffset allowToUpdate=false), revoke the queue (setDropped),
# persist immediately (persist), wake rebalance to rebuild. Getting the freeze/epoch wrong is two silent extremes: the offset bounces back and forth with the broker,
# or after a crash it restarts from an illegal offset and the correction is wasted.
add_executable(rmq_test_offset_illegal_recover test_offset_illegal_recover.cpp)
target_link_libraries(rmq_test_offset_illegal_recover PRIVATE rocketmq::remoting)
add_test(NAME offset_illegal_recover COMMAND rmq_test_offset_illegal_recover)

# 220 RESET_CONSUMER_CLIENT_OFFSET (resetOffset of MQClientInstance):
# the two request-body shapes map/ForC (when the initiator language=JAVA the broker pushes only the map; a language=CPP SDK
# admin side makes the broker push an array, and this port accepts both) + reset = revoke the queue (epoch +1 discards in-flight batches) +
# the new offset persisted via the revoked tail (the broadcast local-offset file must hold the new value and merge with sibling queues).
# An implementation that only writes the in-memory table fails silently on a real machine (an old batch's ack pushes the offset back), so it must be pinned offline.
add_executable(rmq_test_reset_offset test_reset_offset.cpp)
target_link_libraries(rmq_test_reset_offset PRIVATE rocketmq::remoting)
add_test(NAME reset_offset COMMAND rmq_test_reset_offset)

# Pull-mode consumer heartbeat (#98): relies on the instance-level periodic task to register this group
# with the broker (consumeType=CONSUME_ACTIVELY, consumeFromWhere=CONSUME_FROM_LAST_OFFSET,
# subscription set setSubVersion(0L)); lite-pull is also ACTIVELY.
# The criterion uses the wire packet: a fake endpoint receives the JSON body of HEART_BEAT and the extFields of UNREGISTER_CLIENT(35).
add_executable(rmq_test_pull_consumer_heartbeat test_pull_consumer_heartbeat.cpp)
target_link_libraries(rmq_test_pull_consumer_heartbeat PRIVATE rocketmq::remoting)
add_test(NAME pull_consumer_heartbeat COMMAND rmq_test_pull_consumer_heartbeat)

# The balance view of the pull-mode consumer (the internal convention of fetchMessageQueuesInBalance): the official example
# relies on it to decide "pull only my share". This port has no background
# rebalance thread; the view is computed on the spot by the same formula as rebalanceByTopic, so the criteria land on: single instance full set,
# two instances non-overlapping and together covering all, keeping the current allocation when it cannot compute (never monopolizing all), broadcast ignoring cidAll,
# throwing directly when not started, and returning the already-stripped namespace.
add_executable(rmq_test_pull_consumer_balance_view test_pull_consumer_balance_view.cpp)
target_link_libraries(rmq_test_pull_consumer_balance_view PRIVATE rocketmq::remoting)
add_test(NAME pull_consumer_balance_view COMMAND rmq_test_pull_consumer_balance_view)

# Publish address "master only" (#100, findBrokerAddressInPublish):
# the flat table is written by a real updateTopicRouteInfoFromNameServer; on a miss it refreshes the route then re-queries
# (sendKernelImpl); still-missing throws "The broker[X] not exist" with code -1, and
# admin offset queries use the same convention.
# Queue-set filtering is in test_route_heartbeat.cpp; this boundary is the
# watershed between "this end errors immediately" and "sending a write request to a slave and pointlessly taking a round of SYSTEM_BUSY(2)".
add_executable(rmq_test_publish_route_master test_publish_route_master.cpp)
target_link_libraries(rmq_test_publish_route_master PRIVATE rocketmq::remoting)
add_test(NAME publish_route_master COMMAND rmq_test_publish_route_master)

# cleanExpiredMsg suspension escape hatch (coordination on both the consume-service and ProcessQueue sides): the only reclaim path when the listener hangs without returning. Offline pins
# "look only at the head / strictly greater than the threshold / 16 per round / a failed redelivery stays in place / a revoked queue is no longer scanned" and
# coordination with the consume path (in-flight registration, the containsMessage gate, no leak across rounds);
# "successful redelivery -> %RETRY% redelivery -> the second delivery with reconsumeTimes=1" can only be verified on a real machine
# (examples/live_clean_expired_msg.cpp).
add_executable(rmq_test_clean_expired_msg test_clean_expired_msg.cpp)
target_link_libraries(rmq_test_clean_expired_msg PRIVATE rocketmq::remoting)
add_test(NAME clean_expired_msg COMMAND rmq_test_clean_expired_msg)

# The lite-pull consumer's topic queue-set change listener: offline pins "the callback fires once only when the set really changed",
# "registering while running records a snapshot first and the current state is not counted as a change", "each pass re-queries the name server", "a topic whose route is not found skips only itself",
# "a duplicate registration overwrites the old listener", "namespace conventions", and "querying queues before start errors".
add_executable(rmq_test_lite_topic_queue_change test_lite_topic_queue_change.cpp)
target_link_libraries(rmq_test_lite_topic_queue_change PRIVATE rocketmq::remoting)
add_test(NAME lite_topic_queue_change COMMAND rmq_test_lite_topic_queue_change)

# Static-topic route: mock broker-name derivation + endpoint table (cases for the five conventions)
add_executable(rmq_test_static_topic test_static_topic.cpp)
target_link_libraries(rmq_test_static_topic PRIVATE rocketmq::remoting)
add_test(NAME static_topic COMMAND rmq_test_static_topic)
