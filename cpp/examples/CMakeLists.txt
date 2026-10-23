# Self-check binary: protocol-layer round-trip self-check (does not depend on a real cluster)
add_executable(rmq_selfcheck selfcheck.cpp)
target_link_libraries(rmq_selfcheck PRIVATE rocketmq::remoting)

# Interop tool: cross-language encode/decode cross-validation against the Python reference client
add_executable(rmq_interop interop_tool.cpp)
target_link_libraries(rmq_interop PRIVATE rocketmq::remoting)

# Real-cluster message-type integration (requires nameServer+broker up first; not packed into ctest)
add_executable(rmq_live_message_types live_message_types.cpp)
target_link_libraries(rmq_live_message_types PRIVATE rocketmq::remoting)

# Oneway send-first + FIRST_OFFSET ordering check (requires nameServer+broker up first; not packed into ctest)
add_executable(rmq_oneway_first_live oneway_first_live.cpp)
target_link_libraries(rmq_oneway_first_live PRIVATE rocketmq::remoting)

add_executable(rmq_live_redelivery live_redelivery.cpp)
target_link_libraries(rmq_live_redelivery PRIVATE rocketmq::remoting)

# Real-cluster admin-side integration (requires nameServer+broker up first; not packed into ctest)
add_executable(rmq_admin_live admin_live.cpp)
target_link_libraries(rmq_admin_live PRIVATE rocketmq::remoting)

# Batch admin-method real-machine integration (not packed into ctest)
add_executable(rmq_admin_batch_live admin_batch_live.cpp)
target_link_libraries(rmq_admin_batch_live PRIVATE rocketmq::remoting)

# Real-cluster auto-compression integration (self-produce/self-consume + cross-client interop; not packed into ctest)
add_executable(rmq_compression_live compression_live.cpp)
target_link_libraries(rmq_compression_live PRIVATE rocketmq::remoting)

# ACL authentication real-machine integration (requires the broker to enable authenticationEnabled; not packed into ctest)
add_executable(rmq_live_acl live_acl.cpp)
target_link_libraries(rmq_live_acl PRIVATE rocketmq::remoting)

# TLS + traceparent real-machine integration (requires the broker to enable tls.test.mode.enable; not packed into ctest)
add_executable(rmq_live_tls live_tls.cpp)
target_link_libraries(rmq_live_tls PRIVATE rocketmq::remoting)

# #25 negative probe: under strict CA validation, trusting a wrong CA must make the producer error carry a CA/certificate marker
# (route fetch failed threaded through; not packed into ctest)
add_executable(rmq_live_tls_negative live_tls_negative.cpp)
target_link_libraries(rmq_live_tls_negative PRIVATE rocketmq::remoting)

# Active-pull consumer (DefaultMQPullConsumer) real-machine integration
add_executable(rmq_live_pull live_pull.cpp)
target_link_libraries(rmq_live_pull PRIVATE rocketmq::remoting)

# Static-topic real-machine leg (seven conventions: 513 topic creation + endpoint derivation + instance rewrite + end-to-end send/recv)
add_executable(rmq_live_static_topic live_static_topic.cpp)
target_link_libraries(rmq_live_static_topic PRIVATE rocketmq::remoting)

# Lite-pull consumer (DefaultLitePullConsumer) real-machine integration: subscribe/assign dual mode + poll
add_executable(rmq_live_lite_pull live_lite_pull.cpp)
target_link_libraries(rmq_live_lite_pull PRIVATE rocketmq::remoting)

# lite-pull cursor real-machine verification (#105: NO_MATCHED_MSG following past a whole span + OFFSET_ILLEGAL out-of-range self-healing)
add_executable(rmq_live_lite_pull_cursor live_lite_pull_cursor.cpp)
target_link_libraries(rmq_live_lite_pull_cursor PRIVATE rocketmq::remoting)

# lite-pull request codes/broker switch real-machine verification (#107: 361 + the LITE_PULL_MESSAGE bit + litePullMessageEnable)
add_executable(rmq_live_lite_pull_code live_lite_pull_code.cpp)
target_link_libraries(rmq_live_lite_pull_code PRIVATE rocketmq::remoting)

# lite-pull topic queue-set change listener real-machine verification (1s check period + real scale up/down => proves each comparison pass re-queries the route)
add_executable(rmq_live_lite_topic_queue_change live_lite_topic_queue_change.cpp)
target_link_libraries(rmq_live_lite_topic_queue_change PRIVATE rocketmq::remoting)

# Request-Reply (5.x) real-machine integration (request -> reply -> 326 push-back; not packed into ctest)
add_executable(rmq_live_request_reply live_rr.cpp)
target_link_libraries(rmq_live_request_reply PRIVATE rocketmq::remoting)

# Send-latency fault tolerance real-machine integration (default off/on, isolation degradation chain, recover on expiry; not packed into ctest)
add_executable(rmq_live_latency live_latency.cpp)
target_link_libraries(rmq_live_latency PRIVATE rocketmq::remoting)

# POP mode (5.x lightweight consumption) real-machine integration: POP/ACK/extend invisible time/revive redelivery (not packed into ctest)
add_executable(rmq_live_pop live_pop.cpp)
target_link_libraries(rmq_live_pop PRIVATE rocketmq::remoting)

# POP **consumer side** real-machine integration (consume loop + ack + extend invisible time; not packed into ctest)
add_executable(rmq_live_pop_consumer live_pop_consumer.cpp)
target_link_libraries(rmq_live_pop_consumer PRIVATE rocketmq::remoting)

# BATCH_ACK_MESSAGE(200151) real-machine leg: the POP consume path acks message by message and never batches,
# so the batch wire path is only reachable by driving the primitive directly (not packed into ctest)
add_executable(rmq_live_pop_batch_ack live_pop_batch_ack.cpp)
target_link_libraries(rmq_live_pop_batch_ack PRIVATE rocketmq::remoting)

# Message trace real-machine integration (requires broker traceTopicEnable=true; not packed into ctest)
add_executable(rmq_live_trace live_trace.cpp)
target_link_libraries(rmq_live_trace PRIVATE rocketmq::remoting)

# Hook real-machine integration: CheckForbiddenHook interception + FilterMessageHook pull/POP two paths (not packed into ctest)
add_executable(rmq_live_hook live_hook.cpp)
target_link_libraries(rmq_live_hook PRIVATE rocketmq::remoting)


# Name validation (Validators/TopicValidator) real-machine integration: local fast-reject + legal names send/recv as usual + round-trip comparison (not packed into ctest)
add_executable(rmq_validators_live validators_live.cpp)
target_link_libraries(rmq_validators_live PRIVATE rocketmq::remoting)

# Scheduled-message recall (370) real-machine integration: handle round-trip + after recall it is not delivered at the due time + broker switch restored (not packed into ctest)
add_executable(rmq_recall_live recall_live.cpp)
target_link_libraries(rmq_recall_live PRIVATE rocketmq::remoting)

# unitName / unitMode / enableStreamRequestType real-machine integration: clientId suffix + the broker-side unit bit (not packed into ctest)
add_executable(rmq_live_unit_config live_unit_config.cpp)
target_link_libraries(rmq_live_unit_config PRIVATE rocketmq::remoting)

# SQL92 filtering + CHECK_CLIENT_CONFIG(46) real-machine integration (requires broker enablePropertyFilter=true; not packed into ctest)
add_executable(rmq_sql92_live sql92_live.cpp)
target_link_libraries(rmq_sql92_live PRIVATE rocketmq::remoting)

# Async-send backpressure (two fair semaphores) real-machine integration: throttle wording + broker-side reconciliation + runtime scale-up (not packed into ctest)
add_executable(rmq_live_backpressure live_backpressure.cpp)
target_link_libraries(rmq_live_backpressure PRIVATE rocketmq::remoting)

# Async-send kernel (the prepare-section/callback-thread conventions, concurrency without cross-talk, pinned/intercepted/batched/closed-pool drain) real-machine integration (not packed into ctest)
add_executable(rmq_live_async_send live_async_send.cpp)
target_link_libraries(rmq_live_async_send PRIVATE rocketmq::remoting)

# Send-header c/d/n three fields real-machine integration (the template topic decides the auto-created queue count; not packed into ctest)
add_executable(rmq_live_send_header live_send_header.cpp)
target_link_libraries(rmq_live_send_header PRIVATE rocketmq::remoting)

# The three per-queue pre-pull flow-control thresholds and the topic budget divided into them, real-machine
# integration: the gate really gets hit + after a hit not one message is lost (not packed into ctest)
add_executable(rmq_live_flow_control live_flow_control.cpp)
target_link_libraries(rmq_live_flow_control PRIVATE rocketmq::remoting)

# Producer shutdown unregister UNREGISTER_CLIENT(35) real-machine integration: frame capture proves this one went online + 204 before/after comparison (not packed into ctest)
add_executable(rmq_live_producer_unregister live_producer_unregister.cpp)
target_link_libraries(rmq_live_producer_unregister PRIVATE rocketmq::remoting)

# When a broker really dies, in-flight requests are judged dead immediately (failFast) real-machine integration: stops the broker once then brings it back up (not packed into ctest)
add_executable(rmq_live_fail_fast live_fail_fast.cpp)
target_link_libraries(rmq_live_fail_fast PRIVATE rocketmq::remoting)

# Route-refresh period / offset-persist period real-machine integration: two instances with different periods, when do the cache/offset move (not packed into ctest)
add_executable(rmq_live_scheduled_intervals live_scheduled_intervals.cpp)
target_link_libraries(rmq_live_scheduled_intervals PRIVATE rocketmq::remoting)

# Post-hoc subscribe takes effect immediately real-machine integration: after subscribe the broker's topic->group table registers this group at once (not packed into ctest)
add_executable(rmq_live_subscribe live_subscribe.cpp)
target_link_libraries(rmq_live_subscribe PRIVATE rocketmq::remoting)

# Pinned-send topic consistency guard real-machine integration: rejected on this end (broker maxOffset does not move), the allowed leg receives all as usual (not packed into ctest)
add_executable(rmq_live_pinned_guard live_pinned_guard.cpp)
target_link_libraries(rmq_live_pinned_guard PRIVATE rocketmq::remoting)

# Empty-reply (NO_NEW_MSG/NO_MATCHED_MSG) offset correction real-machine integration: zero delivery but the broker-side offset advances (not packed into ctest)
add_executable(rmq_live_correct_tags_offset live_correct_tags_offset.cpp)
target_link_libraries(rmq_live_correct_tags_offset PRIVATE rocketmq::remoting)

# OFFSET_ILLEGAL recovery real-machine integration: discard the whole batch's in-flight/buffered messages + rebuild by the corrected offset + persist immediately (not packed into ctest)
add_executable(rmq_live_offset_illegal live_offset_illegal.cpp)
target_link_libraries(rmq_live_offset_illegal PRIVATE rocketmq::remoting)

# 220 reset-consumer-offset real-machine integration: after the broker pushes 220 it persists at once + in-flight batches discarded + queue rebuilt at the new offset (not packed into ctest)
add_executable(rmq_live_reset_offset live_reset_offset.cpp)
target_link_libraries(rmq_live_reset_offset PRIVATE rocketmq::remoting)

# Pull-mode consumer heartbeat real-machine integration: 203/38 visibility + consumeType/consumeFromWhere/subscription table + 35 unregister immediately (not packed into ctest)
add_executable(rmq_live_pull_heartbeat live_pull_heartbeat.cpp)
target_link_libraries(rmq_live_pull_heartbeat PRIVATE rocketmq::remoting)

# Publish route skipping a broker with no master real-machine integration: stop a master once, publish queues go to zero/subscription unchanged/slave receives messages (not packed into ctest)
add_executable(rmq_live_publish_route_master live_publish_route_master.cpp)
target_link_libraries(rmq_live_publish_route_master PRIVATE rocketmq::remoting)

# cleanExpiredMsg suspension escape hatch real-machine integration: the listener hangs for 2 minutes -> sweep redelivers -> the %RETRY% redelivery arrives
# (about 4 minutes, waiting for two sweep periods; not packed into ctest)
add_executable(rmq_live_clean_expired_msg live_clean_expired_msg.cpp)
target_link_libraries(rmq_live_clean_expired_msg PRIVATE rocketmq::remoting)

# SET_MESSAGE_REQUEST_MODE(401) request shape live leg: the four values ride the JSON body, the only in-protocol
# read-back is GET_ALL_MESSAGE_REQUEST_MODE(402), and the header form is a paired control that must be rejected
# (not packed into ctest)
add_executable(rmq_live_pop_mode live_pop_mode.cpp)
target_link_libraries(rmq_live_pop_mode PRIVATE rocketmq::remoting)
