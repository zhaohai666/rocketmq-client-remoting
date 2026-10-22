// The collection of real-machine integration tools.
//
// A "single executable + subcommands" layout rather than 5 csproj files: there is only
// one build output and the subcommands share the protocol/client layer code.
using System.Globalization;

namespace RocketMQ.Examples;

internal static class Program
{
    private static int Main(string[] args)
    {
        // Note: the protocol layer's number/date formatting must be independent of the
        // region, so the current thread's region is pinned to the invariant culture first
        // and InvariantCulture is then passed explicitly all the way down (belt and
        // braces, so that nothing like "1,5" is produced under a locale such as de-DE).
        CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
        CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.InvariantCulture;

        if (args.Length == 0)
        {
            PrintUsage();
            return 2;
        }

        string command = args[0];
        string[] rest = args[1..];
        try
        {
            return command switch
            {
                "selfcheck" => SelfCheck.Run(),
                "interop" => InteropTool.Run(rest),
                "message-types" => LiveMessageTypes.Run(rest),
                "admin-live" => AdminLive.Run(rest),
                "admin-batch-live" => AdminBatchLive.Run(rest),
                "compression-live" => CompressionLive.Run(rest),
                "redelivery" => LiveRedelivery.Run(rest),
                "acl" => LiveAcl.Run(rest),
                "pull" => LivePull.Run(rest),
                "pull-heartbeat" => LivePullHeartbeat.Run(rest),
                "lite-pull" => LiveLitePull.Run(rest),
                "lite-pull-cursor" => LiveLitePullCursor.Run(rest),
                "lite-pull-code" => LiveLitePullCode.Run(rest),
                "lite-queue-change" => LiveLiteTopicQueueChange.Run(rest),
                "rr" => LiveRR.Run(rest),
                "reqreply" => LiveRR.Run(rest),
                "latency" => LiveLatency.Run(rest),
                "pop" => LivePop.Run(rest),
                "popc" => LivePopConsumer.Run(rest),
                "pop-batch-ack" => LivePopBatchAck.Run(rest),
                "trace" => LiveTrace.Run(rest),
                "tls" => LiveTls.Run(rest),
                "hook" => LiveHook.Run(rest),
                "backpressure" => LiveBackPressure.Run(rest),
                "flow-control" => LiveFlowControl.Run(rest),
                "async-send" => LiveAsyncSend.Run(rest),
                "validators-live" => LiveValidators.Run(rest),
                "recall" => LiveRecall.Run(rest),
                "unit-config" => LiveUnitConfig.Run(rest),
                "send-header" => LiveSendHeader.Run(rest),
                "sql92" => LiveSql92.Run(rest),
                "static-topic-live" => LiveStaticTopic.Run(rest),
                "unreg-live" => ProducerUnregisterLive.Run(rest),
                "fail-fast" => LiveFailFast.Run(rest),
                "scheduled-intervals" => LiveScheduledIntervals.Run(rest),
                "subscribe" => LiveSubscribe.Run(rest),
                "pinned-guard" => LivePinnedGuard.Run(rest),
                "offset-illegal" => LiveOffsetIllegal.Run(rest),
                "reset-offset" => LiveResetOffset.Run(rest),
                "publish-route-master" => LivePublishRouteMaster.Run(rest),
                "clean-expired-msg" => LiveCleanExpiredMsg.Run(rest),
                _ => Unknown(command),
            };
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[FATAL] {command}: {ex}");
            return 1;
        }
    }

    private static int Unknown(string command)
    {
        Console.Error.WriteLine($"unknown command: {command}");
        PrintUsage();
        return 2;
    }

    private static void PrintUsage()
    {
        Console.Error.WriteLine(
            """
            usage: rmq <command> [args...]

              selfcheck                     protocol codec self-check (no cluster dependency)
              interop <mode> [args...]      cross-language interop (message-level interop with the protocol reference implementation)
              message-types [namesrv] [ak] [sk]
                                            real cluster integration of 11 message capabilities (with ak/sk every client gets ACL credentials, and a no-credentials reverse assertion runs first)
              admin-live   [namesrv]        real cluster admin-side full-chain integration
              admin-batch-live [namesrv]    real cluster batch admin integration
              compression-live <mode> ...   the compression cross-client matrix
              redelivery    [namesrv]       real cluster consume-side capability integration (resend/offsets/orderly/broadcasting/flow control)
              acl     [namesrv] [ak] [sk]   real cluster ACL authentication integration (needs the broker authenticationEnabled)
              pull         [namesrv]        real cluster active pull consumer integration (manual pull / manual offsets / resend)
              pull-heartbeat [namesrv] [master] [slave]
                                            pull-mode consumer heartbeat integration (203/38 visible on master and slave, 35 unregister removes it at once, ghost group control)
              lite-pull    [namesrv]       real cluster lightweight pull consumer integration (both subscribe/assign modes + poll)
              lite-pull-cursor [namesrv]   real cluster lite-pull cursor integration (NO_MATCHED_MSG follows past the whole range + out-of-range self-heals)
              lite-pull-code [namesrv]     real cluster lite-pull request code / switch integration (361 is intercepted by litePullMessageEnable only for lite, 11 and push as controls)
              lite-queue-change [namesrv]  real cluster lite-pull topic queue set change listener integration (a 1s interval + real scale up/down => proves the comparison round asks for the route afresh)
              rr           [namesrv]        real cluster Request-Reply (5.x) integration (request/response/concurrency/timeout)
              latency      [namesrv]        real cluster send latency fault tolerance integration (off by default / on / isolation degradation chain / recovery on expiry)
              pop          [namesrv]        real cluster POP mode (5.x) integration (POP/ACK/extending invisible/revived redelivery)
              popc         [namesrv]        real cluster POP **consume side** integration (consume loop / ack / delayed redelivery)
              pop-batch-ack <namesrv> [legs] [stamp]
                                            real cluster BATCH_ACK_MESSAGE(200151) integration (one request clears a whole batch / leaving exactly one checkpoint out revives only that message, whose retry body carries marker 1 on the business topic / two pop rounds of one queue keep distinct merge keys; needs no broker switch and no restart)
              trace        [namesrv]        real cluster message trace integration (needs the broker traceTopicEnable=true)
              hook         [namesrv]        real cluster CheckForbidden/FilterMessage hook integration
              backpressure [namesrv]        real cluster async send backpressure integration (count/byte gates, reconciling rejections, growing at runtime)
              flow-control [namesrv]        real cluster pre-pull flow control's five thresholds integration (count/byte/span/topic level, nothing lost after a hit)
              async-send   [namesrv]        real cluster async send kernel integration (the caller is not blocked, the thread rules, concurrency does not cross wires, draining on shutdown)
              validators-live [namesrv]     real cluster name validation integration (fast local reject + send/receive with legal names + a round-trip control)
              recall     [namesrv]        real cluster scheduled message recall integration (needs the broker recallMessageEnable, the script restores it)
              unit-config [namesrv]      real cluster unitName/unitMode/stream integration (the clientId suffix, the topic's UNIT bit, %RETRY%'s UNIT_SUB bit)
              send-header [namesrv]      real cluster send header c/d/n integration (the template topic decides the auto-created queue count, five entry points landing one by one)
              sql92      [namesrv]        real cluster SQL92 filtering + CHECK_CLIENT_CONFIG(46) integration (needs the broker enablePropertyFilter)
              static-topic-live [namesrv] real cluster static topic integration (513 createStaticTopic + endpoint set derivation + instance rewriting + send/receive)
              unreg-live [namesrv]        real cluster exit unregister integration (capturing the UNREGISTER_CLIENT(35) frame + a 204 before/after control + a control group)
              fail-fast  [namesrv]        in-flight requests are declared dead at once when the broker really dies (failFast) integration (restarts the broker)
              scheduled-intervals [namesrv] real cluster periodic task integration (route refresh interval 1s/30s, offset flush interval 1s/60s timed on a real machine)
              subscribe  [namesrv]        real cluster subscribe-after-start integration (subscribe after start() pushes a heartbeat at once, the new topic really gets consumed)
              pinned-guard [namesrv]      real cluster pinned send topic guard integration (a real route is not rejected by mistake, the rejection is local and leaves no trace on the broker, oneway has no guard)
              offset-illegal [namesrv]    real cluster OFFSET_ILLEGAL correction integration (invalidating the in-flight/buffer messages as a whole and rebuilding at the corrected offset; the corrected offset is flushed at once)
              reset-offset [namesrv]      real cluster 220 reset consumer offset integration (flush at once after the broker pushes 220 + in-flight batches invalidated + the queue rebuilt at the new offset)
              publish-route-master [namesrv] [master] [slave]
                                            publish route skipping a broker with no master integration (stop a master once: the publish queues drop to zero, the subscription does not change, and consumption from the slave still works)
              clean-expired-msg [namesrv] real cluster cleanExpiredMsg escape hatch integration (hang the listener past the timeout -> the sweep resends -> the %RETRY% redelivery arrives; about 4 minutes)
            """);
    }
}
