// 真机联调工具集合（对应 cpp/examples 的 5 个可执行文件）。
//
// 用「单可执行文件 + 子命令」而不是 5 个 csproj：构建产物只有一个，
// 各子命令共享协议层/客户端层代码，调用方式也与 cpp 的工具一一对应。
using System.Globalization;

namespace RocketMQ.Examples;

internal static class Program
{
    private static int Main(string[] args)
    {
        // ⚠ 协议层的数值/日期格式化必须与区域设置无关，先把当前线程的区域锁定为不变文化，
        // 再层层显式传 InvariantCulture（双保险，避免在 de-DE 等区域下产出 "1,5"）。
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
                "compression-live" => CompressionLive.Run(rest),
                "redelivery" => LiveRedelivery.Run(rest),
                "acl" => LiveAcl.Run(rest),
                "pull" => LivePull.Run(rest),
                "lite-pull" => LiveLitePull.Run(rest),
                "rr" => LiveRR.Run(rest),
                "reqreply" => LiveRR.Run(rest),
                "latency" => LiveLatency.Run(rest),
                "pop" => LivePop.Run(rest),
                "popc" => LivePopConsumer.Run(rest),
                "trace" => LiveTrace.Run(rest),
                "tls" => LiveTls.Run(rest),
                "hook" => LiveHook.Run(rest),
                "backpressure" => LiveBackPressure.Run(rest),
                "async-send" => LiveAsyncSend.Run(rest),
                "validators-live" => LiveValidators.Run(rest),
                "recall" => LiveRecall.Run(rest),
                "unit-config" => LiveUnitConfig.Run(rest),
                "send-header" => LiveSendHeader.Run(rest),
                "sql92" => LiveSql92.Run(rest),
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

              selfcheck                     协议编解码自检（不依赖集群）
              interop <mode> [args...]      跨语言互操作（与 Python 参考客户端对齐）
              message-types [namesrv]       真实集群 7 类消息能力联调
              admin-live   [namesrv]        真实集群管理端全链路联调
              compression-live <mode> ...   压缩跨客户端矩阵
              redelivery    [namesrv]       真实集群消费侧能力联调（回投/位点/顺序/广播/流控）
              acl     [namesrv] [ak] [sk]   真实集群 ACL 鉴权联调（需 broker 开 authenticationEnabled）
              pull         [namesrv]       真实集群主动拉取消费者联调（手动拉取/手动位点/回投）
              rr           [namesrv]        真实集群 Request-Reply（5.x）联调（请求/应答/并发/超时）
              latency      [namesrv]        真实集群发送延迟故障容错联调（默认关/开/隔离退化链/到期恢复）
              pop          [namesrv]        真实集群 POP 模式（5.x）联调（POP/ACK/延长不可见/复活重投）
              popc         [namesrv]        真实集群 POP **消费侧**联调（消费循环/ack/延迟重投）
              trace        [namesrv]        真实集群消息轨迹联调（需 broker traceTopicEnable=true）
              hook         [namesrv]        真实集群 CheckForbidden/FilterMessage 钩子联调
              backpressure [namesrv]        真实集群异步发送背压联调（条数/字节闸、拒绝对账、运行时扩容）
              async-send   [namesrv]        真实集群异步发送内核联调（不阻塞调用方、线程口径、并发不串台、关池排空）
              validators-live [namesrv]     真实集群名字校验联调（本地快拒 + 合法名字收发 + 往返对照）
              recall     [namesrv]        真实集群定时消息撤回联调（需 broker 开 recallMessageEnable，脚本负责还原）
              unit-config [namesrv]      真实集群 unitName/unitMode/stream 联调（clientId 后缀、topic UNIT 位、%RETRY% UNIT_SUB 位）
              send-header [namesrv]      真实集群发送头 c/d/n 联调（模板 topic 决定自动建出来的队列数、五种入口逐条落地）
              sql92      [namesrv]        真实集群 SQL92 过滤 + CHECK_CLIENT_CONFIG(46) 联调（需 broker 开 enablePropertyFilter）
            """);
    }
}
