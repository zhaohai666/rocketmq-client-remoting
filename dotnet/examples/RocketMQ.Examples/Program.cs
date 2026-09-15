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
            """);
    }
}
