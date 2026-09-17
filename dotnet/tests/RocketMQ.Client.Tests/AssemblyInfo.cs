// 测试工程全局：**关闭测试并行**。
// LoggingTests 会临时改全局日志文件/级别并断言落盘行数；TopAddressingTests 会设置
// 全局环境变量 ROCKETMQ_NAMESRV_DOMAIN —— 都是非线程安全的进程级状态，一旦与其它
// 测试类并行跑就会互相污染（实测 LevelFilter_SuppressesDebugAtInfo 里混进并发线程的
// INFO 行）。全量本就秒级，串行跑没有吞吐损失。
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]
