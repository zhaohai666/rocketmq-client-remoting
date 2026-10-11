// MQPullConsumerScheduleService 的本端口对应物（对齐
// org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService，连同 PullTaskCallback /
// PullTaskContext 两个配角）：按 topic 注册回调，服务把**本实例应得的 MessageQueue 集合**
// 逐队列展开成定时拉取任务；每个任务跑完一轮后按 PullTaskContext.PullNextDelayTimeMillis
// 决定下一轮何时再来（回调里拿到 PullResult 后调节奏，等价 Java 示例里的 pullLater 用法）。
//
// 与 Java 的三处结构性差异（都是本端口拉模式门面「无后台 rebalance 线程」的连带决定，
// 见 PullConsumer.cs 文件头）：
//   1. Java 靠 DefaultMQPullConsumer 的后台 rebalance 回调 MessageQueueListener → putTask
//      来展开/收缩任务表；本端口改由自己的扫描循环周期调用 FetchMessageQueuesInBalance
//      （rebalanceByTopic 同一条公式的当场计算版）驱动 PutTask。周期取 Java
//      scheduledRebalanceEveryTimeSeconds 的 20s 默认（RebalanceScanIntervalMillis 可调），
//      Start 后立刻先扫一遍 ≡ Java 的 rebalanceImmediately（首个周期 ≤1s 提前到 0）。
//   2. Java 的 callbackTable 以 withNamespace 后的 topic 为键（任务表里的队列也带前缀，
//      :99）；本端口 FetchMessageQueuesInBalance 返回**剥掉命名空间**的裸名队列，所以
//      callbackTable 统一用裸名键，整条链对用户都是裸名（Pull 时由门面再加前缀）。
//   3. Java 的 ScheduledThreadPoolExecutor 一个对象兼任「延迟调度」与「执行池」；这里拆成
//      「每队列一个一次性延迟 + ConsumeExecutor 执行池」，PullThreadNums 语义不变（同时在
//      跑的队列任务数不超过它）。Shutdown 之后不再重排 —— Java 里调度器关停后再 schedule
//      会抛 RejectedExecutionException，效果同样是任务终结，只是这里安静。
//
// queue 维度串行（同一队列同时只有一笔在途 pull）与 Java 同构且是结构性的：一条队列一个
// 任务对象，下一轮**只在上一轮回调返回之后**才被排上，不存在同一队列两个在途轮次。
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

/// <summary>org.apache.rocketmq.client.consumer.PullTaskCallback。</summary>
public interface IPullTaskCallback
{
    /// <summary>对应 Java doPullTask(mq, context)：在本轮里发起拉取（通常经
    /// <see cref="PullTaskContext.PullConsumer"/> 同步 Pull），并按结果设置
    /// <see cref="PullTaskContext.PullNextDelayTimeMillis"/> 决定下一轮的快慢。</summary>
    void DoPullTask(MessageQueue mq, PullTaskContext context);
}

/// <summary>org.apache.rocketmq.client.consumer.PullTaskContext（默认延迟 200ms 也与 Java 一致）。</summary>
public sealed class PullTaskContext
{
    /// <summary>Java 默认 pullNextDelayTimeMillis = 200（PullTaskContext:21）。</summary>
    public const int DefaultNextDelayMillis = 200;

    /// <summary>回调抛异常后的强制重排延迟（Java PullTaskImpl.run:183 的 setPullNextDelayTimeMillis(1000)）。</summary>
    public const int FailureNextDelayMillis = 1000;

    /// <summary>发起本轮拉取用的门面（Java context.getPullConsumer()）。</summary>
    public DefaultMQPullConsumer PullConsumer { get; }

    /// <summary>下一轮的延迟毫秒（Java get/setPullNextDelayTimeMillis）：回调拿到 PullResult
    /// 后按状态/流量自己调它 —— FOUND 想立刻续拉就置 0，空了想省资源就调大。</summary>
    public int PullNextDelayTimeMillis { get; set; } = DefaultNextDelayMillis;

    internal PullTaskContext(DefaultMQPullConsumer pullConsumer) => PullConsumer = pullConsumer;
}

/// <summary>
/// 拉模式消费者的托管调度服务（Java MQPullConsumerScheduleService）：注册「topic → 回调」，
/// Start 之后本实例每把应得队列各跑一条串行拉取任务，队列增减由周期扫描自动展开/收缩，
/// 节奏由回调经 <see cref="PullTaskContext.PullNextDelayTimeMillis"/> 说了算。
/// </summary>
public sealed class PullConsumerScheduleService : IDisposable
{
    private readonly object _lock = new();

    // Java taskTable（MessageQueue → PullTaskImpl）与 callbackTable（topic → PullTaskCallback）
    private readonly ConcurrentDictionary<MessageQueue, PullTask> _taskTable = new();
    private readonly ConcurrentDictionary<string, IPullTaskCallback> _callbackTable =
        new(StringComparer.Ordinal);

    private DefaultMQPullConsumer _pullConsumer;
    // Java pullThreadNums 默认 20（MQPullConsumerScheduleService:46）
    private int _pullThreadNums = 20;
    // 差异 1：本端口的 rebalance 驱动周期，取 Java scheduledRebalanceEveryTimeSeconds 的 20s
    private int _rebalanceScanIntervalMillis = 20000;

    private ConsumeExecutor? _executor;
    private Thread? _scanThread;
    private volatile bool _scanStop;
    private volatile bool _stopped = true;
    private bool _started;

    /// <summary>对应 Java MQPullConsumerScheduleService(consumerGroup)：内部现建一个
    /// DefaultMQPullConsumer 并把消费模型置 CLUSTERING（:51-58）。</summary>
    public PullConsumerScheduleService(string consumerGroup = MixAll.DefaultConsumerGroup)
    {
        _pullConsumer = new DefaultMQPullConsumer(consumerGroup);
        _pullConsumer.SetMessageModel(MessageModel.Clustering);
    }

    /// <summary>对应 Java 的 (consumerGroup, rpcHook) 构造：钩子在门面 Start() 前挂上即可。</summary>
    public PullConsumerScheduleService(string consumerGroup, IRpcHook rpcHook)
        : this(consumerGroup)
    {
        _pullConsumer.SetRpcHook(rpcHook);
    }

    // ---------------- 配置 ----------------

    /// <summary>底层拉模式门面（Java getDefaultMQPullConsumer()）：名字服务器地址、命名空间、
    /// 心跳、SOCKS 等配置都从它走；必须 Start() 之前改。</summary>
    public DefaultMQPullConsumer PullConsumer
    {
        get
        {
            lock (_lock)
            {
                return _pullConsumer;
            }
        }
    }

    /// <summary>整体更换门面（Java setDefaultMQPullConsumer），仅 Start() 之前允许。</summary>
    public void SetPullConsumer(DefaultMQPullConsumer consumer)
    {
        lock (_lock)
        {
            if (_started)
            {
                throw new MQClientException("pull consumer can not be replaced after start");
            }

            _pullConsumer = consumer ?? throw new MQClientException("pull consumer is null");
        }
    }

    /// <summary>执行池线程数（Java get/setPullThreadNums，默认 20）。Start() 之后改不生效
    ///（池已按旧值建好 —— Java 同名 setter 也是这个口径）。</summary>
    public int PullThreadNums
    {
        get => _pullThreadNums;
        set => _pullThreadNums = Math.Max(1, value);
    }

    /// <summary>扫描周期毫秒（差异 1 的旋钮，默认 20000，下限钳 100）。改在下一圈生效。</summary>
    public int RebalanceScanIntervalMillis
    {
        get => _rebalanceScanIntervalMillis;
        set => _rebalanceScanIntervalMillis = value;
    }

    /// <summary>回调表只读视图（Java getCallbackTable()），键为本端口口径的裸 topic（差异 2）。</summary>
    public IReadOnlyDictionary<string, IPullTaskCallback> CallbackTable => _callbackTable;

    /// <summary>在表任务数。Java 没有公开 accessor，本端口留给测试/排障观测 ——
    /// 任务展开、收缩是否到位，看这个数字最省事。</summary>
    public int TaskCount => _taskTable.Count;

    public bool IsStarted => _started;

    public string ConsumerGroup => _pullConsumer.ConsumerGroup;

    // ---------------- 注册 ----------------

    /// <summary>
    /// 对应 Java registerPullTaskCallback(topic, callback)（:98-101）：入回调表 + 把 topic
    /// 登记进门面的 RegisterTopics（registerMessageQueueListener(topic, null) —— 心跳订阅集
    /// 与路由刷新的来源，Java 就是传 null listener 只登记不监听）。
    /// ⚠ 键是裸 topic（差异 2）。Start() 之后注册同样会生效：扫描循环实时读回调表，
    /// 下一轮扫描（≤ RebalanceScanIntervalMillis）就为新 topic 展开任务。
    /// </summary>
    public void RegisterPullTaskCallback(string topic, IPullTaskCallback callback)
    {
        ArgumentNullException.ThrowIfNull(callback);
        if (string.IsNullOrEmpty(topic))
        {
            // Java :99 对空串也会 put，但 :100 的 registerMessageQueueListener 对空 topic
            // 直接 return —— 登记了路由也不会刷，任务永远起不来，这里同样拒绝入表。
            return;
        }

        _callbackTable[topic] = callback;
        _pullConsumer.RegisterMessageQueueListener(topic, null);
    }

    /// <summary>delegate 形状的同款注册（本端口的函数式便车，语义与接口版逐字一致）。</summary>
    public void RegisterPullTaskCallback(string topic, Action<MessageQueue, PullTaskContext> callback) =>
        RegisterPullTaskCallback(topic, new DelegatePullTaskCallback(callback));

    // ---------------- 生命周期 ----------------

    /// <summary>对应 Java start()（:83-96）：先建执行池，再 Start 门面，然后立刻做第一轮
    /// 任务展开并拉起扫描循环。</summary>
    public void Start()
    {
        lock (_lock)
        {
            if (_started)
            {
                return;
            }

            DefaultMQPullConsumer consumer = _pullConsumer;
            _stopped = false;
            _scanStop = false;
            // Java :85-87 先建池再 consumer.start()：池先活着，等 rebalance 投任务。
            // 线程名前缀对齐 Java ThreadFactoryImpl("PullMsgThread-" + group) 的形状。
            _executor = new ConsumeExecutor(_pullThreadNums, _pullThreadNums,
                threadNamePrefix: "rmq-pull-sched-" + consumer.ConsumerGroup);
            try
            {
                // Java :90-92 是 setMessageQueueListener(内部实现) + defaultMQPullConsumer.start()；
                // 本端口的「listener → putTask」一环由扫描循环代劳（差异 1）。
                consumer.Start();
            }
            catch
            {
                _executor.Shutdown();
                _executor = null;
                _stopped = true;
                throw;
            }

            _started = true;
        }

        // Start 后立刻铺开第一轮任务（≡ Java rebalanceImmediately），随后交给周期扫描循环。
        ScanOnce();
        _scanThread = new Thread(ScanLoop)
        {
            IsBackground = true,
            Name = "rmq-pull-sched-scan-" + ConsumerGroup,
        };
        _scanThread.Start();
        ClientLog.Info("PullConsumerScheduleService start OK, " + ConsumerGroup
                       + ", callbacks=" + _callbackTable.Count);
    }

    /// <summary>对应 Java shutdown()（:103-111）：调度池 shutdown → 门面 shutdown。
    /// 「排空」口径与 Java 相同：不硬切正在跑的那一轮回调，它在 <see cref="RunTask"/> 的
    /// 取消/停机检查处自然终结、不再重排；已在延迟里的轮次被撤掉。</summary>
    public void Shutdown()
    {
        lock (_lock)
        {
            if (!_started)
            {
                return;
            }

            _started = false;
        }

        _stopped = true;
        _scanStop = true;
        if (_scanThread is { IsAlive: true })
        {
            _scanThread.Join(2000);
        }

        foreach (PullTask task in _taskTable.Values.ToArray())
        {
            task.Cancelled = true;
            task.Disarm();
        }

        _taskTable.Clear();

        // Java executor.shutdown() 不等待已排任务跑完；这里同样只收池不 Join（差异 3 的
        // 「Shutdown 后不再重排」由 _stopped 保证），最后关停门面。
        _executor?.Shutdown();
        _executor = null;
        _pullConsumer.Shutdown();
        ClientLog.Info("PullConsumerScheduleService shutdown OK, " + ConsumerGroup);
    }

    public void Dispose() => Shutdown();

    // ---------------- 任务展开 ----------------

    /// <summary>扫描循环（Start 尾挂起的后台线程）：每 <see cref="RebalanceScanIntervalMillis"/>
    /// 对账一次。分片睡（每 100ms 复查停机位，口径同门面 HeartbeatLoop），Shutdown 最多
    /// 100ms 就能退出当前睡眠段；异常一律在 ScanOnce 内部被逐 topic 接住，这里再兜一层
    /// 防循环线程被打穿（打穿 = 队列增减从此不再响应）。</summary>
    private void ScanLoop()
    {
        while (!_scanStop && !_stopped)
        {
            int interval = Math.Max(100, _rebalanceScanIntervalMillis);
            for (int slept = 0; slept < interval && !_scanStop; slept += 100)
            {
                Thread.Sleep(Math.Min(100, interval - slept));
            }

            if (_scanStop || _stopped)
            {
                break;
            }

            try
            {
                ScanOnce();
            }
            catch (Exception e)
            {
                ClientLog.Warn("pull schedule: scan loop error: " + e.Message);
            }
        }
    }

    /// <summary>一轮「平衡视图 → 任务表」的对账：Java 里这条链是
    /// rebalanceByTopic → messageQueueChanged → putTask；本端口直接对每个注册了回调的
    /// topic 取 FetchMessageQueuesInBalance（BROADCASTING 全量 / CLUSTERING 按分配策略取
    /// 本实例那一份，公式同源），再交给 <see cref="PutTask"/> 展开/收缩。</summary>
    public void ScanOnce()
    {
        foreach (string topic in _callbackTable.Keys.ToArray())
        {
            try
            {
                PutTask(topic, _pullConsumer.FetchMessageQueuesInBalance(topic));
            }
            catch (Exception e)
            {
                // 单 topic 算不动不能带走整轮扫描（Java 的 rebalance 异常同样只打日志；
                // 门面未启动 / 路由暂缺都在这一档被吃掉，等下一轮）
                ClientLog.Warn("pull schedule: rebalance scan for " + topic + " failed: "
                               + e.Message);
            }
        }
    }

    /// <summary>
    /// 对应 Java putTask(topic, mqNewSet)（:61-81）：topic 下**不在**新集合里的任务置取消并
    /// 摘除（在途的那轮跑完自己的回调后自然退出）；新集合里还没建过任务的建任务并立即排
    /// 一轮（delay 0）。
    /// </summary>
    public void PutTask(string topic, IReadOnlyCollection<MessageQueue> mqNewSet)
    {
        var mqSet = new HashSet<MessageQueue>(mqNewSet);
        foreach (KeyValuePair<MessageQueue, PullTask> pair in _taskTable.ToArray())
        {
            if (pair.Key.Topic != topic)
            {
                continue;
            }

            if (!mqSet.Contains(pair.Key))
            {
                // Java :66-69：setCancelled(true) + it.remove()
                pair.Value.Cancelled = true;
                pair.Value.Disarm();
                _taskTable.TryRemove(pair.Key, out _);
            }
        }

        foreach (MessageQueue mq in mqSet)
        {
            var task = new PullTask(mq);
            if (_taskTable.TryAdd(mq, task))
            {
                ScheduleTask(task, 0);
            }
        }
    }

    /// <summary>把任务在 delayMillis 后排上（Java scheduledThreadPoolExecutor.schedule）。
    /// 延迟只负责「什么时候醒」，醒来把活交给执行池 —— 池满时排队，等价 Java 池线程全忙
    /// 时新轮次只能等空闲。</summary>
    private void ScheduleTask(PullTask task, int delayMillis)
    {
        if (task.Cancelled || _stopped)
        {
            return;
        }

        ConsumeExecutor? executor = _executor;
        if (executor is null)
        {
            return;
        }

        task.Arm(() =>
        {
            // 池已关（Shutdown 竞态）只丢这一轮，不让异常炸掉延迟载体
            try
            {
                executor.Submit(() => RunTask(task));
            }
            catch (Exception e)
            {
                ClientLog.Debug("pull schedule: task submit rejected: " + e.Message);
            }
        }, Math.Max(0, delayMillis));
    }

    /// <summary>
    /// 对应 Java PullTaskImpl.run()（:171-199）逐段：取消检查 → 查回调（缺失只 warn 且
    /// **不重排**，:193-195）→ 建 context → 跑回调（抛 Throwable → 下一轮延迟罚为 1000ms，
    /// :182-185）→ 未被取消则按 context 的延迟重排下一轮。回调里发起 Pull 的串行性由
    /// 「重排发生在回调返回之后」这一条结构保证（文件头）。
    /// </summary>
    private void RunTask(PullTask task)
    {
        if (task.Cancelled)
        {
            ClientLog.Warn("The Pull Task is cancelled, " + task.Mq);
            return;
        }

        if (!_callbackTable.TryGetValue(task.Mq.Topic, out IPullTaskCallback? callback))
        {
            ClientLog.Warn("Pull Task Callback not exist , " + task.Mq.Topic);
            return;
        }

        var context = new PullTaskContext(_pullConsumer);
        try
        {
            callback.DoPullTask(task.Mq, context);
        }
        catch (Exception e)
        {
            context.PullNextDelayTimeMillis = PullTaskContext.FailureNextDelayMillis;
            ClientLog.Error("doPullTask Exception, mq=" + task.Mq + ": " + e.Message);
        }

        if (task.Cancelled)
        {
            ClientLog.Warn("The Pull Task is cancelled after doPullTask, " + task.Mq);
            return;
        }

        ScheduleTask(task, context.PullNextDelayTimeMillis);
    }

    /// <summary>delegate → IPullTaskCallback 的薄包装（给函数式注册重载用）。</summary>
    private sealed class DelegatePullTaskCallback(Action<MessageQueue, PullTaskContext> inner)
        : IPullTaskCallback
    {
        public void DoPullTask(MessageQueue mq, PullTaskContext context) => inner(mq, context);
    }

    /// <summary>
    /// Java PullTaskImpl 的替身：一条队列一个；Cancelled 语义逐字对齐（置位后在途轮次跑完
    /// 回调即退出、不再重排）。延迟载体是「一次性 Task.Delay + 可取消句柄」：重排 = 撤旧换新，
    /// 同一任务同时至多一个在等点位的轮次。
    /// </summary>
    private sealed class PullTask
    {
        private readonly object _gate = new();
        private CancellationTokenSource? _pending;

        public PullTask(MessageQueue mq) => Mq = mq;

        internal MessageQueue Mq { get; }

        internal volatile bool Cancelled;

        /// <summary>delayMillis 后跑 fire（fire 本身通常只把活投进执行池）。已被取消的任务
        /// 不再排。</summary>
        internal void Arm(Action fire, int delayMillis)
        {
            CancellationTokenSource? previous;
            var cts = new CancellationTokenSource();
            lock (_gate)
            {
                if (Cancelled)
                {
                    cts.Dispose();
                    return;
                }

                previous = _pending;
                _pending = cts;
            }

            // 撤旧一轮的等点位（正常路径旧的已触发，这里是防御性收口）
            CancelQuietly(previous);

            _ = Task.Delay(Math.Max(0, delayMillis), cts.Token).ContinueWith(armed =>
            {
                // 被取消/已释放的一轮直接作废；到点成功才投递
                if (!armed.IsCompletedSuccessfully)
                {
                    return;
                }

                try
                {
                    fire();
                }
                catch (Exception e)
                {
                    ClientLog.Debug("pull schedule: delayed arm failed: " + e.Message);
                }
            }, TaskScheduler.Default);
        }

        /// <summary>撤掉在等点位的一轮（取消 + 释放句柄）。已触发或在跑的轮次不受影响，
        /// 由 RunTask 的 Cancelled 检查收口。</summary>
        internal void Disarm()
        {
            CancellationTokenSource? pending;
            lock (_gate)
            {
                pending = _pending;
                _pending = null;
            }

            CancelQuietly(pending);
        }

        private static void CancelQuietly(CancellationTokenSource? cts)
        {
            if (cts is null)
            {
                return;
            }

            try
            {
                cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // 已被别处释放：等价于已撤防，无需处理
            }

            cts.Dispose();
        }
    }
}
