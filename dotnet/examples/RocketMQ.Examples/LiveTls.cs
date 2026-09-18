// TLS + traceparent 真机联调（对应 cpp live_tls.cpp / run_tls_live.sh）。
//
// 验证点（单进程闭环）：
//   1. TLS 生产者/消费者（test-mode，信任 broker 自签证书）真连真发真收；
//   2. 生产侧注入的 traceparent 属性随消息走完整链路，消费侧可提取且合法。
//
// 用法：dotnet run ... -- tls <namesrv> <topic> <group>
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

internal static class LiveTls
{
    public static int Run(string[] args)
    {
        if (args.Length < 3)
        {
            Console.WriteLine("usage: tls <namesrv> <topic> <group>");
            return 2;
        }

        string namesrv = args[0];
        string topic = args[1];
        string group = args[2];

        // 1) 明文生产者预建 topic（约定：先建 topic 再起消费者）
        var prep = new DefaultMQProducer("GID_TLS_PREP");
        prep.NamesrvAddr = namesrv;
        prep.Start();
        try
        {
            prep.CreateTopic("init", topic, 4);
        }
        catch (Exception e)
        {
            Console.WriteLine("  预建 topic 失败（改用自动创建）: " + e.Message);
        }

        Thread.Sleep(3000);

        // 2) TLS 消费者先起
        var listener = new CountListener();
        var consumer = new DefaultMQPushConsumer(group);
        consumer.SetNamesrvAddr(namesrv);
        consumer.TlsEnable = true;
        consumer.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
        consumer.SetMessageListener(listener);
        consumer.Subscribe(topic, "*");
        consumer.Start();

        // 3) TLS 生产者发送 3 条（trace 注入开启）
        var producer = new DefaultMQProducer("GID_TLS_PROD");
        producer.NamesrvAddr = namesrv;
        producer.TlsEnable = true;
        producer.EnableTraceContext = true;
        producer.Start();

        int sent = 0;
        for (int i = 0; i < 3; i++)
        {
            var result = producer.Send(new Message(topic, System.Text.Encoding.UTF8.GetBytes("tls-live-" + i)));
            if (result.SendStatus == SendStatus.SendOk)
            {
                sent++;
            }
        }

        // 4) 等收齐
        for (int i = 0; i < 80 && listener.Count() < sent; i++)
        {
            Thread.Sleep(250);
        }

        producer.Shutdown();

        long got = listener.Count();
        string tp = listener.LastTraceparent();
        bool traceOk = !string.IsNullOrEmpty(tp) && TraceParentContext.IsValid(tp);

        consumer.Shutdown();
        prep.Shutdown();

        bool ok = sent == 3 && got == 3 && traceOk;
        Console.WriteLine("sent=" + sent + " consumed=" + got
            + " traceparent=" + (traceOk ? tp : "<missing/invalid>"));
        Console.WriteLine("  props=" + listener.LastProps());
        Console.WriteLine(ok ? "  [PASS]" : "  [FAIL]");
        return ok ? 0 : 1;
    }

    private sealed class CountListener : IMessageListenerConcurrently
    {
        private readonly object _lock = new();
        private long _count;
        private string _lastTraceparent = string.Empty;
        private string _lastProps = string.Empty;

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext ctx)
        {
            foreach (MessageExt m in msgs)
            {
                string tp = m.GetProperty(TraceParentContext.TraceContextProperty);
                lock (_lock)
                {
                    _count++;
                    _lastTraceparent = tp;
                    if (_lastProps.Length == 0 && m.Properties != null)
                    {
                        foreach (var kv in m.Properties)
                        {
                            _lastProps += kv.Key + "=" + kv.Value + ";";
                        }
                    }
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public long Count()
        {
            lock (_lock) return _count;
        }

        public string LastTraceparent()
        {
            lock (_lock) return _lastTraceparent;
        }

        public string LastProps()
        {
            lock (_lock) return _lastProps;
        }
    }
}
