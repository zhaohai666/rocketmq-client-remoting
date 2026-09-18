// W3C traceparent 透传单测：生成/校验/子 span/注入不覆盖/属性提取。
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class TraceParentContextTests
{
    [Fact]
    public void Generate_IsCanonical()
    {
        string tp = TraceParentContext.Generate();
        Assert.Equal(55, tp.Length);
        Assert.Equal("00-", tp.Substring(0, 3));
        Assert.Equal("-", tp.Substring(35, 1));
        Assert.Equal("-", tp.Substring(52, 1));
        Assert.Equal("01", tp.Substring(53, 2));
        Assert.True(TraceParentContext.IsValid(tp));
    }

    [Fact]
    public void IsValid_CanonicalAndEdgeCases()
    {
        Assert.True(TraceParentContext.IsValid(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"));
        Assert.True(TraceParentContext.IsValid(
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-00"));   // 大写宽松
        Assert.False(TraceParentContext.IsValid(null));
        Assert.False(TraceParentContext.IsValid(""));
        Assert.False(TraceParentContext.IsValid(
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01"));  // 全 0 trace-id
        Assert.False(TraceParentContext.IsValid(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"));  // 全 0 parent-id
        Assert.False(TraceParentContext.IsValid("00-4bf92f-00f067aa0ba902b7-01"));
        Assert.False(TraceParentContext.IsValid(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-zz"));
        Assert.False(TraceParentContext.IsValid(
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"));  // 版本 ff
    }

    [Fact]
    public void Child_SameTraceId_DifferentParentId()
    {
        string parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        string? child = TraceParentContext.Child(parent);
        Assert.NotNull(child);
        Assert.True(TraceParentContext.IsValid(child));
        Assert.Equal(parent.Substring(3, 32), child!.Substring(3, 32));
        Assert.NotEqual(parent.Substring(36, 16), child.Substring(36, 16));
        Assert.Null(TraceParentContext.Child("garbage"));
    }

    [Fact]
    public void Inject_Extract_NoOverwrite()
    {
        var msg = new Message();
        msg.PutProperty("k", "v");

        string injected = TraceParentContext.Inject(msg);
        Assert.True(TraceParentContext.IsValid(injected));
        Assert.Equal(injected, TraceParentContext.Extract(new MessageExt(msg)));

        // 已有值不覆盖
        var msg2 = new Message();
        msg2.PutProperty(TraceParentContext.TraceContextProperty,
            "00-11111111111111111111111111111111-2222222222222222-01");
        Assert.Equal("00-11111111111111111111111111111111-2222222222222222-01",
            TraceParentContext.Inject(msg2));

        // 未注入返回 null
        var msg3 = new Message();
        Assert.Null(TraceParentContext.Extract(new MessageExt(msg3)));
    }
}
