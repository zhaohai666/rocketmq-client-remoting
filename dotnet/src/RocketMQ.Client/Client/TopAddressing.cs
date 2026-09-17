// 动态 name server 取址（对应 org.apache.rocketmq.common.namesrv.TopAddressing /
// DefaultTopAddressing 与 MixAll.getWSAddr；Python 参考实现 client/top_addressing.py）。
//
// Java 语义（5.5.1 逐条核对，与 Python/C++ 侧逐条同源）：
//   * WS 地址：``http://<domain>:8080/rocketmq/<subgroup>``；domain 自带端口（含 ':'）
//     时不追加 :8080；
//   * unitName 非空白 → URL 追加 ``-<unitName>?nofix=1``；para 非空 → ``?k=v&...``；
//   * HTTP GET（超时 3000ms）code==200 → 响应体 clearNewLine（trim 后截断到第一个
//     \r 或 \n）作为 NS 地址串；失败返回 null；
//   * FetchAndApply（Java MQClientAPIImpl.fetchNameServerAddr）：地址**变化才应用**；
//   * MQClientInstance：只在未配置静态地址时，Start() fetch 一次 + 10s/2min 周期刷新。
//
// 有意差异（与 Python/C++ 参考实现一致）：Java 默认 domain 是 jmenv.tbsite.net（依赖
// /etc/hosts 绑定），我们若照抄，未配置的用户会被必然失败的域名拖 3s。因此 domain
// 必须显式给出（构造参数或环境变量 ROCKETMQ_NAMESRV_DOMAIN），未配置 = 关闭动态取址。
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;

using RocketMQ.Common;

namespace RocketMQ.Client;

public sealed class DefaultTopAddressing
{
    // Java MixAll.DEFAULT_NAMESRV_ADDR_LOOKUP / 默认 subgroup
    public const string DefaultNamesrvAddrLookup = "jmenv.tbsite.net";
    public const string DefaultDomainSubgroup = "nsaddr";

    private readonly object _gate = new();
    private readonly HttpClient _http;
    private readonly bool _ownsHttp;
    private readonly int _timeoutMillis;
    private readonly string _unitName;
    private readonly IReadOnlyDictionary<string, string>? _para;

    public string WsAddr { get; }
    // Java 的 nameSrvAddr 缓存：上次成功应用的地址串
    private string _nsAddr = string.Empty;

    public DefaultTopAddressing(string? wsAddr = null, string? unitName = null,
                                IReadOnlyDictionary<string, string>? para = null,
                                int timeoutMillis = 3000,
                                string? domain = null, string? subgroup = null,
                                HttpClient? http = null)
    {
        _timeoutMillis = timeoutMillis;
        _unitName = unitName ?? string.Empty;
        _para = para;
        // ⚠ UseProxy=false 必须显式设：HttpClient 默认读系统代理，配了代理的机器上
        // 连 loopback 都会被拖到代理超时（实测每请求 ~10s）。
        _http = http ?? new HttpClient(new HttpClientHandler
        {
            UseProxy = false,
            AllowAutoRedirect = false,
        })
        {
            Timeout = TimeSpan.FromMilliseconds(timeoutMillis),
        };
        _ownsHttp = http == null;
        string? dom = domain ?? Environment.GetEnvironmentVariable("RocketMqNamesrvDomain")
            ?? Environment.GetEnvironmentVariable("ROCKETMQ_NAMESRV_DOMAIN");
        WsAddr = !string.IsNullOrEmpty(wsAddr)
            ? wsAddr!
            : (string.IsNullOrEmpty(dom) ? string.Empty : GetWsAddr(dom, subgroup));
    }

    /// <summary>对应 MixAll.getWSAddr：domain 自带端口（含 ':'）时不追加默认 :8080。</summary>
    public static string GetWsAddr(string domain, string? subgroup = null)
    {
        string grp = subgroup ?? DefaultDomainSubgroup;
        return domain.Contains(':')
            ? "http://" + domain + "/rocketmq/" + grp
            : "http://" + domain + ":8080/rocketmq/" + grp;
    }

    /// <summary>动态取址是否可用（domain 已显式配置）。</summary>
    public static bool IsConfigured()
    {
        string? env = Environment.GetEnvironmentVariable("ROCKETMQ_NAMESRV_DOMAIN")
            ?? Environment.GetEnvironmentVariable("RocketMqNamesrvDomain");
        return !string.IsNullOrEmpty(env);
    }

    /// <summary>对应 fetchNSAddr 里的 URL 拼装（unitName / para 规则逐条照抄）。</summary>
    public string BuildUrl()
    {
        string url = WsAddr;
        if (_para is { Count: > 0 })
        {
            if (!string.IsNullOrWhiteSpace(_unitName))
            {
                url += "-" + _unitName + "?nofix=1&";
            }
            else
            {
                url += "?";
            }
            bool first = true;
            foreach (var kv in _para)
            {
                if (!first) url += "&";
                url += kv.Key + "=" + kv.Value;
                first = false;
            }
        }
        else if (!string.IsNullOrWhiteSpace(_unitName))
        {
            url += "-" + _unitName + "?nofix=1";
        }
        return url;
    }

    /// <summary>Java DefaultTopAddressing.clearNewLine：trim 后截断到第一个 \r 或 \n。</summary>
    public static string ClearNewLine(string content)
    {
        string s = content.Trim();
        int idx = s.IndexOf('\r');
        if (idx >= 0) return s[..idx];
        idx = s.IndexOf('\n');
        if (idx >= 0) return s[..idx];
        return s;
    }

    /// <summary>取一次 NS 地址串；不可用 / 非 200 / 网络失败返回 null（Java 返回 null）。</summary>
    public string? FetchNsAddr(bool verbose = true)
    {
        if (string.IsNullOrEmpty(WsAddr)) return null;
        string url = BuildUrl();
        try
        {
            using HttpResponseMessage resp = _http.GetAsync(url).ConfigureAwait(false).GetAwaiter().GetResult();
            if ((int)resp.StatusCode == 200)
            {
                string body = resp.Content.ReadAsStringAsync()
                    .ConfigureAwait(false).GetAwaiter().GetResult();
                return ClearNewLine(body);
            }
            if (verbose)
            {
                ClientLog.Warn("fetch nameserver address failed. statusCode="
                    + (int)resp.StatusCode + " url=" + url);
            }
        }
        catch (Exception e)
        {
            if (verbose)
            {
                ClientLog.Debug("fetch name server address exception url=" + url + ": " + e.Message);
            }
        }
        return null;
    }

    /// <summary>Java MQClientAPIImpl.fetchNameServerAddr：地址变化才返回新串并记录。</summary>
    public string? FetchAndApply()
    {
        string? addrs = FetchNsAddr();
        if (!string.IsNullOrWhiteSpace(addrs))
        {
            lock (_gate)
            {
                if (addrs != _nsAddr)
                {
                    ClientLog.Info("name server address changed, old=" + _nsAddr + ", new=" + addrs);
                    _nsAddr = addrs;
                    return _nsAddr;
                }
            }
        }
        return null;
    }
}
