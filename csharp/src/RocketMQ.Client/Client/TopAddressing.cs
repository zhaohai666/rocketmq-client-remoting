using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;

using RocketMQ.Common;

namespace RocketMQ.Client;

public sealed class DefaultTopAddressing
{
    public const string DefaultNamesrvAddrLookup = "jmenv.tbsite.net";
    public const string DefaultDomainSubgroup = "nsaddr";

    private readonly object _gate = new();
    private readonly HttpClient _http;
    private readonly bool _ownsHttp;
    private readonly int _timeoutMillis;
    private readonly string _unitName;
    private readonly IReadOnlyDictionary<string, string>? _para;

    public string WsAddr { get; }
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
        // WARNING: UseProxy=false must be set explicitly: HttpClient reads system proxies by
        // default, and on machines with a proxy configured even loopback connections get dragged
        // into the proxy timeout (measured ~10s per request).
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

    /// <summary>Corresponds to MixAll.getWSAddr: when the domain already carries a port (contains ':'), the default :8080 is not appended.</summary>
    public static string GetWsAddr(string domain, string? subgroup = null)
    {
        string grp = subgroup ?? DefaultDomainSubgroup;
        return domain.Contains(':')
            ? "http://" + domain + "/rocketmq/" + grp
            : "http://" + domain + ":8080/rocketmq/" + grp;
    }

    /// <summary>Whether dynamic address fetching is available (domain explicitly configured).</summary>
    public static bool IsConfigured()
    {
        string? env = Environment.GetEnvironmentVariable("ROCKETMQ_NAMESRV_DOMAIN")
            ?? Environment.GetEnvironmentVariable("RocketMqNamesrvDomain");
        return !string.IsNullOrEmpty(env);
    }

    /// <summary>Corresponds to the URL assembly in fetchNSAddr (unitName / para rules copied verbatim).</summary>
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

    public static string ClearNewLine(string content)
    {
        string s = content.Trim();
        int idx = s.IndexOf('\r');
        if (idx >= 0) return s[..idx];
        idx = s.IndexOf('\n');
        if (idx >= 0) return s[..idx];
        return s;
    }

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
