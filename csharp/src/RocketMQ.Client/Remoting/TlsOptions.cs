// TLS 细项（对齐 PHP RemotingClient $tlsOptions / Java TlsSystemConfig certPath 族）。
//
//   - CaCert 给出 → **严格校验**（Java tls.test.mode.enable=false 口径）：broker 证书
//     必须链到该 CA 且主机名/SAN 匹配（默认用连接 host 校验，ServerName 可覆盖）；
//     不给 → test-mode：信任自签（历史行为）。
//   - ClientCert/ClientKey → mTLS 客户端证书与私钥（PEM；cert 内含私钥时 key 可省）。
namespace RocketMQ.Remoting;

public sealed class TlsOptions
{
    /// <summary>CA 证书 PEM 路径（严格校验的信任锚）。</summary>
    public string? CaCert { get; set; }

    /// <summary>mTLS 客户端证书 PEM 路径。</summary>
    public string? ClientCert { get; set; }

    /// <summary>mTLS 客户端私钥 PEM 路径（PKCS#8；cert 内含私钥时可省）。</summary>
    public string? ClientKey { get; set; }

    /// <summary>主机名/SNI 覆盖（缺省用连接 host 做 SAN 校验）。</summary>
    public string? ServerName { get; set; }
}
