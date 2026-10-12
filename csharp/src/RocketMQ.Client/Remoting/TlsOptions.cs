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
