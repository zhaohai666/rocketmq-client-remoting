namespace RocketMQ.Remoting;

public sealed class TlsOptions
{
    /// <summary>CA certificate PEM path (the trust anchor for strict validation).</summary>
    public string? CaCert { get; set; }

    /// <summary>mTLS client certificate PEM path.</summary>
    public string? ClientCert { get; set; }

    /// <summary>mTLS client private key PEM path (PKCS#8; can be omitted when the cert embeds the key).</summary>
    public string? ClientKey { get; set; }

    /// <summary>Hostname/SNI override (defaults to the connection host for SAN validation).</summary>
    public string? ServerName { get; set; }
}
