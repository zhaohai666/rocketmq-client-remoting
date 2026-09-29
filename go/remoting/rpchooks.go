// The two infrastructure RPC hooks the Java client installs around the user's
// hook (Java MQClientAPIImpl's constructor, lines 329-335):
//
//	remotingClient.registerRPCHook(new NamespaceRpcHook(clientConfig));
//	// Inject stream rpc hook first to make reserve field signature
//	if (clientConfig.isEnableStreamRequestType()) {
//	    remotingClient.registerRPCHook(new StreamTypeRPCHook());
//	}
//	remotingClient.registerRPCHook(rpcHook);            // the USER's (e.g. ACL)
//	remotingClient.registerRPCHook(new DynamicalExtFieldRPCHook());
//
// The ORDER is load-bearing, and it is why these live here rather than in the
// client package:
//
//   - Hooks run in registration order, and the ACL signature covers every
//     extField present when the ACL hook runs. So the namespace and ReqT fields
//     MUST be written BEFORE the user's hook (otherwise the broker sees fields
//     that carry no signature and rejects the request: "reserve field
//     signature"), while the zone fields are written AFTER it and are
//     deliberately NOT signed.
//   - Getting this backwards still produces a valid-looking signature; it just
//     never verifies. See TestAclStreamHookOrderDecidesSignature.
package remoting

import (
	"os"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// NamespaceRpcHook mirrors org.apache.rocketmq.client.rpchook.NamespaceRpcHook:
// when a namespace-v2 is configured, every request carries
//
//	nsd = "true", ns = <namespaceV2>
//
// This is the SERVER-side namespace mechanism (the broker resolves the real
// topic from the header). It is a different mechanism from the classic
// `namespace` field, which is applied by mangling topic names client-side.
type NamespaceRpcHook struct {
	namespaceV2 string
}

// NewNamespaceRpcHook builds the hook. An empty namespace makes it a no-op,
// exactly like Java's `StringUtils.isNotEmpty` guard.
func NewNamespaceRpcHook(namespaceV2 string) *NamespaceRpcHook {
	return &NamespaceRpcHook{namespaceV2: namespaceV2}
}

// DoBeforeRequest writes the namespace fields when configured.
func (h *NamespaceRpcHook) DoBeforeRequest(_ string, request *RemotingCommand) {
	if h.namespaceV2 == "" {
		return
	}
	request.AddExtField(common.NamespacedField, "true")
	request.AddExtField(common.NamespaceV2Field, h.namespaceV2)
}

// DoAfterResponse is a no-op, exactly like Java.
func (h *NamespaceRpcHook) DoAfterResponse(_ string, _ *RemotingCommand, _ *RemotingCommand) {}

// DynamicalExtFieldRPCHook mirrors
// org.apache.rocketmq.remoting.rpchook.DynamicalExtFieldRPCHook: stamp the
// zone name / zone mode onto every request when the process is configured with
// one.
//
// Java looks up `System.getProperty(name, System.getenv(env))` — a JVM system
// property wins over the environment variable. Go has no system properties, so
// the environment variable is the only source; the property name is kept as
// the env-var-free constant for documentation.
type DynamicalExtFieldRPCHook struct{}

// DoBeforeRequest writes __ZONE_NAME / __ZONE_MODE when set. Registered LAST,
// so these fields are outside the ACL signature (that is Java's order, not an
// oversight here).
func (DynamicalExtFieldRPCHook) DoBeforeRequest(_ string, request *RemotingCommand) {
	if zone := os.Getenv(common.ZoneNameEnv); zone != "" {
		request.AddExtField(common.ZoneNameField, zone)
	}
	if mode := os.Getenv(common.ZoneModeEnv); mode != "" {
		request.AddExtField(common.ZoneModeField, mode)
	}
}

// DoAfterResponse is a no-op, exactly like Java.
func (DynamicalExtFieldRPCHook) DoAfterResponse(_ string, _ *RemotingCommand, _ *RemotingCommand) {}

var (
	_ RPCHook = (*NamespaceRpcHook)(nil)
	_ RPCHook = DynamicalExtFieldRPCHook{}
)
