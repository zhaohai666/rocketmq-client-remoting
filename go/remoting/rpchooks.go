// The two infrastructure RPC hooks that bracket the user's hook. The
// canonical registration order is:
//
//	RegisterRPCHook(NewNamespaceRpcHook(clientConfig));
//	// stream hook first, so ReqT is inside the signed "reserve field"
//	if enableStreamRequestType {
//	    RegisterRPCHook(StreamTypeRPCHook{});
//	}
//	RegisterRPCHook(rpcHook);            // the USER's (e.g. ACL)
//	RegisterRPCHook(DynamicalExtFieldRPCHook{});
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

// NamespaceRpcHook: when a namespace-v2 is configured, every request carries
//
//	nsd = "true", ns = <namespaceV2>
//
// This is the SERVER-side namespace mechanism (the broker resolves the real
// topic from the header). It is a different mechanism from the classic
// `namespace` field, which is applied by mangling topic names client-side.
type NamespaceRpcHook struct {
	namespaceV2 string
}

// NewNamespaceRpcHook builds the hook; an empty namespace makes it a no-op.
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

// DoAfterResponse is a no-op.
func (h *NamespaceRpcHook) DoAfterResponse(_ string, _ *RemotingCommand, _ *RemotingCommand) {}

// DynamicalExtFieldRPCHook stamps the zone name / zone mode onto every
// request when the process is configured with one.
//
// The classic lookup order is system property first, environment variable
// second. Go has no system properties, so the environment variable is the
// only source; the property name is kept as the env-var-free constant for
// documentation.
type DynamicalExtFieldRPCHook struct{}

// DoBeforeRequest writes __ZONE_NAME / __ZONE_MODE when set. Registered LAST,
// so these fields are deliberately outside the ACL signature.
func (DynamicalExtFieldRPCHook) DoBeforeRequest(_ string, request *RemotingCommand) {
	if zone := os.Getenv(common.ZoneNameEnv); zone != "" {
		request.AddExtField(common.ZoneNameField, zone)
	}
	if mode := os.Getenv(common.ZoneModeEnv); mode != "" {
		request.AddExtField(common.ZoneModeField, mode)
	}
}

// DoAfterResponse is a no-op.
func (DynamicalExtFieldRPCHook) DoAfterResponse(_ string, _ *RemotingCommand, _ *RemotingCommand) {}

var (
	_ RPCHook = (*NamespaceRpcHook)(nil)
	_ RPCHook = DynamicalExtFieldRPCHook{}
)
