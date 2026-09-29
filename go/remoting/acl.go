// ACL client-side authentication (Java
// org.apache.rocketmq.acl.common.{SessionCredentials, AclClientRPCHook, AclUtils,
// AclSigner} and org.apache.rocketmq.remoting.rpchook.StreamTypeRPCHook).
//
// The signature is byte-for-byte the one Java produces:
//
//	content  = values of every extField, in KEY-SORTED order, skipping the
//	           "Signature" key itself (values only — no keys, no separators),
//	           followed by the RAW body bytes.
//	signature = Base64Std( HMAC-SHA1(key = secretKey, data = content) )
//
// Two ordering rules that are easy to get wrong:
//
//  1. AccessKey (and SecurityToken, when set) must be IN extFields before the
//     content is built — they are part of what gets signed, so a signature
//     computed before they are written can never verify.
//  2. Signature is written LAST — it is the one key the content skips.
//
// The hook must be registered so that it runs BEFORE the command is encoded
// (RemotingClient.invokeWithTimeout does exactly that); signing after encode
// would sign a different set of bytes than the ones that reach the broker.
package remoting

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// SessionCredentials extField names (Java SessionCredentials constants). The
// strings go on the wire, so they are part of the protocol, not decoration.
const (
	CredentialAccessKey     = "AccessKey"
	CredentialSecretKey     = "SecretKey"
	CredentialSignature     = "Signature"
	CredentialSecurityToken = "SecurityToken"
)

// CredentialKeyFileEnv overrides the key file path. Java reads the
// `rocketmq.client.keyFile` system property; the Go analogue of a system
// property is an environment variable.
const CredentialKeyFileEnv = "ROCKETMQ_CLIENT_KEY_FILE"

// SessionCredentials mirrors org.apache.rocketmq.acl.common.SessionCredentials.
type SessionCredentials struct {
	AccessKey     string
	SecretKey     string
	SecurityToken string
}

// NewSessionCredentials mirrors SessionCredentials(accessKey, secretKey).
func NewSessionCredentials(accessKey, secretKey string) *SessionCredentials {
	return &SessionCredentials{AccessKey: accessKey, SecretKey: secretKey}
}

// NewSessionCredentialsWithToken mirrors
// SessionCredentials(accessKey, secretKey, securityToken).
func NewSessionCredentialsWithToken(accessKey, secretKey, securityToken string) *SessionCredentials {
	return &SessionCredentials{AccessKey: accessKey, SecretKey: secretKey, SecurityToken: securityToken}
}

// KeyFilePath is Java SessionCredentials.KEY_FILE:
// `rocketmq.client.keyFile` (here: $ROCKETMQ_CLIENT_KEY_FILE), else $HOME/key.
func KeyFilePath() string {
	if v := os.Getenv(CredentialKeyFileEnv); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "key"
	}
	return filepath.Join(home, "key")
}

// LoadSessionCredentials mirrors the no-arg SessionCredentials() constructor:
// read the key file and take AccessKey / SecretKey / SecurityToken from it.
// A missing or unreadable file is NOT an error in Java (the fields just stay
// empty); this port reports it so a typo'd path is not silently ignored.
func LoadSessionCredentials() (*SessionCredentials, error) {
	path := KeyFilePath()
	f, err := os.Open(path)
	if err != nil {
		return &SessionCredentials{}, err
	}
	defer f.Close()

	creds := &SessionCredentials{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		key, value, ok := parseJavaPropertyLine(sc.Text())
		if !ok {
			continue
		}
		switch key {
		case CredentialAccessKey:
			creds.AccessKey = strings.TrimSpace(value)
		case CredentialSecretKey:
			creds.SecretKey = strings.TrimSpace(value)
		case CredentialSecurityToken:
			creds.SecurityToken = strings.TrimSpace(value)
		}
	}
	if err := sc.Err(); err != nil {
		return creds, err
	}
	return creds, nil
}

// parseJavaPropertyLine handles the subset of java.util.Properties#load the
// key file actually uses: `key=value` / `key:value`, `#`/`!` comments, blank
// lines. Line continuations and backslash escapes are NOT handled — the key
// file is a two-line credentials file, and guessing at escapes would be worse
// than refusing. Returns ok=false for lines with no separator (Properties
// treats a bare key as an empty value; nobody means that here).
func parseJavaPropertyLine(line string) (string, string, bool) {
	trimmed := strings.TrimLeft(line, " \t\f")
	if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "!") {
		return "", "", false
	}
	idx := strings.IndexAny(trimmed, "=:")
	if idx < 0 {
		return "", "", false
	}
	return strings.TrimSpace(trimmed[:idx]), trimmed[idx+1:], true
}

// ------------------------------------------------------------------ signer

// CombineRequestContent mirrors Java AclUtils#combineRequestContent over
// AclClientRPCHook#parseRequestContent: makeCustomHeaderToNet first (the custom
// header's non-nil fields must be in extFields to be signed), then every
// extField VALUE in key-sorted order except "Signature", then the raw body.
//
// Sorting is Go's byte-wise string order; Java uses TreeMap's natural order
// (UTF-16 code units). The two agree for every ASCII field name, which is what
// the protocol uses; they could disagree for names mixing BMP and
// supplementary-plane characters, which no RocketMQ field name does.
func CombineRequestContent(request *RemotingCommand) []byte {
	request.MakeCustomHeaderToNet()
	ext := request.ExtFields()
	keys := ext.Keys()
	sort.Strings(keys)
	var buf []byte
	for _, k := range keys {
		if k == CredentialSignature {
			continue
		}
		v, _ := ext.Get(k)
		buf = append(buf, v...)
	}
	if len(request.Body) > 0 {
		buf = append(buf, request.Body...)
	}
	return buf
}

// CalcSignature mirrors Java AclSigner#calSignature:
// Base64Std(HMAC-SHA1(secretKey, data)). The client always uses HMAC-SHA1
// (AclSigner.DEFAULT_ALGORITHM); the broker-side choice among
// HmacSHA1/HmacMD5/HmacSHA256 is a server config the client never negotiates.
func CalcSignature(content []byte, secretKey string) string {
	mac := hmac.New(sha1.New, []byte(secretKey))
	mac.Write(content)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// ------------------------------------------------------------------ hooks

// AclClientRPCHook mirrors org.apache.rocketmq.acl.common.AclClientRPCHook.
type AclClientRPCHook struct {
	credentials *SessionCredentials
}

// NewAclClientRPCHook builds the hook. A nil/empty credentials pointer is
// rejected rather than allowed to sign with empty keys: an ACL cluster would
// answer every request with a signature failure, and that is much harder to
// read than a construction error.
func NewAclClientRPCHook(credentials *SessionCredentials) (*AclClientRPCHook, error) {
	if credentials == nil {
		return nil, common.ClientError("acl: session credentials must not be nil")
	}
	if credentials.AccessKey == "" || credentials.SecretKey == "" {
		return nil, common.ClientError("acl: accessKey and secretKey must not be empty")
	}
	return &AclClientRPCHook{credentials: credentials}, nil
}

// Credentials returns the hook's credentials.
func (h *AclClientRPCHook) Credentials() *SessionCredentials { return h.credentials }

// DoBeforeRequest mirrors Java AclClientRPCHook#doBeforeRequest, in the same
// order: AccessKey, SecurityToken, then the signature over both.
func (h *AclClientRPCHook) DoBeforeRequest(_ string, request *RemotingCommand) {
	request.AddExtField(CredentialAccessKey, h.credentials.AccessKey)
	// Java: "The SecurityToken value is unnecessary, user can choose this one."
	if h.credentials.SecurityToken != "" {
		request.AddExtField(CredentialSecurityToken, h.credentials.SecurityToken)
	}
	request.AddExtField(CredentialSignature, h.Sign(request))
}

// Sign returns the signature this hook would put on the request.
func (h *AclClientRPCHook) Sign(request *RemotingCommand) string {
	return CalcSignature(CombineRequestContent(request), h.credentials.SecretKey)
}

// DoAfterResponse is a no-op, exactly like Java.
func (h *AclClientRPCHook) DoAfterResponse(_ string, _ *RemotingCommand, _ *RemotingCommand) {}

// RequestType mirrors org.apache.rocketmq.remoting.protocol.RequestType, whose
// only member is STREAM with code (byte) 0.
type RequestType int8

// RequestTypeStream is RequestType.STREAM.
const RequestTypeStream RequestType = 0

// StreamTypeRPCHook mirrors
// org.apache.rocketmq.remoting.rpchook.StreamTypeRPCHook: one line writing
// `ReqT = "0"`.
//
// Registration ORDER matters. Java MQClientAPIImpl:329-332 registers this hook
// BEFORE the user's rpcHook ("Inject stream rpc hook first to make reserve
// field signature"), so that ReqT is already in extFields when the ACL hook
// builds its content — otherwise the broker would reject the request for
// carrying an unsigned field.
type StreamTypeRPCHook struct{}

// DoBeforeRequest writes ReqT. Java uses String.valueOf((byte) 0) → "0".
func (StreamTypeRPCHook) DoBeforeRequest(_ string, request *RemotingCommand) {
	request.AddExtField(common.ReqT, "0")
}

// DoAfterResponse is a no-op, exactly like Java.
func (StreamTypeRPCHook) DoAfterResponse(_ string, _ *RemotingCommand, _ *RemotingCommand) {}

var (
	_ RPCHook = (*AclClientRPCHook)(nil)
	_ RPCHook = StreamTypeRPCHook{}
)
