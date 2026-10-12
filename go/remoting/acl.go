// ACL client-side authentication.
//
// The signature must be byte-for-byte what the broker's ACL validator
// recomputes:
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

// SessionCredentials extField names. The strings go on the wire and the
// broker matches them verbatim, so they are part of the protocol, not
// decoration.
const (
	CredentialAccessKey     = "AccessKey"
	CredentialSecretKey     = "SecretKey"
	CredentialSignature     = "Signature"
	CredentialSecurityToken = "SecurityToken"
)

// CredentialKeyFileEnv overrides the key file path — the environment analogue
// of the `rocketmq.client.keyFile` config key.
const CredentialKeyFileEnv = "ROCKETMQ_CLIENT_KEY_FILE"

type SessionCredentials struct {
	AccessKey     string
	SecretKey     string
	SecurityToken string
}

func NewSessionCredentials(accessKey, secretKey string) *SessionCredentials {
	return &SessionCredentials{AccessKey: accessKey, SecretKey: secretKey}
}

func NewSessionCredentialsWithToken(accessKey, secretKey, securityToken string) *SessionCredentials {
	return &SessionCredentials{AccessKey: accessKey, SecretKey: secretKey, SecurityToken: securityToken}
}

// KeyFilePath resolves the key file: $ROCKETMQ_CLIENT_KEY_FILE if set, else
// $HOME/key.
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

// LoadSessionCredentials reads the key file and takes AccessKey / SecretKey /
// SecurityToken from it. A missing or unreadable file is reported as an error
// so a typo'd path is not silently ignored.
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
		key, value, ok := parsePropertyLine(sc.Text())
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

// parsePropertyLine handles the subset of the .properties format the
// key file actually uses: `key=value` / `key:value`, `#`/`!` comments, blank
// lines. Line continuations and backslash escapes are NOT handled — the key
// file is a two-line credentials file, and guessing at escapes would be worse
// than refusing. Returns ok=false for lines with no separator (the properties
// format treats a bare key as an empty value; nobody means that here).
func parsePropertyLine(line string) (string, string, bool) {
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

// CombineRequestContent builds the signed content: makeCustomHeaderToNet
// first (the custom header's non-nil fields must be in extFields to be
// signed), then every extField VALUE in key-sorted order except "Signature",
// then the raw body.
//
// Sorting is Go's byte-wise string order; the broker sorts the same values
// by UTF-16 code units. The two agree for every ASCII field name, which is
// what the protocol uses; they could disagree for names mixing BMP and
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

// CalcSignature returns Base64Std(HMAC-SHA1(secretKey, data)). The client
// always uses HMAC-SHA1; the broker-side choice among
// HmacSHA1/HmacMD5/HmacSHA256 is a server config the client never negotiates.
func CalcSignature(content []byte, secretKey string) string {
	mac := hmac.New(sha1.New, []byte(secretKey))
	mac.Write(content)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// ------------------------------------------------------------------ hooks

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

// DoBeforeRequest writes extFields in a fixed order: AccessKey,
// SecurityToken, then the signature over both.
func (h *AclClientRPCHook) DoBeforeRequest(_ string, request *RemotingCommand) {
	request.AddExtField(CredentialAccessKey, h.credentials.AccessKey)
	// SecurityToken is optional, but must be in extFields before signing.
	if h.credentials.SecurityToken != "" {
		request.AddExtField(CredentialSecurityToken, h.credentials.SecurityToken)
	}
	request.AddExtField(CredentialSignature, h.Sign(request))
}

// Sign returns the signature this hook would put on the request.
func (h *AclClientRPCHook) Sign(request *RemotingCommand) string {
	return CalcSignature(CombineRequestContent(request), h.credentials.SecretKey)
}

// DoAfterResponse is a no-op.
func (h *AclClientRPCHook) DoAfterResponse(_ string, _ *RemotingCommand, _ *RemotingCommand) {}

// RequestType is the protocol request-type tag; its only member is STREAM
// with code (byte) 0.
type RequestType int8

// RequestTypeStream is RequestType.STREAM.
const RequestTypeStream RequestType = 0

// StreamTypeRPCHook is one line writing `ReqT = "0"`.
//
// Registration ORDER matters: this hook must be registered BEFORE the user's
// rpcHook (the ACL hook), so that ReqT is already in extFields when the ACL
// hook builds its content — otherwise the broker would reject the request for
// carrying an unsigned field.
type StreamTypeRPCHook struct{}

// DoBeforeRequest writes ReqT as "0" (the wire text of the byte value 0).
func (StreamTypeRPCHook) DoBeforeRequest(_ string, request *RemotingCommand) {
	request.AddExtField(common.ReqT, "0")
}

// DoAfterResponse is a no-op.
func (StreamTypeRPCHook) DoAfterResponse(_ string, _ *RemotingCommand, _ *RemotingCommand) {}

var (
	_ RPCHook = (*AclClientRPCHook)(nil)
	_ RPCHook = StreamTypeRPCHook{}
)
