package remoting

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

// The vectors below are the SAME ones python/verify_acl_java_parity.py asserts
// against, and they were produced by a Java probe (/tmp/aclprobe/AclProbe.java
// printing content_hex and the signature of the real AclClientRPCHook). So a
// green run here means the Go signer is byte-identical to Java's, not merely
// self-consistent.
const (
	v1ContentHex = "414b5f544553546c6173743166616c73654d7947726f75704d79546f7069630102030405"
	v1Signature  = "qQhdzvXfV+g0r8LdwNCt+chJ4XY="
	v2ContentHex = "414b5f54455354544f4b454e2d4142436c6173743166616c73654d7947726f75704d79546f7069630102030405"
	v2Signature  = "5YIp2FNQL8pxQP3w6YKnSv3kAsw="
	v3ContentHex = "414b3132"
	v3Signature  = "d3vJKL2iRdr4ZykZfY+lxfQlfdc="
)

// aclCmd builds a bare command with the given extFields and body. Code 310 is
// what the Java probe used; the code does not enter the signature.
func aclCmd(ext map[string]string, body []byte) *RemotingCommand {
	cmd := CreateRequestCommand(310, nil)
	for k, v := range ext {
		cmd.AddExtField(k, v)
	}
	cmd.SetBody(body)
	return cmd
}

func v1Ext() map[string]string {
	return map[string]string{
		"topic":         "MyTopic",
		"producerGroup": "MyGroup",
		"a":             "1",
		"Zz":            "last",
		"batch":         "false",
	}
}

func mustHook(t *testing.T, ak, sk, token string) *AclClientRPCHook {
	t.Helper()
	creds := NewSessionCredentials(ak, sk)
	if token != "" {
		creds = NewSessionCredentialsWithToken(ak, sk, token)
	}
	hook, err := NewAclClientRPCHook(creds)
	if err != nil {
		t.Fatalf("NewAclClientRPCHook: %v", err)
	}
	return hook
}

// TestAclV1JavaVector: the base case, with a body.
func TestAclV1JavaVector(t *testing.T) {
	cmd := aclCmd(v1Ext(), []byte{1, 2, 3, 4, 5})
	mustHook(t, "AK_TEST", "SK_TEST_SECRET_12345678", "").DoBeforeRequest("127.0.0.1:9876", cmd)

	if got := hex.EncodeToString(CombineRequestContent(cmd)); got != v1ContentHex {
		t.Errorf("content hex = %s, want %s", got, v1ContentHex)
	}
	if got, _ := cmd.GetExtField(CredentialSignature); got != v1Signature {
		t.Errorf("signature = %q, want %q", got, v1Signature)
	}
	if got, _ := cmd.GetExtField(CredentialAccessKey); got != "AK_TEST" {
		t.Errorf("AccessKey extField = %q, want AK_TEST", got)
	}
	if _, ok := cmd.GetExtField(CredentialSecurityToken); ok {
		t.Error("SecurityToken must be absent when the credentials carry none")
	}
}

// TestAclV2WithSecurityToken: the token joins the signed content.
func TestAclV2WithSecurityToken(t *testing.T) {
	cmd := aclCmd(v1Ext(), []byte{1, 2, 3, 4, 5})
	mustHook(t, "AK_TEST", "SK_TEST_SECRET_12345678", "TOKEN-ABC").DoBeforeRequest("127.0.0.1:9876", cmd)

	if got := hex.EncodeToString(CombineRequestContent(cmd)); got != v2ContentHex {
		t.Errorf("content hex = %s, want %s", got, v2ContentHex)
	}
	if got, _ := cmd.GetExtField(CredentialSignature); got != v2Signature {
		t.Errorf("signature = %q, want %q", got, v2Signature)
	}
	if got, _ := cmd.GetExtField(CredentialSecurityToken); got != "TOKEN-ABC" {
		t.Errorf("SecurityToken extField = %q, want TOKEN-ABC", got)
	}
}

// TestAclV3NoBody: a nil body must not add a separator or break the content.
func TestAclV3NoBody(t *testing.T) {
	cmd := aclCmd(map[string]string{"a": "1", "b": "2"}, nil)
	mustHook(t, "AK", "SK", "").DoBeforeRequest("127.0.0.1:9876", cmd)

	if got := hex.EncodeToString(CombineRequestContent(cmd)); got != v3ContentHex {
		t.Errorf("content hex = %s, want %s", got, v3ContentHex)
	}
	if got, _ := cmd.GetExtField(CredentialSignature); got != v3Signature {
		t.Errorf("signature = %q, want %q", got, v3Signature)
	}
}

// TestAclV4SignatureKeyExcluded: a pre-existing Signature is skipped, so
// re-signing a command is stable instead of signing the old signature.
func TestAclV4SignatureKeyExcluded(t *testing.T) {
	cmd := aclCmd(map[string]string{"topic": "MyTopic", CredentialSignature: "SHOULD_BE_EXCLUDED"}, nil)
	if got := string(CombineRequestContent(cmd)); got != "MyTopic" {
		t.Errorf("content = %q, want %q", got, "MyTopic")
	}
}

// TestAclV5InsertOrderIrrelevant: extFields is INSERTION-ordered in this port
// (common.StringMap), so the signer has to sort explicitly. Two maps holding
// the same pairs in opposite insertion order must sign identically — this is
// the test that would fail if someone iterated with Range instead of sorting.
func TestAclV5InsertOrderIrrelevant(t *testing.T) {
	a := aclCmd(v1Ext(), []byte{1, 2, 3, 4, 5})
	b := aclCmd(map[string]string{
		"batch":         "false",
		"Zz":            "last",
		"a":             "1",
		"producerGroup": "MyGroup",
		"topic":         "MyTopic",
	}, []byte{1, 2, 3, 4, 5})

	hook := mustHook(t, "AK_TEST", "SK_TEST_SECRET_12345678", "")
	hook.DoBeforeRequest("x", a)
	hook.DoBeforeRequest("x", b)

	sa, _ := a.GetExtField(CredentialSignature)
	sb, _ := b.GetExtField(CredentialSignature)
	if sa != sb {
		t.Errorf("insertion order changed the signature: %q vs %q", sa, sb)
	}
	if sa != v1Signature {
		t.Errorf("signature = %q, want %q", sa, v1Signature)
	}
}

// TestAclStreamHookOrderDecidesSignature: Java registers StreamTypeRPCHook
// BEFORE the ACL hook so ReqT is inside the signature. Running them in the
// other order must produce a DIFFERENT signature — that is the whole reason
// registration order is documented as load-bearing.
func TestAclStreamHookOrderDecidesSignature(t *testing.T) {
	streamFirst := aclCmd(map[string]string{"topic": "MyTopic"}, nil)
	StreamTypeRPCHook{}.DoBeforeRequest("x", streamFirst)
	mustHook(t, "AK", "SK", "").DoBeforeRequest("x", streamFirst)

	aclFirst := aclCmd(map[string]string{"topic": "MyTopic"}, nil)
	mustHook(t, "AK", "SK", "").DoBeforeRequest("x", aclFirst)
	StreamTypeRPCHook{}.DoBeforeRequest("x", aclFirst)

	if got, want := mustField(t, streamFirst, commonReqT), "0"; got != want {
		t.Errorf("ReqT = %q, want %q", got, want)
	}
	s1, _ := streamFirst.GetExtField(CredentialSignature)
	s2, _ := aclFirst.GetExtField(CredentialSignature)
	if s1 == s2 {
		t.Error("signing before/after the stream hook must differ — ReqT is unsigned in one of them")
	}
	// The signed one covers ReqT's value, so "0" is inside its content.
	if !containsBytes(CombineRequestContent(streamFirst), []byte("0")) {
		t.Error("ReqT value is missing from the signed content")
	}
}

const commonReqT = "ReqT"

func mustField(t *testing.T, cmd *RemotingCommand, key string) string {
	t.Helper()
	v, ok := cmd.GetExtField(key)
	if !ok {
		t.Fatalf("extField %q missing", key)
	}
	return v
}

func containsBytes(haystack, needle []byte) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if string(haystack[i:i+len(needle)]) == string(needle) {
			return true
		}
	}
	return false
}

// TestAclRejectsEmptyCredentials: signing with an empty key would make every
// broker call fail with an opaque signature error instead of at construction.
func TestAclRejectsEmptyCredentials(t *testing.T) {
	if _, err := NewAclClientRPCHook(nil); err == nil {
		t.Error("nil credentials must be rejected")
	}
	if _, err := NewAclClientRPCHook(&SessionCredentials{AccessKey: "AK"}); err == nil {
		t.Error("empty secretKey must be rejected")
	}
	if _, err := NewAclClientRPCHook(&SessionCredentials{SecretKey: "SK"}); err == nil {
		t.Error("empty accessKey must be rejected")
	}
}

// TestAclCalSignatureMatchesStdlib: CalcSignature must be plain
// HMAC-SHA1 + standard Base64, padding included.
func TestAclCalSignatureMatchesStdlib(t *testing.T) {
	// HMAC-SHA1("key", "The quick brown fox jumps over the lazy dog")
	const want = "3nybhbi3iqa8ino29wqQcBydtNk="
	if got := CalcSignature([]byte("The quick brown fox jumps over the lazy dog"), "key"); got != want {
		t.Errorf("CalcSignature = %q, want %q", got, want)
	}
}

// TestAclKeyFileLoading covers the no-arg SessionCredentials() constructor.
//
// The keys are CASE-SENSITIVE, exactly like Java's Properties.getProperty
// ("secretKey=..." would be ignored by Java too). The `:` separator IS legal
// (java.util.Properties#load accepts `=` and `:`), so it is covered here.
func TestAclKeyFileLoading(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "key")
	content := "# rocketmq credentials\n" +
		"\n" +
		"AccessKey = AK_FROM_FILE\n" +
		"SecretKey: SK_FROM_FILE\n" +
		"SecurityToken=TOKEN_FROM_FILE\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(CredentialKeyFileEnv, path)

	creds, err := LoadSessionCredentials()
	if err != nil {
		t.Fatalf("LoadSessionCredentials: %v", err)
	}
	if creds.AccessKey != "AK_FROM_FILE" {
		t.Errorf("AccessKey = %q", creds.AccessKey)
	}
	if creds.SecretKey != "SK_FROM_FILE" {
		t.Errorf("SecretKey = %q", creds.SecretKey)
	}
	if creds.SecurityToken != "TOKEN_FROM_FILE" {
		t.Errorf("SecurityToken = %q", creds.SecurityToken)
	}
}

// TestAclKeyFileMissing: Java silently leaves the fields empty; this port
// returns an error AND empty credentials so neither behaviour surprises.
func TestAclKeyFileMissing(t *testing.T) {
	t.Setenv(CredentialKeyFileEnv, filepath.Join(t.TempDir(), "nope"))
	creds, err := LoadSessionCredentials()
	if err == nil {
		t.Error("a missing key file must be reported")
	}
	if creds == nil || creds.AccessKey != "" {
		t.Errorf("credentials must be empty on a missing file, got %+v", creds)
	}
}

// TestAclKeyFilePathDefault pins $HOME/key when the env var is unset.
func TestAclKeyFilePathDefault(t *testing.T) {
	t.Setenv(CredentialKeyFileEnv, "")
	home, _ := os.UserHomeDir()
	if got, want := KeyFilePath(), filepath.Join(home, "key"); got != want {
		t.Errorf("KeyFilePath = %q, want %q", got, want)
	}
}
