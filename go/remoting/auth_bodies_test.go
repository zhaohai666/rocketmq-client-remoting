// Auth-management body codec tests: the two request shapes a user or ACL change
// can take, and the four answer shapes the read requests can come back with.
//
// Wire facts pinned here:
//   - an unset field is ABSENT from the body — no `null` value and no empty
//     string standing in for it; the convenience shapes differ from the object
//     ones by exactly one key (`userStatus`, `policyType`);
//   - the enum spellings are byte-exact and their case is deliberately
//     inconsistent, so a normalised value is a different member, not an
//     equivalent one;
//   - an absent answer body is the zero record / the empty list, never an
//     error, while a body of the wrong SHAPE is an error rather than "nothing".
package remoting

import (
	"reflect"
	"strings"
	"testing"
)

func TestNewAuthUserLeavesUserStatusAbsent(t *testing.T) {
	got := string(NewAuthUser("alice", "s3cret", AuthUserTypeNormal).Encode())
	want := `{"password":"s3cret","userType":"Normal","username":"alice"}`
	if got != want {
		t.Errorf("convenience user body =\n  %s\nwant\n  %s", got, want)
	}
	// The control: the same record WITH a status gains exactly that one key.
	full := string((&AuthUser{
		Username: "alice", Password: "s3cret",
		UserType: AuthUserTypeSuper, UserStatus: AuthUserStatusDisable,
	}).Encode())
	wantFull := `{"password":"s3cret","userStatus":"disable","userType":"Super","username":"alice"}`
	if full != wantFull {
		t.Errorf("object user body =\n  %s\nwant\n  %s", full, wantFull)
	}
	if strings.Contains(got, "userStatus") {
		t.Errorf("the convenience body must not carry userStatus at all: %s", got)
	}
}

func TestAuthUserEncodeOmitsUnsetFields(t *testing.T) {
	if got := string((&AuthUser{}).Encode()); got != `{}` {
		t.Errorf("empty user body = %s, want {}", got)
	}
	// An empty string counts as unset exactly like an absent one; nothing on
	// this surface may put `"password":""` on the wire.
	if got := string((&AuthUser{Username: "alice", Password: ""}).Encode()); got != `{"username":"alice"}` {
		t.Errorf("body with an unset password = %s", got)
	}
	if got := string((&AuthUser{Username: "alice", UserStatus: AuthUserStatusEnable}).Encode()); got != `{"userStatus":"enable","username":"alice"}` {
		t.Errorf("status-only body = %s", got)
	}
}

func TestAuthEnumWireStrings(t *testing.T) {
	for _, pair := range []struct{ got, want string }{
		{AuthUserTypeSuper, "Super"},
		{AuthUserTypeNormal, "Normal"},
		{AuthUserStatusEnable, "enable"},
		{AuthUserStatusDisable, "disable"},
		{AuthPolicyTypeCustom, "Custom"},
		{AuthPolicyTypeDefault, "Default"},
		{AuthDecisionAllow, "Allow"},
		{AuthDecisionDeny, "Deny"},
	} {
		if pair.got != pair.want {
			t.Errorf("wire string = %q, want %q", pair.got, pair.want)
		}
	}
	// Case is part of the contract: the status family is lower-case while the
	// other three are initial-capital. EqualFold would let a slip through.
	if AuthUserStatusEnable == "Enable" {
		t.Error("userStatus enable is lower-case on the wire")
	}
	if AuthUserTypeNormal == "normal" {
		t.Error("userType Normal is initial-capital on the wire")
	}
}

func TestDecodeAuthUserServerShape(t *testing.T) {
	text := `{"username":"alice","password":"s3cret","userType":"Super","userStatus":"enable"}`
	user, err := DecodeAuthUser([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	want := &AuthUser{Username: "alice", Password: "s3cret", UserType: "Super", UserStatus: "enable"}
	if *user != *want {
		t.Errorf("decoded = %+v, want %+v", *user, *want)
	}
	// A key the server sends as null reads back as unset, and re-encodes as
	// absent rather than as null.
	partial, err := DecodeAuthUser([]byte(`{"username":"bob","userStatus":null}`))
	if err != nil {
		t.Fatal(err)
	}
	if partial.UserStatus != "" || partial.Password != "" || partial.Username != "bob" {
		t.Errorf("null fields read as %+v", *partial)
	}
	if got := string(partial.Encode()); got != `{"username":"bob"}` {
		t.Errorf("re-encode of a null-bearing record = %s", got)
	}
}

func TestDecodeAuthUserEmptyBodyIsZeroValue(t *testing.T) {
	for _, data := range [][]byte{nil, {}, []byte(""), []byte("null")} {
		user, err := DecodeAuthUser(data)
		if err != nil {
			t.Fatalf("body %q must not be an error: %v", data, err)
		}
		if user == nil || *user != (AuthUser{}) {
			t.Errorf("body %q gave %+v, want the zero user", data, user)
		}
	}
}

func TestDecodeAuthUserRejectsWrongShape(t *testing.T) {
	for _, text := range []string{`not json`, `[{"username":"a"}]`, `"a string"`, `1`} {
		if _, err := DecodeAuthUser([]byte(text)); err == nil {
			t.Errorf("%q must not decode as a single user", text)
		}
	}
}

func TestDecodeAuthUserList(t *testing.T) {
	users, err := DecodeAuthUserList([]byte(
		`[{"username":"alice","userType":"Normal","userStatus":"enable"},` +
			`{"username":"bob","userStatus":"disable"}]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 {
		t.Fatalf("got %d users, want 2", len(users))
	}
	if users[0].Username != "alice" || users[0].UserType != "Normal" || users[0].UserStatus != "enable" {
		t.Errorf("first user = %+v", *users[0])
	}
	if users[1].Username != "bob" || users[1].Password != "" || users[1].UserStatus != "disable" {
		t.Errorf("second user = %+v", *users[1])
	}

	// An empty list, an empty envelope object and an absent body all read as
	// the empty list — none of them is an error.
	for _, data := range [][]byte{nil, {}, []byte(""), []byte("[]"), []byte("null")} {
		got, err := DecodeAuthUserList(data)
		if err != nil {
			t.Fatalf("body %q: %v", data, err)
		}
		if len(got) != 0 {
			t.Errorf("body %q gave %d users, want 0", data, len(got))
		}
	}
	// A single object where a list was promised is a shape error, not "no
	// users": the latter would look like an empty table.
	if _, err := DecodeAuthUserList([]byte(`{"username":"alice"}`)); err == nil {
		t.Error("an object body must not decode as a user list")
	}
	if _, err := DecodeAuthUserList([]byte(`[1]`)); err == nil {
		t.Error("a non-object element must be an error, not a dropped row")
	}
}

func TestNewAuthAclBuildsOneTypelessPolicyPerResource(t *testing.T) {
	acl := NewAuthAcl("Subject:OrderTopic",
		[]string{"topicA", "topicB"},
		[]string{"Publish", "Subscribe"},
		[]string{"192.168.1.7"},
		AuthDecisionAllow)

	got := string(acl.Encode())
	want := `{"policies":[{"entries":[` +
		`{"actions":["Publish","Subscribe"],"decision":"Allow","resource":"topicA","sourceIps":["192.168.1.7"]},` +
		`{"actions":["Publish","Subscribe"],"decision":"Allow","resource":"topicB","sourceIps":["192.168.1.7"]}` +
		`]}],"subject":"Subject:OrderTopic"}`
	if got != want {
		t.Errorf("convenience acl body =\n  %s\nwant\n  %s", got, want)
	}

	// The convenience shape has no policy type at all; that absence is the whole
	// difference to the object form below.
	if strings.Contains(got, "policyType") {
		t.Errorf("the convenience acl body must not carry policyType: %s", got)
	}
	if len(acl.Policies) != 1 {
		t.Fatalf("got %d policies, want exactly one whatever the resource count", len(acl.Policies))
	}
	if len(acl.Policies[0].Entries) != 2 {
		t.Fatalf("got %d entries, want one per resource", len(acl.Policies[0].Entries))
	}
	first, second := acl.Policies[0].Entries[0], acl.Policies[0].Entries[1]
	if !reflect.DeepEqual(first.Actions, second.Actions) ||
		!reflect.DeepEqual(first.SourceIPs, second.SourceIPs) ||
		first.Decision != second.Decision {
		t.Errorf("every entry must repeat the same grant: %+v vs %+v", first, second)
	}

	// The control for the assertion above: the object form does name its policy
	// type, and that is the only key the two shapes differ on.
	typed := &AuthAcl{Subject: "Subject:OrderTopic", Policies: []*AuthPolicy{{
		PolicyType: AuthPolicyTypeCustom,
		Entries:    acl.Policies[0].Entries,
	}}}
	if !strings.Contains(string(typed.Encode()), `"policyType":"Custom"`) {
		t.Errorf("object form lost its policy type: %s", typed.Encode())
	}
}

func TestNewAuthAclEntriesDoNotShareOneSlice(t *testing.T) {
	acl := NewAuthAcl("Subject:S", []string{"r1", "r2"}, []string{"Publish"}, nil, AuthDecisionAllow)
	entries := acl.Policies[0].Entries
	entries[0].Actions[0] = "Subscribe"
	if entries[1].Actions[0] != "Publish" {
		t.Errorf("the second entry followed the first: %v", entries[1].Actions)
	}
}

func TestAuthAclEncodeOmitsUnsetFields(t *testing.T) {
	if got := string((&AuthAcl{}).Encode()); got != `{}` {
		t.Errorf("empty acl body = %s, want {}", got)
	}
	// An empty list is unset here too: `"actions":[]` is a claim the server has
	// nothing to say about.
	acl := &AuthAcl{Subject: "Subject:topicA", Policies: []*AuthPolicy{{
		Entries: []*AuthEntry{{Resource: "topicA", Actions: []string{}, Decision: AuthDecisionDeny}},
	}}}
	got := string(acl.Encode())
	want := `{"policies":[{"entries":[{"decision":"Deny","resource":"topicA"}]}],"subject":"Subject:topicA"}`
	if got != want {
		t.Errorf("acl body =\n  %s\nwant\n  %s", got, want)
	}
}

func TestAuthAclObjectFormWithSeveralPolicies(t *testing.T) {
	acl := &AuthAcl{Subject: "Subject:topicA", Policies: []*AuthPolicy{
		{PolicyType: AuthPolicyTypeDefault, Entries: []*AuthEntry{
			{Resource: "topicA", Actions: []string{"*"}, Decision: AuthDecisionDeny},
		}},
		{PolicyType: AuthPolicyTypeCustom, Entries: []*AuthEntry{
			{Resource: "topicB", SourceIPs: []string{"10.0.0.1"}, Decision: AuthDecisionAllow},
		}},
	}}
	got := string(acl.Encode())
	want := `{"policies":[` +
		`{"entries":[{"actions":["*"],"decision":"Deny","resource":"topicA"}],"policyType":"Default"},` +
		`{"entries":[{"decision":"Allow","resource":"topicB","sourceIps":["10.0.0.1"]}],"policyType":"Custom"}` +
		`],"subject":"Subject:topicA"}`
	if got != want {
		t.Errorf("two-policy body =\n  %s\nwant\n  %s", got, want)
	}
}

const serverAclBody = `{"subject":"Subject:topicA","policies":[` +
	`{"policyType":"Custom","entries":[` +
	`{"resource":"topicA","actions":["Publish","Subscribe"],"sourceIps":["192.168.1.7","10.0.0.0/8"],"decision":"Allow"}` +
	`]},` +
	`{"policyType":"Default","entries":[{"resource":"topicB","actions":["*"],"decision":"Deny"}]}]}`

func TestDecodeAuthAclServerShape(t *testing.T) {
	acl, err := DecodeAuthAcl([]byte(serverAclBody))
	if err != nil {
		t.Fatal(err)
	}
	if acl.Subject != "Subject:topicA" {
		t.Errorf("subject = %q", acl.Subject)
	}
	if len(acl.Policies) != 2 {
		t.Fatalf("got %d policies, want 2", len(acl.Policies))
	}
	first := acl.Policies[0]
	if first.PolicyType != "Custom" || len(first.Entries) != 1 {
		t.Fatalf("first policy = %+v", first)
	}
	entry := first.Entries[0]
	if entry.Resource != "topicA" || entry.Decision != "Allow" {
		t.Errorf("entry = %+v", entry)
	}
	if !reflect.DeepEqual(entry.Actions, []string{"Publish", "Subscribe"}) {
		t.Errorf("actions = %v", entry.Actions)
	}
	if !reflect.DeepEqual(entry.SourceIPs, []string{"192.168.1.7", "10.0.0.0/8"}) {
		t.Errorf("sourceIps = %v", entry.SourceIPs)
	}
	// The second policy carries no source list: a missing key stays unset and
	// re-encodes as absent.
	second := acl.Policies[1].Entries[0]
	if second.SourceIPs != nil || second.Resource != "topicB" || second.Decision != "Deny" {
		t.Errorf("second entry = %+v", second)
	}
	if got := string(EncodeJSON(acl.Policies[1].ToJSONValue())); got !=
		`{"entries":[{"actions":["*"],"decision":"Deny","resource":"topicB"}],"policyType":"Default"}` {
		t.Errorf("re-encode of the source-less policy = %s", got)
	}
}

func TestAuthAclRoundTrip(t *testing.T) {
	acl, err := DecodeAuthAcl([]byte(serverAclBody))
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeAuthAcl(acl.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if string(back.Encode()) != string(acl.Encode()) {
		t.Errorf("round trip changed the body:\n  %s\n  %s", acl.Encode(), back.Encode())
	}
	// Decoding twice into one record must replace, not accumulate.
	if err := back.FromJSONValue(mustDecode(t, []byte(serverAclBody))); err != nil {
		t.Fatal(err)
	}
	if len(back.Policies) != 2 || len(back.Policies[0].Entries) != 1 {
		t.Errorf("FromJSONValue accumulated: %d policies", len(back.Policies))
	}
}

func TestDecodeAuthAclEmptyBodyIsZeroValue(t *testing.T) {
	for _, data := range [][]byte{nil, {}, []byte(""), []byte("null")} {
		acl, err := DecodeAuthAcl(data)
		if err != nil {
			t.Fatalf("body %q must not be an error: %v", data, err)
		}
		if acl == nil || acl.Subject != "" || len(acl.Policies) != 0 {
			t.Errorf("body %q gave %+v, want the zero acl", data, acl)
		}
	}
}

func TestDecodeAuthAclWrongShape(t *testing.T) {
	if _, err := DecodeAuthAcl([]byte(`[{"subject":"S"}]`)); err == nil {
		t.Error("an array body must not decode as a single acl")
	}
	if _, err := DecodeAuthAcl([]byte(`{"policies":[{"entries":[1]}]}`)); err == nil {
		t.Error("a non-object entry must be an error, not a dropped grant")
	}
	// The list form promises a list.
	if _, err := DecodeAuthAclList([]byte(`{"subject":"S"}`)); err == nil {
		t.Error("an object body must not decode as an acl list")
	}
	acls, err := DecodeAuthAclList([]byte(`[` + serverAclBody + `]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(acls) != 1 || len(acls[0].Policies) != 2 {
		t.Fatalf("acl list = %+v", acls)
	}
	for _, data := range [][]byte{nil, {}, []byte("[]")} {
		got, err := DecodeAuthAclList(data)
		if err != nil || len(got) != 0 {
			t.Errorf("body %q gave %d acls, err %v", data, len(got), err)
		}
	}
}
