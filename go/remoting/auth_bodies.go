// Auth-management wire beans: the user and ACL records carried by the
// 3001..3010 request bodies, and the answers those requests read back.
//
// Wire facts that are load-bearing here, do NOT "tidy" them:
//   - every enum travels as a bare string and is compared byte-for-byte,
//     case included. The case pattern is deliberately inconsistent
//     (`Super` but `enable`, `Custom` but `Allow`); nothing normalises it,
//     because a normalised value is simply a different enum member;
//   - an unset field is ABSENT from the body. No key ever carries `null`, and
//     an empty string counts as unset exactly the way an omitted key does;
//   - the convenience shape of an ACL has no `policyType` at all, while a
//     policy read back from the server normally does — the key's presence, not
//     its position, is what the two shapes differ on.
package remoting

// Enum wire strings of the auth-management family. Spelling and case are part
// of the contract.
const (
	AuthUserTypeSuper  = "Super"
	AuthUserTypeNormal = "Normal"

	AuthUserStatusEnable  = "enable"
	AuthUserStatusDisable = "disable"

	AuthPolicyTypeCustom  = "Custom"
	AuthPolicyTypeDefault = "Default"

	AuthDecisionAllow = "Allow"
	AuthDecisionDeny  = "Deny"
)

// ---------------- user ----------------

// AuthUser is one user record.
type AuthUser struct {
	Username   string
	Password   string
	UserType   string
	UserStatus string
}

// NewAuthUser builds the user of the convenience shape: identity, credential
// and type only. The status stays unset, so the encoded body has no
// `userStatus` key.
func NewAuthUser(username, password, userType string) *AuthUser {
	return &AuthUser{Username: username, Password: password, UserType: userType}
}

func (u *AuthUser) ToJSONValue() map[string]any {
	out := map[string]any{}
	putAuthText(out, "username", u.Username)
	putAuthText(out, "password", u.Password)
	putAuthText(out, "userType", u.UserType)
	putAuthText(out, "userStatus", u.UserStatus)
	return out
}

func (u *AuthUser) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("AuthUser: expected a JSON object")
	}
	u.Username = authText(obj, "username")
	u.Password = authText(obj, "password")
	u.UserType = authText(obj, "userType")
	u.UserStatus = authText(obj, "userStatus")
	return nil
}

// Encode renders the JSON body a user-management request carries.
func (u *AuthUser) Encode() []byte { return EncodeJSON(u.ToJSONValue()) }

// DecodeAuthUser parses a single user object. An absent body is the zero user,
// not an error: a record the server has nothing to report about comes back with
// no bytes at all.
func DecodeAuthUser(data []byte) (*AuthUser, error) {
	user := &AuthUser{}
	value, err := authRecordBody(data)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return user, nil
	}
	if err := user.FromJSONValue(value); err != nil {
		return nil, err
	}
	return user, nil
}

// DecodeAuthUserList parses an array of user objects. An absent or empty body
// is the empty list.
func DecodeAuthUserList(data []byte) ([]*AuthUser, error) {
	items, err := decodeAuthList("AuthUser list", data)
	if err != nil {
		return nil, err
	}
	out := make([]*AuthUser, 0, len(items))
	for _, item := range items {
		user := &AuthUser{}
		if err := user.FromJSONValue(item); err != nil {
			return nil, err
		}
		out = append(out, user)
	}
	return out, nil
}

// ---------------- acl ----------------

// AuthEntry is one grant: a resource, what may be done to it, from where, and
// with what verdict.
type AuthEntry struct {
	Resource  string
	Actions   []string
	SourceIPs []string
	Decision  string
}

func (e *AuthEntry) ToJSONValue() map[string]any {
	out := map[string]any{}
	putAuthText(out, "resource", e.Resource)
	putAuthList(out, "actions", e.Actions)
	putAuthList(out, "sourceIps", e.SourceIPs)
	putAuthText(out, "decision", e.Decision)
	return out
}

func (e *AuthEntry) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("AuthEntry: expected a JSON object")
	}
	e.Resource = authText(obj, "resource")
	e.Actions = authTextList(obj, "actions")
	e.SourceIPs = authTextList(obj, "sourceIps")
	e.Decision = authText(obj, "decision")
	return nil
}

// AuthPolicy groups the entries of one policy type. The convenience shape
// leaves PolicyType unset, which drops the key entirely.
type AuthPolicy struct {
	PolicyType string
	Entries    []*AuthEntry
}

func (p *AuthPolicy) ToJSONValue() map[string]any {
	out := map[string]any{}
	putAuthText(out, "policyType", p.PolicyType)
	if len(p.Entries) > 0 {
		rows := make([]any, 0, len(p.Entries))
		for _, entry := range p.Entries {
			rows = append(rows, entry.ToJSONValue())
		}
		out["entries"] = rows
	}
	return out
}

func (p *AuthPolicy) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("AuthPolicy: expected a JSON object")
	}
	p.PolicyType = authText(obj, "policyType")
	p.Entries = nil
	for _, item := range jsonArray(obj, "entries") {
		entry := &AuthEntry{}
		if err := entry.FromJSONValue(item); err != nil {
			return err
		}
		p.Entries = append(p.Entries, entry)
	}
	return nil
}

// AuthAcl is the subject those policies are attached to.
type AuthAcl struct {
	Subject  string
	Policies []*AuthPolicy
}

// NewAuthAcl builds the acl of the convenience shape: exactly ONE policy, with
// no policy type, holding one entry per resource. Every entry repeats the same
// actions, source IPs and decision.
//
// The slices are copied per entry: sharing one backing array would let a later
// change to one entry silently rewrite all the others.
func NewAuthAcl(subject string, resources, actions, sourceIPs []string, decision string) *AuthAcl {
	entries := make([]*AuthEntry, 0, len(resources))
	for _, resource := range resources {
		entries = append(entries, &AuthEntry{
			Resource:  resource,
			Actions:   append([]string(nil), actions...),
			SourceIPs: append([]string(nil), sourceIPs...),
			Decision:  decision,
		})
	}
	return &AuthAcl{
		Subject:  subject,
		Policies: []*AuthPolicy{{Entries: entries}},
	}
}

func (a *AuthAcl) ToJSONValue() map[string]any {
	out := map[string]any{}
	putAuthText(out, "subject", a.Subject)
	if len(a.Policies) > 0 {
		rows := make([]any, 0, len(a.Policies))
		for _, policy := range a.Policies {
			rows = append(rows, policy.ToJSONValue())
		}
		out["policies"] = rows
	}
	return out
}

func (a *AuthAcl) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("AuthAcl: expected a JSON object")
	}
	a.Subject = authText(obj, "subject")
	a.Policies = nil
	for _, item := range jsonArray(obj, "policies") {
		policy := &AuthPolicy{}
		if err := policy.FromJSONValue(item); err != nil {
			return err
		}
		a.Policies = append(a.Policies, policy)
	}
	return nil
}

// Encode renders the JSON body an acl-management request carries.
func (a *AuthAcl) Encode() []byte { return EncodeJSON(a.ToJSONValue()) }

// DecodeAuthAcl parses a single acl object; see DecodeAuthUser for the absent
// body rule.
func DecodeAuthAcl(data []byte) (*AuthAcl, error) {
	acl := &AuthAcl{}
	value, err := authRecordBody(data)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return acl, nil
	}
	if err := acl.FromJSONValue(value); err != nil {
		return nil, err
	}
	return acl, nil
}

// DecodeAuthAclList parses an array of acl objects.
func DecodeAuthAclList(data []byte) ([]*AuthAcl, error) {
	items, err := decodeAuthList("AuthAcl list", data)
	if err != nil {
		return nil, err
	}
	out := make([]*AuthAcl, 0, len(items))
	for _, item := range items {
		acl := &AuthAcl{}
		if err := acl.FromJSONValue(item); err != nil {
			return nil, err
		}
		out = append(out, acl)
	}
	return out, nil
}

// ---------------- field readers / writers ----------------

// putAuthText writes a string key only when it has something to say: unset
// values stay off the body entirely.
func putAuthText(out map[string]any, key, value string) {
	if value == "" {
		return
	}
	out[key] = value
}

func putAuthList(out map[string]any, key string, values []string) {
	if len(values) == 0 {
		return
	}
	rows := make([]any, 0, len(values))
	for _, value := range values {
		rows = append(rows, value)
	}
	out[key] = rows
}

func authText(obj map[string]any, key string) string {
	return jsonStringOr(obj, key, "")
}

// authTextList keeps a missing key distinct from a present empty one: nil when
// the server said nothing about it, an empty slice when it sent `[]`. Both
// re-encode as absent, so the difference never reaches the wire.
func authTextList(obj map[string]any, key string) []string {
	raw, ok := obj[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// authRecordBody decodes the body of a read that promises one record. A nil
// value means the server sent nothing — no bytes, or a bare null — which the
// callers read as the zero record rather than as a failure.
func authRecordBody(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	return DecodeJSON(data)
}

// decodeAuthList parses a list answer's envelope. An absent body, and a body
// that is nothing but a null, are the empty list; a body of any other shape is
// an error, because reading it as "nothing there" would hide it from the
// caller.
func decodeAuthList(what string, data []byte) ([]any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	arr, ok := value.([]any)
	if !ok {
		return nil, decodeErrf("%s: expected a JSON array", what)
	}
	return arr, nil
}
