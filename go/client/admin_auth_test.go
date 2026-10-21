// Auth user and ACL management through DefaultMQAdminExt: the 3001..3010
// family.
//
// Every assertion here is about the RECORDED REQUEST, because the risk of this
// surface sits entirely on the request side. A wrong header key, a `userStatus`
// that leaked onto the wire, a `policyType` in the convenience shape or a target
// address that moved on the way out all still produce a perfectly plausible
// decoded answer, so a test that only checks the answer proves nothing.
//
// Wire facts pinned here:
//   - each request carries exactly the header keys of its own operation and no
//     others: an unset field is left out, never sent empty;
//   - the write requests send a body, the void ones send none, and a void one
//     does not read the answer at all;
//   - the address the caller passed is the address that is used, with the
//     fast-channel switch on as well as off, and the nameserver is never asked
//     anything;
//   - SUCCESS is the only accepted code.
package client

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

var authCodes = []int32{
	remoting.ReqAuthCreateUser, remoting.ReqAuthUpdateUser, remoting.ReqAuthDeleteUser,
	remoting.ReqAuthGetUser, remoting.ReqAuthListUser,
	remoting.ReqAuthCreateACL, remoting.ReqAuthUpdateACL, remoting.ReqAuthDeleteACL,
	remoting.ReqAuthGetACL, remoting.ReqAuthListACL,
}

// authCluster is the mock cluster with the whole family answering SUCCESS and no
// body; a test overrides the code it is about.
func authCluster(t *testing.T) *adminCluster {
	t.Helper()
	c := newAdminCluster(t)
	for _, code := range authCodes {
		c.brokerAnswer[code] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
			return respOK(nil)
		}
	}
	return c
}

// requireAuthExt pins the whole header of a recorded request: every expected key
// with its exact value and no other key at all. Both a leaked unset field and a
// missing one fail here. Call it with no pairs to assert the header is empty.
func requireAuthExt(t *testing.T, req *remoting.RemotingCommand, want ...string) {
	t.Helper()
	if req == nil {
		t.Fatal("no request was recorded")
	}
	ext := req.ExtFields()
	if ext.Len() != len(want)/2 {
		t.Fatalf("header = %v, want exactly the keys %v", ext.Keys(), want)
	}
	for i := 0; i+1 < len(want); i += 2 {
		got, ok := ext.Get(want[i])
		if !ok {
			t.Fatalf("header is missing %q (has %v)", want[i], ext.Keys())
		}
		if got != want[i+1] {
			t.Errorf("header %q = %q, want %q", want[i], got, want[i+1])
		}
		if got == "" || got == "null" {
			t.Errorf("header %q = %q: an unset field must be left out, not sent empty", want[i], got)
		}
	}
}

func requireAuthBody(t *testing.T, req *remoting.RemotingCommand, want string) {
	t.Helper()
	if req == nil {
		t.Fatal("no request was recorded")
	}
	if got := string(req.Body); got != want {
		t.Errorf("body =\n  %s\nwant\n  %s", got, want)
	}
}

// authErr keeps only the error of a two-result auth read.
func authErr[T any](_ T, err error) error { return err }

const (
	serverUserBody = `{"username":"alice","password":"s3cret","userType":"Super","userStatus":"enable"}`
	serverAclBody  = `{"subject":"Subject:OrderTopic","policies":[{"policyType":"Custom","entries":[` +
		`{"resource":"topicA","actions":["Publish","Subscribe"],"sourceIps":["192.168.1.7"],"decision":"Allow"}]}]}`
)

const (
	convenienceUserBody = `{"password":"s3cret","userType":"Normal","username":"alice"}`
	objectUserBody      = `{"password":"s3cret","userStatus":"disable","userType":"Super","username":"alice"}`
	convenienceAclBody  = `{"policies":[{"entries":[` +
		`{"actions":["Publish"],"decision":"Allow","resource":"topicA","sourceIps":["192.168.1.7"]},` +
		`{"actions":["Publish"],"decision":"Allow","resource":"topicB","sourceIps":["192.168.1.7"]}` +
		`]}],"subject":"Subject:OrderTopic"}`
)

// ------------------------------------------------------------------- users

func TestAdminCreateUserSendsUsernameHeaderAndStatuslessBody(t *testing.T) {
	c := authCluster(t)
	requireNoError(t, "createUser",
		c.admin.CreateUser(c.brokerSrv.addr, "alice", "s3cret", remoting.AuthUserTypeNormal))

	req := c.brokerLog.first(remoting.ReqAuthCreateUser)
	requireAuthExt(t, req, "username", "alice")
	requireAuthBody(t, req, convenienceUserBody)
	if strings.Contains(string(req.Body), "userStatus") {
		t.Errorf("the convenience body must leave userStatus absent: %s", req.Body)
	}

	// The paired control: the same record with a status gains exactly that one
	// key, so the absence above is the unset field and not an encoder that never
	// writes it.
	requireNoError(t, "createUserObject", c.admin.CreateUserObject(c.brokerSrv.addr, &remoting.AuthUser{
		Username: "alice", Password: "s3cret",
		UserType: remoting.AuthUserTypeSuper, UserStatus: remoting.AuthUserStatusDisable,
	}))
	all := c.brokerLog.all(remoting.ReqAuthCreateUser)
	if len(all) != 2 {
		t.Fatalf("recorded %d create-user requests, want 2", len(all))
	}
	requireAuthExt(t, all[1], "username", "alice")
	requireAuthBody(t, all[1], objectUserBody)

	// Nothing else of the family was touched.
	if got := c.brokerLog.codes(); len(got) != 2 {
		t.Fatalf("request codes = %v, want the two create-user calls only", got)
	}
}

// The two write codes are distinct requests, so one must not be able to reach
// the other's handler.
func TestAdminUpdateUserUsesItsOwnCode(t *testing.T) {
	c := authCluster(t)
	requireNoError(t, "createUser",
		c.admin.CreateUser(c.brokerSrv.addr, "alice", "s3cret", remoting.AuthUserTypeNormal))
	requireNoError(t, "updateUser",
		c.admin.UpdateUser(c.brokerSrv.addr, "alice", "new-pass", remoting.AuthUserTypeNormal))
	requireNoError(t, "updateUserObject", c.admin.UpdateUserObject(c.brokerSrv.addr, &remoting.AuthUser{
		Username: "alice", UserStatus: remoting.AuthUserStatusEnable,
	}))

	got := c.brokerLog.codes()
	want := []int32{remoting.ReqAuthCreateUser, remoting.ReqAuthUpdateUser, remoting.ReqAuthUpdateUser}
	if len(got) != len(want) {
		t.Fatalf("request codes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("request %d = %d, want %d", i, got[i], want[i])
		}
	}
	requireAuthBody(t, c.brokerLog.all(remoting.ReqAuthUpdateUser)[0],
		`{"password":"new-pass","userType":"Normal","username":"alice"}`)
	// An update that names only a status sends only that field.
	requireAuthBody(t, c.brokerLog.all(remoting.ReqAuthUpdateUser)[1],
		`{"userStatus":"enable","username":"alice"}`)
}

func TestAdminDeleteUserCarriesOnlyUsernameAndNeverReadsTheAnswer(t *testing.T) {
	c := authCluster(t)
	// The answer this operation must not look at, and it is not even JSON: a
	// void op that tried to decode would fail here.
	c.brokerAnswer[remoting.ReqAuthDeleteUser] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte("not json at all"))
	}
	requireNoError(t, "deleteUser", c.admin.DeleteUser(c.brokerSrv.addr, "alice"))

	req := c.brokerLog.first(remoting.ReqAuthDeleteUser)
	requireAuthExt(t, req, "username", "alice")
	if len(req.Body) != 0 {
		t.Errorf("delete user must be sent without a body, got %q", req.Body)
	}
	// The control for "only the fields that are set": an unset name puts no key
	// on the request at all rather than an empty one.
	requireNoError(t, "delete unnamed", c.admin.DeleteUser(c.brokerSrv.addr, ""))
	requireAuthExt(t, c.brokerLog.all(remoting.ReqAuthDeleteUser)[1])
}

func TestAdminGetUserDecodesTheSingleRecord(t *testing.T) {
	c := authCluster(t)
	c.brokerAnswer[remoting.ReqAuthGetUser] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte(serverUserBody))
	}
	user, err := c.admin.GetUser(c.brokerSrv.addr, "alice")
	requireNoError(t, "getUser", err)
	want := remoting.AuthUser{Username: "alice", Password: "s3cret", UserType: "Super", UserStatus: "enable"}
	if *user != want {
		t.Errorf("decoded user = %+v, want %+v", *user, want)
	}
	requireAuthExt(t, c.brokerLog.first(remoting.ReqAuthGetUser), "username", "alice")

	// Empty-body control: no record is the zero record, not an error and not a
	// half-filled one.
	c.brokerAnswer[remoting.ReqAuthGetUser] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	}
	empty, err := c.admin.GetUser(c.brokerSrv.addr, "ghost")
	requireNoError(t, "getUser with no record", err)
	if *empty != (remoting.AuthUser{}) {
		t.Errorf("empty answer decoded to %+v", *empty)
	}
}

func TestAdminListUserSendsTheFilterOnlyWhenSet(t *testing.T) {
	c := authCluster(t)
	c.brokerAnswer[remoting.ReqAuthListUser] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte(`[{"username":"alice","userType":"Normal","userStatus":"enable"},` +
			`{"username":"alfred","userStatus":"disable"}]`))
	}
	users, err := c.admin.ListUser(c.brokerSrv.addr, "ali")
	requireNoError(t, "listUser", err)
	if len(users) != 2 || users[0].Username != "alice" || users[1].Username != "alfred" {
		t.Fatalf("decoded users = %+v", users)
	}
	if users[1].Password != "" || users[0].Password != "" {
		t.Errorf("a list answer must not invent credentials: %+v", users)
	}
	// The key is `filter`, and it is the only one this request sends.
	requireAuthExt(t, c.brokerLog.first(remoting.ReqAuthListUser), "filter", "ali")

	// Control: no filter means no key, which asks for the whole table; an empty
	// filter value would ask for names that are empty.
	requireNoError(t, "listUser unfiltered", authErr(c.admin.ListUser(c.brokerSrv.addr, "")))
	requireAuthExt(t, c.brokerLog.all(remoting.ReqAuthListUser)[1])

	// Empty-body and empty-array controls both give the empty list.
	for name, body := range map[string][]byte{"no body": nil, "empty array": []byte("[]")} {
		c.brokerAnswer[remoting.ReqAuthListUser] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
			return respOK(body)
		}
		got, err := c.admin.ListUser(c.brokerSrv.addr, "none")
		requireNoError(t, "listUser "+name, err)
		if len(got) != 0 {
			t.Errorf("listUser with %s gave %d users", name, len(got))
		}
	}
}

// --------------------------------------------------------------------- acls

func TestAdminCreateAclConvenienceShapeHasNoPolicyType(t *testing.T) {
	c := authCluster(t)
	requireNoError(t, "createAcl", c.admin.CreateAcl(c.brokerSrv.addr, "Subject:OrderTopic",
		[]string{"topicA", "topicB"}, []string{"Publish"}, []string{"192.168.1.7"},
		remoting.AuthDecisionAllow))

	req := c.brokerLog.first(remoting.ReqAuthCreateACL)
	requireAuthExt(t, req, "subject", "Subject:OrderTopic")
	requireAuthBody(t, req, convenienceAclBody)
	if strings.Contains(string(req.Body), "policyType") {
		t.Errorf("the convenience shape has no policy type: %s", req.Body)
	}
	// The control: the object form does name its policy type, and that single key
	// is the only difference between the two shapes.
	requireNoError(t, "createAclObject", c.admin.CreateAclObject(c.brokerSrv.addr, &remoting.AuthAcl{
		Subject: "Subject:OrderTopic",
		Policies: []*remoting.AuthPolicy{{
			PolicyType: remoting.AuthPolicyTypeCustom,
			Entries:    []*remoting.AuthEntry{{Resource: "topicA", Decision: remoting.AuthDecisionDeny}},
		}},
	}))
	requireAuthBody(t, c.brokerLog.all(remoting.ReqAuthCreateACL)[1],
		`{"policies":[{"entries":[{"decision":"Deny","resource":"topicA"}],"policyType":"Custom"}],`+
			`"subject":"Subject:OrderTopic"}`)
}

func TestAdminUpdateAclUsesItsOwnCode(t *testing.T) {
	c := authCluster(t)
	requireNoError(t, "updateAcl", c.admin.UpdateAcl(c.brokerSrv.addr, "Subject:OrderTopic",
		[]string{"topicA"}, nil, nil, remoting.AuthDecisionDeny))
	requireNoError(t, "updateAclObject", c.admin.UpdateAclObject(c.brokerSrv.addr, &remoting.AuthAcl{
		Subject: "Subject:OrderTopic",
	}))

	all := c.brokerLog.all(remoting.ReqAuthUpdateACL)
	if len(all) != 2 {
		t.Fatalf("AUTH_UPDATE_ACL saw %d requests, want 2: %v", len(all), c.brokerLog.codes())
	}
	if c.brokerLog.count(remoting.ReqAuthCreateACL) != 0 {
		t.Error("an update must not reach the create handler")
	}
	// The convenience shape with no grant data besides the decision: an unset
	// action list stays off the body instead of arriving as `[]`.
	requireAuthBody(t, all[0],
		`{"policies":[{"entries":[{"decision":"Deny","resource":"topicA"}]}],"subject":"Subject:OrderTopic"}`)
	// An update that names only a subject sends only that field.
	requireAuthBody(t, all[1], `{"subject":"Subject:OrderTopic"}`)
}

func TestAdminDeleteAclCarriesSubjectAndResourceAndNothingElse(t *testing.T) {
	c := authCluster(t)
	requireNoError(t, "deleteAcl", c.admin.DeleteAcl(c.brokerSrv.addr, "Subject:OrderTopic", "topicA"))

	req := c.brokerLog.first(remoting.ReqAuthDeleteACL)
	requireAuthExt(t, req, "subject", "Subject:OrderTopic", "resource", "topicA")
	if len(req.Body) != 0 {
		t.Errorf("delete acl must be sent without a body, got %q", req.Body)
	}
	// The controls: no `username`, no `filter`, no `subjectFilter`, no
	// `resourceFilter` and no policy shape can leak into this header, and an
	// unset resource is left out rather than sent empty.
	for _, banned := range []string{"username", "filter", "subjectFilter", "resourceFilter", "policyType"} {
		if hasExt(req, banned) {
			t.Errorf("delete acl header carries %q: %v", banned, req.ExtFields().Keys())
		}
	}
	requireNoError(t, "deleteAcl subject only", c.admin.DeleteAcl(c.brokerSrv.addr, "Subject:B", ""))
	requireAuthExt(t, c.brokerLog.all(remoting.ReqAuthDeleteACL)[1], "subject", "Subject:B")
}

func TestAdminGetAclDecodesTheSingleRecord(t *testing.T) {
	c := authCluster(t)
	c.brokerAnswer[remoting.ReqAuthGetACL] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte(serverAclBody))
	}
	acl, err := c.admin.GetAcl(c.brokerSrv.addr, "Subject:OrderTopic")
	requireNoError(t, "getAcl", err)
	if acl.Subject != "Subject:OrderTopic" || len(acl.Policies) != 1 {
		t.Fatalf("decoded acl = %+v", acl)
	}
	policy := acl.Policies[0]
	if policy.PolicyType != "Custom" || len(policy.Entries) != 1 {
		t.Fatalf("policy = %+v", policy)
	}
	entry := policy.Entries[0]
	if entry.Resource != "topicA" || entry.Decision != "Allow" ||
		len(entry.Actions) != 2 || entry.Actions[1] != "Subscribe" ||
		len(entry.SourceIPs) != 1 || entry.SourceIPs[0] != "192.168.1.7" {
		t.Errorf("entry = %+v", entry)
	}
	requireAuthExt(t, c.brokerLog.first(remoting.ReqAuthGetACL), "subject", "Subject:OrderTopic")

	c.brokerAnswer[remoting.ReqAuthGetACL] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	}
	empty, err := c.admin.GetAcl(c.brokerSrv.addr, "Subject:none")
	requireNoError(t, "getAcl with no record", err)
	if empty.Subject != "" || len(empty.Policies) != 0 {
		t.Errorf("empty answer decoded to %+v", *empty)
	}
}

func TestAdminListAclSendsBothFiltersOrNone(t *testing.T) {
	c := authCluster(t)
	c.brokerAnswer[remoting.ReqAuthListACL] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK([]byte(`[` + serverAclBody + `]`))
	}
	acls, err := c.admin.ListAcl(c.brokerSrv.addr, "Subject:", "topic")
	requireNoError(t, "listAcl", err)
	if len(acls) != 1 || acls[0].Subject != "Subject:OrderTopic" {
		t.Fatalf("decoded acls = %+v", acls)
	}
	// The two filter keys keep their exact names, and they are the only keys.
	requireAuthExt(t, c.brokerLog.first(remoting.ReqAuthListACL),
		"subjectFilter", "Subject:", "resourceFilter", "topic")

	// One-sided control: only the filter that was set appears.
	requireNoError(t, "listAcl subject only", authErr(c.admin.ListAcl(c.brokerSrv.addr, "Subject:", "")))
	requireAuthExt(t, c.brokerLog.all(remoting.ReqAuthListACL)[1], "subjectFilter", "Subject:")

	requireNoError(t, "listAcl unfiltered", authErr(c.admin.ListAcl(c.brokerSrv.addr, "", "")))
	requireAuthExt(t, c.brokerLog.all(remoting.ReqAuthListACL)[2])

	c.brokerAnswer[remoting.ReqAuthListACL] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(nil)
	}
	got, err := c.admin.ListAcl(c.brokerSrv.addr, "", "")
	requireNoError(t, "listAcl with no body", err)
	if len(got) != 0 {
		t.Errorf("empty answer gave %d acls", len(got))
	}
}

// ------------------------------------------------------- addressing, codes

// The auth family keeps the address it is handed: the fast-channel port shift
// that the rest of the admin surface applies is a rewrite of the target, and a
// rewritten target means administering a different broker than the one named.
func TestAdminAuthNeverRewritesTheTargetAddress(t *testing.T) {
	c := authCluster(t)
	c.admin.SetVipChannelEnabled(true)

	requireNoError(t, "createUser on the listening port",
		c.admin.CreateUser(c.brokerSrv.addr, "alice", "s3cret", remoting.AuthUserTypeNormal))
	if got := c.brokerLog.count(remoting.ReqAuthCreateUser); got != 1 {
		t.Fatalf("AUTH_CREATE_USER arrived %d times, want 1 on the port that was passed", got)
	}

	// Control that the switch really is on for this fixture: an admin RPC that
	// does ride the fast channel only lands when its address is moved down.
	if _, err := c.admin.FetchBrokerRuntimeStats(shiftPort(t, c.brokerSrv.addr, 2), 2000); err != nil {
		t.Fatalf("the runtime-stats read should have been shifted onto the stub: %v", err)
	}

	// And the auth request itself is not shifted: an address two ports above the
	// listener has nothing on it, so the call must fail rather than arrive.
	unreachable := shiftPort(t, c.brokerSrv.addr, 2)
	if err := c.admin.DeleteUser(unreachable, "alice"); err == nil {
		t.Fatal("a request aimed at a port nothing listens on must fail")
	}
	if got := c.brokerLog.count(remoting.ReqAuthDeleteUser); got != 0 {
		t.Errorf("AUTH_DELETE_USER arrived %d times on an address that was never listened on", got)
	}

	// Same discipline for every read of the family.
	requireNoError(t, "getUser on the listening port", authErr(
		c.admin.GetUser(c.brokerSrv.addr, "alice")))
	if got := c.brokerLog.count(remoting.ReqAuthGetUser); got != 1 {
		t.Errorf("AUTH_GET_USER arrived %d times, want 1", got)
	}
}

// The family is broker-directed by design: no route lookup, no nameserver hop.
func TestAdminAuthDoesNotConsultTheNameServer(t *testing.T) {
	c := authCluster(t)
	before := len(c.nsLog.snapshot())

	requireNoError(t, "createUser", c.admin.CreateUser(c.brokerSrv.addr, "alice", "p", remoting.AuthUserTypeNormal))
	requireNoError(t, "getUser", authErr(c.admin.GetUser(c.brokerSrv.addr, "alice")))
	requireNoError(t, "listUser", authErr(c.admin.ListUser(c.brokerSrv.addr, "")))
	requireNoError(t, "createAcl", c.admin.CreateAcl(c.brokerSrv.addr, "Subject:S", []string{"t"}, nil, nil, remoting.AuthDecisionAllow))
	requireNoError(t, "getAcl", authErr(c.admin.GetAcl(c.brokerSrv.addr, "Subject:S")))
	requireNoError(t, "listAcl", authErr(c.admin.ListAcl(c.brokerSrv.addr, "", "")))
	requireNoError(t, "deleteAcl", c.admin.DeleteAcl(c.brokerSrv.addr, "Subject:S", "t"))

	if after := len(c.nsLog.snapshot()); after != before {
		t.Errorf("the nameserver saw %d requests on top of the %d from setup: these RPCs are broker-directed", after-before, before)
	}
	for _, code := range authCodes {
		if got := c.nsLog.count(code); got != 0 {
			t.Errorf("the nameserver received %d requests of code %d", got, code)
		}
	}
}

// A rejected change is a rejection: only SUCCESS is accepted, and the code and
// remark must survive rather than decode into a plausible-looking zero.
func TestAdminAuthAcceptsOnlySuccess(t *testing.T) {
	c := authCluster(t)

	c.brokerAnswer[remoting.ReqAuthCreateUser] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "user alice already exists")
	}
	err := c.admin.CreateUser(c.brokerSrv.addr, "alice", "p", remoting.AuthUserTypeNormal)
	if err == nil {
		t.Fatal("a rejected create must fail the call")
	}
	if code, ok := errCodeOf(err); !ok || code != remoting.RespSystemError {
		t.Errorf("the broker code must survive, got %v (ok=%v)", err, ok)
	}
	if !strings.Contains(err.Error(), "user alice already exists") {
		t.Errorf("the broker remark must reach the caller, got %v", err)
	}

	// The read side: the rejection must not be swallowed into a zero record.
	c.brokerAnswer[remoting.ReqAuthGetUser] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespNoPermission, "not authorized to read users")
	}
	user, err := c.admin.GetUser(c.brokerSrv.addr, "alice")
	if err == nil {
		t.Fatal("a rejected read must fail the call")
	}
	if user != nil {
		t.Errorf("no record may be handed back beside an error: %+v", *user)
	}
	if code, ok := errCodeOf(err); !ok || code != remoting.RespNoPermission {
		t.Errorf("the read lost its broker code: %v", err)
	}

	// Every code of the family rejects, not just the two probed above.
	for _, code := range authCodes {
		if code == remoting.ReqAuthCreateUser || code == remoting.ReqAuthGetUser {
			continue
		}
		c.brokerAnswer[code] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
			return okCode(remoting.RespRequestCodeNotSupported, "auth management is disabled")
		}
	}
	calls := map[string]func() error{
		"updateUser": func() error { return c.admin.UpdateUser(c.brokerSrv.addr, "a", "p", remoting.AuthUserTypeNormal) },
		"deleteUser": func() error { return c.admin.DeleteUser(c.brokerSrv.addr, "a") },
		"listUser":   func() error { _, err := c.admin.ListUser(c.brokerSrv.addr, ""); return err },
		"createAcl": func() error {
			return c.admin.CreateAcl(c.brokerSrv.addr, "Subject:a", []string{"t"}, nil, nil, remoting.AuthDecisionAllow)
		},
		"updateAcl": func() error {
			return c.admin.UpdateAcl(c.brokerSrv.addr, "Subject:a", []string{"t"}, nil, nil, remoting.AuthDecisionAllow)
		},
		"deleteAcl": func() error { return c.admin.DeleteAcl(c.brokerSrv.addr, "Subject:a", "t") },
		"getAcl":    func() error { _, err := c.admin.GetAcl(c.brokerSrv.addr, "Subject:a"); return err },
		"listAcl":   func() error { _, err := c.admin.ListAcl(c.brokerSrv.addr, "", ""); return err },
	}
	for name, call := range calls {
		if err := call(); err == nil {
			t.Errorf("%s accepted a non-SUCCESS answer", name)
		}
	}
}

// A nil record is refused before anything leaves, so a caller cannot wipe a
// broker's user table by handing over an empty pointer.
func TestAdminAuthRejectsNilRecordsWithoutSending(t *testing.T) {
	c := authCluster(t)
	for name, call := range map[string]func() error{
		"CreateUserObject": func() error { return c.admin.CreateUserObject(c.brokerSrv.addr, nil) },
		"UpdateUserObject": func() error { return c.admin.UpdateUserObject(c.brokerSrv.addr, nil) },
		"CreateAclObject":  func() error { return c.admin.CreateAclObject(c.brokerSrv.addr, nil) },
		"UpdateAclObject":  func() error { return c.admin.UpdateAclObject(c.brokerSrv.addr, nil) },
	} {
		if err := call(); err == nil {
			t.Errorf("%s accepted a nil record", name)
		}
	}
	if got := len(c.brokerLog.snapshot()); got != 0 {
		t.Errorf("a refused call sent %d requests, want none", got)
	}
}
