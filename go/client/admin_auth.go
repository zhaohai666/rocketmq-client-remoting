// Auth user and ACL management: the 3001..3010 family of the admin surface.
//
// Two rules bind every method here, and the first is why none of them can ride
// `invokeBroker`:
//   - the request goes to the address the caller passed, verbatim — no
//     fast-channel port shift (invokeBrokerPlain, not invokeBroker), no master
//     selection and no nameserver indirection. The caller aims at one broker on
//     purpose, so moving or replacing that address would write the change to a
//     different server than the one being administered;
//   - SUCCESS is the only accepted answer. Any other code is the broker's own
//     rejection and travels up as an error carrying its code and remark; a
//     "partial success" reading would let a rejected user or ACL change look
//     applied.
package client

import (
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// authHeader builds one auth-management request's extFields from key/value
// pairs. Every field of this family is optional on the wire and an unset one is
// left out entirely — never sent empty, never sent as "null" — so a request
// carries exactly the keys that name its target.
func authHeader(kv ...string) *common.StringMap {
	out := common.NewStringMap()
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i+1] != "" {
			out.Put(kv[i], kv[i+1])
		}
	}
	return out
}

// invokeAuth sends one auth-management request to `brokerAddr` unchanged.
func (a *DefaultMQAdminExt) invokeAuth(brokerAddr string, code int32,
	ext *common.StringMap, body []byte) (*remoting.RemotingCommand, error) {

	return a.invokeBrokerPlain(brokerAddr, code, ext, body, 0)
}

// ---------------- user management ----------------

// CreateUser sends AUTH_CREATE_USER(3001) for one user record. The status is
// left unset, so the body carries no `userStatus` key and the broker applies
// its own.
func (a *DefaultMQAdminExt) CreateUser(brokerAddr, username, password, userType string) error {
	return a.CreateUserObject(brokerAddr, remoting.NewAuthUser(username, password, userType))
}

// CreateUserObject is the object form of AUTH_CREATE_USER(3001): the caller
// supplies the whole record, including a status.
func (a *DefaultMQAdminExt) CreateUserObject(brokerAddr string, user *remoting.AuthUser) error {
	if user == nil {
		return common.ClientError("createUserObject: user must not be nil")
	}
	_, err := a.invokeAuth(brokerAddr, remoting.ReqAuthCreateUser,
		authHeader("username", user.Username), user.Encode())
	return err
}

// UpdateUser sends AUTH_UPDATE_USER(3002).
func (a *DefaultMQAdminExt) UpdateUser(brokerAddr, username, password, userType string) error {
	return a.UpdateUserObject(brokerAddr, remoting.NewAuthUser(username, password, userType))
}

// UpdateUserObject is the object form of AUTH_UPDATE_USER(3002).
func (a *DefaultMQAdminExt) UpdateUserObject(brokerAddr string, user *remoting.AuthUser) error {
	if user == nil {
		return common.ClientError("updateUserObject: user must not be nil")
	}
	_, err := a.invokeAuth(brokerAddr, remoting.ReqAuthUpdateUser,
		authHeader("username", user.Username), user.Encode())
	return err
}

// DeleteUser sends AUTH_DELETE_USER(3003). The answer's body is not read: this
// either took effect or was rejected, and there is nothing to decode in between.
func (a *DefaultMQAdminExt) DeleteUser(brokerAddr, username string) error {
	_, err := a.invokeAuth(brokerAddr, remoting.ReqAuthDeleteUser,
		authHeader("username", username), nil)
	return err
}

// GetUser sends AUTH_GET_USER(3004) and decodes the single user object.
func (a *DefaultMQAdminExt) GetUser(brokerAddr, username string) (*remoting.AuthUser, error) {
	response, err := a.invokeAuth(brokerAddr, remoting.ReqAuthGetUser,
		authHeader("username", username), nil)
	if err != nil {
		return nil, err
	}
	return remoting.DecodeAuthUser(response.Body)
}

// ListUser sends AUTH_LIST_USER(3005). `filter` is a prefix the broker
// matches against the user names; an empty filter is left off the request
// rather than sent as "", which asks for the whole table.
func (a *DefaultMQAdminExt) ListUser(brokerAddr, filter string) ([]*remoting.AuthUser, error) {
	response, err := a.invokeAuth(brokerAddr, remoting.ReqAuthListUser,
		authHeader("filter", filter), nil)
	if err != nil {
		return nil, err
	}
	return remoting.DecodeAuthUserList(response.Body)
}

// ---------------- ACL management ----------------

// CreateAcl sends AUTH_CREATE_ACL(3006) in its convenience shape: one policy of
// no policy type, holding one entry per resource and repeating the same actions,
// source IPs and decision in every entry.
func (a *DefaultMQAdminExt) CreateAcl(brokerAddr, subject string,
	resources, actions, sourceIPs []string, decision string) error {
	return a.CreateAclObject(brokerAddr,
		remoting.NewAuthAcl(subject, resources, actions, sourceIPs, decision))
}

// CreateAclObject is the object form of AUTH_CREATE_ACL(3006): the caller
// supplies the whole acl, including policy types and several policies.
func (a *DefaultMQAdminExt) CreateAclObject(brokerAddr string, acl *remoting.AuthAcl) error {
	if acl == nil {
		return common.ClientError("createAclObject: acl must not be nil")
	}
	_, err := a.invokeAuth(brokerAddr, remoting.ReqAuthCreateACL,
		authHeader("subject", acl.Subject), acl.Encode())
	return err
}

// UpdateAcl sends AUTH_UPDATE_ACL(3007) in its convenience shape.
func (a *DefaultMQAdminExt) UpdateAcl(brokerAddr, subject string,
	resources, actions, sourceIPs []string, decision string) error {
	return a.UpdateAclObject(brokerAddr,
		remoting.NewAuthAcl(subject, resources, actions, sourceIPs, decision))
}

// UpdateAclObject is the object form of AUTH_UPDATE_ACL(3007).
func (a *DefaultMQAdminExt) UpdateAclObject(brokerAddr string, acl *remoting.AuthAcl) error {
	if acl == nil {
		return common.ClientError("updateAclObject: acl must not be nil")
	}
	_, err := a.invokeAuth(brokerAddr, remoting.ReqAuthUpdateACL,
		authHeader("subject", acl.Subject), acl.Encode())
	return err
}

// DeleteAcl revokes one resource of one subject via AUTH_DELETE_ACL(3008). The
// request carries `subject` and `resource` and nothing else.
func (a *DefaultMQAdminExt) DeleteAcl(brokerAddr, subject, resource string) error {
	_, err := a.invokeAuth(brokerAddr, remoting.ReqAuthDeleteACL,
		authHeader("subject", subject, "resource", resource), nil)
	return err
}

// GetAcl sends AUTH_GET_ACL(3009) and decodes the single acl object.
func (a *DefaultMQAdminExt) GetAcl(brokerAddr, subject string) (*remoting.AuthAcl, error) {
	response, err := a.invokeAuth(brokerAddr, remoting.ReqAuthGetACL,
		authHeader("subject", subject), nil)
	if err != nil {
		return nil, err
	}
	return remoting.DecodeAuthAcl(response.Body)
}

// ListAcl sends AUTH_LIST_ACL(3010). Both filters are optional: an unset one is
// left off the request, so passing "" for either widens the answer instead of
// narrowing it to nothing.
func (a *DefaultMQAdminExt) ListAcl(brokerAddr, subjectFilter, resourceFilter string) ([]*remoting.AuthAcl, error) {
	response, err := a.invokeAuth(brokerAddr, remoting.ReqAuthListACL,
		authHeader("subjectFilter", subjectFilter, "resourceFilter", resourceFilter), nil)
	if err != nil {
		return nil, err
	}
	return remoting.DecodeAuthAclList(response.Body)
}
