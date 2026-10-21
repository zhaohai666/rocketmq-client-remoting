<?php

declare(strict_types=1);

/**
 * Offline self-test for the user and access-control management RPCs of the admin client
 * (request codes 3001-3010) -- no cluster, no socket, no sleeping.
 *
 * Everything is asserted on RECORDED REQUEST STATE: a transport double captures the address, the
 * command (code, extension fields, body) and the timeout of every call it is handed, and answers a
 * scripted response. Each positive assertion is paired with a control that provably goes red when
 * the behaviour is reverted -- so a key written as null, a stray field on a request that must carry
 * only two, an accepted error code, or a decoder that guesses at the body shape cannot survive.
 *
 * Two timing rules this suite obeys:
 * - Extension fields are asserted after the header has actually been encoded, because that is the
 *   moment they are materialised; reading them before would assert the argument list, not the wire.
 * - A request built without a header object is told apart by "customHeader === null", never by an
 *   empty extension-field map, which the encoder would happily produce either way.
 *
 * Run: php tests/RunClientAuthAdmin.php
 * On success prints "ALL TESTS PASSED (N checks)"; any failure lists details and exits non-zero.
 */

// Pinned before anything can log: a suite must never grow a log tree in the repository or under the
// account's home directory.
putenv('ROCKETMQ_CLIENT_LOG_DIR=' . sys_get_temp_dir() . '/rmq_php_auth_suite_logs_' . getmypid());

require_once __DIR__ . '/../bootstrap.php';

use RocketMQ\Client\DefaultMQAdminExt;
use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\MQClientInstance;
use RocketMQ\Remoting\Protocol\AuthAccessEntry;
use RocketMQ\Remoting\Protocol\AuthAclInfo;
use RocketMQ\Remoting\Protocol\AuthDecision;
use RocketMQ\Remoting\Protocol\AuthPolicy;
use RocketMQ\Remoting\Protocol\AuthPolicyType;
use RocketMQ\Remoting\Protocol\AuthUserInfo;
use RocketMQ\Remoting\Protocol\AuthUserStatus;
use RocketMQ\Remoting\Protocol\AuthUserType;
use RocketMQ\Remoting\Protocol\ClusterInfo;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;

// ==================================================================== doubles

/** Admin that records every synchronous call instead of putting it on the wire. */
final class FakeAuthAdmin extends DefaultMQAdminExt
{
    /** @var list<array{addr: string, cmd: RemotingCommand, timeout: int|null}> */
    public array $requests = [];

    /** @var callable(RemotingCommand, string): ?RemotingCommand|null */
    public $handler = null;

    protected function invokeSyncOn(MQClientInstance $client, string $addr, RemotingCommand $request, ?int $timeoutMillis): RemotingCommand
    {
        $this->requests[] = ['addr' => $addr, 'cmd' => $request, 'timeout' => $timeoutMillis];
        $h = $this->handler;
        $resp = $h !== null ? $h($request, $addr) : null;
        return $resp ?? RemotingCommand::createResponseCommand(ResponseCode::SUCCESS);
    }

    /** @return array{addr: string, cmd: RemotingCommand, timeout: int|null} */
    public function last(): array
    {
        return $this->requests[count($this->requests) - 1];
    }
}

/**
 * Client double whose name-server side is out of reach on purpose: the management calls must be
 * answered by the broker address the caller spelled out, so a route lookup or a cluster query
 * would both show up in these counters and hang on the empty address list.
 */
final class FakeAuthClient extends MQClientInstance
{
    public int $clusterInfoCalls = 0;
    /** @var list<string> */
    public array $routeRefreshes = [];

    public function __construct()
    {
        parent::__construct('auth-admin-test', []);
    }

    public function getBrokerClusterInfo(int $timeoutMillis = 10000): ClusterInfo
    {
        $this->clusterInfoCalls++;
        return new ClusterInfo();
    }

    public function updateTopicRouteInfoFromNameServer(string $topic, ?int $timeoutMillis = null, bool $isDefault = false): bool
    {
        $this->routeRefreshes[] = $topic;
        return false;
    }
}

// ==================================================================== check plumbing

$checks = 0;
$failures = [];

$ck = static function (bool $cond, string $label) use (&$checks, &$failures): void {
    $checks++;
    if (!$cond) {
        $failures[] = $label;
        fwrite(STDOUT, "FAIL: {$label}\n");
    }
};

$ckSame = static function (mixed $expected, mixed $actual, string $label) use (&$checks, &$failures): void {
    $checks++;
    if ($expected !== $actual) {
        $failures[] = sprintf('%s expected=%s actual=%s', $label, var_export($expected, true), var_export($actual, true));
        fwrite(STDOUT, sprintf("FAIL: %s expected=%s actual=%s\n", $label, var_export($expected, true), var_export($actual, true)));
    }
};

/**
 * The extension fields as they leave the process: the header encoder is what materialises them,
 * so they are read back out of the encoded header instead of off the in-memory command.
 *
 * @return array<string, string>
 */
$wireExt = static function (RemotingCommand $cmd): array {
    $header = json_decode($cmd->headerEncode(), true);
    return $header['extFields'] ?? [];
};

/** @return list<string> */
$wireKeys = static function (RemotingCommand $cmd) use ($wireExt): array {
    return array_keys($wireExt($cmd));
};

/** Builds an admin whose transport answers with $body / $code and records every call. */
$newAdmin = static function (?string $body = null, int $code = ResponseCode::SUCCESS, string $remark = ''): array {
    $client = new FakeAuthClient();
    $admin = new FakeAuthAdmin();
    $admin->setMqClientInstance($client);
    if ($body !== null || $code !== ResponseCode::SUCCESS) {
        $admin->handler = static function () use ($body, $code, $remark): RemotingCommand {
            $resp = RemotingCommand::createResponseCommand($code, $remark);
            $resp->body = $body;
            return $resp;
        };
    }
    return [$admin, $client];
};

$ADDR = '10.0.0.9:10911';

// ==================================================================== 1. spelling tables

{
    // The literals are matched byte for byte by the store, and their case is deliberately not
    // uniform across the four tables -- an "improved" spelling is a wire break.
    $ckSame('Super', AuthUserType::SUPER, 'userType.Super');
    $ckSame('Normal', AuthUserType::NORMAL, 'userType.Normal');
    $ckSame('enable', AuthUserStatus::ENABLE, 'userStatus.enable lower-case');
    $ckSame('disable', AuthUserStatus::DISABLE, 'userStatus.disable lower-case');
    $ckSame('Custom', AuthPolicyType::CUSTOM, 'policyType.Custom');
    $ckSame('Default', AuthPolicyType::DEFAULT, 'policyType.Default');
    $ckSame('Allow', AuthDecision::ALLOW, 'decision.Allow');
    $ckSame('Deny', AuthDecision::DENY, 'decision.Deny');
    $ck(AuthUserStatus::ENABLE !== 'Enable' && AuthDecision::DENY !== 'deny',
        'status/decision spellings are not normalised to each other');
}

// ==================================================================== 2. request codes

{
    $ckSame(3001, RequestCode::AUTH_CREATE_USER, 'AUTH_CREATE_USER=3001');
    $ckSame(3002, RequestCode::AUTH_UPDATE_USER, 'AUTH_UPDATE_USER=3002');
    $ckSame(3003, RequestCode::AUTH_DELETE_USER, 'AUTH_DELETE_USER=3003');
    $ckSame(3004, RequestCode::AUTH_GET_USER, 'AUTH_GET_USER=3004');
    $ckSame(3005, RequestCode::AUTH_LIST_USER, 'AUTH_LIST_USER=3005');
    $ckSame(3006, RequestCode::AUTH_CREATE_ACL, 'AUTH_CREATE_ACL=3006');
    $ckSame(3007, RequestCode::AUTH_UPDATE_ACL, 'AUTH_UPDATE_ACL=3007');
    $ckSame(3008, RequestCode::AUTH_DELETE_ACL, 'AUTH_DELETE_ACL=3008');
    $ckSame(3009, RequestCode::AUTH_GET_ACL, 'AUTH_GET_ACL=3009');
    $ckSame(3010, RequestCode::AUTH_LIST_ACL, 'AUTH_LIST_ACL=3010');
}

// ==================================================================== 3. create / update user

{
    [$admin] = $newAdmin();
    $admin->createUser($ADDR, 'rocketmq', 'secret', AuthUserType::NORMAL);
    $rec = $admin->last();
    $ckSame(3001, $rec['cmd']->code, 'createUser routes to 3001');
    $ckSame(['username' => 'rocketmq'], $wireExt($rec['cmd']), 'createUser header carries exactly username');
    // The convenience form never says anything about the status: the key must be absent, not null.
    $ckSame('{"username":"rocketmq","password":"secret","userType":"Normal"}', $rec['cmd']->body,
        'createUser body omits the unset userStatus');
    $ck(!str_contains((string) $rec['cmd']->body, 'userStatus'), 'createUser convenience body has no userStatus key');
    $ck(!str_contains((string) $rec['cmd']->body, 'null'), 'createUser body carries no null literal');
    $ck($rec['cmd']->customHeader === null, 'createUser sends no header object');

    [$admin2] = $newAdmin();
    $admin2->updateUser($ADDR, new AuthUserInfo('rocketmq', 'rotated', AuthUserType::SUPER, AuthUserStatus::DISABLE));
    $rec2 = $admin2->last();
    $ckSame(3002, $rec2['cmd']->code, 'updateUser routes to 3002');
    $ckSame(['username' => 'rocketmq'], $wireExt($rec2['cmd']), 'updateUser header carries exactly username');
    $ckSame('{"username":"rocketmq","password":"rotated","userType":"Super","userStatus":"disable"}', $rec2['cmd']->body,
        'updateUser object form writes all four attributes');
    $ck(str_contains((string) $rec2['cmd']->body, '"userStatus":"disable"'),
        'updateUser keeps a status the caller spelled out');
}

// ==================================================================== 4. delete / get / list user

{
    [$admin] = $newAdmin();
    $admin->deleteUser($ADDR, 'rocketmq');
    $rec = $admin->last();
    $ckSame(3003, $rec['cmd']->code, 'deleteUser routes to 3003');
    $ckSame(['username' => 'rocketmq'], $wireExt($rec['cmd']), 'deleteUser header carries exactly username');
    $ck($rec['cmd']->body === null, 'deleteUser sends no body');

    $oneUser = '{"username":"rocketmq","password":"secret","userType":"Normal","userStatus":"enable"}';
    [$adminGet] = $newAdmin($oneUser);
    $user = $adminGet->getUser($ADDR, 'rocketmq');
    $ckSame(3004, $adminGet->last()['cmd']->code, 'getUser routes to 3004');
    $ck($user instanceof AuthUserInfo, 'getUser decodes one user object');
    if ($user instanceof AuthUserInfo) {
        $ckSame('rocketmq', $user->username, 'getUser keeps username');
        $ckSame('secret', $user->password, 'getUser keeps password');
        $ckSame(AuthUserType::NORMAL, $user->userType, 'getUser keeps userType');
        $ckSame(AuthUserStatus::ENABLE, $user->userStatus, 'getUser keeps userStatus');
    }

    // Control for the decoder: no body means no user, never an empty object dressed up as one.
    [$adminEmpty] = $newAdmin('');
    $ck($adminEmpty->getUser($ADDR, 'ghost') === null, 'getUser on an empty body yields null');
    [$adminNullBody] = $newAdmin(null);
    $ck($adminNullBody->getUser($ADDR, 'ghost') === null, 'getUser on an absent body yields null');

    $twoUsers = '[{"username":"a","userType":"Super","userStatus":"enable"},'
        . '{"username":"b","userType":"Normal","userStatus":"disable"}]';
    [$adminList] = $newAdmin($twoUsers);
    $users = $adminList->listUser($ADDR, 'rc');
    $ckSame(3005, $adminList->last()['cmd']->code, 'listUser routes to 3005');
    $ckSame(['filter' => 'rc'], $wireExt($adminList->last()['cmd']), 'listUser header carries exactly filter');
    $ckSame(2, count($users), 'listUser decodes every element of the array body');
    $ckSame(['a', 'b'], array_map(static fn(AuthUserInfo $u): ?string => $u->username, $users),
        'listUser preserves the order of the array body');
    $ck($users !== [] && $users[0]->userStatus === AuthUserStatus::ENABLE, 'listUser decodes nested status');

    // An object-shaped body is one user, not a list of the object's field values.
    [$adminObject] = $newAdmin($oneUser);
    $asList = $adminObject->listUser($ADDR, 'rc');
    $ckSame(1, count($asList), 'listUser reads an object body as one user');
    $ckSame('rocketmq', $asList[0]->username ?? null, 'listUser object body keeps the username field');

    [$adminNoBody] = $newAdmin('');
    $ckSame([], $adminNoBody->listUser($ADDR, 'rc'), 'listUser on an empty body yields an empty list');
}

// ==================================================================== 5. unset filters are absent

{
    [$admin] = $newAdmin('[]');
    $admin->listUser($ADDR);
    $ckSame([], $wireExt($admin->last()['cmd']), 'listUser without a filter sends no extension fields');
    $ckSame([], $wireKeys($admin->last()['cmd']), 'listUser without a filter sends no keys at all');

    [$admin2] = $newAdmin('[]');
    $admin2->listUser($ADDR, '');
    $ck(!array_key_exists('filter', $wireExt($admin2->last()['cmd'])),
        'an empty filter is dropped rather than sent as an empty value');
    $ck(!in_array('', $wireExt($admin2->last()['cmd']), true), 'no extension field travels as an empty string');

    [$admin3] = $newAdmin();
    $admin3->deleteUser($ADDR, '');
    $ckSame([], $wireExt($admin3->last()['cmd']), 'deleteUser with an empty username sends no username key');
}

// ==================================================================== 6. create / update acl

{
    [$admin] = $newAdmin();
    $admin->createAcl($ADDR, 'rocketmq', ['TopicA@*', 'GroupB@*'], ['Pub', 'Sub'], ['192.168.1.0/24'], AuthDecision::ALLOW);
    $rec = $admin->last();
    $ckSame(3006, $rec['cmd']->code, 'createAcl routes to 3006');
    $ckSame(['subject' => 'rocketmq'], $wireExt($rec['cmd']), 'createAcl header carries exactly subject');
    $ckSame(
        '{"subject":"rocketmq","policies":[{"entries":['
        . '{"resource":"TopicA@*","actions":["Pub","Sub"],"sourceIps":["192.168.1.0/24"],"decision":"Allow"},'
        . '{"resource":"GroupB@*","actions":["Pub","Sub"],"sourceIps":["192.168.1.0/24"],"decision":"Allow"}'
        . ']}]}',
        $rec['cmd']->body,
        'createAcl convenience body is one policy with one entry per resource',
    );
    $ck(!str_contains((string) $rec['cmd']->body, 'policyType'),
        'the convenience policy carries no policy type key');
    $ck(str_contains((string) $rec['cmd']->body, '"decision":"Allow"'), 'entry keeps the exact decision spelling');

    [$admin2] = $newAdmin();
    $admin2->updateAcl($ADDR, new AuthAclInfo('rocketmq', [
        new AuthPolicy(AuthPolicyType::CUSTOM, [new AuthAccessEntry('TopicA@*', ['Sub'], ['10.0.0.0/8'], AuthDecision::DENY)]),
        new AuthPolicy(AuthPolicyType::DEFAULT, [new AuthAccessEntry('GroupB@*', null, null, AuthDecision::ALLOW)]),
    ]));
    $rec2 = $admin2->last();
    $ckSame(3007, $rec2['cmd']->code, 'updateAcl routes to 3007');
    $ckSame(['subject' => 'rocketmq'], $wireExt($rec2['cmd']), 'updateAcl header carries exactly subject');
    $ckSame(
        '{"subject":"rocketmq","policies":['
        . '{"policyType":"Custom","entries":[{"resource":"TopicA@*","actions":["Sub"],"sourceIps":["10.0.0.0/8"],"decision":"Deny"}]},'
        . '{"policyType":"Default","entries":[{"resource":"GroupB@*","decision":"Allow"}]}'
        . ']}',
        $rec2['cmd']->body,
        'updateAcl object form writes every policy with its own type',
    );
    $ck(str_contains((string) $rec2['cmd']->body, '"policyType":"Custom"'),
        'a policy type the caller supplied is written first inside the policy');
    $ck(!str_contains((string) $rec2['cmd']->body, '"actions":null'),
        'an entry without actions omits the key instead of writing null');
}

// ==================================================================== 7. delete / get / list acl

{
    [$admin] = $newAdmin();
    $admin->deleteAcl($ADDR, 'rocketmq', 'TopicA@*');
    $rec = $admin->last();
    $ckSame(3008, $rec['cmd']->code, 'deleteAcl routes to 3008');
    // Nothing else may ride along: this request identifies the rule by exactly these two fields.
    $ckSame(['subject', 'resource'], $wireKeys($rec['cmd']), 'deleteAcl header is exactly subject then resource');
    $ckSame(['subject' => 'rocketmq', 'resource' => 'TopicA@*'], $wireExt($rec['cmd']), 'deleteAcl header values');
    $ck(!array_key_exists('policyType', $wireExt($rec['cmd'])), 'deleteAcl header carries no policy type');
    $ck(!array_key_exists('actions', $wireExt($rec['cmd'])), 'deleteAcl header carries no actions');
    $ck($rec['cmd']->body === null, 'deleteAcl sends no body');

    [$admin2] = $newAdmin();
    $admin2->deleteAcl($ADDR, 'rocketmq');
    $ckSame(['subject'], $wireKeys($admin2->last()['cmd']), 'deleteAcl without a resource sends only the subject');

    $oneAcl = '{"subject":"rocketmq","policies":[{"policyType":"Custom","entries":['
        . '{"resource":"TopicA@*","actions":["Pub","Sub"],"sourceIps":["192.168.1.0/24"],"decision":"Allow"}]}]}';
    [$adminGet] = $newAdmin($oneAcl);
    $acl = $adminGet->getAcl($ADDR, 'rocketmq');
    $ckSame(3009, $adminGet->last()['cmd']->code, 'getAcl routes to 3009');
    $ckSame(['subject' => 'rocketmq'], $wireExt($adminGet->last()['cmd']), 'getAcl header carries exactly subject');
    $ck($acl instanceof AuthAclInfo, 'getAcl decodes one acl object');
    if ($acl instanceof AuthAclInfo) {
        $ckSame('rocketmq', $acl->subject, 'getAcl keeps the subject');
        $ckSame(1, count($acl->policies), 'getAcl keeps the policy list');
        $ckSame(AuthPolicyType::CUSTOM, $acl->policies[0]->policyType ?? null, 'getAcl keeps the policy type');
        $entry = $acl->policies[0]->entries[0] ?? null;
        $ck($entry instanceof AuthAccessEntry, 'getAcl keeps the entry list');
        if ($entry instanceof AuthAccessEntry) {
            $ckSame('TopicA@*', $entry->resource, 'getAcl keeps the resource');
            $ckSame(['Pub', 'Sub'], $entry->actions, 'getAcl keeps the action list');
            $ckSame(['192.168.1.0/24'], $entry->sourceIps, 'getAcl keeps the source ip list');
            $ckSame(AuthDecision::ALLOW, $entry->decision, 'getAcl keeps the decision');
        }
    }

    [$adminEmpty] = $newAdmin('');
    $ck($adminEmpty->getAcl($ADDR, 'nobody') === null, 'getAcl on an empty body yields null');

    $twoAcls = '[{"subject":"u1","policies":[{"policyType":"Default","entries":[{"resource":"T","decision":"Allow"}]}]},'
        . '{"subject":"u2","policies":[]}]';
    [$adminList] = $newAdmin($twoAcls);
    $acls = $adminList->listAcl($ADDR, 'u', 'T');
    $ckSame(3010, $adminList->last()['cmd']->code, 'listAcl routes to 3010');
    $ckSame(['subjectFilter', 'resourceFilter'], $wireKeys($adminList->last()['cmd']),
        'listAcl header is exactly the two filters');
    $ckSame(2, count($acls), 'listAcl decodes every element of the array body');
    $ckSame(['u1', 'u2'], array_map(static fn(AuthAclInfo $a): ?string => $a->subject, $acls),
        'listAcl preserves the order of the array body');
    $ckSame(AuthPolicyType::DEFAULT, $acls[0]->policies[0]->policyType ?? null, 'listAcl decodes nested policies');

    // An object-shaped body is one acl, never a list built from the object's values.
    [$adminObject] = $newAdmin($oneAcl);
    $singleAsList = $adminObject->listAcl($ADDR, 'u');
    $ckSame(1, count($singleAsList), 'listAcl reads an object body as one acl');
    $ckSame('rocketmq', $singleAsList[0]->subject ?? null, 'listAcl object body keeps the subject');

    [$adminNoBody] = $newAdmin('');
    $ckSame([], $adminNoBody->listAcl($ADDR), 'listAcl on an empty body yields an empty list');

    [$adminFilters] = $newAdmin('[]');
    $adminFilters->listAcl($ADDR, 'u');
    $ckSame(['subjectFilter'], $wireKeys($adminFilters->last()['cmd']),
        'listAcl without a resource filter sends only the subject filter');
    $adminFilters->listAcl($ADDR);
    $ckSame([], $wireExt($adminFilters->last()['cmd']), 'listAcl without filters sends no extension fields');
}

// ==================================================================== 8. addressing and timeout

{
    // The VIP switch is the one thing that would silently rewrite a broker address, so it is ON for
    // the whole sweep and every recorded address must still be the caller's own.
    [$admin, $client] = $newAdmin('[]');
    $admin->setVipChannelEnabled(true);
    $admin->createUser($ADDR, 'u', 'p', AuthUserType::NORMAL);
    $admin->updateUser($ADDR, 'u', 'p', AuthUserType::NORMAL);
    $admin->deleteUser($ADDR, 'u');
    $admin->getUser($ADDR, 'u');
    $admin->listUser($ADDR, 'u');
    $admin->createAcl($ADDR, 'u', ['T'], ['Pub'], ['127.0.0.1'], AuthDecision::ALLOW);
    $admin->updateAcl($ADDR, 'u', ['T'], ['Pub'], ['127.0.0.1'], AuthDecision::DENY);
    $admin->deleteAcl($ADDR, 'u', 'T');
    $admin->getAcl($ADDR, 'u');
    $admin->listAcl($ADDR, 'u', 'T');

    $ckSame(10, count($admin->requests), 'the sweep issues exactly one request per management call');
    $codes = [];
    foreach ($admin->requests as $rec) {
        $codes[] = $rec['cmd']->code;
        $ck($rec['addr'] === $ADDR, sprintf('address %s reaches the transport unchanged for code %d', $rec['addr'], $rec['cmd']->code));
        $ck($rec['cmd']->customHeader === null, sprintf('code %d carries no header object', $rec['cmd']->code));
        // A headerless request still has to be encodable: the extension fields are written at encode time.
        $ck(is_array(json_decode($rec['cmd']->headerEncode(), true)), sprintf('code %d encodes a header', $rec['cmd']->code));
    }
    $ckSame([3001, 3002, 3003, 3004, 3005, 3006, 3007, 3008, 3009, 3010], $codes,
        'the ten management codes are issued in order');
    // No indirection: neither the cluster table nor a route refresh is consulted for a given address.
    $ckSame(0, $client->clusterInfoCalls, 'management calls never ask for the cluster info');
    $ckSame([], $client->routeRefreshes, 'management calls never refresh a route');

    // Timeout: defaulted to the admin's own setting, overridable per call.
    foreach ($admin->requests as $rec) {
        $ck($rec['timeout'] === DefaultMQAdminExt::DEFAULT_TIMEOUT,
            sprintf('code %d uses the admin timeout', $rec['cmd']->code));
    }
    [$admin2] = $newAdmin();
    $admin2->setTimeoutMillis(7000);
    $admin2->deleteUser($ADDR, 'u');
    $ckSame(7000, $admin2->last()['timeout'], 'a changed admin timeout is picked up by the management calls');
    [$admin3] = $newAdmin();
    $admin3->getUser($ADDR, 'u', 2500);
    $ckSame(2500, $admin3->last()['timeout'], 'a per-call timeout overrides the admin default');
    $admin3->deleteAcl($ADDR, 'u', 'T', 1200);
    $ckSame(1200, $admin3->last()['timeout'], 'a per-call timeout reaches the void management calls too');
}

// ==================================================================== 9. response code handling

{
    // Only SUCCESS is accepted: every other code surfaces as the broker exception with the code and
    // the remark the broker sent, and the request itself has already been recorded.
    foreach ([ResponseCode::SYSTEM_ERROR, ResponseCode::NO_PERMISSION, ResponseCode::ILLEGAL_OPERATION] as $errCode) {
        [$admin] = $newAdmin(null, $errCode, 'management failed');
        $threw = null;
        try {
            $admin->createUser($ADDR, 'u', 'p', AuthUserType::NORMAL);
        } catch (MQBrokerException $e) {
            $threw = $e;
        }
        $ck($threw !== null, sprintf('code %d in a response is not accepted for a write', $errCode));
        $ckSame($errCode, $threw?->responseCode, sprintf('the raised exception keeps response code %d', $errCode));
        $ckSame('management failed', $threw?->getMessage(), sprintf('the raised exception keeps the remark of %d', $errCode));
        $ckSame(1, count($admin->requests), sprintf('the rejected request was still sent for %d', $errCode));
    }

    [$admin] = $newAdmin(null, ResponseCode::SYSTEM_ERROR, 'no such user');
    $threw = null;
    try {
        $admin->getUser($ADDR, 'u');
    } catch (MQBrokerException $e) {
        $threw = $e;
    }
    $ck($threw !== null, 'a read call surfaces an error response instead of decoding it');
    $ck(!($threw instanceof MQClientException), 'the error is the broker-side exception type');

    [$adminList] = $newAdmin(null, ResponseCode::SYSTEM_BUSY, 'busy');
    $ckThrows = null;
    try {
        $adminList->listAcl($ADDR, 'u');
    } catch (MQBrokerException $e) {
        $ckThrows = $e;
    }
    $ck($ckThrows !== null && $ckThrows->responseCode === ResponseCode::SYSTEM_BUSY,
        'a list call surfaces an error response instead of returning an empty list');

    // Control: the very same call with a SUCCESS response does not throw.
    [$adminOk] = $newAdmin('[]');
    $adminOk->listAcl($ADDR, 'u');
    $ck(count($adminOk->requests) === 1, 'a SUCCESS response is accepted by the same call that rejects an error');
}

// ==================================================================== 10. void calls never touch the body

{
    // A body the decoders would reject is handed to every write: returning cleanly proves the reply
    // is never parsed on the void paths.
    foreach ([
        'createUser' => static fn(DefaultMQAdminExt $a): mixed => $a->createUser($ADDR, 'u', 'p', AuthUserType::NORMAL),
        'updateUser' => static fn(DefaultMQAdminExt $a): mixed => $a->updateUser($ADDR, 'u', 'p', AuthUserType::NORMAL),
        'deleteUser' => static fn(DefaultMQAdminExt $a): mixed => $a->deleteUser($ADDR, 'u'),
        'createAcl' => static fn(DefaultMQAdminExt $a): mixed => $a->createAcl($ADDR, 'u', ['T'], ['Pub'], ['127.0.0.1'], AuthDecision::ALLOW),
        'updateAcl' => static fn(DefaultMQAdminExt $a): mixed => $a->updateAcl($ADDR, 'u', ['T'], ['Pub'], ['127.0.0.1'], AuthDecision::ALLOW),
        'deleteAcl' => static fn(DefaultMQAdminExt $a): mixed => $a->deleteAcl($ADDR, 'u', 'T'),
    ] as $name => $drive) {
        [$admin] = $newAdmin('{not a body');
        $err = null;
        try {
            $ckSame(null, $drive($admin), "{$name} returns nothing");
        } catch (\Throwable $e) {
            $err = $e;
        }
        $ck($err === null, "{$name} never reads the response body");
        $ckSame(1, count($admin->requests), "{$name} issues exactly one request");
    }
}

// ==================================================================== 11. model round trips

{
    $acl = AuthAclInfo::forResources('u1', ['A', 'B'], ['Pub'], ['1.1.1.1', '2.2.2.2'], AuthDecision::DENY);
    $again = AuthAclInfo::decodeOne($acl->encode());
    $ck($again instanceof AuthAclInfo, 'an encoded acl decodes back into an acl');
    if ($again instanceof AuthAclInfo) {
        $ckSame('u1', $again->subject, 'acl round trip keeps the subject');
        $ckSame(1, count($again->policies), 'acl round trip keeps one policy');
        $ckSame(2, count($again->policies[0]->entries), 'acl round trip keeps one entry per resource');
        $ckSame(['A', 'B'], array_map(static fn(AuthAccessEntry $e): ?string => $e->resource, $again->policies[0]->entries),
            'acl round trip keeps the resources in order');
        $ckSame(AuthDecision::DENY, $again->policies[0]->entries[1]->decision, 'acl round trip keeps each decision');
        $ck($again->policies[0]->policyType === null, 'the convenience policy still has no type after a round trip');
    }

    $user = new AuthUserInfo('u', null, AuthUserType::SUPER);
    $ckSame('{"username":"u","userType":"Super"}', $user->encode(), 'encoding drops every unset attribute of a user');
    $back = AuthUserInfo::decodeOne($user->encode());
    $ck($back instanceof AuthUserInfo && $back->password === null, 'a missing password decodes back as unset, not empty');

    // A list body whose element is not an object is skipped rather than turned into a bogus model.
    $skipped = AuthUserInfo::decodeList('[{"username":"ok"},"bare",3]');
    $ckSame(1, count($skipped), 'a list body only contributes its object elements');
    $ckSame('ok', $skipped[0]->username ?? null, 'the surviving element is the object one');
    $ckSame([], AuthUserInfo::decodeList(null), 'an absent body decodes to an empty list');
    $ck(AuthAclInfo::decodeList('[1,"x"]') === [], 'an acl list body of scalars decodes to an empty list');
}

// ==================================================================== 12. one shape per call

{
    // The attributes are a second way to say the same thing, not an extra: supplying both would
    // silently discard half of the caller's request, so it is rejected before anything is sent.
    [$admin] = $newAdmin();
    $threw = null;
    try {
        $admin->createUser($ADDR, new AuthUserInfo('u', 'p', AuthUserType::NORMAL), 'other');
    } catch (MQClientException $e) {
        $threw = $e;
    }
    $ck($threw !== null, 'a user object plus separate attributes is rejected');
    $ckSame(0, count($admin->requests), 'the rejected user call sends nothing at all');

    [$admin2] = $newAdmin();
    $threw2 = null;
    try {
        $admin2->updateAcl($ADDR, new AuthAclInfo('u', [new AuthPolicy(AuthPolicyType::CUSTOM, [])]), ['T']);
    } catch (MQClientException $e) {
        $threw2 = $e;
    }
    $ck($threw2 !== null, 'an acl object plus separate resources is rejected');
    $ckSame(0, count($admin2->requests), 'the rejected acl call sends nothing at all');

    // Control: each shape on its own is accepted by the very same call.
    [$admin3] = $newAdmin();
    $admin3->createUser($ADDR, new AuthUserInfo('u', 'p', AuthUserType::NORMAL));
    $admin3->updateAcl($ADDR, 'u', ['T'], ['Pub'], ['127.0.0.1'], AuthDecision::ALLOW);
    $ckSame(2, count($admin3->requests), 'one shape per call is accepted');
}

if ($failures === []) {
    fwrite(STDOUT, "ALL TESTS PASSED ({$checks} checks)\n");
    exit(0);
}
fwrite(STDOUT, 'TESTS FAILED: ' . count($failures) . " / {$checks}\n");
foreach ($failures as $f) {
    fwrite(STDOUT, "  - {$f}\n");
}
exit(1);
