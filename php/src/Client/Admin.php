<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Client\Exceptions\RemotingException;
use RocketMQ\Common\BoundaryType;
use RocketMQ\Common\MessageDecoder;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;
use RocketMQ\Common\TopicConfig;
use RocketMQ\Remoting\Protocol\AdminToolResult;
use RocketMQ\Remoting\Protocol\AdminToolsResultCodeEnum;
use RocketMQ\Remoting\Protocol\AuthAclInfo;
use RocketMQ\Remoting\Protocol\AuthUserInfo;
use RocketMQ\Remoting\Protocol\BrokerData;
use RocketMQ\Remoting\Protocol\BrokerOperatorResult;
use RocketMQ\Remoting\Protocol\BrokerReplicasInfo;
use RocketMQ\Remoting\Protocol\CheckRocksdbCqWriteResult;
use RocketMQ\Remoting\Protocol\ClusterInfo;
use RocketMQ\Remoting\Protocol\ConsumeMessageDirectlyResult;
use RocketMQ\Remoting\Protocol\ConsumeStats;
use RocketMQ\Remoting\Protocol\ConsumeStatsList;
use RocketMQ\Remoting\Protocol\ConsumerConnection;
use RocketMQ\Remoting\Protocol\ConsumerRunningInfo;
use RocketMQ\Remoting\Protocol\ExtraInfoUtil;
use RocketMQ\Remoting\Protocol\GetBrokerLiteInfoResponseBody;
use RocketMQ\Remoting\Protocol\GetConsumerListByGroupResponseBody;
use RocketMQ\Remoting\Protocol\GetLiteClientInfoResponseBody;
use RocketMQ\Remoting\Protocol\GetLiteGroupInfoResponseBody;
use RocketMQ\Remoting\Protocol\GetLiteTopicInfoResponseBody;
use RocketMQ\Remoting\Protocol\GetParentTopicInfoResponseBody;
use RocketMQ\Remoting\Protocol\KVTable;
use RocketMQ\Remoting\Protocol\LanguageCode;
use RocketMQ\Remoting\Protocol\OffsetWrapper;
use RocketMQ\Remoting\Protocol\ProducerConnection;
use RocketMQ\Remoting\Protocol\QueueData;
use RocketMQ\Remoting\Protocol\QueryConsumeQueueResponseBody;
use RocketMQ\Remoting\Protocol\QueryConsumeTimeSpanBody;
use RocketMQ\Remoting\Protocol\RemotingCommand;
use RocketMQ\Remoting\Protocol\RequestCode;
use RocketMQ\Remoting\Protocol\ResponseCode;
use RocketMQ\Remoting\Protocol\ResetOffsetBody;
use RocketMQ\Remoting\Protocol\SubscriptionGroupConfig;
use RocketMQ\Remoting\Protocol\SubscriptionGroupWrapper;
use RocketMQ\Remoting\Protocol\TopicConfigSerializeWrapper;
use RocketMQ\Remoting\Protocol\TopicList;
use RocketMQ\Remoting\Protocol\TopicRouteData;
use RocketMQ\Remoting\Protocol\TopicStatsTable;
use RocketMQ\Remoting\Protocol\RemotingSerializable;
use RocketMQ\Remoting\RPCHook;
use RocketMQ\Remoting\SocksProxyConfig;
// Namespace-level function (defined in AdminBodies.php; that file takes effect when any of its classes is autoloaded)
use function RocketMQ\Remoting\Protocol\message_queue_key;

/**
 * Admin side.
 *
 * Covers: Topic create/delete/query/config, Broker cluster info and runtime info/config,
 * NameServer KV config, subscription group management, consumer/producer connections,
 * consume stats, offset management (including real broker offset reset), message query
 * (key / uniqKey / msgId / ConsumeQueue).
 *
 * Protocol essentials (conclusions from live probes + broker behavior checks; do not
 * change from memory):
 * - GET_BROKER_CONFIG's body is a **properties text** (``k=v\n``), not JSON.
 * - UPDATE_AND_CREATE_SUBSCRIPTIONGROUP's body is SubscriptionGroupConfig **JSON**.
 * - GET_TOPIC_CONFIG's request header carries ``topic`` + ``lo``; the response body is TopicConfigAndQueueMapping JSON.
 * - GET_ALL_SUBSCRIPTIONGROUP_CONFIG is a **paged** API (groupSeq / maxGroupNum / dataVersion).
 * - ResetOffsetBody.offsetTable is a Map<MessageQueue, Long>.
 * - KV config requests go to the **NameServer**, and PUT/DELETE must broadcast to **every** NameServer.
 *
 * The admin side is entirely synchronous blocking RPC: ``$client->remotingClient->invokeSync(...)`` (timeout controlled by parameters);
 * non-SUCCESS responses are funneled through this class's ``checkResponse``.
 * Tests/dependency injection can preset an instance via ``setMqClientInstance`` and override ``invokeSyncOn``.
 */
class DefaultMQAdminExt
{
    /** Default timeout for the administrative RPCs. */
    public const DEFAULT_TIMEOUT = 20000;

    /**
     * Timeout the container join/leave RPCs are hard-coded to (the operation is a rare,
     * operator-triggered config load; it does not follow the per-instance timeout).
     */
    public const CONTAINER_TIMEOUT = 20000;

    /** Timeout of the controller-leader routed RPCs (metadata, sync state, clean). */
    public const CONTROLLER_TIMEOUT = 3000;

    /** Header field of SET_COMMITLOG_READ_MODE(2004): the only key of that request. */
    public const READ_AHEAD_MODE = 'READ_AHEAD_MODE';

    /** Namespace name storing the order route config in the NameServer KV. */
    public const NAMESPACE_ORDER_TOPIC_CONFIG = 'ORDER_TOPIC_CONFIG';

    public string $namespace;

    public string $instanceName = 'ADMIN';

    public ?string $clientId = null;

    /** Only non-null after start(). */
    public function getClientId(): ?string
    {
        return $this->clientId;
    }

    /**
     * unitName and enableStreamRequestType only affect the clientId / address-lookup URL /
     * request extension fields; they are equally valid for admin. (unitMode has no
     * foothold here: admin does not send messages and does not register consumers, so
     * there is no request that needs to carry unitMode.)
     */
    public ?string $unitName = null;

    public bool $enableStreamRequestType = false;

    /**
     * 5.x new namespace: when non-empty, **every** request carries the `nsd=true` / `ns=<value>`
     * two extension headers, which the broker resolves to the corresponding serverless
     * instance. This is a separate mechanism from `$namespace` (the client prefixes
     * resource names with `namespace%`); nothing here **changes** any resource name.
     * See NamespaceRpcHook.
     */
    public string $namespaceV2 = '';

    /**
     * VIP channel switch (default false): when true, broker requests go through the VIP
     * port (port - 2). Only affects this admin's broker calls -- admin sends no messages
     * and registers no consumers.
     */
    public bool $vipChannelEnabled = false;

    /** @var list<string> */
    public array $nameServerAddrs = [];

    /** TLS switch (null = delegate to env ROCKETMQ_TLS_ENABLE). */
    public ?bool $tlsEnable = null;

    /** TLS details (caCert/clientCert/clientKey/serverName); semantics see RemotingClient::$tlsOptions. */
    public ?array $tlsOptions = null;

    public ?SocksProxyConfig $socksProxy = null;

    public int $pollNameServerInterval = 30000;

    public ?RPCHook $rpcHook;

    /** KV namespace cleaned up along with topic deletion. */
    public array $kvNamespaceToDeleteList = [];

    public int $timeoutMillis;

    /**
     * Page size of the metadata paging APIs (default 2000): maxTopicNum of GET_ALL_TOPIC_CONFIG
     * and maxGroupNum of GET_ALL_SUBSCRIPTIONGROUP_CONFIG.
     */
    public int $maxPageSizeInGetMetadata = 2000;

    private ?MQClientInstance $mqClient = null;

    private bool $started = false;

    public function __construct(
        ?RPCHook $rpcHook = null,
        string $namespace = '',
        int $timeoutMillis = self::DEFAULT_TIMEOUT,
    ) {
        $this->rpcHook = $rpcHook;
        $this->namespace = $namespace;
        $this->timeoutMillis = $timeoutMillis;
    }

    // ---------------- Config and lifecycle ----------------

    public function setNamesrvAddr(string $addr): void
    {
        $addrs = [];
        foreach (explode(';', $addr) as $a) {
            $a = trim($a);
            if ($a !== '') {
                $addrs[] = $a;
            }
        }
        $this->setNameServerAddresses($addrs);
    }

    /** @param list<string> $addrs */
    public function setNameServerAddresses(array $addrs): void
    {
        $this->nameServerAddrs = array_values($addrs);
        // Setters are usually called before start(); start() uses this list to build the MQClientInstance.
        // This port allows injecting a ready-made instance (setMqClientInstance), in which case
        // it is synced to the client to avoid two address lists drifting between admin and client
        // (invokeNamesrvOne/All read the client side).
        $this->mqClient?->updateNameServerAddressList($this->nameServerAddrs);
    }

    public function setInstanceName(string $name): void
    {
        $this->instanceName = $name;
    }

    /** Unit name: affects the clientId suffix and the dynamic address-lookup URL. */
    public function setUnitName(?string $unitName): void
    {
        $this->unitName = $unitName;
    }

    public function getUnitName(): ?string
    {
        return $this->unitName;
    }

    /** Whether to add the `ReqT=0` flow-control marker to requests (@STREAM clientId suffix). */
    public function setEnableStreamRequestType(bool $enable): void
    {
        $this->enableStreamRequestType = $enable;
    }

    /**
     * Server-side namespace (`nsd`/`ns` extension headers), does not change resource names;
     * unrelated to the `$namespace` "client prefix" mechanism.
     * Changing it after start() also takes effect -- the hook reads {@see MQClientInstance::$namespaceV2} per request in real time.
     */
    public function setNamespaceV2(string $namespaceV2): void
    {
        $this->namespaceV2 = $namespaceV2;
        if ($this->mqClient !== null) {
            $this->mqClient->namespaceV2 = $namespaceV2;
        }
    }

    public function getNamespaceV2(): string
    {
        return $this->namespaceV2;
    }

    public function setSocksProxy(?SocksProxyConfig $proxy): void
    {
        $this->socksProxy = $proxy;
    }

    public function getSocksProxy(): ?SocksProxyConfig
    {
        return $this->socksProxy;
    }

    public function setVipChannelEnabled(bool $enable): void
    {
        $this->vipChannelEnabled = $enable;
        if ($this->mqClient !== null) {
            $this->mqClient->vipChannelEnabled = $enable;
        }
    }

    public function getNameServerAddr(): string
    {
        return implode(';', $this->nameServerAddrs);
    }

    /** @return list<string> */
    public function getNameServerAddressList(): array
    {
        return array_values($this->nameServerAddrs);
    }

    public function setTimeoutMillis(int $timeoutMillis): void
    {
        $this->timeoutMillis = $timeoutMillis;
    }

    /** Page size of the paged metadata APIs; zero or negative values fall back to the default 2000. */
    public function setMaxPageSizeInGetMetadata(int $size): void
    {
        $this->maxPageSizeInGetMetadata = $size > 0 ? $size : 2000;
    }

    public function getMaxPageSizeInGetMetadata(): int
    {
        return $this->maxPageSizeInGetMetadata;
    }

    public function start(): void
    {
        if ($this->started) {
            return;
        }
        if ($this->nameServerAddrs === []) {
            throw new MQClientException('name server address is not set');
        }
        // instanceName unconditionally goes through changeInstanceNameToPID;
        // clientId is then assembled as ``<local IP>@<instanceName>[@<unitName>][@STREAM]``.
        // This client's admin defaults instanceName to "ADMIN" (admin uses a private instance,
        // not shared with other clients), so this step only takes effect when the caller
        // explicitly sets it to "DEFAULT".
        $this->instanceName = MixAll::changeInstanceNameToPid($this->instanceName);
        if ($this->clientId === null) {
            $this->clientId = MixAll::clientIdFor(
                $this->instanceName,
                $this->unitName,
                $this->enableStreamRequestType,
            );
        }
        $this->mqClient = new MQClientInstance(
            $this->clientId,
            $this->nameServerAddrs,
            tlsEnable: $this->tlsEnable,
            enableStreamRequestType: $this->enableStreamRequestType,
            namespaceV2: $this->namespaceV2,
            unitName: $this->unitName,
            pollNameServerInterval: $this->pollNameServerInterval,
            tlsOptions: $this->tlsOptions,
            socksProxy: $this->socksProxy,
            vipChannelEnabled: $this->vipChannelEnabled,
        );
        if ($this->rpcHook !== null) {
            $this->mqClient->remotingClient->registerRpcHook($this->rpcHook);
        }
        $this->mqClient->start();
        $this->started = true;
    }

    public function shutdown(): void
    {
        if (!$this->started) {
            return;
        }
        $this->started = false;
        if ($this->mqClient !== null) {
            $this->mqClient->shutdown();
        }
    }

    public function getMqClientInstance(): MQClientInstance
    {
        return $this->requireClient();
    }

    /**
     * Test / dependency-injection seam: directly preset a ready MQClientInstance (equivalent
     * to the internal state after start()). The production path goes through start().
     */
    public function setMqClientInstance(MQClientInstance $client): void
    {
        $this->mqClient = $client;
        $this->started = true;
    }

    private function requireClient(): MQClientInstance
    {
        if (!$this->started || $this->mqClient === null) {
            throw new MQClientException('admin not started, call start() first');
        }
        return $this->mqClient;
    }

    // ---------------- Low-level call helpers ----------------

    /**
     * The only exit point for synchronous RPC: MQClientInstance's invokeSync is private,
     * so this goes through the public remotingClient; test subclasses can intercept all
     * synchronous calls by overriding this method.
     */
    protected function invokeSyncOn(
        MQClientInstance $client,
        string $addr,
        RemotingCommand $request,
        ?int $timeoutMillis,
    ): RemotingCommand {
        return $client->remotingClient->invokeSync($addr, $request, $timeoutMillis);
    }

    /** Throws MQBrokerException on non-SUCCESS. */
    protected function checkResponse(RemotingCommand $response): RemotingCommand
    {
        if ($response->code === ResponseCode::SUCCESS) {
            return $response;
        }
        throw new MQBrokerException($response->code, $response->remark ?? '');
    }

    /**
     * Send to the specified Broker and verify SUCCESS.
     *
     * `$vip` mirrors the per-request address rewrite: the administration RPCs the broker answers on
     * its VIP port go there while the channel is enabled, and the few calls only ever served on the
     * plain port -- reading a broker's own configuration is one of them -- pass `$vip = false`.
     */
    protected function invokeBroker(
        string $addr,
        int $code,
        ?array $extFields = null,
        ?string $body = null,
        ?int $timeoutMillis = null,
        bool $vip = true,
    ): RemotingCommand {
        $client = $this->requireClient();
        if ($vip) {
            $addr = MixAll::brokerVipChannel($this->vipChannelEnabled, $addr);
        }
        $request = RemotingCommand::createRequestCommand($code, null);
        self::applyExtFields($request, $extFields);
        if ($body !== null) {
            $request->body = $body;
        }
        $response = $this->invokeSyncOn($client, $addr, $request, $timeoutMillis ?? $this->timeoutMillis);
        return $this->checkResponse($response);
    }

    /**
     * KV config write/delete must broadcast to **all** NameServers (name servers do not replicate KV).
     * Any failure throws after all responses are collected (the last error response is kept and thrown uniformly).
     */
    protected function invokeNamesrvAll(int $code, ?array $extFields = null, ?int $timeoutMillis = null): ?RemotingCommand
    {
        $client = $this->requireClient();
        $request = RemotingCommand::createRequestCommand($code, null);
        self::applyExtFields($request, $extFields);
        $errResponse = null;
        foreach ($client->nameServerAddrs as $nsAddr) {
            $response = $this->invokeSyncOn($client, $nsAddr, $request, $timeoutMillis ?? $this->timeoutMillis);
            if ($response->code !== ResponseCode::SUCCESS) {
                $errResponse = $response;
            }
        }
        if ($errResponse !== null) {
            throw new MQClientException(
                $errResponse->remark !== null && $errResponse->remark !== ''
                    ? $errResponse->remark : 'put/delete kv config failed',
                $errResponse->code,
            );
        }
        return null;
    }

    /** Send to the first available NameServer (read requests need no broadcast). */
    protected function invokeNamesrvOne(int $code, ?array $extFields = null, ?int $timeoutMillis = null): RemotingCommand
    {
        $client = $this->requireClient();
        $request = RemotingCommand::createRequestCommand($code, null);
        self::applyExtFields($request, $extFields);
        $lastExc = null;
        foreach ($client->nameServerAddrs as $nsAddr) {
            try {
                return $this->invokeSyncOn($client, $nsAddr, $request, $timeoutMillis ?? $this->timeoutMillis);
            } catch (\Throwable $e) {
                $lastExc = $e;
            }
        }
        throw new MQClientException('all name servers unreachable: ' . ($lastExc !== null ? $lastExc->getMessage() : ''));
    }

    /** Send to the **explicitly given** NameServer and verify SUCCESS (does not go through the VIP channel). */
    protected function invokeNamesrvAddr(string $addr, int $code, ?array $extFields = null, ?int $timeoutMillis = null): RemotingCommand
    {
        $client = $this->requireClient();
        $request = RemotingCommand::createRequestCommand($code, null);
        self::applyExtFields($request, $extFields);
        $response = $this->invokeSyncOn($client, $addr, $request, $timeoutMillis ?? $this->timeoutMillis);
        return $this->checkResponse($response);
    }

    /** @param array<string, string|int|bool|null>|null $extFields */
    private static function applyExtFields(RemotingCommand $request, ?array $extFields): void
    {
        foreach ($extFields ?? [] as $k => $v) {
            $request->extFields[(string) $k] = (string) $v;
        }
    }

    private static function firstBrokerAddr(MQClientInstance $client): string
    {
        try {
            $cluster = $client->getBrokerClusterInfo();
            $addrs = $cluster->getBrokerAddrs();
            if ($addrs !== []) {
                return $addrs[0];
            }
        } catch (\Throwable) {
            // Swallowed, then uniformly throws no broker address available
        }
        throw new MQClientException('no broker address available');
    }

    private function findFirstBrokerAddr(MQClientInstance $client): string
    {
        return self::firstBrokerAddr($client);
    }

    // ---------------- Topic management ----------------

    public function createTopic(string $key, string $newTopic, int $queueNum = 4, int $topicSysFlag = 0): void
    {
        $client = $this->requireClient();
        $client->createTopicInRoute($newTopic, $queueNum, $queueNum, MixAll::READ_PERM_BY_DEFAULT);
    }

    /** Send the topic config to the specified broker (via createTopicKey, i.e. TBW102). */
    public function createAndUpdateTopicConfig(string $addr, TopicConfig $config): void
    {
        $client = $this->requireClient();
        $client->createTopicInBroker(
            $addr,
            MixAll::DEFAULT_TOPIC,
            $config->topicName,
            $config->readQueueNums,
            $config->writeQueueNums,
            $config->perm,
        );
    }

    public function createTopicInBroker(string $brokerAddr, string $topic, int $readQueueNums = 4, int $writeQueueNums = 4, int $perm = 6): void
    {
        $client = $this->requireClient();
        $client->createTopicInBroker($brokerAddr, MixAll::DEFAULT_TOPIC, $topic, $readQueueNums, $writeQueueNums, $perm);
    }

    public function deleteTopicInBroker(string $brokerAddr, string $topic): void
    {
        $this->requireClient()->deleteTopicInBroker($brokerAddr, $topic);
    }

    /**
     * Fan-out topic deletion over the caller's address set: every address is reported in
     * exactly one of the two lists (or in neither, when the deadline cut the batch short).
     * The envelope is always a success — the per-address outcome lives in the payload.
     *
     * @param list<string> $addrs
     */
    public function deleteTopicInBrokerConcurrent(array $addrs, string $topic): AdminToolResult
    {
        $successList = [];
        $failureList = [];
        $this->fanOutWithDeadline(array_values($addrs), function (string $addr, int $left) use ($topic, &$successList, &$failureList): void {
            try {
                $this->invokeBroker($addr, RequestCode::DELETE_TOPIC_IN_BROKER, ['topic' => $topic], timeoutMillis: $left);
                $successList[] = $addr;
            } catch (\Throwable $e) {
                Logger::warning(sprintf('deleteTopicInBroker error. topic=%s, broker=%s: %s', $topic, $addr, $e->getMessage()));
                $failureList[] = $addr;
            }
        });
        $result = new BrokerOperatorResult();
        $result->successList = $successList;
        $result->failureList = $failureList;
        return AdminToolResult::success($result);
    }

    /** @param list<string>|null $addrs */
    public function deleteTopicInNameServer(?array $addrs, string $topic): void
    {
        $client = $this->requireClient();
        $targets = $addrs !== null && $addrs !== [] ? array_values($addrs) : array_values($client->nameServerAddrs);
        // NameServer requests cannot go through invokeBroker: when the VIP switch is on it
        // would also subtract 2 from the NameServer's port, so connect to the NameServer
        // address directly here.
        foreach ($targets as $nsAddr) {
            $this->invokeNamesrvAddr($nsAddr, RequestCode::DELETE_TOPIC_IN_NAMESRV, ['topic' => $topic]);
        }
    }

    /** Compatibility with the old name: delete the topic route on the NameServer. */
    public function deleteTopicInNamesrv(string $topic): void
    {
        $client = $this->requireClient();
        $client->deleteTopicInNamesrv($topic);
    }

    /** Delete a topic (first each broker, then the NameServer route, and finally the KV namespace). */
    public function deleteTopic(string $topic, ?string $clusterName = null): void
    {
        $client = $this->requireClient();
        foreach ($this->brokerAddrsOfCluster($client, $clusterName) as $broker) {
            try {
                $client->deleteTopicInBroker($broker, $topic);
            } catch (\Throwable $e) {
                Logger::warning(sprintf('delete topic %s in broker %s failed: %s', $topic, $broker, $e->getMessage()));
            }
        }
        try {
            $client->deleteTopicInNamesrv($topic);
        } catch (\Throwable $e) {
            Logger::warning(sprintf('delete topic %s in name server failed: %s', $topic, $e->getMessage()));
        }
        foreach ($this->kvNamespaceToDeleteList as $ns) {
            try {
                $this->deleteKvConfig($ns, $topic);
            } catch (\Throwable $e) {
                Logger::warning(sprintf('delete kv config %s/%s failed: %s', $ns, $topic, $e->getMessage()));
            }
        }
    }

    /**
     * @return list<string>
     */
    private function brokerAddrsOfCluster(MQClientInstance $client, ?string $clusterName = null): array
    {
        $cluster = $client->getBrokerClusterInfo();
        if ($clusterName === null || $clusterName === '') {
            return $cluster->getBrokerAddrs();
        }
        // cluster_addr_table: {clusterName: [brokerName, ...]}
        // broker_addr_table : {brokerName: {brokerId: addr}}
        $brokerNames = (array) ($cluster->clusterAddrTable[$clusterName] ?? []);
        $result = [];
        $names = array_map(strval(...), array_values($brokerNames));
        sort($names, SORT_STRING);
        foreach ($names as $brokerName) {
            $addrs = array_map(strval(...), array_values((array) ($cluster->brokerAddrTable[$brokerName] ?? [])));
            sort($addrs, SORT_STRING);
            foreach ($addrs as $addr) {
                if ($addr !== '') {
                    $result[] = $addr;
                }
            }
        }
        return $result !== [] ? $result : $cluster->getBrokerAddrs();
    }

    public function fetchAllTopicList(): TopicList
    {
        return $this->requireClient()->getAllTopicListFromNameServer();
    }

    /**
     * GET_TOPICS_BY_CLUSTER goes to the NameServer.
     *
     * The field name must be ``cluster`` from the protocol header: when it was earlier
     * written as ``clusterName`` the NameServer could not find the cluster (NPE swallowed),
     * only returning SUCCESS + an empty list.
     *
     * Results are deduplicated by topic name before being returned as a list.
     *
     * @return list<string>
     */
    public function fetchTopicsByCluster(string $clusterName): array
    {
        $response = $this->invokeNamesrvOne(RequestCode::GET_TOPICS_BY_CLUSTER, ['cluster' => $clusterName]);
        $topics = [];
        if ($response->code === ResponseCode::SUCCESS && $response->body !== null && $response->body !== '') {
            $obj = RemotingSerializable::decodeJson($response->body);
            foreach ((array) ($obj['topicList'] ?? []) as $t) {
                $topics[(string) $t] = true;
            }
        }
        return array_keys($topics);
    }

    /** Set of cluster names of the brokers hosting this topic's route. @return list<string> */
    public function getClusterList(string $topic): array
    {
        $client = $this->requireClient();
        $clusterInfo = $client->getBrokerClusterInfo();
        $route = $this->examineTopicRoute($topic);
        $brokerNames = [];
        foreach ($route->getBrokerDatas() as $bd) {
            $brokerNames[$bd->brokerName] = true;
        }
        $clusters = [];
        foreach ($clusterInfo->clusterAddrTable as $clusterName => $names) {
            foreach ((array) $names as $n) {
                if (isset($brokerNames[(string) $n])) {
                    $clusters[(string) $clusterName] = true;
                    break;
                }
            }
        }
        return array_keys($clusters);
    }

    /** @return list<string> */
    public function getTopicClusterList(string $topic): array
    {
        return $this->getClusterList($topic);
    }

    /**
     * Fetch the routes of all topics (iterating the Nameserver's topic list).
     *
     * @return list<TopicRouteData>
     */
    public function fetchAllTopicRoute(): array
    {
        $client = $this->requireClient();
        $result = [];
        $topics = $client->getAllTopicListFromNameServer();
        foreach ($topics->getTopicList() as $topic) {
            try {
                $route = $client->getTopicRouteData($topic);
                if ($route !== null) {
                    $result[] = $route;
                }
            } catch (\Throwable) {
                continue;
            }
        }
        return $result;
    }

    public function examineTopicRoute(string $topic): TopicRouteData
    {
        $client = $this->requireClient();
        $route = $client->getTopicRouteData($topic);
        if ($route === null) {
            throw new MQClientException(sprintf('topic %s not exist', $topic), ResponseCode::TOPIC_NOT_EXIST);
        }
        return $route;
    }

    /** GET_TOPIC_CONFIG: body is TopicConfig JSON. */
    public function examineTopicConfig(string $addr, string $topic): TopicConfig
    {
        $response = $this->invokeBroker($addr, RequestCode::GET_TOPIC_CONFIG, ['topic' => $topic, 'lo' => 'true']);
        if ($response->body === null || $response->body === '') {
            throw new MQBrokerException(ResponseCode::SYSTEM_ERROR, sprintf('empty topic config for %s', $topic));
        }
        $obj = RemotingSerializable::fastjsonLoads($response->body);
        return TopicConfig::fromDict(is_array($obj) ? $obj : []);
    }

    /**
     * GET_ALL_TOPIC_CONFIG is a **paged** API: accumulate until topicSeq >= totalTopicNum-1.
     *
     * The first page always sends dataVersion as the empty string (the key is present, not
     * omitted); later pages carry the JSON of the last dataVersion seen. A response without
     * totalTopicNum comes from an old broker and ends the loop after its single page; a
     * changed dataVersion discards the accumulated table and starts paging again from 0.
     */
    public function getAllTopicConfig(string $brokerAddr, ?int $timeoutMillis = null): TopicConfigSerializeWrapper
    {
        $client = $this->requireClient();
        $brokerAddr = MixAll::brokerVipChannel($this->vipChannelEnabled, $brokerAddr);
        $timeout = $timeoutMillis ?? $this->timeoutMillis;
        $pageSize = $this->maxPageSizeInGetMetadata > 0 ? $this->maxPageSizeInGetMetadata : 2000;
        $currentDataVersion = null;
        $topicSeq = 0;
        /** @var array<string, TopicConfig> $table */
        $table = [];
        $begin = hrtime(true) / 1e6;
        while (true) {
            $left = $timeout - (int) ((hrtime(true) / 1e6) - $begin);
            if ($left < 0) {
                throw new MQClientException('invokeSync call timeout');
            }
            $request = RemotingCommand::createRequestCommand(RequestCode::GET_ALL_TOPIC_CONFIG, null);
            $request->extFields['topicSeq'] = (string) $topicSeq;
            $request->extFields['maxTopicNum'] = (string) $pageSize;
            $request->extFields['dataVersion'] = $currentDataVersion === null
                ? ''
                : RemotingSerializable::toJson($currentDataVersion);
            $response = $this->invokeSyncOn($client, $brokerAddr, $request, $left);
            if ($response->code !== ResponseCode::SUCCESS) {
                throw new MQBrokerException($response->code, $response->remark ?? '');
            }
            $wrapper = ($response->body !== null && $response->body !== '')
                ? TopicConfigSerializeWrapper::decode($response->body)
                : new TopicConfigSerializeWrapper();
            foreach ($wrapper->topicConfigTable as $name => $cfg) {
                $table[$name] = $cfg;
            }
            $newVersion = $wrapper->dataVersion;
            if ($currentDataVersion === null) {
                // Remember the version even when the loop breaks right below: old brokers still
                // report it in the merged result.
                $currentDataVersion = $newVersion;
            }
            $totalRaw = $response->extFields['totalTopicNum'] ?? null;
            if ($totalRaw === null) {
                // Old broker: everything arrived at once.
                break;
            }
            $total = (int) $totalRaw;
            if ($currentDataVersion != $newVersion) {
                Logger::warning('topic dataVersion changed, restart paging');
                $currentDataVersion = $newVersion;
                $topicSeq = 0;
                $table = [];
                continue;
            }
            $topicSeq += count($wrapper->topicConfigTable);
            if ($topicSeq >= $total - 1) {
                break;
            }
        }

        $result = new TopicConfigSerializeWrapper();
        $result->topicConfigTable = $table;
        $result->dataVersion = $currentDataVersion ?? [];
        return $result;
    }

    /** Exclude system topics and %RETRY%/%DLQ% (when specialTopic=false, retry/dead-letter queues are excluded too). */
    public function getUserTopicConfig(string $brokerAddr, bool $specialTopic = false, ?int $timeoutMillis = null): TopicConfigSerializeWrapper
    {
        $wrapper = $this->getAllTopicConfig($brokerAddr, $timeoutMillis);
        $sysTopics = [];
        foreach ($this->getSystemTopicListFromBroker($brokerAddr, $timeoutMillis)->getTopicList() as $t) {
            $sysTopics[$t] = true;
        }
        $kept = [];
        foreach ($wrapper->topicConfigTable as $name => $cfg) {
            if (isset($sysTopics[$name]) || MixAll::isSysTopic($name)) {
                continue;
            }
            if (!$specialTopic
                && (str_starts_with($name, MixAll::RETRY_GROUP_TOPIC_PREFIX)
                    || str_starts_with($name, MixAll::DLQ_GROUP_TOPIC_PREFIX))) {
                continue;
            }
            $kept[$name] = $cfg;
        }
        $wrapper->topicConfigTable = $kept;
        return $wrapper;
    }

    public function getSystemTopicListFromBroker(string $brokerAddr, ?int $timeoutMillis = null): TopicList
    {
        $response = $this->invokeBroker($brokerAddr, RequestCode::GET_SYSTEM_TOPIC_LIST_FROM_BROKER, timeoutMillis: $timeoutMillis);
        if ($response->body === null || $response->body === '') {
            return new TopicList();
        }
        return TopicList::decode($response->body);
    }

    /** Merge stats across all brokers of this topic. */
    public function examineTopicStats(string $topic): TopicStatsTable
    {
        $route = $this->examineTopicRoute($topic);
        $merged = new TopicStatsTable();
        foreach ($route->getBrokerDatas() as $bd) {
            $addr = $bd->selectBrokerAddr();
            if ($addr === null || $addr === '') {
                continue;
            }
            try {
                $part = $this->examineTopicStatsByBroker($addr, $topic);
            } catch (\Throwable $e) {
                Logger::warning(sprintf('getTopicStatsInfo error. topic=%s broker=%s: %s', $topic, $addr, $e->getMessage()));
                continue;
            }
            self::mergeMqTable($merged->offsetTable, $part->offsetTable);
            $merged->topicPutTps += $part->topicPutTps;
        }
        if ($merged->offsetTable === []) {
            throw new MQClientException('Not found the topic stats info');
        }
        return $merged;
    }

    public function examineTopicStatsByBroker(string $brokerAddr, string $topic, ?int $timeoutMillis = null): TopicStatsTable
    {
        $response = $this->invokeBroker($brokerAddr, RequestCode::GET_TOPIC_STATS_INFO, ['topic' => $topic], timeoutMillis: $timeoutMillis);
        if ($response->body === null || $response->body === '') {
            return new TopicStatsTable();
        }
        return TopicStatsTable::decode($response->body);
    }

    /**
     * Fan-out topic stats: one stats call per broker of the route, tables merged and the put
     * TPS summed. A broker that fails only shrinks the merge (its error is logged, never
     * raised); an empty route is a success carrying an empty table.
     */
    public function examineTopicStatsConcurrent(string $topic): AdminToolResult
    {
        return $this->adminToolExecute(function () use ($topic): AdminToolResult {
            $table = new TopicStatsTable();
            $route = $this->examineTopicRoute($topic);
            $brokerDatas = $route->getBrokerDatas();
            if ($brokerDatas === []) {
                return AdminToolResult::success($table);
            }
            $this->fanOutWithDeadline($brokerDatas, function (BrokerData $bd, int $left) use ($topic, $table): void {
                $addr = $bd->selectBrokerAddr();
                if ($addr === null || $addr === '') {
                    return;
                }
                try {
                    $part = $this->examineTopicStatsByBroker($addr, $topic, $left);
                    self::mergeMqTable($table->offsetTable, $part->offsetTable);
                    $table->topicPutTps += $part->topicPutTps;
                } catch (\Throwable $e) {
                    Logger::warning(sprintf('getTopicStatsInfo error. topic=%s, broker=%s: %s', $topic, $addr, $e->getMessage()));
                }
            });
            return AdminToolResult::success($table);
        });
    }

    /**
     * Merge a MessageQueue-keyed table with dict.update semantics (same key: later value overwrites earlier).
     *
     * @param list<array{mq: MessageQueue, value: mixed}> $dst
     * @param list<array{mq: MessageQueue, value: mixed}> $src
     */
    private static function mergeMqTable(array &$dst, array $src): void
    {
        $byKey = [];
        foreach ($dst as ['mq' => $mq, 'value' => $v]) {
            $byKey[message_queue_key($mq)] = ['mq' => $mq, 'value' => $v];
        }
        foreach ($src as ['mq' => $mq, 'value' => $v]) {
            $byKey[message_queue_key($mq)] = ['mq' => $mq, 'value' => $v];
        }
        $dst = array_values($byKey);
    }

    // ---------------- Cluster / Broker ----------------

    public function fetchBrokerClusterInfo(): ClusterInfo
    {
        return $this->requireClient()->getBrokerClusterInfo();
    }

    public function examineBrokerClusterInfo(): ClusterInfo
    {
        return $this->fetchBrokerClusterInfo();
    }

    public function fetchBrokerRuntimeStats(string $brokerAddr, ?int $timeoutMillis = null): KVTable
    {
        $response = $this->invokeBroker($brokerAddr, RequestCode::GET_BROKER_RUNTIME_INFO, timeoutMillis: $timeoutMillis);
        if ($response->body === null || $response->body === '') {
            return new KVTable();
        }
        return KVTable::decode($response->body);
    }

    // Compatibility with the old interface name: getBrokerRuntimeInfo
    public function getBrokerRuntimeInfo(string $brokerAddr, ?int $timeoutMillis = null): KVTable
    {
        return $this->fetchBrokerRuntimeStats($brokerAddr, $timeoutMillis);
    }

    /**
     * GET_BROKER_CONFIG: the response body is **properties text**, not JSON/KVTable.
     *
     * A historical implementation bug: parsing the body as KVTable JSON, which necessarily
     * fails on a real broker.
     *
     * @return array<string, string>
     */
    public function getBrokerConfig(string $brokerAddr, ?int $timeoutMillis = null): array
    {
        $response = $this->invokeBroker($brokerAddr, RequestCode::GET_BROKER_CONFIG, timeoutMillis: $timeoutMillis, vip: false);
        $text = $response->body ?? '';
        return MixAll::string2Properties($text);
    }

    /**
     * UPDATE_BROKER_CONFIG: body is properties text; validate the brokerPermission value before sending.
     *
     * @param array<string, string> $properties
     */
    public function updateBrokerConfig(string $brokerAddr, array $properties, ?int $timeoutMillis = null): void
    {
        $brokerPermission = $properties['brokerPermission'] ?? null;
        if ($brokerPermission !== null && !self::permIsValid($brokerPermission)) {
            throw new MQClientException(
                sprintf('brokerPermission value: %s is invalid.', $brokerPermission),
                ResponseCode::NO_PERMISSION,
            );
        }
        $text = MixAll::properties2String($properties);
        if ($text === '') {
            return;
        }
        $this->invokeBroker($brokerAddr, RequestCode::UPDATE_BROKER_CONFIG, body: $text, timeoutMillis: $timeoutMillis);
    }

    public function wipeWritePermOfBroker(string $namesrvAddr, string $brokerName): int
    {
        $response = $this->invokeNamesrvAddr($namesrvAddr, RequestCode::WIPE_WRITE_PERM_OF_BROKER, ['brokerName' => $brokerName]);
        return (int) (($response->extFields['wipeTopicCount'] ?? 0) ?: 0);
    }

    public function addWritePermOfBroker(string $namesrvAddr, string $brokerName): int
    {
        $response = $this->invokeNamesrvAddr($namesrvAddr, RequestCode::ADD_WRITE_PERM_OF_BROKER, ['brokerName' => $brokerName]);
        return (int) (($response->extFields['addTopicCount'] ?? 0) ?: 0);
    }

    /**
     * ADD_BROKER(902): load a broker config file into the container at ``brokerContainerAddr``.
     *
     * The container address is used exactly as passed (no VIP rewrite), the request has no
     * body and a fixed 20s timeout.
     */
    public function addBrokerToContainer(string $brokerContainerAddr, string $brokerConfig): void
    {
        $this->invokeBroker(
            $brokerContainerAddr,
            RequestCode::ADD_BROKER,
            ['configPath' => $brokerConfig],
            timeoutMillis: self::CONTAINER_TIMEOUT,
            vip: false,
        );
    }

    /**
     * REMOVE_BROKER(903): drop one broker (by cluster + name + id) from the container.
     * Same fixed timeout and plain-address routing as the join operation.
     */
    public function removeBrokerFromContainer(string $brokerContainerAddr, string $clusterName, string $brokerName, int $brokerId): void
    {
        $this->invokeBroker(
            $brokerContainerAddr,
            RequestCode::REMOVE_BROKER,
            [
                'brokerClusterName' => $clusterName,
                'brokerName' => $brokerName,
                'brokerId' => $brokerId,
            ],
            timeoutMillis: self::CONTAINER_TIMEOUT,
            vip: false,
        );
    }

    /** Send CLEAN_UNUSED_TOPIC broker by broker; returns false if any fails. */
    public function cleanUnusedTopic(?string $clusterName = null, ?string $topic = null): bool
    {
        $client = $this->requireClient();
        $ok = true;
        foreach ($this->brokerAddrsOfCluster($client, $clusterName) as $addr) {
            try {
                $response = $this->invokeBroker($addr, RequestCode::CLEAN_UNUSED_TOPIC);
                if ($response->code !== ResponseCode::SUCCESS) {
                    $ok = false;
                }
            } catch (\Throwable $e) {
                Logger::warning(sprintf('cleanUnusedTopic on %s failed: %s', $addr, $e->getMessage()));
                $ok = false;
            }
        }
        return $ok;
    }

    public function viewBrokerStatsData(string $brokerAddr, string $statsName, string $statsKey): array
    {
        $response = $this->invokeBroker($brokerAddr, RequestCode::VIEW_BROKER_STATS_DATA, ['statsName' => $statsName, 'statsKey' => $statsKey]);
        if ($response->body === null || $response->body === '') {
            return [];
        }
        $obj = RemotingSerializable::fastjsonLoads($response->body);
        return is_array($obj) ? $obj : [];
    }

    // ---------------- NameServer KV config ----------------

    /** PUT_KV_CONFIG: broadcast to all NameServers. */
    public function createAndUpdateKvConfig(string $namespace, string $key, string $value): void
    {
        $this->invokeNamesrvAll(RequestCode::PUT_KV_CONFIG, ['namespace' => $namespace, 'key' => $key, 'value' => $value]);
    }

    // Compatibility alias: the real implementation is in createAndUpdateKvConfig
    public function putKvConfig(string $namespace, string $key, string $value): void
    {
        $this->createAndUpdateKvConfig($namespace, $key, $value);
    }

    public function getKvConfig(string $namespace, string $key): ?string
    {
        $response = $this->invokeNamesrvOne(RequestCode::GET_KV_CONFIG, ['namespace' => $namespace, 'key' => $key]);
        if ($response->code === ResponseCode::SUCCESS) {
            return $response->extFields['value'] ?? null;
        }
        return null;
    }

    public function deleteKvConfig(string $namespace, string $key): void
    {
        $this->invokeNamesrvAll(RequestCode::DELETE_KV_CONFIG, ['namespace' => $namespace, 'key' => $key]);
    }

    public function getKvListByNamespace(string $namespace): KVTable
    {
        $response = $this->invokeNamesrvOne(RequestCode::GET_KVLIST_BY_NAMESPACE, ['namespace' => $namespace]);
        if ($response->body === null || $response->body === '') {
            return new KVTable();
        }
        return KVTable::decode($response->body);
    }

    // ---------------- Controller ----------------

    /**
     * CONTROLLER_GET_METADATA_INFO(1005): ask one controller who the leader is.
     *
     * Fixed 3s timeout, plain address, and the answer is the response header itself
     * (``controllerLeaderAddress`` is the field the two-phase callers route to).
     *
     * @return array<string, string>
     */
    public function getControllerMetaData(string $controllerAddr): array
    {
        $response = $this->invokeBroker($controllerAddr, RequestCode::CONTROLLER_GET_METADATA_INFO, timeoutMillis: self::CONTROLLER_TIMEOUT, vip: false);
        return $response->extFields;
    }

    /** The leader address from a metadata answer; the two-phase RPCs cannot proceed without it. */
    private static function controllerLeaderAddress(array $meta): string
    {
        $leader = $meta['controllerLeaderAddress'] ?? null;
        if ($leader === null || $leader === '') {
            throw new MQClientException('controller leader address is missing');
        }
        return $leader;
    }

    /**
     * CONTROLLER_GET_SYNC_STATE_DATA(1006), two-phase: the metadata answer names the leader,
     * then the sync-state query goes to that leader with the broker names as the body (a raw
     * JSON string array) and returns the per-broker replicas state.
     *
     * @param list<string> $brokers
     */
    public function getInSyncStateData(string $controllerAddress, array $brokers): BrokerReplicasInfo
    {
        $meta = $this->getControllerMetaData($controllerAddress);
        $leaderAddress = self::controllerLeaderAddress($meta);
        $response = $this->invokeBroker(
            $leaderAddress,
            RequestCode::CONTROLLER_GET_SYNC_STATE_DATA,
            body: RemotingSerializable::encode(array_values($brokers)),
            timeoutMillis: self::CONTROLLER_TIMEOUT,
            vip: false,
        );
        if ($response->body === null || $response->body === '') {
            return new BrokerReplicasInfo();
        }
        return BrokerReplicasInfo::decode($response->body);
    }

    /**
     * GET_CONTROLLER_CONFIG(1010): read every controller's config (properties text).
     *
     * The controller list falls back to the configured name servers when the caller passes
     * none; with no addresses at all there is nothing to ask and null is returned. The body
     * of each answer is ``k=v`` text, not JSON.
     *
     * @param list<string>|null $controllerServers
     * @return array<string, array<string, string>>|null
     */
    public function getControllerConfig(?array $controllerServers = null, ?int $timeoutMillis = null): ?array
    {
        $client = $this->requireClient();
        $targets = ($controllerServers === null || $controllerServers === [])
            ? array_values($client->nameServerAddrs)
            : array_values($controllerServers);
        if ($targets === []) {
            return null;
        }
        $configMap = [];
        foreach ($targets as $controller) {
            try {
                $response = $this->invokeNamesrvAddr($controller, RequestCode::GET_CONTROLLER_CONFIG, timeoutMillis: $timeoutMillis);
            } catch (MQBrokerException $e) {
                throw new MQClientException($e->getMessage(), $e->responseCode, $e);
            }
            $configMap[$controller] = MixAll::string2Properties($response->body ?? '');
        }
        return $configMap;
    }

    /**
     * UPDATE_CONTROLLER_CONFIG(1009): push config entries to every controller.
     *
     * An empty property set (or an empty controller list) is a silent no-op; every response
     * is collected and the LAST failure is thrown as one client exception.
     *
     * @param array<string, string> $properties
     * @param list<string>|null $controllers
     */
    public function updateControllerConfig(array $properties, ?array $controllers = null, ?int $timeoutMillis = null): void
    {
        $text = MixAll::properties2String($properties);
        if ($text === '' || $controllers === null || $controllers === []) {
            return;
        }
        $client = $this->requireClient();
        $request = RemotingCommand::createRequestCommand(RequestCode::UPDATE_CONTROLLER_CONFIG, null);
        $request->body = $text;
        $errResponse = null;
        foreach (array_values($controllers) as $controller) {
            $response = $this->invokeSyncOn($client, $controller, $request, $timeoutMillis ?? $this->timeoutMillis);
            if ($response->code !== ResponseCode::SUCCESS) {
                $errResponse = $response;
            }
        }
        if ($errResponse !== null) {
            throw new MQClientException(
                ($errResponse->remark !== null && $errResponse->remark !== '')
                    ? $errResponse->remark : 'update controller config failed',
                $errResponse->code,
            );
        }
    }

    /**
     * CLEAN_BROKER_DATA(1011): drop one broker's metadata from the controller store.
     *
     * Two-phase like the sync-state read: metadata first, then the leader. The header always
     * carries ``brokerName`` plus the ``isCleanLivingBroker`` flag and the invoke timestamp;
     * the cluster name and the controller-id set travel only when the caller supplies them.
     */
    public function cleanControllerBrokerData(
        string $controllerAddr,
        ?string $clusterName,
        string $brokerName,
        ?string $brokerControllerIdsToClean = null,
        bool $isCleanLivingBroker = false,
    ): void {
        $meta = $this->getControllerMetaData($controllerAddr);
        $leaderAddress = self::controllerLeaderAddress($meta);
        $ext = [
            'brokerName' => $brokerName,
            'isCleanLivingBroker' => $isCleanLivingBroker ? 'true' : 'false',
            'invokeTime' => (string) (int) floor(microtime(true) * 1000.0),
        ];
        if ($clusterName !== null && $clusterName !== '') {
            $ext['clusterName'] = $clusterName;
        }
        if ($brokerControllerIdsToClean !== null && $brokerControllerIdsToClean !== '') {
            $ext['brokerControllerIdsToClean'] = $brokerControllerIdsToClean;
        }
        $this->invokeBroker($leaderAddress, RequestCode::CLEAN_BROKER_DATA, $ext, timeoutMillis: self::CONTROLLER_TIMEOUT, vip: false);
    }

    // ---------------- Subscription group management ----------------

    /** UPDATE_AND_CREATE_SUBSCRIPTIONGROUP: body is SubscriptionGroupConfig JSON. */
    public function createAndUpdateSubscriptionGroupConfig(string $addr, SubscriptionGroupConfig $config): void
    {
        $this->invokeBroker($addr, RequestCode::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, body: $config->encode());
    }

    /** Pull all subscription groups, then take the target group. */
    public function examineSubscriptionGroupConfig(string $addr, string $group): ?SubscriptionGroupConfig
    {
        $wrapper = $this->getAllSubscriptionGroup($addr);
        return $wrapper->subscriptionGroupTable[$group] ?? null;
    }

    /** GET_SUBSCRIPTIONGROUP_CONFIG: single-group query. */
    public function getSubscriptionGroupConfig(string $addr, string $group): ?SubscriptionGroupConfig
    {
        $response = $this->invokeBroker($addr, RequestCode::GET_SUBSCRIPTIONGROUP_CONFIG, ['group' => $group]);
        if ($response->body === null || $response->body === '') {
            return null;
        }
        return SubscriptionGroupConfig::decode($response->body);
    }

    /**
     * GET_ALL_SUBSCRIPTIONGROUP_CONFIG is a **paged** API: accumulate until groupSeq >= totalGroupNum-1.
     *
     * Old brokers do not carry totalGroupNum, in which case everything is returned at once (ends in a single round).
     */
    public function getAllSubscriptionGroup(string $brokerAddr, ?int $timeoutMillis = null): SubscriptionGroupWrapper
    {
        $client = $this->requireClient();
        $brokerAddr = MixAll::brokerVipChannel($this->vipChannelEnabled, $brokerAddr);
        $timeout = $timeoutMillis ?? $this->timeoutMillis;
        $pageSize = $this->maxPageSizeInGetMetadata > 0 ? $this->maxPageSizeInGetMetadata : 2000;
        $currentDataVersion = null;
        $groupSeq = 0;
        /** @var array<string, SubscriptionGroupConfig> $table */
        $table = [];
        /** @var array<string, mixed> $forbidden */
        $forbidden = [];
        $begin = hrtime(true) / 1e6;
        while (true) {
            $left = $timeout - (int) ((hrtime(true) / 1e6) - $begin);
            if ($left < 0) {
                throw new MQClientException('invokeSync call timeout');
            }
            $ext = [
                'groupSeq' => (string) $groupSeq,
                'maxGroupNum' => (string) $pageSize,
            ];
            // The key is always present; the first page carries the empty string, later pages the
            // JSON of the last dataVersion seen (same rule as the topic paging loop).
            $ext['dataVersion'] = $currentDataVersion === null
                ? ''
                : RemotingSerializable::toJson($currentDataVersion);
            $request = RemotingCommand::createRequestCommand(RequestCode::GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
            foreach ($ext as $k => $v) {
                $request->extFields[$k] = $v;
            }
            $response = $this->invokeSyncOn($client, $brokerAddr, $request, $left);
            if ($response->code !== ResponseCode::SUCCESS) {
                throw new MQBrokerException($response->code, $response->remark ?? '');
            }
            $wrapper = ($response->body !== null && $response->body !== '')
                ? SubscriptionGroupWrapper::decode($response->body)
                : new SubscriptionGroupWrapper();
            foreach ($wrapper->subscriptionGroupTable as $k => $cfg) {
                $table[$k] = $cfg;
            }
            foreach ($wrapper->forbiddenTable as $k => $v) {
                $forbidden[(string) $k] = $v;
            }
            $newVersion = $wrapper->dataVersion;
            if ($currentDataVersion === null) {
                $currentDataVersion = $newVersion;
            }
            $groupSeq += count($wrapper->subscriptionGroupTable);

            $totalRaw = $response->extFields['totalGroupNum'] ?? null;
            if ($totalRaw === null) {
                // Old broker: returns everything at once
                break;
            }
            $total = (int) $totalRaw;
            if ($currentDataVersion != $newVersion) {
                Logger::warning('subscription group dataVersion changed, restart paging');
                $currentDataVersion = $newVersion;
                $groupSeq = 0;
                $table = [];
                $forbidden = [];
                continue;
            }
            if ($groupSeq >= $total - 1) {
                break;
            }
        }

        $result = new SubscriptionGroupWrapper();
        $result->subscriptionGroupTable = $table;
        $result->forbiddenTable = $forbidden;
        $result->dataVersion = $currentDataVersion ?? [];
        return $result;
    }

    public function getUserSubscriptionGroup(string $brokerAddr, ?int $timeoutMillis = null): SubscriptionGroupWrapper
    {
        $wrapper = $this->getAllSubscriptionGroup($brokerAddr, $timeoutMillis);
        $kept = [];
        foreach ($wrapper->subscriptionGroupTable as $k => $v) {
            if (MixAll::isSysConsumerGroup($k) || MixAll::isPredefinedGroup($k)) {
                continue;
            }
            $kept[$k] = $v;
        }
        $wrapper->subscriptionGroupTable = $kept;
        return $wrapper;
    }

    public function deleteSubscriptionGroup(string $addr, string $groupName, bool $removeOffset = false): void
    {
        $this->invokeBroker($addr, RequestCode::DELETE_SUBSCRIPTIONGROUP, [
            'groupName' => $groupName,
            'cleanOffset' => $removeOffset ? 'true' : 'false',
        ]);
    }

    // ---------------- Consumer / producer connections ----------------

    public function examineConsumerConnectionInfo(string $consumerGroup, ?string $brokerAddr = null): ConsumerConnection
    {
        $addr = $brokerAddr ?? $this->findFirstBrokerAddr($this->requireClient());
        $response = $this->invokeBroker($addr, RequestCode::GET_CONSUMER_CONNECTION_LIST, ['consumerGroup' => $consumerGroup]);
        if ($response->body === null || $response->body === '') {
            throw new MQClientException(sprintf('consumer group %s not online', $consumerGroup));
        }
        return ConsumerConnection::decode($response->body);
    }

    public function examineConsumerConnection(string $consumerGroup, ?string $brokerAddr = null): ConsumerConnection
    {
        return $this->examineConsumerConnectionInfo($consumerGroup, $brokerAddr);
    }

    public function examineProducerConnectionInfo(string $producerGroup, ?string $brokerAddr = null): ProducerConnection
    {
        $addr = $brokerAddr ?? $this->findFirstBrokerAddr($this->requireClient());
        $response = $this->invokeBroker($addr, RequestCode::GET_PRODUCER_CONNECTION_LIST, ['producerGroup' => $producerGroup]);
        if ($response->body === null || $response->body === '') {
            return new ProducerConnection();
        }
        return ProducerConnection::decode($response->body);
    }

    public function examineConsumerRunningInfo(string $consumerGroup, string $clientId, bool $jstack = false, ?string $brokerAddr = null): ConsumerRunningInfo
    {
        $addr = $brokerAddr ?? $this->findFirstBrokerAddr($this->requireClient());
        $response = $this->invokeBroker($addr, RequestCode::GET_CONSUMER_RUNNING_INFO, [
            'consumerGroup' => $consumerGroup,
            'clientId' => $clientId,
            'jstackEnable' => $jstack ? 'true' : 'false',
        ]);
        if ($response->body === null || $response->body === '') {
            throw new MQClientException(sprintf('no running info for client %s', $clientId));
        }
        return ConsumerRunningInfo::decode($response->body);
    }

    public function getConsumerRunningInfo(string $consumerGroup, string $clientId, bool $jstack = false, ?string $brokerAddr = null): ConsumerRunningInfo
    {
        return $this->examineConsumerRunningInfo($consumerGroup, $clientId, $jstack, $brokerAddr);
    }

    public function getConsumerListByGroup(string $consumerGroup, ?string $brokerAddr = null): GetConsumerListByGroupResponseBody
    {
        $client = $this->requireClient();
        $addr = $brokerAddr ?? $this->findFirstBrokerAddr($client);
        return $client->getConsumerListByGroup($consumerGroup, addr: $addr);
    }

    // ---------------- Consume stats ----------------

    public function examineConsumeStats(string $brokerAddr, string $consumerGroup, ?string $topic = null, ?array $topicList = null, ?int $timeoutMillis = null): ConsumeStats
    {
        $ext = ['consumerGroup' => $consumerGroup];
        if ($topic !== null) {
            $ext['topic'] = $topic;
        }
        if ($topicList !== null) {
            $ext['topicList'] = implode(';', $topicList);
        }
        $response = $this->invokeBroker($brokerAddr, RequestCode::GET_CONSUME_STATS, $ext, timeoutMillis: $timeoutMillis);
        if ($response->body === null || $response->body === '') {
            return new ConsumeStats();
        }
        return ConsumeStats::decode($response->body);
    }

    public function fetchConsumeStatsInBroker(string $brokerAddr, bool $isOrder = false, ?int $timeoutMillis = null): ConsumeStatsList
    {
        $response = $this->invokeBroker(
            $brokerAddr,
            RequestCode::GET_BROKER_CONSUME_STATS,
            ['isOrder' => $isOrder ? 'true' : 'false'],
            timeoutMillis: $timeoutMillis,
        );
        if ($response->body === null || $response->body === '') {
            return new ConsumeStatsList();
        }
        return ConsumeStatsList::decode($response->body);
    }

    /**
     * The broker returns a groupList; it is deduplicated here before being returned as a list.
     *
     * @return list<string>
     */
    public function queryTopicConsumeByWho(string $brokerAddr, string $topic): array
    {
        $response = $this->invokeBroker($brokerAddr, RequestCode::QUERY_TOPIC_CONSUME_BY_WHO, ['topic' => $topic]);
        if ($response->body === null || $response->body === '') {
            return [];
        }
        $obj = RemotingSerializable::decodeJson($response->body);
        $groups = [];
        foreach ((array) ($obj['groupList'] ?? []) as $g) {
            $groups[(string) $g] = true;
        }
        return array_keys($groups);
    }

    // ---------------- Message tracking ----------------

    /**
     * ``examineConsumeStats(group[, topic])``: fan out to all brokers per the
     * ``%RETRY%<group>`` route, fetching and merging stats broker by broker
     * (offsetTable merged in, consumeTps accumulated);
     * throws MQClientException when everything is empty.
     */
    public function examineConsumeStatsGroup(string $consumerGroup, ?string $topic = null): ConsumeStats
    {
        $route = $this->examineTopicRoute(MixAll::getRetryTopic($consumerGroup));
        $result = new ConsumeStats();
        foreach ($route->brokerDatas as $bd) {
            $addr = $bd->selectBrokerAddr();
            if ($addr !== null && $addr !== '') {
                $part = $this->examineConsumeStats($addr, $consumerGroup, $topic);
                self::mergeMqTable($result->offsetTable, $part->offsetTable);
                $result->consumeTps += $part->consumeTps;
            }
        }
        if ($result->offsetTable === []) {
            throw new MQClientException(sprintf('no consume stats for group %s', $consumerGroup));
        }
        return $result;
    }

    /**
     * Fan-out consume stats: the route is looked up by the retry topic first, then the plain
     * topic, then the pop-retry topic (first hit wins); the per-broker tables are merged and
     * the TPS summed. An empty merged table is not a success: the group's connections are
     * probed and the empty result is mapped to CONSUMER_NOT_ONLINE or, for a broadcasting
     * group, to BROADCAST_CONSUMPTION.
     */
    public function examineConsumeStatsConcurrent(string $consumerGroup, ?string $topic = null): AdminToolResult
    {
        return $this->adminToolExecute(function () use ($consumerGroup, $topic): AdminToolResult {
            $route = null;
            $routeTopics = [MixAll::getRetryTopic($consumerGroup)];
            if ($topic !== null) {
                $routeTopics[] = $topic;
                $routeTopics[] = ExtraInfoUtil::buildPopRetryTopic($topic, $consumerGroup);
            }
            foreach ($routeTopics as $routeTopic) {
                try {
                    $route = $this->examineTopicRoute($routeTopic);
                    break;
                } catch (\Throwable) {
                    continue;
                }
            }
            if ($route === null || $route->getBrokerDatas() === []) {
                return AdminToolResult::failure(AdminToolsResultCodeEnum::TOPIC_ROUTE_INFO_NOT_EXIST, 'topic router info not found');
            }

            $result = new ConsumeStats();
            $consumerTpsMap = [];
            $this->fanOutWithDeadline($route->getBrokerDatas(), function (BrokerData $bd, int $left) use ($consumerGroup, $topic, $result, &$consumerTpsMap): void {
                $addr = $bd->selectBrokerAddr();
                if ($addr === null || $addr === '') {
                    return;
                }
                try {
                    $stats = $this->examineConsumeStats($addr, $consumerGroup, $topic, timeoutMillis: $left);
                    self::mergeMqTable($result->offsetTable, $stats->offsetTable);
                    $consumerTpsMap[$addr] = $stats->consumeTps;
                } catch (\Throwable $e) {
                    Logger::warning(sprintf('getConsumeStats error. topic=%s, consumerGroup=%s: %s', (string) $topic, $consumerGroup, $e->getMessage()));
                }
            });
            foreach ($consumerTpsMap as $tps) {
                $result->consumeTps += $tps;
            }

            if ($result->offsetTable === []) {
                try {
                    $connection = $this->examineConsumerConnectionInfo($consumerGroup);
                } catch (\Throwable) {
                    return AdminToolResult::failure(
                        AdminToolsResultCodeEnum::CONSUMER_NOT_ONLINE,
                        'Not found the consumer group consume stats, because return offset table is empty, '
                        . 'maybe the consumer not consume any message',
                    );
                }
                if ($connection->messageModel === 'BROADCASTING') {
                    return AdminToolResult::failure(
                        AdminToolsResultCodeEnum::BROADCAST_CONSUMPTION,
                        'Not found the consumer group consume stats, because return offset table is empty, '
                        . 'the consumer is under the broadcast mode',
                    );
                }
            }
            return AdminToolResult::success($result);
        });
    }

    /**
     * Whether the group's consumerOffset on this queue has already passed this message's
     * queueOffset (offset passed => consumed).
     */
    public function consumed(MessageExt $msg, string $group): bool
    {
        $cstats = $this->examineConsumeStatsGroup($group);
        $ci = $this->examineBrokerClusterInfo();
        $storeHost = $msg->getStoreHostString();
        foreach ($cstats->offsetTable as ['mq' => $mq, 'value' => $wrapper]) {
            if ($mq->topic === $msg->topic && $mq->queueId === $msg->queueId) {
                $brokerAddrs = $ci->brokerAddrTable[$mq->brokerName] ?? null;
                if ($brokerAddrs !== null) {
                    $addr = $brokerAddrs[MixAll::MASTER_ID] ?? null;
                    // Broker addresses are registered as ip:port, so compare directly with the message's storeHost.
                    if ($addr !== null && $storeHost !== null && $storeHost !== '' && $addr === $storeHost) {
                        if ($wrapper->consumerOffset > $msg->queueOffset) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Query who is consuming this topic, judging per group CONSUMED / FILTERED / PULL / NOT_ONLINE / BROADCASTING...
     *
     * @return list<MessageTrack>
     */
    public function messageTrackDetail(MessageExt $msg): array
    {
        $result = [];
        $route = $this->examineTopicRoute($msg->topic);
        $brokerAddr = null;
        foreach ($route->brokerDatas as $bd) {
            $brokerAddr = $bd->selectBrokerAddr();
            if ($brokerAddr !== null) {
                break;
            }
        }
        if ($brokerAddr === null) {
            return $result;
        }
        $groups = $this->queryTopicConsumeByWho($brokerAddr, $msg->topic);
        // The broker's return order is unstable; sort here to make the output deterministic
        $sorted = $groups;
        sort($sorted, SORT_STRING);
        foreach ($sorted as $group) {
            $result[] = $this->trackMessageForGroup($msg, $group);
        }
        return $result;
    }

    /**
     * The per-group verdict shared by the sequential and the fan-out message tracking:
     * connection errors are encoded into the track (NOT_ONLINE for 206) instead of failing
     * the whole call, and the pull / push / broadcast / filtered cases follow the group's
     * consume type.
     */
    private function trackMessageForGroup(MessageExt $msg, string $group): MessageTrack
    {
        $mt = new MessageTrack(consumerGroup: $group);
        try {
            $cc = $this->examineConsumerConnectionInfo($group);
        } catch (MQBrokerException $e) {
            if ($e->responseCode === ResponseCode::CONSUMER_NOT_ONLINE) {
                $mt->trackType = TrackType::NOT_ONLINE;
            }
            $mt->exceptionDesc = sprintf('CODE:%s DESC:%s', $e->responseCode, $e->getMessage());
            return $mt;
        } catch (\Throwable $e) {
            $mt->exceptionDesc = $e->getMessage();
            return $mt;
        }

        if ($cc->consumeType === 'CONSUME_ACTIVELY') {
            $mt->trackType = TrackType::PULL;
            return $mt;
        }
        if ($cc->consumeType === 'CONSUME_PASSIVELY') {
            try {
                $ifConsumed = $this->consumed($msg, $group);
            } catch (MQBrokerException|MQClientException $e) {
                if ($e->responseCode === ResponseCode::CONSUMER_NOT_ONLINE) {
                    $mt->trackType = TrackType::NOT_ONLINE;
                    $mt->exceptionDesc = sprintf('CODE:%s DESC:%s', $e->responseCode, $e->getMessage());
                } elseif ($e->responseCode === ResponseCode::BROADCAST_CONSUMPTION) {
                    $mt->trackType = TrackType::CONSUME_BROADCASTING;
                }
                return $mt;
            } catch (\Throwable $e) {
                $mt->exceptionDesc = $e->getMessage();
                return $mt;
            }

            if ($ifConsumed) {
                $mt->trackType = TrackType::CONSUMED;
                // Also check this topic's tagsSet in the subscription table: non-empty and containing
                // neither the message tag nor "*" => the subscription is narrower than the message,
                // so the message is the filtered-out part (SQL92 subscriptions have an empty
                // tagsSet and likewise fall back to CONSUMED -- the existing judgment
                // convention, kept unchanged).
                $sub = $cc->subscriptionTable[$msg->topic] ?? null;
                if (is_array($sub)) {
                    $tagsSet = array_map(strval(...), array_values((array) ($sub['tagsSet'] ?? [])));
                    $msgTag = $msg->getTags();
                    if ($tagsSet !== [] && !in_array('*', $tagsSet, true) && !in_array((string) $msgTag, $tagsSet, true)) {
                        $mt->trackType = TrackType::CONSUMED_BUT_FILTERED;
                    }
                }
            } else {
                $mt->trackType = TrackType::NOT_CONSUME_YET;
            }
        }
        return $mt;
    }

    /**
     * Fan-out message tracking: the groups are discovered once (first broker of the topic's
     * route answers QUERY_TOPIC_CONSUME_BY_WHO) and then every group is tracked independently.
     *
     * The result is the plain track list: a group whose probing fails keeps its own
     * MessageTrack (exceptionDesc + trackType), the batch itself never fails.
     *
     * @return list<MessageTrack>
     */
    public function messageTrackDetailConcurrent(MessageExt $msg): array
    {
        $result = [];
        $route = $this->examineTopicRoute($msg->topic);
        $brokerAddr = null;
        foreach ($route->brokerDatas as $bd) {
            $brokerAddr = $bd->selectBrokerAddr();
            if ($brokerAddr !== null) {
                break;
            }
        }
        if ($brokerAddr === null) {
            return $result;
        }
        $groups = $this->queryTopicConsumeByWho($brokerAddr, $msg->topic);
        $this->fanOutWithDeadline($groups, function (string $group) use ($msg, &$result): void {
            $result[] = $this->trackMessageForGroup($msg, $group);
        });
        return $result;
    }

    /**
     * The raw single-broker call of QUERY_TOPICS_BY_CONSUMER(343).
     *
     * The broker side reverse-looks-up which topics this group has consumed from the offset
     * table (``topic@group`` keys). So an empty table is returned when the group has never
     * committed offsets; this is expected, not a bug.
     */
    public function queryTopicsByConsumerToBroker(string $brokerAddr, string $group, ?int $timeoutMillis = null): TopicList
    {
        $response = $this->invokeBroker($brokerAddr, RequestCode::QUERY_TOPICS_BY_CONSUMER, ['group' => $group], timeoutMillis: $timeoutMillis);
        if ($response->body === null || $response->body === '') {
            return new TopicList();
        }
        return TopicList::decode($response->body);
    }

    /**
     * First query the route by ``%RETRY%<group>``, then send 343 to every broker in the route and merge.
     * The protocol's topicList has set semantics; it is deduplicated here before being returned as a list.
     */
    public function queryTopicsByConsumer(string $group): TopicList
    {
        $route = $this->examineTopicRoute(MixAll::getRetryTopic($group));
        $result = new TopicList();
        $seen = [];
        foreach ($route->getBrokerDatas() as $bd) {
            $addr = $bd->selectBrokerAddr();
            if ($addr === null || $addr === '') {
                continue;
            }
            foreach ($this->queryTopicsByConsumerToBroker($addr, $group)->getTopicList() as $topic) {
                if (!isset($seen[$topic])) {
                    $seen[$topic] = true;
                    $result->topicList[] = $topic;
                }
            }
        }
        return $result;
    }

    /**
     * Fan-out topic query by consumer: route by the retry topic, one QUERY_TOPICS_BY_CONSUMER
     * per broker, results unioned (a failing broker only shrinks the union).
     */
    public function queryTopicsByConsumerConcurrent(string $group): AdminToolResult
    {
        return $this->adminToolExecute(function () use ($group): AdminToolResult {
            // The route lookup answers null (not an error) when the retry topic has no route,
            // so the fixed failure message below covers both the null and empty cases.
            $route = $this->requireClient()->getTopicRouteData(MixAll::getRetryTopic($group));
            $brokerDatas = $route?->getBrokerDatas() ?? [];
            if ($brokerDatas === []) {
                return AdminToolResult::failure(AdminToolsResultCodeEnum::TOPIC_ROUTE_INFO_NOT_EXIST, 'router info not found.');
            }
            $result = new TopicList();
            $seen = [];
            $this->fanOutWithDeadline($brokerDatas, function (BrokerData $bd, int $left) use ($group, $result, &$seen): void {
                $addr = $bd->selectBrokerAddr();
                if ($addr === null || $addr === '') {
                    return;
                }
                try {
                    foreach ($this->queryTopicsByConsumerToBroker($addr, $group, $left)->getTopicList() as $topic) {
                        if (!isset($seen[$topic])) {
                            $seen[$topic] = true;
                            $result->topicList[] = $topic;
                        }
                    }
                } catch (\Throwable $e) {
                    Logger::warning(sprintf('queryTopicsByConsumer error. group=%s, broker=%s: %s', $group, $addr, $e->getMessage()));
                }
            });
            return AdminToolResult::success($result);
        });
    }

    public function querySubscription(string $brokerAddr, string $group, string $topic): ?array
    {
        $response = $this->invokeBroker($brokerAddr, RequestCode::QUERY_SUBSCRIPTION_BY_CONSUMER, ['group' => $group, 'topic' => $topic]);
        if ($response->body === null || $response->body === '') {
            return null;
        }
        $obj = RemotingSerializable::fastjsonLoads($response->body);
        return is_array($obj) ? $obj : null;
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    public function getConsumeStatus(string $brokerAddr, string $topic, string $group, string $clientAddr = ''): array
    {
        $response = $this->invokeBroker(
            $brokerAddr,
            RequestCode::INVOKE_BROKER_TO_GET_CONSUMER_STATUS,
            ['topic' => $topic, 'group' => $group, 'clientAddr' => $clientAddr],
        );
        if ($response->body === null || $response->body === '') {
            return [];
        }
        $obj = RemotingSerializable::fastjsonLoads($response->body);
        $table = is_array($obj) ? (array) ($obj['consumerTable'] ?? []) : [];
        return $table;
    }

    public function cloneGroupOffset(string $brokerAddr, string $srcGroup, string $destGroup, string $topic, bool $isOffline = false): void
    {
        $this->invokeBroker($brokerAddr, RequestCode::CLONE_GROUP_OFFSET, [
            'srcGroup' => $srcGroup,
            'destGroup' => $destGroup,
            'topic' => $topic,
            'offline' => $isOffline ? 'true' : 'false',
        ]);
    }

    // ---------------- Offset management ----------------

    public function maxOffset(MessageQueue $mq): int
    {
        return $this->requireClient()->getMaxOffset($mq);
    }

    public function minOffset(MessageQueue $mq): int
    {
        return $this->requireClient()->getMinOffset($mq);
    }

    /** SEARCH_OFFSET_BY_TIMESTAMP: explicitly send the LOWER boundary. */
    public function searchOffset(MessageQueue $mq, int $timestamp): int
    {
        return $this->requireClient()->searchOffsetByTimestamp($mq, $timestamp, boundaryType: BoundaryType::LOWER);
    }

    /** Find the lower-bound offset by timestamp. */
    public function searchLowerBoundaryOffset(MessageQueue $mq, int $timestamp): int
    {
        return $this->requireClient()->searchOffsetByTimestamp($mq, $timestamp, boundaryType: BoundaryType::LOWER);
    }

    /**
     * The UPPER boundary of SEARCH_OFFSET_BY_TIMESTAMP.
     *
     * The difference from LOWER is only visible when multiple messages share a storeTime,
     * or when the timestamp falls in a gap / at the tail: past the tail, UPPER returns the
     * last message's own offset, while LOWER returns its next offset (maxOffset).
     */
    public function searchUpperBoundaryOffset(MessageQueue $mq, int $timestamp): int
    {
        return $this->requireClient()->searchOffsetByTimestamp($mq, $timestamp, boundaryType: BoundaryType::UPPER);
    }

    public function earliestMsgStoreTime(MessageQueue $mq): int
    {
        $client = $this->requireClient();
        // earliestMsgStoreTime has the same shape as max/min/search:
        // master only, refresh the route once and re-query; if still unavailable, throw "The broker[X] not exist".
        $addr = $this->publishAddrInAdmin($client, $mq);
        $response = $this->invokeBroker($addr, RequestCode::GET_EARLIEST_MSG_STORETIME, [
            'topic' => $mq->topic,
            'queueId' => $mq->queueId,
            'brokerName' => $mq->brokerName,
        ]);
        return (int) (($response->extFields['timestamp'] ?? 0) ?: 0);
    }

    public function examineConsumerOffset(string $consumerGroup, MessageQueue $mq): ?int
    {
        return $this->requireClient()->queryConsumerOffset($consumerGroup, $mq);
    }

    public function updateConsumerOffset(string $consumerGroup, MessageQueue $mq, int $offset): void
    {
        $this->requireClient()->updateConsumerOffset($consumerGroup, $mq, $offset);
    }

    public function updateConsumerOffsetToBroker(string $brokerAddr, string $consumerGroup, MessageQueue $mq, int $offset): void
    {
        $this->requireClient()->updateConsumerOffset($consumerGroup, $mq, $offset, addr: $brokerAddr);
    }

    /**
     * Reset offsets by timestamp.
     *
     * Send INVOKE_BROKER_TO_RESET_OFFSET broker by broker (the broker side computes the new
     * offset by timestamp, syncs online consumers + updates the offset table), aggregating
     * ``Map<MessageQueue, Long>``.
     *
     * ``is_cpp`` only affects the shape of the 220 message the broker pushes to **online
     * consumers**: the broker judges by the initiator's (i.e. this request's) language;
     * CPP returns ``ResetOffsetBodyForC`` (JSON array), others return ``ResetOffsetBody``
     * (object-as-key map). The admin side defaults to the latter per protocol;
     * this port defaults to ``false``.
     *
     * Note: this **no longer** uses the old local implementation of "per-queue searchOffset +
     * updateConsumerOffset" -- that does not sync online consumers and does no
     * broker-side consistency check.
     *
     * @return list<array{mq: MessageQueue, offset: int}>
     */
    public function resetOffsetByTimestamp(
        string $topic,
        string $group,
        int $timestamp,
        bool $isForce = true,
        ?string $clusterName = null,
        bool $isCpp = false,
    ): array {
        $routeTopic = $topic;
        if ($topic !== '' && (MixAll::isLmq($topic) || $topic === MixAll::SYSTEM_TOPIC_PREFIX . 'wheel_timer') && $clusterName !== null) {
            $routeTopic = $clusterName;
        }
        $route = $this->examineTopicRoute($routeTopic);
        /** @var list<array{mq: MessageQueue, offset: int}> $allOffsets */
        $allOffsets = [];
        foreach ($route->getBrokerDatas() as $bd) {
            $addr = $bd->selectBrokerAddr();
            if ($addr === null || $addr === '') {
                continue;
            }
            self::mergeOffsetTable($allOffsets, $this->invokeBrokerResetOffset($addr, $topic, $group, $timestamp, $isForce, $isCpp));
        }
        if ($allOffsets === []) {
            throw new MQClientException('reset offset failed, no broker returned offset table');
        }
        return $allOffsets;
    }

    /**
     * One ``INVOKE_BROKER_TO_RESET_OFFSET``(222), returning the queue list the broker actually reset.
     *
     * 222 has two usages: with only timestamp + isForce it resets the whole topic by
     * timestamp; with ``queueId`` + ``offset`` it resets a single queue. When ``offset``
     * is null, write ``-1`` per protocol (the broker treats it as null and switches to
     * computing the offset by timestamp).
     *
     * @return list<array{mq: MessageQueue, offset: int}>
     */
    private function invokeBrokerResetOffset(
        string $brokerAddr,
        string $topic,
        string $group,
        int $timestamp,
        bool $isForce,
        bool $isCpp,
        ?int $queueId = null,
        ?int $offset = null,
        ?int $timeoutMillis = null,
    ): array {
        $client = $this->requireClient();
        $request = RemotingCommand::createRequestCommand(RequestCode::INVOKE_BROKER_TO_RESET_OFFSET, null);
        $ext = [
            'topic' => $topic,
            'group' => $group,
            'timestamp' => (string) $timestamp,
            // The key name is **isForce**, not force: the broker side reads the request header field named isForce.
            // When written as force, the broker's isForce is always false and the reset branch degrades to
            // "take the timestamp offset"; a rewind (timestamp=-1) echoes consumerOffset as-is
            // instead of jumping to maxOffset.
            // 5.5.1 live probe: {"force":"true", timestamp:-1} -> target 3 (=consumerOffset),
            //               {"isForce":"true", timestamp:-1} -> target 10 (=maxOffset).
            'isForce' => $isForce ? 'true' : 'false',
            // offset=-1 in the protocol means offset is empty
            'offset' => (string) ($offset ?? -1),
        ];
        if ($queueId !== null) {
            $ext['queueId'] = (string) $queueId;
        }
        foreach ($ext as $k => $v) {
            $request->extFields[$k] = $v;
        }
        if ($isCpp) {
            $request->language = LanguageCode::CPP->value;
        }
        $vipAddr = MixAll::brokerVipChannel($this->vipChannelEnabled, $brokerAddr);
        $response = $this->invokeSyncOn($client, $vipAddr, $request, $timeoutMillis ?? $this->timeoutMillis);
        if ($response->code !== ResponseCode::SUCCESS) {
            throw new MQClientException(
                ($response->remark !== null && $response->remark !== '') ? $response->remark : 'reset offset failed',
                $response->code,
            );
        }
        if ($response->body === null || $response->body === '') {
            // A SUCCESS without the offset table is not an empty reset: the queue table is the
            // only payload, so its absence is reported as the response code the broker answered with.
            throw new MQClientException(
                ($response->remark !== null && $response->remark !== '') ? $response->remark : 'reset offset failed',
                $response->code,
            );
        }
        return ResetOffsetBody::decode($response->body)->offsetTable;
    }

    /**
     * Reset offsets by queue.
     *
     * **Two** RPCs must be made, neither is optional:
     * 1. ``updateConsumerOffset``(25) directly changes offsetTable to the target offset;
     * 2. the 222 with ``queueId`` + ``offset`` goes through the broker's reset entry,
     *    first validating the target offset by ``[min, max+1]`` (out-of-range returns SYSTEM_ERROR
     *    ``Target offset N not in consume queue range [min-max]``), then writing to the
     *    broker's resetOffset table -- which writes both ``resetOffsetTable`` (one-shot,
     *    taken by the next pull via ``queryThenEraseResetOffset``) and ``offsetTable``, and clears
     *    that queue's POP in-flight count. Doing only step 1 leaves online consumers
     *    continuing to pull with their own in-memory offsets.
     *
     * The protocol's return value itself carries no result (only logging); here the queue
     * list the broker reports is returned for the caller to verify.
     *
     * WARNING: measured (5.5.1 live) these two RPCs are **not atomic**: the broker's commitOffset
     * only does an overwrite write (even a shrinking offset only logs a ``[NOTIFYME]``
     * warn, with no range check), so when the second RPC is rejected with
     * ``Target offset N not in consume queue range [min-max]``,
     * the first RPC has already persisted the illegal offset. No protective rollback is done here either.
     *
     * @return list<array{mq: MessageQueue, offset: int}>
     */
    public function resetOffsetByQueueId(string $brokerAddr, string $consumerGroup, string $topic, int $queueId, int $resetOffset): array
    {
        $this->updateConsumerOffsetToBroker(
            $brokerAddr,
            $consumerGroup,
            new MessageQueue($topic, '', $queueId),
            $resetOffset,
        );
        // Single-queue reset carries no force (default false) and passes timestamp 0 (offset is already given, not used in computation)
        return $this->invokeBrokerResetOffset(
            $brokerAddr,
            $topic,
            $consumerGroup,
            0,
            false,
            false,
            queueId: $queueId,
            offset: $resetOffset,
        );
    }

    /** Try the new version first (broker-side reset); degrade to the old local implementation when the consumer is not online. */
    public function resetOffsetNew(string $consumerGroup, string $topic, int $timestamp): void
    {
        try {
            $this->resetOffsetByTimestamp($topic, $consumerGroup, $timestamp, true);
        } catch (MQClientException $e) {
            if ($e->responseCode === ResponseCode::CONSUMER_NOT_ONLINE) {
                $this->resetOffsetByTimestampOld($consumerGroup, $topic, $timestamp, true);
                return;
            }
            throw $e;
        }
    }

    /**
     * Fan-out reset: one broker-side reset per broker of the topic route.
     *
     * A broker that answers CONSUMER_NOT_ONLINE is redone with the legacy per-queue reset;
     * SYSTEM_ERROR usually means "the group never came online", which counts as done. The
     * envelope only turns into a failure when at least one broker is missing from the
     * success list — the payload still carries both address lists.
     */
    public function resetOffsetNewConcurrent(string $group, string $topic, int $timestamp): AdminToolResult
    {
        return $this->adminToolExecute(function () use ($group, $topic, $timestamp): AdminToolResult {
            // The route lookup answers null (not an error) when the topic has no route,
            // so the fixed failure message below covers both the null and empty cases.
            $route = $this->requireClient()->getTopicRouteData($topic);
            $brokerDatas = $route?->getBrokerDatas() ?? [];
            if ($brokerDatas === []) {
                return AdminToolResult::failure(AdminToolsResultCodeEnum::TOPIC_ROUTE_INFO_NOT_EXIST, 'topic router info not found');
            }
            $routeMap = [];
            foreach ($route->queueDatas as $queueData) {
                $routeMap[$queueData->brokerName] = $queueData;
            }
            $successList = [];
            $failureList = [];
            $this->fanOutWithDeadline($brokerDatas, function (BrokerData $bd, int $left) use ($group, $topic, $timestamp, $routeMap, &$successList, &$failureList): void {
                $addr = $bd->selectBrokerAddr();
                if ($addr === null || $addr === '') {
                    return;
                }
                try {
                    $this->invokeBrokerResetOffset($addr, $topic, $group, $timestamp, true, false, timeoutMillis: $left);
                    $successList[] = $addr;
                } catch (MQClientException $e) {
                    if ($e->responseCode === ResponseCode::CONSUMER_NOT_ONLINE) {
                        try {
                            $this->resetOffsetByTimestampOldOnBroker($addr, $routeMap[$bd->brokerName] ?? null, $group, $topic, $timestamp, true);
                            $successList[] = $addr;
                        } catch (\Throwable $e2) {
                            Logger::warning(sprintf('resetOffsetByTimestampOld error. addr=%s, topic=%s, group=%s, timestamp=%d: %s', $addr, $topic, $group, $timestamp, $e2->getMessage()));
                            $failureList[] = $addr;
                        }
                    } elseif ($e->responseCode === ResponseCode::SYSTEM_ERROR) {
                        // CODE: 1  DESC: the consumer group not exist, never online
                        $successList[] = $addr;
                    } else {
                        $failureList[] = $addr;
                        Logger::warning(sprintf('resetOffsetNewConcurrent error. addr=%s, topic=%s, group=%s, timestamp=%d: %s', $addr, $topic, $group, $timestamp, $e->getMessage()));
                    }
                } catch (\Throwable $e) {
                    $failureList[] = $addr;
                    Logger::warning(sprintf('resetOffsetNewConcurrent error. addr=%s, topic=%s, group=%s, timestamp=%d: %s', $addr, $topic, $group, $timestamp, $e->getMessage()));
                }
            });
            $result = new BrokerOperatorResult();
            $result->successList = $successList;
            $result->failureList = $failureList;
            if (count($result->successList) === count($brokerDatas)) {
                return AdminToolResult::success($result);
            }
            return AdminToolResult::failure(AdminToolsResultCodeEnum::MQ_BROKER_ERROR, 'operator failure', $result);
        });
    }

    /**
     * The legacy reset restricted to ONE broker: queues the group actually consumed are reset
     * from the consume-stats table; when the group never consumed here, the queue list comes
     * from the route's queue data and the broker's own topic stats.
     *
     * @return list<array<string, mixed>>
     */
    private function resetOffsetByTimestampOldOnBroker(
        string $brokerAddr,
        ?QueueData $queueData,
        string $consumerGroup,
        string $topic,
        int $timestamp,
        bool $force,
    ): array {
        $client = $this->requireClient();
        if ($queueData === null) {
            throw new MQClientException(sprintf('no queue data for broker %s', $brokerAddr));
        }
        $stats = $this->examineConsumeStats($brokerAddr, $consumerGroup);
        /** @var list<array<string, mixed>> $result */
        $result = [];
        $hasConsumed = false;
        foreach ($stats->offsetTable as ['mq' => $mq, 'value' => $wrapper]) {
            if ($mq->topic === $topic) {
                $hasConsumed = true;
                $result[] = $this->resetOffsetConsumeOffset($client, $brokerAddr, $consumerGroup, $mq, $wrapper, $timestamp, $force);
            }
        }
        if (!$hasConsumed) {
            $topicStatus = [];
            foreach ($this->examineTopicStatsByBroker($brokerAddr, $topic)->offsetTable as ['mq' => $mq, 'value' => $to]) {
                $topicStatus[message_queue_key($mq)] = $to;
            }
            for ($i = 0; $i < $queueData->readQueueNums; $i++) {
                $mq = new MessageQueue($topic, $queueData->brokerName, $i);
                $offset = $topicStatus[message_queue_key($mq)] ?? null;
                $wrapper = new OffsetWrapper(
                    $offset?->maxOffset ?? 0,
                    $offset?->minOffset ?? 0,
                );
                $result[] = $this->resetOffsetConsumeOffset($client, $brokerAddr, $consumerGroup, $mq, $wrapper, $timestamp, $force);
            }
        }
        return $result;
    }

    /** One queue of the legacy reset: find the offset by timestamp, write it back when forced or behind. */
    private function resetOffsetConsumeOffset(
        MQClientInstance $client,
        string $brokerAddr,
        string $consumeGroup,
        MessageQueue $queue,
        OffsetWrapper $offsetWrapper,
        int $timestamp,
        bool $force,
    ): array {
        if ($timestamp === -1) {
            $resetOffset = $client->getMaxOffset($queue, timeoutMillis: $this->timeoutMillis, addr: $brokerAddr);
        } else {
            $resetOffset = $client->searchOffsetByTimestamp($queue, $timestamp, timeoutMillis: $this->timeoutMillis, addr: $brokerAddr);
        }
        if ($force || $resetOffset <= $offsetWrapper->consumerOffset) {
            $client->updateConsumerOffset($consumeGroup, $queue, $resetOffset, timeoutMillis: $this->timeoutMillis, addr: $brokerAddr);
        }
        return [
            'brokerName' => $queue->brokerName,
            'queueId' => $queue->queueId,
            'brokerOffset' => $offsetWrapper->brokerOffset,
            'consumerOffset' => $offsetWrapper->consumerOffset,
            'timestampOffset' => $resetOffset,
            'rollbackOffset' => ($force || $resetOffset <= $offsetWrapper->consumerOffset) ? $resetOffset : $offsetWrapper->consumerOffset,
        ];
    }

    /**
     * Old-style reset by timestamp: searchOffset per queue, then write back per the force decision (does not notify online consumers).
     *
     * @return list<array{mq: MessageQueue, offset: int}>
     */
    public function resetOffsetByTimestampOld(string $consumerGroup, string $topic, int $timestamp, bool $force = true): array
    {
        $client = $this->requireClient();
        $route = $this->examineTopicRoute($topic);
        /** @var list<array{mq: MessageQueue, offset: int}> $result */
        $result = [];
        foreach ($route->getBrokerDatas() as $bd) {
            $addr = $bd->selectBrokerAddr();
            if ($addr === null || $addr === '') {
                continue;
            }
            foreach ($route->queueDatas as $qd) {
                if ($qd->brokerName !== $bd->brokerName) {
                    continue;
                }
                for ($queueId = 0; $queueId < $qd->readQueueNums; $queueId++) {
                    $mq = new MessageQueue($topic, $bd->brokerName, $queueId);
                    try {
                        $consumerOffset = $client->queryConsumerOffset($consumerGroup, $mq, addr: $addr) ?? 0;
                    } catch (\Throwable) {
                        $consumerOffset = 0;
                    }
                    if ($timestamp === -1) {
                        $resetOffset = $client->getMaxOffset($mq, addr: $addr);
                    } else {
                        $resetOffset = $client->searchOffsetByTimestamp($mq, $timestamp, addr: $addr);
                    }
                    if ($force || $resetOffset <= $consumerOffset) {
                        $client->updateConsumerOffset($consumerGroup, $mq, $resetOffset, addr: $addr);
                        $result[] = ['mq' => $mq, 'offset' => $resetOffset];
                    }
                }
            }
        }
        return $result;
    }

    /**
     * Merge a ResetOffset-shaped offset table with dict.update semantics (same key: later value overwrites earlier).
     *
     * @param list<array{mq: MessageQueue, offset: int}> $dst
     * @param list<array{mq: MessageQueue, offset: int}> $src
     */
    private static function mergeOffsetTable(array &$dst, array $src): void
    {
        $byKey = [];
        foreach ($dst as ['mq' => $mq, 'offset' => $v]) {
            $byKey[message_queue_key($mq)] = ['mq' => $mq, 'offset' => $v];
        }
        foreach ($src as ['mq' => $mq, 'offset' => $v]) {
            $byKey[message_queue_key($mq)] = ['mq' => $mq, 'offset' => $v];
        }
        $dst = array_values($byKey);
    }

    /** Admin's offset query convention: master only; report an error when the master is gone. */
    private function publishAddrInAdmin(MQClientInstance $client, MessageQueue $mq): string
    {
        // The corresponding implementation in MQClientInstance is private (publishAddrInAdmin); assembled here with the same convention:
        // look up the publish address (master only) -> refresh the route once -> re-query -> if still unavailable, throw "The broker[X] not exist".
        $addr = $client->findBrokerAddressInPublish($mq->brokerName);
        if ($addr === null || $addr === '') {
            $client->updateTopicRouteInfoFromNameServer($mq->topic);
            $addr = $client->findBrokerAddressInPublish($mq->brokerName);
        }
        if ($addr === null || $addr === '') {
            throw new MQClientException(sprintf('The broker[%s] not exist', $mq->brokerName));
        }
        return $addr;
    }

    // ---------------- Message query ----------------

    /** Query messages by KEYS: query all brokers + client-side secondary key validation. */
    public function queryMessage(string $topic, string $key, int $maxNum, int $begin, int $end): array
    {
        return $this->requireClient()->queryMessageAllBrokers(
            $topic,
            $key,
            $maxNum,
            $begin,
            $end,
            indexType: MessageConst::INDEX_KEY_TYPE,
            uniqKey: false,
        );
    }

    /**
     * Query messages by uniqKey: indexType="U" + extFields["_UNIQUE_KEY_QUERY"]="true".
     *
     * Note: the broker-side uniqKey index is only supported by the RocksDB index
     * implementation (IndexRocksDBStore); under the default file index this query may
     * return empty, which is a broker configuration difference rather than a client issue.
     */
    public function queryMessageByUniqKey(string $topic, string $uniqKey): ?MessageExt
    {
        $messages = $this->requireClient()->queryMessageAllBrokers(
            $topic,
            $uniqKey,
            32,
            0,
            (int) floor(microtime(true) * 1000.0) + 60 * 60 * 1000,
            indexType: MessageConst::INDEX_UNIQUE_TYPE,
            uniqKey: true,
        );
        return $messages[0] ?? null;
    }

    /** Query by the normal KEYS index (corresponds to the NORMAL mode of the queryMsgByKey tool).
     *
     * @return list<MessageExt>
     */
    public function queryMessageByKey(string $topic, string $key, int $maxNum = 32): array
    {
        return $this->requireClient()->queryMessageAllBrokers(
            $topic,
            $key,
            $maxNum,
            0,
            (int) floor(microtime(true) * 1000.0) + 60 * 60 * 1000,
            indexType: MessageConst::INDEX_KEY_TYPE,
            uniqKey: false,
        );
    }

    /**
     * View a single message.
     *
     * First decode the broker address + commitLog offset from offsetMsgId and go through
     * VIEW_MESSAGE_BY_ID, falling back to the UNIQ_KEY index on **any** failure. This
     * fallback must be kept: a 5.x client's msgId is a client-generated uniqKey, also 32
     * hex digits, so hard-decoding would assemble a nonexistent ip:port
     * (a port beyond 16 bits is not a valid offsetMsgId; it is validated here and the
     * fallback is taken, without throwing the exception to the caller and breaking the exception contract).
     */
    public function viewMessage(string $topic, string $msgId): MessageExt
    {
        $byIdError = null;
        try {
            [$ip, $port, $offset] = MessageDecoder::decodeMessageId($msgId);
            if (!($port > 0 && $port <= 65535)) {
                throw new MQClientException(sprintf('not a valid offset msgId: %s', $msgId));
            }
            $response = $this->invokeBroker(
                sprintf('%s:%d', $ip, $port),
                RequestCode::VIEW_MESSAGE_BY_ID,
                ['topic' => $topic, 'offset' => (string) $offset],
            );
            if ($response->body !== null && $response->body !== '') {
                $msg = MessageDecoder::decodeMessage($response->body);
                if ($msg !== null) {
                    return $msg;
                }
                $byIdError = new MQBrokerException(ResponseCode::NO_MESSAGE, sprintf('message not found: %s', $msgId));
            } else {
                $byIdError = new MQBrokerException(ResponseCode::NO_MESSAGE, sprintf('message not found: %s', $msgId));
            }
        } catch (\Throwable $e) {
            // Only record the error and take the uniqKey fallback
            $byIdError = $e;
        }

        $found = $this->queryMessageByUniqKey($topic, $msgId);
        if ($found !== null) {
            return $found;
        }
        throw new MQClientException(
            sprintf(
                'viewMessage failed: neither offset msgId nor uniq key matched message %s of %s',
                $msgId,
                $topic,
            ),
            ResponseCode::NO_MESSAGE,
            $byIdError,
        );
    }

    public function consumeMessageDirectly(
        string $consumerGroup,
        string $clientId,
        string $topic,
        string $msgId,
        ?string $brokerAddr = null,
    ): ConsumeMessageDirectlyResult {
        $addr = $brokerAddr;
        $outMsgId = $msgId;
        if ($addr === null || $addr === '') {
            $msg = $this->viewMessage($topic, $msgId);
            $addr = $msg->getStoreHostString();
            $uniqKey = $msg->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
            if ($uniqKey !== null && $msg->getOffsetMsgId() !== null) {
                $outMsgId = (string) $msg->getOffsetMsgId();
            }
        }
        if ($addr === null || $addr === '') {
            throw new MQClientException(sprintf('no broker address for message %s of %s', $msgId, $topic));
        }
        $response = $this->invokeBroker($addr, RequestCode::CONSUME_MESSAGE_DIRECTLY, [
            'consumerGroup' => $consumerGroup,
            'clientId' => $clientId,
            'msgId' => $outMsgId,
            'topic' => $topic,
        ]);
        if ($response->body === null || $response->body === '') {
            throw new MQClientException(sprintf('no consume result for client %s', $clientId));
        }
        return ConsumeMessageDirectlyResult::decode($response->body);
    }

    public function queryConsumeQueue(string $brokerAddr, string $topic, int $queueId, int $index, int $count = 32, string $consumerGroup = ''): QueryConsumeQueueResponseBody
    {
        $response = $this->invokeBroker(
            $brokerAddr,
            RequestCode::QUERY_CONSUME_QUEUE,
            ['topic' => $topic, 'queueId' => $queueId, 'index' => $index, 'count' => $count, 'consumerGroup' => $consumerGroup],
        );
        if ($response->body === null || $response->body === '') {
            return new QueryConsumeQueueResponseBody();
        }
        return QueryConsumeQueueResponseBody::decode($response->body);
    }


    public function createAndUpdateTopicConfigList(string $brokerAddr, array $configs): void
    {
        if ($configs === []) {
            throw new MQClientException('createAndUpdateTopicConfigList: empty topicConfigList');
        }
        // No per-config validation here: the batch form takes the list as-is, unlike the single form.
        $body = RemotingSerializable::encode([
            'topicConfigList' => array_map(static fn(TopicConfig $c): array => $c->toDict(), $configs),
        ]);
        try {
            $this->invokeBroker($brokerAddr, RequestCode::UPDATE_AND_CREATE_TOPIC_LIST, body: $body);
        } catch (MQBrokerException $e) {
            throw new MQClientException($e->getMessage(), $e->responseCode, $e);
        }
    }

    public function createAndUpdateSubscriptionGroupConfigList(string $brokerAddr, array $configs): void
    {
        if ($configs === []) {
            throw new MQClientException('createAndUpdateSubscriptionGroupConfigList: empty groupConfigList');
        }
        $body = RemotingSerializable::encode([
            'groupConfigList' => array_map(static fn(SubscriptionGroupConfig $c): array => $c->toDict(), $configs),
        ]);
        try {
            $this->invokeBroker($brokerAddr, RequestCode::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST, body: $body);
        } catch (MQBrokerException $e) {
            throw new MQClientException($e->getMessage(), $e->responseCode, $e);
        }
    }

    public function createStaticTopic(string $brokerAddr, string $defaultTopic, TopicConfig $config, array $mappingDetail, bool $force = false): void
    {
        $header = [
            'topic' => $config->topicName,
            'defaultTopic' => $defaultTopic,
            'readQueueNums' => $config->readQueueNums,
            'writeQueueNums' => $config->writeQueueNums,
            'perm' => $config->perm,
            'topicFilterType' => $config->topicFilterType !== '' ? $config->topicFilterType : 'SINGLE_TAG',
            'topicSysFlag' => $config->topicSysFlag,
            'order' => $config->order ? 'true' : 'false',
            'force' => $force ? 'true' : 'false',
        ];
        $this->invokeBroker($brokerAddr, RequestCode::UPDATE_AND_CREATE_STATIC_TOPIC, extFields: $header, body: RemotingSerializable::encode($mappingDetail));
    }

    public function updateAndGetGroupReadForbidden(string $brokerAddr, string $group, string $topic, ?bool $readable = null): array
    {
        if ($group === '' || $topic === '') {
            throw new MQClientException('updateAndGetGroupReadForbidden: group/topic required');
        }
        $ext = ['group' => $group, 'topic' => $topic];
        if ($readable !== null) {
            $ext['readable'] = $readable ? 'true' : 'false';
        }
        $response = $this->invokeBroker($brokerAddr, RequestCode::UPDATE_AND_GET_GROUP_FORBIDDEN, extFields: $ext);
        if ($response->body === null || $response->body === '') {
            throw new MQClientException('updateAndGetGroupReadForbidden: empty response body');
        }
        $obj = RemotingSerializable::fastjsonLoads($response->body);
        return is_array($obj) ? $obj : [];
    }

    public function resumeCheckHalfMessage(string $brokerAddr, string $topic, string $msgId = ''): bool
    {
        if ($topic === '') {
            throw new MQClientException('resumeCheckHalfMessage: topic required');
        }
        $ext = ['topic' => $topic];
        if ($msgId !== '') {
            $ext['msgId'] = $msgId;
        }
        $client = $this->requireClient();
        $addr = MixAll::brokerVipChannel($this->vipChannelEnabled, $brokerAddr);
        $request = RemotingCommand::createRequestCommand(RequestCode::RESUME_CHECK_HALF_MESSAGE, null);
        foreach ($ext as $k => $v) {
            $request->extFields[$k] = $v;
        }
        $response = $this->invokeSyncOn($client, $addr, $request, $this->timeoutMillis);
        return $response->code === ResponseCode::SUCCESS;
    }

    public function createOrUpdateOrderConf(string $key, string $value, bool $isCluster = false): void
    {
        if ($key === '' || $value === '') {
            throw new MQClientException('createOrUpdateOrderConf: key/value required');
        }
        if ($isCluster) {
            $this->putKvConfig(self::NAMESPACE_ORDER_TOPIC_CONFIG, $key, $value);
            return;
        }
        try {
            $oldConfs = $this->getKvConfig(self::NAMESPACE_ORDER_TOPIC_CONFIG, $key) ?? '';
        } catch (\Throwable) {
            $oldConfs = '';
        }
        $entries = [];
        foreach (explode(';', $oldConfs) as $entry) {
            $entry = trim($entry);
            if ($entry === '') {
                continue;
            }
            $entryKey = explode(':', $entry, 2)[0];
            $entries[$entryKey] = $entry;
        }
        $newKey = explode(':', $value, 2)[0];
        if ($newKey === '') {
            throw new MQClientException('createOrUpdateOrderConf: value must start with a key');
        }
        $entries[$newKey] = $value;
        $this->putKvConfig(self::NAMESPACE_ORDER_TOPIC_CONFIG, $key, implode(';', array_values($entries)));
    }

    // ---------------- Operational cleanup ----------------

    public function cleanExpiredConsumerQueue(string $brokerAddr, int $timeHours): void
    {
        $this->invokeBroker($brokerAddr, RequestCode::CLEAN_EXPIRED_CONSUMEQUEUE, ['time' => $timeHours]);
    }

    public function cleanExpiredConsumerQueueByAddr(array $addrs, int $timeHours): array
    {
        $failed = [];
        foreach ($addrs as $addr) {
            try {
                $this->cleanExpiredConsumerQueue($addr, $timeHours);
            } catch (\Throwable) {
                $failed[] = $addr;
            }
        }
        return $failed;
    }

    public function deleteExpiredCommitLog(string $brokerAddr, int $timeHours): void
    {
        $this->invokeBroker($brokerAddr, RequestCode::DELETE_EXPIRED_COMMITLOG, ['time' => $timeHours]);
    }

    public function deleteExpiredCommitLogByAddr(array $addrs, int $timeHours): array
    {
        $failed = [];
        foreach ($addrs as $addr) {
            try {
                $this->deleteExpiredCommitLog($addr, $timeHours);
            } catch (\Throwable) {
                $failed[] = $addr;
            }
        }
        return $failed;
    }

    public function cleanUnusedTopicByAddr(string $brokerAddr): void
    {
        $this->invokeBroker($brokerAddr, RequestCode::CLEAN_UNUSED_TOPIC);
    }

    // ---------------- Cold data flow control / commitlog read mode ----------------

    /**
     * UPDATE_COLD_DATA_FLOW_CTR_CONFIG(2001): the properties travel as ``k=v`` text in the
     * body. An empty property set sends nothing at all — the request would only clear what
     * the broker already has.
     *
     * @param array<string, string> $properties
     */
    public function updateColdDataFlowCtrGroupConfig(string $addr, array $properties, ?int $timeoutMillis = null): void
    {
        $text = MixAll::properties2String($properties);
        if ($text === '') {
            return;
        }
        $this->invokeBroker($addr, RequestCode::UPDATE_COLD_DATA_FLOW_CTR_CONFIG, body: $text, timeoutMillis: $timeoutMillis);
    }

    /**
     * REMOVE_COLD_DATA_FLOW_CTR_CONFIG(2002): the body is the bare group name, not JSON.
     * An empty group name sends nothing.
     */
    public function removeColdDataFlowCtrGroupConfig(string $addr, string $consumerGroup, ?int $timeoutMillis = null): void
    {
        if ($consumerGroup === '') {
            return;
        }
        $this->invokeBroker($addr, RequestCode::REMOVE_COLD_DATA_FLOW_CTR_CONFIG, body: $consumerGroup, timeoutMillis: $timeoutMillis);
    }

    /**
     * GET_COLD_DATA_FLOW_CTR_INFO(2003): the raw info text, or null when the broker answers
     * without a body. Served on the plain port only.
     */
    public function getColdDataFlowCtrInfo(string $addr, ?int $timeoutMillis = null): ?string
    {
        $response = $this->invokeBroker($addr, RequestCode::GET_COLD_DATA_FLOW_CTR_INFO, timeoutMillis: $timeoutMillis, vip: false);
        $body = $response->body;
        return ($body !== null && $body !== '') ? $body : null;
    }

    /**
     * SET_COMMITLOG_READ_MODE(2004): the header is built by hand around the single
     * ``READ_AHEAD_MODE`` key; the answer is the response remark (no body), null when the
     * broker sends none. Plain port.
     */
    public function setCommitLogReadAheadMode(string $addr, string $mode, ?int $timeoutMillis = null): ?string
    {
        $response = $this->invokeBroker(
            $addr,
            RequestCode::SET_COMMITLOG_READ_MODE,
            [self::READ_AHEAD_MODE => $mode],
            timeoutMillis: $timeoutMillis,
            vip: false,
        );
        $remark = $response->remark;
        return ($remark !== null && $remark !== '') ? $remark : null;
    }

    // ---------------- Pop record / RocksDB maintenance ----------------

    /**
     * POP_ROLLBACK(200056): ask the broker to dump its pop records. No header, no body,
     * plain port. The timeout is the caller's, verbatim — this operation does not follow
     * the admin-wide timeout.
     */
    public function exportPopRecords(string $brokerAddr, int $timeout): void
    {
        $this->invokeBroker($brokerAddr, RequestCode::POP_ROLLBACK, timeoutMillis: $timeout, vip: false);
    }

    /**
     * CHECK_ROCKSDB_CQ_WRITE_PROGRESS(354): probe the RocksDB consume-queue write progress
     * of one topic. Plain port; the progress check is answered with a JSON body.
     */
    public function checkRocksdbCqWriteProgress(string $brokerAddr, string $topic, int $checkStoreTime, ?int $timeoutMillis = null): ?CheckRocksdbCqWriteResult
    {
        try {
            $response = $this->invokeBroker(
                $brokerAddr,
                RequestCode::CHECK_ROCKSDB_CQ_WRITE_PROGRESS,
                ['topic' => $topic, 'checkStoreTime' => $checkStoreTime],
                timeoutMillis: $timeoutMillis,
                vip: false,
            );
        } catch (MQBrokerException $e) {
            throw new MQClientException($e->getMessage(), $e->responseCode, $e);
        }
        if ($response->body === null || $response->body === '') {
            return null;
        }
        return CheckRocksdbCqWriteResult::decode($response->body);
    }

    /**
     * EXPORT_ROCKSDB_CONFIG_TO_JSON(355): ask the broker to dump config tables as JSON files.
     * Code-only answer (no body to read). The type names travel ';'-separated, each entry
     * followed by the separator, exactly as the header field is written.
     *
     * @param list<string> $configTypes
     */
    public function exportRocksDBConfigToJson(string $brokerAddr, array $configTypes, ?int $timeoutMillis = null): void
    {
        $known = ['topics', 'subscriptiongroups', 'consumeroffsets'];
        $text = '';
        foreach ($configTypes as $type) {
            $name = strtolower(trim((string) $type));
            if (!in_array($name, $known, true)) {
                throw new MQClientException(sprintf('unknown config type: %s', $type));
            }
            $text .= $name . ';';
        }
        try {
            $this->invokeBroker(
                $brokerAddr,
                RequestCode::EXPORT_ROCKSDB_CONFIG_TO_JSON,
                ['configType' => $text],
                timeoutMillis: $timeoutMillis,
                vip: false,
            );
        } catch (MQBrokerException $e) {
            throw new MQClientException($e->getMessage(), $e->responseCode, $e);
        }
    }

    public function queryConsumeTimeSpan(string $topic, string $group): array
    {
        $route = $this->examineTopicRoute($topic);
        $spans = [];
        foreach ($route->brokerDatas as $bd) {
            $addr = $bd->selectBrokerAddr();
            if ($addr === null || $addr === '') {
                continue;
            }
            $response = $this->invokeBroker($addr, RequestCode::QUERY_CONSUME_TIME_SPAN, ['topic' => $topic, 'group' => $group]);
            if ($response->body === null || $response->body === '') {
                continue;
            }
            $body = QueryConsumeTimeSpanBody::decode($response->body);
            foreach ($body->consumeTimeSpanSet as $span) {
                $spans[] = $span;
            }
        }
        return $spans;
    }

    /**
     * Fan-out consume time spans: one QUERY_CONSUME_TIME_SPAN per broker of the route, the
     * per-broker span lists concatenated in route order. A route without brokers is a
     * success carrying an empty list.
     */
    public function queryConsumeTimeSpanConcurrent(string $topic, string $group): AdminToolResult
    {
        return $this->adminToolExecute(function () use ($topic, $group): AdminToolResult {
            $spans = [];
            $route = $this->examineTopicRoute($topic);
            $brokerDatas = $route->getBrokerDatas();
            if ($brokerDatas === []) {
                return AdminToolResult::success($spans);
            }
            $this->fanOutWithDeadline($brokerDatas, function (BrokerData $bd, int $left) use ($topic, $group, &$spans): void {
                $addr = $bd->selectBrokerAddr();
                if ($addr === null || $addr === '') {
                    return;
                }
                try {
                    $response = $this->invokeBroker($addr, RequestCode::QUERY_CONSUME_TIME_SPAN, ['topic' => $topic, 'group' => $group], timeoutMillis: $left);
                    if ($response->body !== null && $response->body !== '') {
                        foreach (QueryConsumeTimeSpanBody::decode($response->body)->consumeTimeSpanSet as $span) {
                            $spans[] = $span;
                        }
                    }
                } catch (\Throwable $e) {
                    Logger::warning(sprintf('queryConsumeTimeSpan error. topic=%s, group=%s, broker=%s: %s', $topic, $group, $addr, $e->getMessage()));
                }
            });
            return AdminToolResult::success($spans);
        });
    }

    // ---------------- NameServer config (318/319) ----------------

    public function updateNameServerConfig(array $properties, ?int $timeoutMillis = null): void
    {
        $text = MixAll::properties2String($properties);
        if ($text === '') {
            return;
        }
        $client = $this->requireClient();
        $request = RemotingCommand::createRequestCommand(RequestCode::UPDATE_NAMESRV_CONFIG, null);
        $request->body = $text;
        $errResponse = null;
        foreach ($client->nameServerAddrs as $nsAddr) {
            $response = $this->invokeSyncOn($client, $nsAddr, $request, $timeoutMillis ?? $this->timeoutMillis);
            if ($response->code !== ResponseCode::SUCCESS) {
                $errResponse = $response;
            }
        }
        if ($errResponse !== null) {
            throw new MQClientException(
                ($errResponse->remark !== null && $errResponse->remark !== '')
                    ? $errResponse->remark : 'update name server config failed',
                $errResponse->code,
            );
        }
    }

    public function getNameServerConfig(?array $namesrvAddrs = null, ?int $timeoutMillis = null): array
    {
        $client = $this->requireClient();
        $targets = $namesrvAddrs !== null && $namesrvAddrs !== []
            ? array_values($namesrvAddrs)
            : array_values($client->nameServerAddrs);
        $result = [];
        $lastExc = null;
        foreach ($targets as $nsAddr) {
            $request = RemotingCommand::createRequestCommand(RequestCode::GET_NAMESRV_CONFIG, null);
            try {
                $response = $this->invokeSyncOn($client, $nsAddr, $request, $timeoutMillis ?? $this->timeoutMillis);
            } catch (\Throwable $e) {
                $lastExc = $e;
                continue;
            }
            if ($response->code !== ResponseCode::SUCCESS) {
                $lastExc = new MQClientException(
                    ($response->remark !== null && $response->remark !== '')
                        ? $response->remark : 'get name server config failed',
                    $response->code,
                );
                continue;
            }
            $result[$nsAddr] = MixAll::string2Properties($response->body ?? '');
        }
        if ($result === [] && $lastExc !== null) {
            throw $lastExc;
        }
        return $result;
    }

    public function setMessageRequestMode(string $brokerAddr, string $topic, string $consumerGroup, string $mode, int $popShareQueueNum = 0): void
    {
        $body = [
            'topic' => $topic,
            'consumerGroup' => $consumerGroup,
            'mode' => $mode === '' ? 'PULL' : $mode,
            'popShareQueueNum' => $popShareQueueNum,
        ];
        $this->invokeBroker($brokerAddr, RequestCode::SET_MESSAGE_REQUEST_MODE, body: json_encode($body));
    }

    // ---------------- User and access control management ----------------

    /**
     * AUTH_CREATE_USER(3001): stores a new user.
     *
     * Two accepted shapes: the user object as it should be stored, or the plain attributes. The
     * attribute form leaves ``userStatus`` unset unless the caller spells it out, and an unset
     * attribute drops its key from the body -- the broker then applies its own default instead of
     * being told to store a null.
     */
    public function createUser(
        string $brokerAddr,
        AuthUserInfo|string $username,
        ?string $password = null,
        ?string $userType = null,
        ?string $userStatus = null,
        ?int $timeoutMillis = null,
    ): void {
        $user = self::userOf('createUser', $username, $password, $userType, $userStatus);
        $this->invokeBroker(
            $brokerAddr,
            RequestCode::AUTH_CREATE_USER,
            self::authExt(['username' => $user->username]),
            body: $user->encode(),
            timeoutMillis: $timeoutMillis,
            vip: false,
        );
    }

    /** AUTH_UPDATE_USER(3002): overwrites the stored attributes of an existing user. */
    public function updateUser(
        string $brokerAddr,
        AuthUserInfo|string $username,
        ?string $password = null,
        ?string $userType = null,
        ?string $userStatus = null,
        ?int $timeoutMillis = null,
    ): void {
        $user = self::userOf('updateUser', $username, $password, $userType, $userStatus);
        $this->invokeBroker(
            $brokerAddr,
            RequestCode::AUTH_UPDATE_USER,
            self::authExt(['username' => $user->username]),
            body: $user->encode(),
            timeoutMillis: $timeoutMillis,
            vip: false,
        );
    }

    /** AUTH_DELETE_USER(3003): removes the user and everything granted to it. */
    public function deleteUser(string $brokerAddr, string $username, ?int $timeoutMillis = null): void
    {
        $this->invokeBroker(
            $brokerAddr,
            RequestCode::AUTH_DELETE_USER,
            self::authExt(['username' => $username]),
            timeoutMillis: $timeoutMillis,
            vip: false,
        );
    }

    /** AUTH_GET_USER(3004): the stored user, or null when the body is empty. */
    public function getUser(string $brokerAddr, string $username, ?int $timeoutMillis = null): ?AuthUserInfo
    {
        $response = $this->invokeBroker(
            $brokerAddr,
            RequestCode::AUTH_GET_USER,
            self::authExt(['username' => $username]),
            timeoutMillis: $timeoutMillis,
            vip: false,
        );
        return AuthUserInfo::decodeOne($response->body);
    }

    /**
     * AUTH_LIST_USER(3005): the users matching ``filter``.
     *
     * A filter the caller leaves out is not sent at all -- an empty string on the wire is a filter
     * of its own, not the absence of one.
     *
     * @return list<AuthUserInfo>
     */
    public function listUser(string $brokerAddr, ?string $filter = null, ?int $timeoutMillis = null): array
    {
        $response = $this->invokeBroker(
            $brokerAddr,
            RequestCode::AUTH_LIST_USER,
            self::authExt(['filter' => $filter]),
            timeoutMillis: $timeoutMillis,
            vip: false,
        );
        return AuthUserInfo::decodeList($response->body);
    }

    /**
     * AUTH_CREATE_ACL(3006): stores the rules of one subject.
     *
     * Accepted shapes: the acl object (subject plus whatever policies it carries), or the plain
     * attributes -- which build exactly one policy with no policy type and one entry per resource,
     * every entry repeating the same actions, source ips and decision.
     */
    public function createAcl(
        string $brokerAddr,
        AuthAclInfo|string $subject,
        array $resources = [],
        array $actions = [],
        array $sourceIps = [],
        ?string $decision = null,
        ?int $timeoutMillis = null,
    ): void {
        $acl = self::aclOf('createAcl', $subject, $resources, $actions, $sourceIps, $decision);
        $this->invokeBroker(
            $brokerAddr,
            RequestCode::AUTH_CREATE_ACL,
            self::authExt(['subject' => $acl->subject]),
            body: $acl->encode(),
            timeoutMillis: $timeoutMillis,
            vip: false,
        );
    }

    /** AUTH_UPDATE_ACL(3007): replaces the stored rules of one subject. */
    public function updateAcl(
        string $brokerAddr,
        AuthAclInfo|string $subject,
        array $resources = [],
        array $actions = [],
        array $sourceIps = [],
        ?string $decision = null,
        ?int $timeoutMillis = null,
    ): void {
        $acl = self::aclOf('updateAcl', $subject, $resources, $actions, $sourceIps, $decision);
        $this->invokeBroker(
            $brokerAddr,
            RequestCode::AUTH_UPDATE_ACL,
            self::authExt(['subject' => $acl->subject]),
            body: $acl->encode(),
            timeoutMillis: $timeoutMillis,
            vip: false,
        );
    }

    /**
     * AUTH_DELETE_ACL(3008): removes one rule of a subject.
     *
     * This request names only the subject and the resource -- the rule to remove is identified by
     * those two, so nothing else may travel with it: a body or an extra key would be read as a
     * different request shape.
     */
    public function deleteAcl(string $brokerAddr, string $subject, ?string $resource = null, ?int $timeoutMillis = null): void
    {
        $this->invokeBroker(
            $brokerAddr,
            RequestCode::AUTH_DELETE_ACL,
            self::authExt(['subject' => $subject, 'resource' => $resource]),
            timeoutMillis: $timeoutMillis,
            vip: false,
        );
    }

    /** AUTH_GET_ACL(3009): the rules stored for one subject, or null when the body is empty. */
    public function getAcl(string $brokerAddr, string $subject, ?int $timeoutMillis = null): ?AuthAclInfo
    {
        $response = $this->invokeBroker(
            $brokerAddr,
            RequestCode::AUTH_GET_ACL,
            self::authExt(['subject' => $subject]),
            timeoutMillis: $timeoutMillis,
            vip: false,
        );
        return AuthAclInfo::decodeOne($response->body);
    }

    /**
     * AUTH_LIST_ACL(3010): the stored rules matching the two filters.
     *
     * @return list<AuthAclInfo>
     */
    public function listAcl(
        string $brokerAddr,
        ?string $subjectFilter = null,
        ?string $resourceFilter = null,
        ?int $timeoutMillis = null,
    ): array {
        $response = $this->invokeBroker(
            $brokerAddr,
            RequestCode::AUTH_LIST_ACL,
            self::authExt(['subjectFilter' => $subjectFilter, 'resourceFilter' => $resourceFilter]),
            timeoutMillis: $timeoutMillis,
            vip: false,
        );
        return AuthAclInfo::decodeList($response->body);
    }

    /**
     * Header fields of the management requests: an unset (or empty) value gets no key, because the
     * broker reads an empty string as a value that was deliberately supplied.
     *
     * @param array<string, string|null> $fields
     * @return array<string, string>
     */
    private static function authExt(array $fields): array
    {
        $out = [];
        foreach ($fields as $name => $value) {
            if ($value === null || $value === '') {
                continue;
            }
            $out[$name] = $value;
        }
        return $out;
    }

    /** @param AuthUserInfo|string $username */
    private static function userOf(string $method, AuthUserInfo|string $username, ?string $password, ?string $userType, ?string $userStatus): AuthUserInfo
    {
        if ($username instanceof AuthUserInfo) {
            if ($password !== null || $userType !== null || $userStatus !== null) {
                throw new MQClientException(sprintf('%s: pass either a user object or the plain attributes, not both', $method));
            }
            return $username;
        }
        return new AuthUserInfo($username, $password, $userType, $userStatus);
    }

    /** @param AuthAclInfo|string $subject @param list<string> $resources @param list<string> $actions @param list<string> $sourceIps */
    private static function aclOf(string $method, AuthAclInfo|string $subject, array $resources, array $actions, array $sourceIps, ?string $decision): AuthAclInfo
    {
        if ($subject instanceof AuthAclInfo) {
            if ($resources !== [] || $actions !== [] || $sourceIps !== [] || $decision !== null) {
                throw new MQClientException(sprintf('%s: pass either an acl object or the plain attributes, not both', $method));
            }
            return $subject;
        }
        return AuthAclInfo::forResources($subject, $resources, $actions, $sourceIps, $decision);
    }

    // ---------------- Lite admin ----------------

    /**
     * GET_BROKER_LITE_INFO(200074): the broker's lite-store footprint. No request header.
     */
    public function getBrokerLiteInfo(string $brokerAddr, ?int $timeoutMillis = null): ?GetBrokerLiteInfoResponseBody
    {
        return $this->invokeBrokerMethod($brokerAddr, RequestCode::GET_BROKER_LITE_INFO, null, GetBrokerLiteInfoResponseBody::class, $timeoutMillis);
    }

    /** GET_PARENT_TOPIC_INFO(200075): the lite layout hanging under one parent topic. */
    public function getParentTopicInfo(string $brokerAddr, string $topic, ?int $timeoutMillis = null): ?GetParentTopicInfoResponseBody
    {
        return $this->invokeBrokerMethod($brokerAddr, RequestCode::GET_PARENT_TOPIC_INFO, ['topic' => $topic], GetParentTopicInfoResponseBody::class, $timeoutMillis);
    }

    /** GET_LITE_TOPIC_INFO(200076): one lite topic's state; the lite name is optional. */
    public function getLiteTopicInfo(string $brokerAddr, string $parentTopic, ?string $liteTopic = null, ?int $timeoutMillis = null): ?GetLiteTopicInfoResponseBody
    {
        return $this->invokeBrokerMethod(
            $brokerAddr,
            RequestCode::GET_LITE_TOPIC_INFO,
            self::nonNullExt(['parentTopic' => $parentTopic, 'liteTopic' => $liteTopic]),
            GetLiteTopicInfoResponseBody::class,
            $timeoutMillis,
        );
    }

    /**
     * GET_LITE_CLIENT_INFO(200077): one client's lite consumption state. ``maxCount`` is a
     * primitive of the request header with a fixed default, so it always travels.
     */
    public function getLiteClientInfo(string $brokerAddr, string $parentTopic, string $group, ?string $clientId = null, ?int $timeoutMillis = null): ?GetLiteClientInfoResponseBody
    {
        return $this->invokeBrokerMethod(
            $brokerAddr,
            RequestCode::GET_LITE_CLIENT_INFO,
            self::nonNullExt([
                'parentTopic' => $parentTopic,
                'group' => $group,
                'clientId' => $clientId,
                'maxCount' => 1000,
            ]),
            GetLiteClientInfoResponseBody::class,
            $timeoutMillis,
        );
    }

    /** GET_LITE_GROUP_INFO(200078): a group's lite lag picture; ``topK`` always travels. */
    public function getLiteGroupInfo(string $brokerAddr, string $group, ?string $liteTopic = null, int $topK = 0, ?int $timeoutMillis = null): ?GetLiteGroupInfoResponseBody
    {
        return $this->invokeBrokerMethod(
            $brokerAddr,
            RequestCode::GET_LITE_GROUP_INFO,
            self::nonNullExt([
                'group' => $group,
                'liteTopic' => $liteTopic,
                'topK' => $topK,
            ]),
            GetLiteGroupInfoResponseBody::class,
            $timeoutMillis,
        );
    }

    /** TRIGGER_LITE_DISPATCH(200079): nudge the broker to dispatch one group's lite topics. */
    public function triggerLiteDispatch(string $brokerAddr, string $group, ?string $clientId = null, ?int $timeoutMillis = null): void
    {
        $this->invokeBrokerMethod(
            $brokerAddr,
            RequestCode::TRIGGER_LITE_DISPATCH,
            self::nonNullExt(['group' => $group, 'clientId' => $clientId]),
            null,
            $timeoutMillis,
        );
    }

    // ---------------------------------------------------------------- Helpers

    /**
     * The one exit point the lite admin queries share: VIP routing, SUCCESS-only, and a
     * body-less SUCCESS is a null result rather than an error. A non-SUCCESS answer is a
     * broker exception carrying the response code and remark.
     *
     * @param array<string, string|int|null>|null $extFields
     * @param class-string<\RocketMQ\Remoting\Protocol\RemotingSerializable>|null $responseClass
     */
    private function invokeBrokerMethod(string $addr, int $requestCode, ?array $extFields, ?string $responseClass, ?int $timeoutMillis = null): ?object
    {
        $client = $this->requireClient();
        $addr = MixAll::brokerVipChannel($this->vipChannelEnabled, $addr);
        $request = RemotingCommand::createRequestCommand($requestCode, null);
        self::applyExtFields($request, $extFields);
        $response = $this->invokeSyncOn($client, $addr, $request, $timeoutMillis ?? $this->timeoutMillis);
        if ($response->code === ResponseCode::SUCCESS) {
            if ($response->body !== null && $response->body !== '' && $responseClass !== null) {
                return $responseClass::decode($response->body);
            }
            return null;
        }
        throw new MQBrokerException($response->code, $response->remark ?? '');
    }

    /**
     * Wraps one batch operation into the AdminToolResult envelope: remoting-level failures
     * map to REMOTING_ERROR, a client exception naming a missing topic route maps to
     * TOPIC_ROUTE_INFO_NOT_EXIST, everything else to MQ_BROKER_ERROR / MQ_CLIENT_ERROR.
     */
    private function adminToolExecute(callable $handler): AdminToolResult
    {
        try {
            return $handler();
        } catch (RemotingException $e) {
            return AdminToolResult::failure(AdminToolsResultCodeEnum::REMOTING_ERROR, $e->getMessage());
        } catch (MQClientException $e) {
            if ($e->responseCode === ResponseCode::TOPIC_NOT_EXIST) {
                return AdminToolResult::failure(AdminToolsResultCodeEnum::TOPIC_ROUTE_INFO_NOT_EXIST, $e->getMessage());
            }
            return AdminToolResult::failure(AdminToolsResultCodeEnum::MQ_CLIENT_ERROR, $e->getMessage());
        } catch (\Throwable $e) {
            return AdminToolResult::failure(AdminToolsResultCodeEnum::MQ_BROKER_ERROR, $e->getMessage());
        }
    }

    /**
     * Sequential counterpart of the per-address task fan-out that is bounded by ONE overall
     * deadline: every entry is visited in order, each call budgeted by whatever is left of
     * the deadline, and entries reached after the deadline are simply absent from the result
     * (neither successes nor failures). The visitor owns its errors exactly like the
     * per-task catch block does — nothing thrown inside propagates to the caller.
     *
     * @param list<mixed> $items
     * @param callable(mixed, int): void $visit receives (item, remainingMillis)
     */
    private function fanOutWithDeadline(array $items, callable $visit): void
    {
        $begin = hrtime(true) / 1e6;
        foreach ($items as $item) {
            $left = $this->timeoutMillis - (int) ((hrtime(true) / 1e6) - $begin);
            if ($left <= 0) {
                return;
            }
            $visit($item, $left);
        }
    }

    /** Header fields of the lite requests: an unset optional key gets no wire entry at all. */
    private static function nonNullExt(array $fields): array
    {
        $out = [];
        foreach ($fields as $name => $value) {
            if ($value === null) {
                continue;
            }
            $out[$name] = $value;
        }
        return $out;
    }

    public static function permIsValid(int|string|null $value): bool
    {
        if ($value === null) {
            return false;
        }
        try {
            return PermName::isValid($value);
        } catch (\ValueError) {
            return false;
        }
    }
}


final class TrackType
{
    public const CONSUMED = 'CONSUMED';
    public const CONSUMED_BUT_FILTERED = 'CONSUMED_BUT_FILTERED';
    public const PULL = 'PULL';
    public const NOT_CONSUME_YET = 'NOT_CONSUME_YET';
    public const NOT_ONLINE = 'NOT_ONLINE';
    public const CONSUME_BROADCASTING = 'CONSUME_BROADCASTING';
    public const UNKNOWN = 'UNKNOWN';
}

final class MessageTrack
{
    public function __construct(
        public ?string $consumerGroup = null,
        public string $trackType = TrackType::UNKNOWN,
        public ?string $exceptionDesc = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = ['consumerGroup' => $this->consumerGroup, 'trackType' => $this->trackType];
        if ($this->exceptionDesc !== null) {
            $d['exceptionDesc'] = $this->exceptionDesc;
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        return new self(
            isset($d['consumerGroup']) ? (string) $d['consumerGroup'] : null,
            (string) ($d['trackType'] ?? '') !== '' ? (string) $d['trackType'] : TrackType::UNKNOWN,
            isset($d['exceptionDesc']) ? (string) $d['exceptionDesc'] : null,
        );
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    public static function decode(string $data): self
    {
        $obj = RemotingSerializable::fastjsonLoads($data);
        return self::fromDict(is_array($obj) ? $obj : []);
    }

    public function __toString(): string
    {
        return sprintf(
            'MessageTrack [consumerGroup=%s, trackType=%s, exceptionDesc=%s]',
            $this->consumerGroup ?? 'None',
            $this->trackType,
            $this->exceptionDesc ?? 'None',
        );
    }
}
