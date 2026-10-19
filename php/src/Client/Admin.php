<?php

declare(strict_types=1);

namespace RocketMQ\Client;

use RocketMQ\Client\Exceptions\MQBrokerException;
use RocketMQ\Client\Exceptions\MQClientException;
use RocketMQ\Common\BoundaryType;
use RocketMQ\Common\MessageDecoder;
use RocketMQ\Common\MessageExt;
use RocketMQ\Common\MessageConst;
use RocketMQ\Common\MessageQueue;
use RocketMQ\Common\MixAll;
use RocketMQ\Common\PermName;
use RocketMQ\Common\TopicConfig;
use RocketMQ\Remoting\Protocol\ClusterInfo;
use RocketMQ\Remoting\Protocol\ConsumeMessageDirectlyResult;
use RocketMQ\Remoting\Protocol\ConsumeStats;
use RocketMQ\Remoting\Protocol\ConsumeStatsList;
use RocketMQ\Remoting\Protocol\ConsumerConnection;
use RocketMQ\Remoting\Protocol\ConsumerRunningInfo;
use RocketMQ\Remoting\Protocol\GetConsumerListByGroupResponseBody;
use RocketMQ\Remoting\Protocol\KVTable;
use RocketMQ\Remoting\Protocol\LanguageCode;
use RocketMQ\Remoting\Protocol\ProducerConnection;
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
    /** Default timeout, 5000 * 3. */
    public const DEFAULT_TIMEOUT = 5000 * 3;

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
            throw new MQClientException(sprintf('topic %s not exist', $topic));
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

    public function getAllTopicConfig(string $brokerAddr, ?int $timeoutMillis = null): TopicConfigSerializeWrapper
    {
        $response = $this->invokeBroker($brokerAddr, RequestCode::GET_ALL_TOPIC_CONFIG, timeoutMillis: $timeoutMillis);
        if ($response->body === null || $response->body === '') {
            return new TopicConfigSerializeWrapper();
        }
        return TopicConfigSerializeWrapper::decode($response->body);
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

    public function examineTopicStatsByBroker(string $brokerAddr, string $topic): TopicStatsTable
    {
        $response = $this->invokeBroker($brokerAddr, RequestCode::GET_TOPIC_STATS_INFO, ['topic' => $topic]);
        if ($response->body === null || $response->body === '') {
            return new TopicStatsTable();
        }
        return TopicStatsTable::decode($response->body);
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
                'maxGroupNum' => '10000',
            ];
            if ($currentDataVersion !== null) {
                $ext['dataVersion'] = RemotingSerializable::toJson($currentDataVersion);
            }
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

    public function examineConsumeStats(string $brokerAddr, string $consumerGroup, ?string $topic = null, ?array $topicList = null): ConsumeStats
    {
        $ext = ['consumerGroup' => $consumerGroup];
        if ($topic !== null) {
            $ext['topic'] = $topic;
        }
        if ($topicList !== null) {
            $ext['topicList'] = implode(';', $topicList);
        }
        $response = $this->invokeBroker($brokerAddr, RequestCode::GET_CONSUME_STATS, $ext);
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
            $mt = new MessageTrack(consumerGroup: $group);
            try {
                $cc = $this->examineConsumerConnectionInfo($group);
            } catch (MQBrokerException $e) {
                if ($e->responseCode === ResponseCode::CONSUMER_NOT_ONLINE) {
                    $mt->trackType = TrackType::NOT_ONLINE;
                }
                $mt->exceptionDesc = sprintf('CODE:%s DESC:%s', $e->responseCode, $e->getMessage());
                $result[] = $mt;
                continue;
            } catch (\Throwable $e) {
                $mt->exceptionDesc = $e->getMessage();
                $result[] = $mt;
                continue;
            }

            if ($cc->consumeType === 'CONSUME_ACTIVELY') {
                $mt->trackType = TrackType::PULL;
            } elseif ($cc->consumeType === 'CONSUME_PASSIVELY') {
                try {
                    $ifConsumed = $this->consumed($msg, $group);
                } catch (MQBrokerException|MQClientException $e) {
                    if ($e->responseCode === ResponseCode::CONSUMER_NOT_ONLINE) {
                        $mt->trackType = TrackType::NOT_ONLINE;
                        $mt->exceptionDesc = sprintf('CODE:%s DESC:%s', $e->responseCode, $e->getMessage());
                    } elseif ($e->responseCode === ResponseCode::BROADCAST_CONSUMPTION) {
                        $mt->trackType = TrackType::CONSUME_BROADCASTING;
                    }
                    $result[] = $mt;
                    continue;
                } catch (\Throwable $e) {
                    $mt->exceptionDesc = $e->getMessage();
                    $result[] = $mt;
                    continue;
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
            $result[] = $mt;
        }
        return $result;
    }

    /**
     * The raw single-broker call of QUERY_TOPICS_BY_CONSUMER(343).
     *
     * The broker side reverse-looks-up which topics this group has consumed from the offset
     * table (``topic@group`` keys). So an empty table is returned when the group has never
     * committed offsets; this is expected, not a bug.
     */
    public function queryTopicsByConsumerToBroker(string $brokerAddr, string $group): TopicList
    {
        $response = $this->invokeBroker($brokerAddr, RequestCode::QUERY_TOPICS_BY_CONSUMER, ['group' => $group]);
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
        $response = $this->invokeSyncOn($client, $vipAddr, $request, $this->timeoutMillis);
        if ($response->code !== ResponseCode::SUCCESS) {
            throw new MQClientException(
                ($response->remark !== null && $response->remark !== '') ? $response->remark : 'reset offset failed',
                $response->code,
            );
        }
        if ($response->body === null || $response->body === '') {
            return [];
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
        $body = RemotingSerializable::encode([
            'topicConfigList' => array_map(static fn(TopicConfig $c): array => $c->toDict(), $configs),
        ]);
        $this->invokeBroker($brokerAddr, RequestCode::UPDATE_AND_CREATE_TOPIC_LIST, body: $body);
    }

    public function createAndUpdateSubscriptionGroupConfigList(string $brokerAddr, array $configs): void
    {
        if ($configs === []) {
            throw new MQClientException('createAndUpdateSubscriptionGroupConfigList: empty groupConfigList');
        }
        $body = RemotingSerializable::encode([
            'groupConfigList' => array_map(static fn(SubscriptionGroupConfig $c): array => $c->toDict(), $configs),
        ]);
        $this->invokeBroker($brokerAddr, RequestCode::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST, body: $body);
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

    // ---------------------------------------------------------------- Helpers

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
