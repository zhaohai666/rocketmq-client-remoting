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
// 命名空间级函数（定义于 AdminBodies.php；该文件随其中任一类被 autoload 时一并生效）
use function RocketMQ\Remoting\Protocol\message_queue_key;

/**
 * 管理端（对应 org.apache.rocketmq.client.admin.DefaultMQAdminExt / MQAdminExt
 * 与 org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl，
 * 移植自 python/client/admin.py）。
 *
 * 覆盖：Topic 增删查/配置、Broker 集群信息与运行时信息/配置、NameServer KV 配置、
 * 订阅组管理、消费者/生产者连接、消费统计、offset 管理（含真实 broker 位点重置）、
 * 消息查询（key / uniqKey / msgId / ConsumeQueue）。
 *
 * 对齐要点（均为 Java 5.x 探针 + 源码核对结论，勿凭记忆改）：
 * - GET_BROKER_CONFIG 的 body 是 **properties 文本**（``k=v\n``），不是 JSON。
 * - UPDATE_AND_CREATE_SUBSCRIPTIONGROUP 的 body 是 SubscriptionGroupConfig **JSON**。
 * - GET_TOPIC_CONFIG 请求头带 ``topic`` + ``lo``，响应体是 TopicConfigAndQueueMapping JSON。
 * - GET_ALL_SUBSCRIPTIONGROUP_CONFIG 是**分页**接口（groupSeq / maxGroupNum / dataVersion）。
 * - ResetOffsetBody.offsetTable 是 Map<MessageQueue, Long>。
 * - KV 配置类请求打到 **NameServer**，且 PUT/DELETE 要广播到**每一个** NameServer。
 *
 * 单线程适配：Python 的同步 RPC（``client._invoke_sync``）在 PHP 侧等价于
 * ``$client->remotingClient->invokeSync(...)``（阻塞式、超时由参数控制）；
 * ``client._check_response`` 的 SUCCESS 校验收敛为本类的 ``checkResponse``。
 * 测试/依赖注入可 ``setMqClientInstance`` 预置实例并覆写 ``invokeSyncOn``。
 */
class DefaultMQAdminExt
{
    /** 默认超时（对应 DefaultMQAdminExt.DEFAULT_TIMEOUT = 5000 * 3）。 */
    public const DEFAULT_TIMEOUT = 5000 * 3;

    /** org.apache.rocketmq.common.namesrv.NamesrvUtil#NAMESPACE_ORDER_TOPIC_CONFIG */
    public const NAMESPACE_ORDER_TOPIC_CONFIG = 'ORDER_TOPIC_CONFIG';

    public string $namespace;

    public string $instanceName = 'ADMIN';

    public ?string $clientId = null;

    /** 对应 Python ``admin.client_id``：start() 之后才非 null。 */
    public function getClientId(): ?string
    {
        return $this->clientId;
    }

    /**
     * Java 的 ``DefaultMQAdminExt extends ClientConfig``，因此 unitName 与
     * enableStreamRequestType 这两个只影响 clientId / 取址 URL / 请求扩展字段的
     * 开关对 admin 同样有效。（unitMode 在这里没有落点：admin 不发消息，
     * 也不注册消费者，Java 里它只是个继承来的字段。）
     */
    public ?string $unitName = null;

    public bool $enableStreamRequestType = false;

    /**
     * 5.x 新命名空间（对应 Java ClientConfig.namespaceV2）：非空时**每笔**请求带
     * `nsd=true` / `ns=<该值>` 两个扩展头，由 broker 解析到对应 serverless 实例。
     * 与 `$namespace`（客户端给资源名拼 `namespace%` 前缀）是两套机制，这里**不**改
     * 任何资源名。见 NamespaceRpcHook。
     */
    public string $namespaceV2 = '';

    /**
     * Java ``ClientConfig#vipChannelEnabled``（5.x 默认 false）：true 时 broker 请求
     * 改走 VIP 端口（端口 - 2）。只对本 admin 的 broker 调用生效——admin 不发消息、
     * 不注册消费者。
     */
    public bool $vipChannelEnabled = false;

    /** @var list<string> */
    public array $nameServerAddrs = [];

    /** TLS（Java tls.enable 等价物；null = 交给 env ROCKETMQ_TLS_ENABLE）。 */
    public ?bool $tlsEnable = null;

    /** TLS 细项（caCert/clientCert/clientKey/serverName），语义见 RemotingClient::$tlsOptions。 */
    public ?array $tlsOptions = null;

    /**
     * 路由刷新周期（对应 Java ClientConfig.pollNameServerInterval 默认 30000ms）；
     * 只在 start() 建 MQClientInstance 时透传一次。
     */
    public int $pollNameServerInterval = 30000;

    public ?RPCHook $rpcHook;

    /** 删除 topic 时一并清理的 KV namespace（Java 的 kvNamespaceToDeleteList）。 */
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

    // ---------------- 配置与生命周期 ----------------

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
        // Python：setter 在 start() 前调用，start() 用该列表构建 MQClientInstance。
        // PHP 允许注入现成实例（setMqClientInstance），此时同步给 client，
        // 避免 admin 与 client 两份地址列表漂移（invokeNamesrvOne/All 读 client 侧）。
        $this->mqClient?->updateNameServerAddressList($this->nameServerAddrs);
    }

    public function setInstanceName(string $name): void
    {
        $this->instanceName = $name;
    }

    /** 对应 Java ``ClientConfig#setUnitName``：影响 clientId 后缀与动态取址 URL。 */
    public function setUnitName(?string $unitName): void
    {
        $this->unitName = $unitName;
    }

    public function getUnitName(): ?string
    {
        return $this->unitName;
    }

    /** 对应 Java ``ClientConfig#setEnableStreamRequestType``。 */
    public function setEnableStreamRequestType(bool $enable): void
    {
        $this->enableStreamRequestType = $enable;
    }

    /**
     * 对应 Java `ClientConfig#setNamespaceV2`：服务端命名空间（`nsd`/`ns` 扩展头），
     * 不改资源名；与 `$namespace` 那套「客户端拼前缀」的机制互不相干。
     * start() 之后改也生效——钩子每笔请求实时读 {@see MQClientInstance::$namespaceV2}。
     */
    public function setNamespaceV2(string $namespaceV2): void
    {
        $this->namespaceV2 = $namespaceV2;
        if ($this->mqClient !== null) {
            $this->mqClient->namespaceV2 = $namespaceV2;
        }
    }

    /** 对应 Java `ClientConfig#getNamespaceV2`。 */
    public function getNamespaceV2(): string
    {
        return $this->namespaceV2;
    }

    /** 对应 Java ``ClientConfig#setVipChannelEnabled``。 */
    public function setVipChannelEnabled(bool $enable): void
    {
        $this->vipChannelEnabled = $enable;
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
        // Java ``DefaultMQAdminExtImpl#start``:161 无条件 ``changeInstanceNameToPID``，
        // clientId 再走 ``ClientConfig#buildMQClientId`` 的
        // ``<本机 IP>@<instanceName>[@<unitName>][@STREAM]``。
        // 本客户端的 admin 默认 instanceName 是 "ADMIN"（不是 Java 的 "DEFAULT"，因为
        // admin 用私有实例、不和其他客户端共用），所以这一步只在调用方显式设成
        // "DEFAULT" 时才起作用。
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
     * 测试 / 依赖注入缝：直接预置一个已就绪的 MQClientInstance（等价于 start()
     * 之后的内部状态）。生产路径请走 start()。
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

    // ---------------- 底层调用助手 ----------------

    /**
     * 对应 Python ``client._invoke_sync``：MQClientInstance 里该方法是私有的，
     * 这里走公共的 remotingClient；测试子类覆写本方法即可拦截全部同步调用。
     */
    protected function invokeSyncOn(
        MQClientInstance $client,
        string $addr,
        RemotingCommand $request,
        ?int $timeoutMillis,
    ): RemotingCommand {
        return $client->remotingClient->invokeSync($addr, $request, $timeoutMillis);
    }

    /** 对应 Python ``client._check_response``：非 SUCCESS 抛 MQBrokerException。 */
    protected function checkResponse(RemotingCommand $response): RemotingCommand
    {
        if ($response->code === ResponseCode::SUCCESS) {
            return $response;
        }
        throw new MQBrokerException($response->code, $response->remark ?? '');
    }

    /** 发到指定 Broker 并校验 SUCCESS。 */
    protected function invokeBroker(
        string $addr,
        int $code,
        ?array $extFields = null,
        ?string $body = null,
        ?int $timeoutMillis = null,
    ): RemotingCommand {
        $client = $this->requireClient();
        $addr = MixAll::brokerVipChannel($this->vipChannelEnabled, $addr);
        $request = RemotingCommand::createRequestCommand($code, null);
        self::applyExtFields($request, $extFields);
        if ($body !== null) {
            $request->body = $body;
        }
        $response = $this->invokeSyncOn($client, $addr, $request, $timeoutMillis ?? $this->timeoutMillis);
        return $this->checkResponse($response);
    }

    /**
     * 广播到所有 NameServer（Java putKVConfigValue / deleteKVConfigValue 语义）。
     * 任一失败即在收齐后抛错（Java 记 errResponse 最后统一抛）。
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

    /** 发到第一个可用 NameServer（Java invokeSync(null, ...) 语义）。 */
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

    /** 发到**显式给定**的 NameServer 并校验 SUCCESS（不走 VIP 通道）。 */
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
            // 对齐 Python：吞掉后统一抛 no broker address available
        }
        throw new MQClientException('no broker address available');
    }

    private function findFirstBrokerAddr(MQClientInstance $client): string
    {
        return self::firstBrokerAddr($client);
    }

    // ---------------- Topic 管理 ----------------

    public function createTopic(string $key, string $newTopic, int $queueNum = 4, int $topicSysFlag = 0): void
    {
        $client = $this->requireClient();
        $client->createTopicInRoute($newTopic, $queueNum, $queueNum, MixAll::READ_PERM_BY_DEFAULT);
    }

    /** 对应 Java DefaultMQAdminExtImpl.createAndUpdateTopicConfig（走 createTopicKey）。 */
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
        // NameServer 请求不能走 invokeBroker：VIP 开关打开时它会把 NameServer
        // 的端口也 -2（Java 的 deleteTopicInNameServer 同样直连 NameServer）。
        foreach ($targets as $nsAddr) {
            $this->invokeNamesrvAddr($nsAddr, RequestCode::DELETE_TOPIC_IN_NAMESRV, ['topic' => $topic]);
        }
    }

    /** 兼容旧名：删除 NameServer 上的 topic 路由。 */
    public function deleteTopicInNamesrv(string $topic): void
    {
        $client = $this->requireClient();
        $client->deleteTopicInNamesrv($topic);
    }

    /** 删除 topic（先清各 broker，再清 NameServer 路由，最后清 KV namespace）。 */
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
     * 对应 Java fetchTopicsByCLuster（GET_TOPICS_BY_CLUSTER 打到 NameServer）。
     *
     * 字段名必须是 Java GetTopicsByClusterRequestHeader 的 ``cluster``：早先写成
     * ``clusterName`` 时 NameServer 查不到集群（NPE 被吞），只回 SUCCESS + 空列表。
     *
     * Python 返回 set；PHP 用去重后的 list<string> 表达。
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

    /** 对应 Java getClusterList：包含该 topic 的路由 broker 所在集群名集合。 @return list<string> */
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
     * 拉取全部 topic 的路由（遍历 Nameserver 的 topic 列表）。
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

    /** 对应 Java examineTopicConfig（GET_TOPIC_CONFIG，body 为 TopicConfig JSON）。 */
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

    /** 对应 Java getUserTopicConfig：剔除系统 topic 与 %RETRY%/%DLQ%。 */
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

    /** 对应 Java examineTopicStats：遍历该 topic 的所有 broker 合并统计。 */
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
     * dict.update 语义合并以 MessageQueue 为键的表（同键后值覆盖前值）。
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

    // ---------------- 集群 / Broker ----------------

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

    // Java 旧接口名：getBrokerRuntimeInfo
    public function getBrokerRuntimeInfo(string $brokerAddr, ?int $timeoutMillis = null): KVTable
    {
        return $this->fetchBrokerRuntimeStats($brokerAddr, $timeoutMillis);
    }

    /**
     * 对应 Java getBrokerConfig：响应体是 **properties 文本**，不是 JSON/KVTable。
     *
     * 历史实现的 bug：把 body 当 KVTable JSON 解析，真实 broker 上必然失败。
     *
     * @return array<string, string>
     */
    public function getBrokerConfig(string $brokerAddr, ?int $timeoutMillis = null): array
    {
        $response = $this->invokeBroker($brokerAddr, RequestCode::GET_BROKER_CONFIG, timeoutMillis: $timeoutMillis);
        $text = $response->body ?? '';
        return MixAll::string2Properties($text);
    }

    /**
     * 对应 Java updateBrokerConfig（含 Validators.checkBrokerConfig 的 brokerPermission 校验）。
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

    /** 对应 Java cleanUnusedTopic：逐 broker 下发 CLEAN_UNUSED_TOPIC。 */
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

    // ---------------- NameServer KV 配置 ----------------

    /** 对应 Java createAndUpdateKvConfig → putKVConfigValue（广播所有 NameServer）。 */
    public function createAndUpdateKvConfig(string $namespace, string $key, string $value): void
    {
        $this->invokeNamesrvAll(RequestCode::PUT_KV_CONFIG, ['namespace' => $namespace, 'key' => $key, 'value' => $value]);
    }

    // Java 接口名（DefaultMQAdminExtImpl.putKVConfig 为空实现，真正的实现在 createAndUpdateKvConfig）
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

    // ---------------- 订阅组管理 ----------------

    /** 对应 Java createAndUpdateSubscriptionGroupConfig：body 为 SubscriptionGroupConfig JSON。 */
    public function createAndUpdateSubscriptionGroupConfig(string $addr, SubscriptionGroupConfig $config): void
    {
        $this->invokeBroker($addr, RequestCode::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, body: $config->encode());
    }

    /** 对应 Java examineSubscriptionGroupConfig：拉全部订阅组后取目标 group。 */
    public function examineSubscriptionGroupConfig(string $addr, string $group): ?SubscriptionGroupConfig
    {
        $wrapper = $this->getAllSubscriptionGroup($addr);
        return $wrapper->subscriptionGroupTable[$group] ?? null;
    }

    /** 对应 Java getSubscriptionGroupConfig（GET_SUBSCRIPTIONGROUP_CONFIG 单查）。 */
    public function getSubscriptionGroupConfig(string $addr, string $group): ?SubscriptionGroupConfig
    {
        $response = $this->invokeBroker($addr, RequestCode::GET_SUBSCRIPTIONGROUP_CONFIG, ['group' => $group]);
        if ($response->body === null || $response->body === '') {
            return null;
        }
        return SubscriptionGroupConfig::decode($response->body);
    }

    /**
     * 对应 Java getAllSubscriptionGroup：**分页**累积，直到 groupSeq >= totalGroupNum-1。
     *
     * 老版本 broker 不带 totalGroupNum，此时一次性返回全部（单轮即结束）。
     */
    public function getAllSubscriptionGroup(string $brokerAddr, ?int $timeoutMillis = null): SubscriptionGroupWrapper
    {
        $client = $this->requireClient();
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
                // 老 broker：一次返回全部
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

    // ---------------- 消费者 / 生产者连接 ----------------

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

    // ---------------- 消费统计 ----------------

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
     * Python 返回 set；PHP 用去重后的 list<string> 表达。
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

    // ---------------- 消息轨迹（Java DefaultMQAdminExtImpl.messageTrackDetail） ----------------

    /**
     * 对应 Java ``examineConsumeStats(group[, topic])``（:389-424）：按
     * ``%RETRY%<group>`` 的路由扇出全部 broker，逐台取统计并合并（offsetTable
     * 并入、consumeTps 累加）；全空时抛错（Java 的 MQClientException 同口径）。
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
     * 对应 Java ``DefaultMQAdminExtImpl.consumed:1533-1557``：该组在本队列的
     * consumerOffset 是否已越过这条消息的 queueOffset（位点越过 ⇒ 已消费）。
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
                    // Java 先把 master 地址规范化成 ip:port 再比对（convert2IpString）；
                    // 四端存的 broker 地址本来就是注册时的 ip:port 形态，直接比。
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
     * 对应 Java ``DefaultMQAdminExtImpl.messageTrackDetail:1349-1427``：查谁在消费
     * 这个 topic，逐组判 CONSUMED / FILTERED / PULL / NOT_ONLINE / BROADCASTING…。
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
        // Java 按 broker 返回顺序遍历；这里排序让输出确定（Python 侧 sorted(set)）
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
                    // Java 遍历订阅表找本 topic：tagsSet 非空、既不含消息 tag 也不含
                    // "*" ⇒ 订阅比消息窄，消息是被过滤掉的那部分（SQL92 订阅 tagsSet
                    // 为空，同样落回 CONSUMED —— 忠实保留 Java 语义）。
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
     * 对应 Java ``MQClientAPIImpl#queryTopicsByConsumer:2525``（343）的单 broker 原始调用。
     *
     * broker 端走 ``AdminBrokerProcessor#queryTopicsByConsumer:2421`` →
     * ``ConsumerOffsetManager#whichTopicByConsumer``：**从位点表**（``topic@group`` 键）反查该组
     * 消费过哪些 topic。所以组从没提交过位点时回空表，这是预期而不是 bug。
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
     * 对应 Java ``DefaultMQAdminExt#queryTopicsByConsumer``（``DefaultMQAdminExtImpl:1078``）。
     *
     * Java 先按 ``%RETRY%<group>`` 查路由，再对路由里每个 broker 下发 343 并合并。
     * 合并口径对齐 Java 的 ``TopicList.topicList``（那是个 ``Set<String>``），这里去重后回列表。
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

    // ---------------- Offset 管理 ----------------

    public function maxOffset(MessageQueue $mq): int
    {
        return $this->requireClient()->getMaxOffset($mq);
    }

    public function minOffset(MessageQueue $mq): int
    {
        return $this->requireClient()->getMinOffset($mq);
    }

    /** 对应 Java MQAdminImpl#searchOffset(mq, ts)：显式下发 LOWER 边界。 */
    public function searchOffset(MessageQueue $mq, int $timestamp): int
    {
        return $this->requireClient()->searchOffsetByTimestamp($mq, $timestamp, boundaryType: BoundaryType::LOWER);
    }

    /** 对应 Java DefaultMQAdminExt#searchLowerBoundaryOffset(:133)。 */
    public function searchLowerBoundaryOffset(MessageQueue $mq, int $timestamp): int
    {
        return $this->requireClient()->searchOffsetByTimestamp($mq, $timestamp, boundaryType: BoundaryType::LOWER);
    }

    /**
     * 对应 Java DefaultMQAdminExt#searchUpperBoundaryOffset(:137)。
     *
     * 与 LOWER 的差异只在多条消息共享 storeTime、或时间戳落在空档/队尾时可见：
     * 队尾之后 UPPER 回最后一条自己的位点，LOWER 回它的下一个位点（maxOffset）。
     */
    public function searchUpperBoundaryOffset(MessageQueue $mq, int $timestamp): int
    {
        return $this->requireClient()->searchOffsetByTimestamp($mq, $timestamp, boundaryType: BoundaryType::UPPER);
    }

    public function earliestMsgStoreTime(MessageQueue $mq): int
    {
        $client = $this->requireClient();
        // Java MQAdminImpl:250 的 earliestMsgStoreTime 与 max/min/search 同一个形状：
        // 只认 master，刷一次路由重查，仍拿不到照 :264 抛「The broker[X] not exist」。
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
     * 对应 Java resetOffsetByTimestamp：
     *
     * 逐 broker 下发 INVOKE_BROKER_TO_RESET_OFFSET（broker 端按 timestamp 计算新位点，
     * 并同步在线消费者 + 更新 offset 表），汇总 ``Map<MessageQueue, Long>``。
     *
     * ``is_cpp`` 只影响 broker 推给**在线消费者**的 220 报文形状：broker 按发起方
     * （也就是本请求）的 language 判断，CPP 回 ``ResetOffsetBodyForC``（JSON 数组），
     * 其余回 ``ResetOffsetBody``（对象即键的 map）。Java 的管理端两个重载传的都是
     * ``false``（``MQClientAPIImpl:2408``），本端口同样默认 ``false``。
     *
     * 注意：这里**不再**走「逐队列 searchOffset + updateConsumerOffset」的旧本地实现——
     * 那不会同步在线消费者，也不会做 broker 端一致性校验。
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
     * 一笔 ``INVOKE_BROKER_TO_RESET_OFFSET``(222)，回 broker 实际重置的队列表。
     *
     * Java 在这里有**两个**重载：``invokeBrokerToResetOffset(..., isForce, ...)`` 按时间戳
     * 重置整个 topic，另一个带 ``queueId`` + ``offset`` 的只重置单个队列。``offset`` 为 null
     * 时按 Java 口径写 ``-1``（broker 判成 null，转去按 timestamp 算位点）。
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
            // 键名是 **isForce** 不是 force：Java ``RemotingCommand.makeCustomHeaderToNet:437-450``
            // 拿 requestHeader 的**字段名**做 ext key，而 ``ResetOffsetRequestHeader`` 声明的字段是
            // ``private boolean isForce``（getter ``isForce()`` 不参与命名）。写成 force 时 broker 侧
            // isForce 恒为 false ⇒ ``Broker2Client.resetOffset:152-158`` 的分支退化成「取时间戳位点」，
            // 前重（timestamp=-1）会把 consumerOffset 原样回显而不是跳到 maxOffset。
            // 5.5.1 真机探针：{"force":"true", timestamp:-1} → 目标 3（=consumerOffset），
            //               {"isForce":"true", timestamp:-1} → 目标 10（=maxOffset）。
            'isForce' => $isForce ? 'true' : 'false',
            // Java：offset=-1 表示 offset 为空
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
        $response = $this->invokeSyncOn($client, $brokerAddr, $request, $this->timeoutMillis);
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
     * 对应 Java ``DefaultMQAdminExt#resetOffsetByQueueId``（``DefaultMQAdminExtImpl:1827``）。
     *
     * Java 打**两笔** RPC，缺一不可：
     * 1. ``updateConsumerOffset``(25) 直接把 offsetTable 改成目标位点；
     * 2. 带 ``queueId`` + ``offset`` 的 222 走 ``AdminBrokerProcessor#resetOffsetInner``，
     *    先按 ``[min, max+1]`` 校验目标位点（越界回 SYSTEM_ERROR
     *    ``Target offset N not in consume queue range [min-max]``），再
     *    ``ConsumerOffsetManager#assignResetOffset``——它同时写 ``resetOffsetTable``（一次性，
     *    下次 pull 用 ``queryThenEraseResetOffset`` 取走）和 ``offsetTable``，并清掉该队列的
     *    POP 在途计数。只做第 1 步的话在线消费者仍按自己内存里的位点继续拉。
     *
     * Java 返回值是 void（只打日志），这里把 broker 报回的队列表返回，便于调用方核对。
     *
     * ⚠ 实测（5.5.1 真机）这两笔 RPC **不是原子的**：``ConsumerOffsetManager#commitOffset``
     * 只做覆盖写（连 offset 变小都只打 ``[NOTIFYME]`` warn，不做区间校验），所以第 2 笔
     * 被 ``resetOffsetInner`` 以 ``Target offset N not in consume queue range [min-max]`` 拒绝时，
     * 第 1 笔已经把非法位点落库。Java 同样如此，这里不做保护性回滚。
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
        // Java 的单个队列重载不传 force（默认 false）、timestamp 传 0（offset 已给定，不参与算）
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

    /** 对应 Java resetOffsetNew：先试新版（broker 端重置），失败再退化到旧版。 */
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
     * 对应 Java resetOffsetByTimestampOld：逐队列 searchOffset 后按 force 决策写回。
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
     * dict.update 语义合并 ResetOffset 形态的 offset 表（同键后值覆盖前值）。
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

    /** Java MQAdminImpl 的 offset 查询口径：只打主，主没了就报错。 */
    private function publishAddrInAdmin(MQClientInstance $client, MessageQueue $mq): string
    {
        // MQClientInstance 里对应实现是私有的（publishAddrInAdmin），这里按同口径组装：
        // 查发布地址（只认 master）→ 刷一次路由 → 重查 → 仍拿不到抛「The broker[X] not exist」。
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

    // ---------------- 消息查询 ----------------

    /** 对应 Java MQAdminImpl.queryMessage(正常 key)：查所有 broker + 客户端侧 key 二次校验。 */
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
     * 对应 Java queryMessageByUniqKey：indexType="U" + extFields["_UNIQUE_KEY_QUERY"]="true"。
     *
     * 注意：broker 侧 uniqKey 索引只有 RocksDB 索引实现支持（IndexRocksDBStore）；
     * 默认的文件索引下该查询可能返回空，这属于 broker 配置差异而非客户端问题。
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

    /** 按普通 KEYS 索引查询（对应工具 queryMsgByKey 的 NORMAL 模式）。
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
     * 对应 Java DefaultMQAdminExtImpl.viewMessage（:578-587）。
     *
     * Java 先按 offsetMsgId 解出 broker 地址 + commitLog 偏移走 VIEW_MESSAGE_BY_ID，
     * **任何**失败都退回按 UNIQ_KEY 查索引。必须留这条兜底：5.x 客户端的 msgId 是
     * 客户端生成的 uniqKey，同样是 32 位十六进制，硬解会拼出一个不存在的 ip:port
     * （Java 取 4 字节端口所以永远落在 uint16 内，Python 不校验就会把 OverflowError
     * 抛到调用方手里，异常契约直接破掉）。
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
            // Java 同样只 warn 后走兜底
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

    // ---------------- 批量配置（Java 有实现、此前 Python 侧缺失） ----------------

    /**
     * 对应 Java createAndUpdateTopicConfigList → UPDATE_AND_CREATE_TOPIC_LIST(18)。
     *
     * wire 事实：custom header 为**空**（topic 名在 body 里做授权资源），
     * body 是 CreateTopicListRequestBody JSON，即 {"topicConfigList": [...]}。
     *
     * @param list<TopicConfig> $configs
     */
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

    /**
     * 对应 Java createAndUpdateSubscriptionGroupConfigList → 225。
     *
     * 与 topic-list 同形，但 body 的 key 是 ``groupConfigList``；
     * 单组版（200）的 body 是裸 SubscriptionGroupConfig 对象，两处字段名不同。
     *
     * @param list<SubscriptionGroupConfig> $configs
     */
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

    /**
     * 对应 Java createStaticTopic → UPDATE_AND_CREATE_STATIC_TOPIC(513)。
     *
     * header 复用 CreateTopicRequestHeader 字段（topic/defaultTopic/队列数/perm/
     * topicFilterType/topicSysFlag/order/force），body 是 TopicQueueMappingDetail
     * 的 JSON 编码（单元化静态 topic 的映射文档）。
     *
     * @param array<string, mixed> $mappingDetail
     */
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

    /**
     * 对应 Java updateAndGetGroupReadForbidden → UPDATE_AND_GET_GROUP_FORBIDDEN(353)。
     *
     * readable=null 表示仅查询（Java 不设该字段）；响应体是 GroupForbidden JSON。
     *
     * @return array<string, mixed>
     */
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

    /**
     * 对应 Java resumeCheckHalfMessage → RESUME_CHECK_HALF_MESSAGE(323)。
     *
     * Java（MQClientAPIImpl:3279）对非 SUCCESS **返回 False 而不抛错**——
     * 网络层异常才上抛；broker 拒绝（如 msgId 不是半消息，SYSTEM_ERROR）
     * 通过返回值表达。
     */
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

    /**
     * 对应 Java createOrUpdateOrderConf：**不是独立请求**，是 NameServer KV
     * namespace=ORDER_TOPIC_CONFIG 上的读改写。
     *
     * 集群模式把 value 原样写入；非集群模式把存储值当作 ";" 分隔的
     * "topic:conf" 列表，替换 key 匹配的条目后整体写回。
     */
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
            // Java 打印后按空表继续（首写是常态）
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

    // ---------------- 运维清理类 ----------------

    /** 对应 Java cleanExpiredConsumerQueue → CLEAN_EXPIRED_CONSUMEQUEUE(306)。 */
    public function cleanExpiredConsumerQueue(string $brokerAddr, int $timeHours): void
    {
        $this->invokeBroker($brokerAddr, RequestCode::CLEAN_EXPIRED_CONSUMEQUEUE, ['time' => $timeHours]);
    }

    /** 对应 Java 的 ByAddr 形态：逐个地址执行，返回**失败的地址**列表。
     *
     * @param list<string> $addrs
     * @return list<string>
     */
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

    /** 对应 Java deleteExpiredCommitLog → DELETE_EXPIRED_COMMITLOG(329)。 */
    public function deleteExpiredCommitLog(string $brokerAddr, int $timeHours): void
    {
        $this->invokeBroker($brokerAddr, RequestCode::DELETE_EXPIRED_COMMITLOG, ['time' => $timeHours]);
    }

    /** 对应 Java 的 ByAddr 形态：逐个地址执行，返回**失败的地址**列表。
     *
     * @param list<string> $addrs
     * @return list<string>
     */
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

    /**
     * 对应 Java cleanUnusedTopicByAddr → MQClientAPIImpl:2696：**单请求**
     * CLEAN_UNUSED_TOPIC(316)，由 broker 自行清理未使用 topic。
     *
     * 客户端不要遍历 topic 表逐个删——broker 自建的 BenchmarkTest、
     * 重试/死信 topic 会被 broker 以 SYSTEM_ERROR 拒绝。
     */
    public function cleanUnusedTopicByAddr(string $brokerAddr): void
    {
        $this->invokeBroker($brokerAddr, RequestCode::CLEAN_UNUSED_TOPIC);
    }

    /**
     * 对应 Java queryConsumeTimeSpan：按路由遍历 master，聚合各 broker 的
     * QUERY_CONSUME_TIME_SPAN(303) 结果（body 是 consumeTimeSpanSet JSON）。
     *
     * @return list<array<string, mixed>>
     */
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

    // ---------------- NameServer 配置（318/319） ----------------

    /**
     * 对应 Java updateNameServerConfig → UPDATE_NAMESRV_CONFIG(318)：
     * properties 以 **k=v\n 文本**进 body，广播到每个 NameServer，
     * 任一失败即抛（Java 记 errResponse 最后统一抛）。
     *
     * @param array<string, string> $properties
     */
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

    /**
     * 对应 Java getNameServerConfig → GET_NAMESRV_CONFIG(319)：逐个 NameServer
     * 查询，body 是 properties 文本；返回 {地址: properties 字典}。
     *
     * @param list<string>|null $namesrvAddrs
     * @return array<string, array<string, string>>
     */
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
                // Java 收集后统一抛
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

    /** 对应 Java setMessageRequestMode → SET_MESSAGE_REQUEST_MODE(401)：
     * 在 POP 与 Pull 模式间切换消费组（单元化场景）。
     *
     * ⚠ wire 口径：broker 对 401 **没有 header**（QueryAssignmentProcessor 直接纳
     * body 里的 SetMessageRequestModeRequestBody）——字段必须进 body JSON（Java 属性
     * 名 topic/consumerGroup/mode/popShareQueueNum），放 ext_fields 时 broker 侧
     * requestBody 反序列化为 null → NPE。mode 缺省按 Java 字段初始化值为 PULL。 */
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

    // ---------------------------------------------------------------- 辅助

    /**
     * 对应 Java PermName.isValid(String)（数字解析失败等价于抛 NumberFormatException）
     * 与 Python 模块级 ``_perm_is_valid``。
     */
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

// ---------------------------------------------------------------- 消息轨迹 DTO（org.apache.rocketmq.tools.admin.api） ----------------

/** 对应 Java ``TrackType`` 枚举（字符串值即枚举名）。 */
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

/** 对应 Java ``MessageTrack``：一条消息在某消费组的投递判定。 */
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
