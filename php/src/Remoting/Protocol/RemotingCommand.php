<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Client\Exceptions\RemotingCommandException;
use RocketMQ\Client\Logger;

/**
 * RemotingCommand：RocketMQ 远程命令（对应 org.apache.rocketmq.remoting.protocol.RemotingCommand，
 * 移植自 protocol/remoting_command.py）。
 *
 * 线格式：totalLength(4) | headerLength(带序列化类型高8位, 4) | header | body
 * header（JSON）：RemotingSerializable JSON 编码
 * header（ROCKETMQ）：code(2) language(1) version(2) opaque(4) flag(4) remark(int+utf8) extFields(int + key(short+utf8) value(int+utf8))
 */
final class RemotingCommand
{
    public const SERIALIZE_TYPE_PROPERTY = 'rocketmq.serialize.type';
    public const SERIALIZE_TYPE_ENV = 'ROCKETMQ_SERIALIZE_TYPE';
    public const REMOTING_VERSION_KEY = 'rocketmq.remoting.version';

    /**
     * Java `MQVersion.CURRENT_VERSION`（= `Version.V5_5_1.ordinal()`，本机 5.5.1 集群）。
     * broker 按心跳/请求里记录的客户端版本决定能否把管理请求回调到客户端：
     * `AdminBrokerProcessor#callConsumer`（307）低于 `V3_1_8_SNAPSHOT`（ordinal 62）时
     * 直接回 "The Consumer <x> Version <0> too low to finish"；`Broker2Client#getConsumeStatus`
     * （223→221）低于 `V3_0_7_SNAPSHOT`（ordinal 28）时回 "the client does not support this
     * feature. version=..."，`resetOffset` 的在线分支同样按 28 跳过。发 0 等于自降为不可回调。
     */
    public const CURRENT_VERSION = 515;

    public const RPC_TYPE = 0;
    public const RPC_ONEWAY = 1;

    private static int $serializeTypeConfigInThisServer = -1;
    private static int $requestId = 0;

    public int $code;
    /** @var int LanguageCode 的 int 码（线上统一按 int 传输） */
    public int $language;
    public int $version = 0;
    public int $opaque;
    public int $flag = 0;
    public ?string $remark;
    /** @var array<string, string> */
    public array $extFields = [];
    public ?object $customHeader;
    public ?string $body;
    /** @var int SerializeType 的 int 码 */
    public int $serializeTypeCurrentRpc;
    public ?object $cachedHeader = null;

    public function __construct(
        int $code = 0,
        ?object $customHeader = null,
        ?string $remark = null,
        ?int $opaque = null,
        int $flag = 0,
        ?string $body = null,
    ) {
        $this->code = $code;
        $this->language = LanguageCode::PHP->value;
        $this->version = 0;
        $this->opaque = $opaque ?? self::createNewRequestId();
        $this->flag = $flag;
        $this->remark = $remark;
        $this->extFields = [];
        $this->customHeader = $customHeader;
        $this->body = $body;
        if (self::$serializeTypeConfigInThisServer < 0) {
            self::$serializeTypeConfigInThisServer = self::loadSerializeTypeConfig();
        }
        $this->serializeTypeCurrentRpc = self::$serializeTypeConfigInThisServer;
    }

    // ---------------- 工厂方法 ----------------

    /**
     * 对应 Java ``RemotingCommand.createNewRequestId()``。
     *
     * 异步发送换 broker 重试时必须给**同一个请求对象**换一个新 opaque
     * （Java ``onExceptionImpl:728``）：旧请求还挂在 responseTable 里等超时，
     * 复用 opaque 会让两次尝试的应答串台。
     */
    public static function createNewRequestId(): int
    {
        return self::$requestId++;
    }

    public static function createRequestCommand(int $code, ?object $customHeader = null): self
    {
        $cmd = new self(code: $code, customHeader: $customHeader);
        self::setCmdVersion($cmd);
        return $cmd;
    }

    public static function createResponseCommandWithHeader(int $code, ?object $customHeader = null): self
    {
        $cmd = new self(code: $code, customHeader: $customHeader);
        $cmd->markResponseType();
        self::setCmdVersion($cmd);
        return $cmd;
    }

    /** 与 Java createResponseCommand(int code, String remark, Class classHeader) 对齐。 */
    public static function createResponseCommand(int $code = 0, string $remark = 'not set any response code', ?string $classHeader = null): ?self
    {
        $cmd = new self(code: $code, remark: $remark);
        $cmd->markResponseType();
        self::setCmdVersion($cmd);
        if ($classHeader !== null) {
            try {
                $cmd->customHeader = new $classHeader();
            } catch (\Throwable) {
                return null;
            }
        }
        return $cmd;
    }

    public static function buildErrorResponse(int $code, string $remark): ?self
    {
        return self::createResponseCommand($code, $remark, null);
    }

    /** 返回序列化类型数值（与 Java byte getProtocolType 对齐），供与 SerializeType 常量比较。 */
    public static function getProtocolType(int $source): int
    {
        return ($source >> 24) & 0xFF;
    }

    public static function getHeaderLength(int $length): int
    {
        return $length & 0xFFFFFF;
    }

    public static function markProtocolType(int $source, int $stype): int
    {
        return (($stype & 0xFF) << 24) | ($source & 0x00FFFFFF);
    }

    // ---------------- 编解码 ----------------

    public function markResponseType(): void
    {
        $this->flag |= 1 << self::RPC_TYPE;
    }

    public function isResponseType(): bool
    {
        return ($this->flag & (1 << self::RPC_TYPE)) === (1 << self::RPC_TYPE);
    }

    public function markOnewayRPC(): void
    {
        $this->flag |= 1 << self::RPC_ONEWAY;
    }

    public function isOnewayRPC(): bool
    {
        return ($this->flag & (1 << self::RPC_ONEWAY)) === (1 << self::RPC_ONEWAY);
    }

    public function getType(): string
    {
        return $this->isResponseType()
            ? RemotingCommandType::RESPONSE_COMMAND->value
            : RemotingCommandType::REQUEST_COMMAND->value;
    }

    /** 把 custom_header 的非 None 字段写入 ext_fields（对应 Java 反射行为）。 */
    public function makeCustomHeaderToNet(): void
    {
        if ($this->customHeader !== null) {
            foreach ($this->customHeader->toExtFields() as $name => $value) {
                if ($value !== null) {
                    $this->extFields[$name] = (string)$value;
                }
            }
        }
    }

    public function headerEncode(): string
    {
        $this->makeCustomHeaderToNet();
        if ($this->serializeTypeCurrentRpc === SerializeType::ROCKETMQ->value) {
            return RocketMQSerializable::rocketMqProtocolEncode($this);
        }
        return RemotingSerializable::encode($this->toDict());
    }

    public function encode(): string
    {
        $length = 4;
        $headerData = $this->headerEncode();
        $length += strlen($headerData);
        $body = $this->body;
        if ($body !== null) {
            $length += strlen($body);
        }
        $out = pack('N', $length)
            . pack('N', self::markProtocolType(strlen($headerData), $this->serializeTypeCurrentRpc))
            . $headerData;
        if ($body !== null) {
            $out .= $body;
        }
        return $out;
    }

    public function encodeHeader(int $bodyLength = 0): string
    {
        $length = 4;
        $headerData = $this->headerEncode();
        $length += strlen($headerData) + $bodyLength;
        return pack('N', $length)
            . pack('N', self::markProtocolType(strlen($headerData), $this->serializeTypeCurrentRpc))
            . $headerData;
    }

    /**
     * 解码一帧（含 4 字节 totalLength 前缀）。
     */
    public static function decode(string $data): self
    {
        $offset = 0;
        $totalLength = RocketMQSerializable::unpackSignedInt(substr($data, $offset, 4));
        $offset += 4;
        if ($totalLength > strlen($data) - 4) {
            throw new RemotingCommandException("decode error, bad total length: {$totalLength}");
        }
        $oriHeaderLen = RocketMQSerializable::unpackSignedInt(substr($data, $offset, 4));
        $offset += 4;
        $headerLength = self::getHeaderLength($oriHeaderLen);
        if ($headerLength > strlen($data) - $offset) {
            throw new RemotingCommandException("decode error, bad header length: {$headerLength}");
        }
        $protocolType = self::getProtocolType($oriHeaderLen);
        $headerData = substr($data, $offset, $headerLength);
        $offset += $headerLength;
        if ($protocolType === SerializeType::ROCKETMQ->value) {
            $fields = RocketMQSerializable::rocketMqProtocolDecode($headerData);
            $cmd = new self(code: $fields['code'], remark: $fields['remark'], flag: $fields['flag']);
            $cmd->language = $fields['language'];
            $cmd->version = $fields['version'];
            $cmd->opaque = $fields['opaque'];
            $cmd->extFields = $fields['extFields'] ?? [];
        } else {
            try {
                $obj = json_decode($headerData, true, 512, JSON_THROW_ON_ERROR);
            } catch (\JsonException $e) {
                throw new RemotingCommandException('decode error, invalid json header: ' . $e->getMessage(), 0, $e);
            }
            $cmd = new self(
                code: (int)($obj['code'] ?? 0),
                remark: isset($obj['remark']) ? (string)$obj['remark'] : null,
                flag: (int)($obj['flag'] ?? 0),
            );
            // 5.x NameServer 把 language 序列化为枚举名字符串（如 "JAVA"），4.x 用 int；
            // 这里兼容两种形态，统一成 int 码。
            $lang = $obj['language'] ?? LanguageCode::PYTHON->value;
            if (is_string($lang)) {
                $lang = LanguageCode::nameToCode($lang);
            }
            $cmd->language = (int)$lang;
            $cmd->version = (int)($obj['version'] ?? 0);
            $cmd->opaque = (int)($obj['opaque'] ?? -1);
            $cmd->extFields = $obj['extFields'] ?? [];
        }
        $cmd->serializeTypeCurrentRpc = $protocolType;
        $bodyLength = strlen($data) - $offset;
        $cmd->body = $bodyLength > 0 ? substr($data, $offset) : null;
        return $cmd;
    }

    /**
     * 把 ext_fields 映射到 header 对象（对应 Java decodeCommandCustomHeader）。
     *
     * 优先走 header 自带的 fromExtFields（短字段名等特殊映射），
     * 否则按属性名逐字段赋值并做基础类型转换。
     *
     * @param class-string $headerClass
     */
    public function decodeCommandCustomHeader(string $headerClass, bool $useFastEncode = true): object
    {
        /** @phpstan-ignore-next-line */
        $h = new $headerClass();
        if (method_exists($h, 'fromExtFields')) {
            $h->fromExtFields($this->extFields);
        } else {
            foreach ($this->extFields as $name => $value) {
                if (property_exists($h, $name)) {
                    $h->{$name} = $value;
                }
            }
        }
        $this->cachedHeader = $h;
        return $h;
    }

    /** @return array<string, mixed> */
    private function toDict(): array
    {
        $d = [
            'code' => $this->code,
            'language' => $this->language,
            'version' => $this->version,
            'opaque' => $this->opaque,
            'flag' => $this->flag,
        ];
        if ($this->remark !== null) {
            $d['remark'] = $this->remark;
        }
        if ($this->extFields !== []) {
            $d['extFields'] = $this->extFields;
        }
        return $d;
    }

    public function addExtField(string $key, string $value): void
    {
        $this->extFields[$key] = $value;
    }

    public function getExtField(string $key): ?string
    {
        return $this->extFields[$key] ?? null;
    }

    public function __toString(): string
    {
        return sprintf(
            'RemotingCommand [code=%s, language=%s, version=%s, opaque=%s, flag(B)=%s, remark=%s, extFields=%s, serializeTypeCurrentRPC=%s]',
            $this->code,
            $this->language,
            $this->version,
            $this->opaque,
            decbin($this->flag),
            $this->remark === null ? 'None' : $this->remark,
            json_encode($this->extFields, JSON_UNESCAPED_UNICODE),
            $this->serializeTypeCurrentRpc,
        );
    }

    private static function loadSerializeTypeConfig(): int
    {
        $v = getenv(self::SERIALIZE_TYPE_ENV) ?: getenv(self::SERIALIZE_TYPE_PROPERTY);
        if ($v !== false && $v !== '') {
            return strtoupper(trim($v)) === 'ROCKETMQ'
                ? SerializeType::ROCKETMQ->value
                : SerializeType::JSON->value;
        }
        return SerializeType::JSON->value;
    }

    private static function setCmdVersion(self $cmd): void
    {
        $v = getenv(self::REMOTING_VERSION_KEY);
        if ($v !== false && $v !== '' && preg_match('/^-?\d+$/', $v)) {
            $cmd->version = (int)$v;
            return;
        }
        $cmd->version = self::CURRENT_VERSION;
    }
}
