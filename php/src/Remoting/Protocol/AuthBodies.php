<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

/**
 * User and access-control management models: the JSON bodies sent by the management codes 3001-3010
 * and the bodies those codes answer with.
 *
 * Wire rules (confirmed against what the broker-side readers accept; do not tidy the literals):
 * - The enum spellings are mixed-case and mutually inconsistent ("Super" but "enable", "Custom" but
 *   "Allow"). They are matched byte for byte, so the holders below are spelling tables, not
 *   behaviour: passing a value that is not in the table is the caller's business, and the writer
 *   echoes it unchanged.
 * - A field the caller did not set drops its key entirely instead of travelling as null: the
 *   broker-side deserializer reads a null value as an explicit assignment and overwrites the stored
 *   attribute with it, so "unset" and "null" are not the same request.
 * - toDict() assembles the keys in the order the writer is expected to emit them, because the body
 *   is compared byte for byte when a request is reproduced.
 */

/** ``userType`` spellings as the user store keeps them. */
final class AuthUserType
{
    public const SUPER = 'Super';
    public const NORMAL = 'Normal';
}

/** ``userStatus`` spellings as the user store keeps them: this table is the lower-case one. */
final class AuthUserStatus
{
    public const ENABLE = 'enable';
    public const DISABLE = 'disable';
}

/** ``policyType`` spellings: the wildcard-everything kind versus a hand-written rule set. */
final class AuthPolicyType
{
    public const CUSTOM = 'Custom';
    public const DEFAULT = 'Default';
}

/** ``decision`` spellings: whether the matched entries permit or forbid the operation. */
final class AuthDecision
{
    public const ALLOW = 'Allow';
    public const DENY = 'Deny';
}

/** One stored user: the credentials plus the two attributes the broker assigns meaning to. */
final class AuthUserInfo
{
    public function __construct(
        public ?string $username = null,
        public ?string $password = null,
        public ?string $userType = null,
        public ?string $userStatus = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [];
        if ($this->username !== null) {
            $d['username'] = $this->username;
        }
        if ($this->password !== null) {
            $d['password'] = $this->password;
        }
        if ($this->userType !== null) {
            $d['userType'] = $this->userType;
        }
        if ($this->userStatus !== null) {
            $d['userStatus'] = $this->userStatus;
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        return new self(
            isset($d['username']) ? (string) $d['username'] : null,
            isset($d['password']) ? (string) $d['password'] : null,
            isset($d['userType']) ? (string) $d['userType'] : null,
            isset($d['userStatus']) ? (string) $d['userStatus'] : null,
        );
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    /**
     * Reads one user back out of a response body. No body means no user.
     *
     * A list-shaped body is not the single-object shape: its first element is taken, because
     * iterating an object body as if it were a list would feed the field *values* to fromDict().
     */
    public static function decodeOne(?string $body): ?self
    {
        $obj = self::decodeShape($body);
        if ($obj === null) {
            return null;
        }
        return array_is_list($obj)
            ? (is_array($obj[0] ?? null) ? self::fromDict($obj[0]) : null)
            : self::fromDict($obj);
    }

    /**
     * Reads a collection of users out of a response body; no body means an empty collection.
     * An object-shaped body is one user, never a list of its field values.
     *
     * @return list<self>
     */
    public static function decodeList(?string $body): array
    {
        $obj = self::decodeShape($body);
        if ($obj === null) {
            return [];
        }
        if (!array_is_list($obj)) {
            return [self::fromDict($obj)];
        }
        $out = [];
        foreach ($obj as $item) {
            if (is_array($item)) {
                $out[] = self::fromDict($item);
            }
        }
        return $out;
    }

    /** @return array<array-key, mixed>|null */
    private static function decodeShape(?string $body): ?array
    {
        if ($body === null || $body === '') {
            return null;
        }
        $obj = RemotingSerializable::decodeJson($body);
        return is_array($obj) ? $obj : null;
    }
}

/** One rule of a policy: what a resource may be doing, by whom and from where. */
final class AuthAccessEntry
{
    /** @param list<string>|null $actions @param list<string>|null $sourceIps */
    public function __construct(
        public ?string $resource = null,
        public ?array $actions = null,
        public ?array $sourceIps = null,
        public ?string $decision = null,
    ) {
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [];
        if ($this->resource !== null) {
            $d['resource'] = $this->resource;
        }
        if ($this->actions !== null) {
            $d['actions'] = array_map(strval(...), array_values($this->actions));
        }
        if ($this->sourceIps !== null) {
            $d['sourceIps'] = array_map(strval(...), array_values($this->sourceIps));
        }
        if ($this->decision !== null) {
            $d['decision'] = $this->decision;
        }
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        return new self(
            isset($d['resource']) ? (string) $d['resource'] : null,
            isset($d['actions']) && is_array($d['actions'])
                ? array_map(strval(...), array_values($d['actions'])) : null,
            isset($d['sourceIps']) && is_array($d['sourceIps'])
                ? array_map(strval(...), array_values($d['sourceIps'])) : null,
            isset($d['decision']) ? (string) $d['decision'] : null,
        );
    }
}

/** One policy of a subject: the kind plus its rules. */
final class AuthPolicy
{
    /** @param list<AuthAccessEntry> $entries */
    public function __construct(
        public ?string $policyType = null,
        public array $entries = [],
    ) {
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [];
        if ($this->policyType !== null) {
            $d['policyType'] = $this->policyType;
        }
        $d['entries'] = array_map(static fn(AuthAccessEntry $e): array => $e->toDict(), array_values($this->entries));
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $policy = new self();
        $policy->policyType = isset($d['policyType']) ? (string) $d['policyType'] : null;
        foreach ((array) ($d['entries'] ?? []) as $entry) {
            if (is_array($entry)) {
                $policy->entries[] = AuthAccessEntry::fromDict($entry);
            }
        }
        return $policy;
    }
}

/** The rules attached to one subject (a user name, or the anonymous grant of a user). */
final class AuthAclInfo
{
    /** @param list<AuthPolicy> $policies */
    public function __construct(
        public ?string $subject = null,
        public array $policies = [],
    ) {
    }

    /**
     * The shape a caller spells out resource by resource: **one** policy, no policy type, one entry
     * per resource with the same actions / source ips / decision repeated on every entry.
     *
     * @param list<string> $resources
     * @param list<string> $actions
     * @param list<string> $sourceIps
     */
    public static function forResources(
        string $subject,
        array $resources,
        array $actions,
        array $sourceIps,
        ?string $decision,
    ): self {
        $entries = [];
        foreach (array_values($resources) as $resource) {
            $entries[] = new AuthAccessEntry(
                (string) $resource,
                array_map(strval(...), array_values($actions)),
                array_map(strval(...), array_values($sourceIps)),
                $decision,
            );
        }
        return new self($subject, [new AuthPolicy(null, $entries)]);
    }

    /** @return array<string, mixed> */
    public function toDict(): array
    {
        $d = [];
        if ($this->subject !== null) {
            $d['subject'] = $this->subject;
        }
        $d['policies'] = array_map(static fn(AuthPolicy $p): array => $p->toDict(), array_values($this->policies));
        return $d;
    }

    /** @param array<string, mixed> $d */
    public static function fromDict(array $d): self
    {
        $acl = new self();
        $acl->subject = isset($d['subject']) ? (string) $d['subject'] : null;
        foreach ((array) ($d['policies'] ?? []) as $policy) {
            if (is_array($policy)) {
                $acl->policies[] = AuthPolicy::fromDict($policy);
            }
        }
        return $acl;
    }

    public function encode(): string
    {
        return RemotingSerializable::encode($this->toDict());
    }

    /** Reads one acl back out of a response body; see AuthUserInfo::decodeOne() for the shape rule. */
    public static function decodeOne(?string $body): ?self
    {
        $obj = self::decodeShape($body);
        if ($obj === null) {
            return null;
        }
        return array_is_list($obj)
            ? (is_array($obj[0] ?? null) ? self::fromDict($obj[0]) : null)
            : self::fromDict($obj);
    }

    /**
     * @return list<self>
     */
    public static function decodeList(?string $body): array
    {
        $obj = self::decodeShape($body);
        if ($obj === null) {
            return [];
        }
        if (!array_is_list($obj)) {
            return [self::fromDict($obj)];
        }
        $out = [];
        foreach ($obj as $item) {
            if (is_array($item)) {
                $out[] = self::fromDict($item);
            }
        }
        return $out;
    }

    /** @return array<array-key, mixed>|null */
    private static function decodeShape(?string $body): ?array
    {
        if ($body === null || $body === '') {
            return null;
        }
        $obj = RemotingSerializable::decodeJson($body);
        return is_array($obj) ? $obj : null;
    }
}
