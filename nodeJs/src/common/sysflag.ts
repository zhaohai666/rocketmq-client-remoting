// -*- coding: utf-8 -*-
// System flag bit constants (org.apache.rocketmq.common.sysflag.*).

export const MessageSysFlag = {
  COMPRESSED_FLAG: 0x1,
  MULTI_TAGS_FLAG: 0x1 << 1,
  TRANSACTION_NOT_TYPE: 0,
  TRANSACTION_PREPARED_TYPE: 0x1 << 2,
  TRANSACTION_COMMIT_TYPE: 0x2 << 2,
  TRANSACTION_ROLLBACK_TYPE: 0x3 << 2,
  BORNHOST_V6_FLAG: 0x1 << 4,
  STOREHOSTADDRESS_V6_FLAG: 0x1 << 5,
  NEED_UNWRAP_FLAG: 0x1 << 6,
  INNER_BATCH_FLAG: 0x1 << 7,
  COMPRESSION_LZ4_TYPE: 0x1 << 8,
  COMPRESSION_ZSTD_TYPE: 0x2 << 8,
  COMPRESSION_ZLIB_TYPE: 0x3 << 8,
  COMPRESSION_TYPE_COMPARATOR: 0x7 << 8,
  COMPRESSION_TYPE_SHIFT: 8,
  // legacy aliases
  LZ4_TYPE: 1,
  ZSTD_TYPE: 2,
  ZLIB_TYPE: 3,
  SNAPPY_TYPE: 4,

  getCompressionType(sysFlag) {
    return (sysFlag & MessageSysFlag.COMPRESSION_TYPE_COMPARATOR) >> MessageSysFlag.COMPRESSION_TYPE_SHIFT;
  },
  setCompressionType(sysFlag, compressionType) {
    return (sysFlag & ~MessageSysFlag.COMPRESSION_TYPE_COMPARATOR) |
           ((compressionType << MessageSysFlag.COMPRESSION_TYPE_SHIFT) & MessageSysFlag.COMPRESSION_TYPE_COMPARATOR);
  },
  isCompressed(sysFlag) {
    return (sysFlag & MessageSysFlag.COMPRESSED_FLAG) === MessageSysFlag.COMPRESSED_FLAG;
  },
  clearCompressedFlag(sysFlag) {
    return sysFlag & ~MessageSysFlag.COMPRESSED_FLAG;
  },
  getTransactionValue(flag) {
    return flag & MessageSysFlag.TRANSACTION_ROLLBACK_TYPE;
  },
  resetTransactionValue(flag, transactionType) {
    return (flag & ~MessageSysFlag.TRANSACTION_ROLLBACK_TYPE) | transactionType;
  },
  check(flag, expectedFlag) {
    return (flag & expectedFlag) !== 0;
  },
};

export const PullSysFlag = {
  FLAG_COMMIT_OFFSET: 0x1,
  FLAG_SUSPEND: 0x1 << 1,
  FLAG_SUBSCRIPTION: 0x1 << 2,
  FLAG_CLASS_FILTER: 0x1 << 3,
  FLAG_LITE_PULL_MESSAGE: 0x1 << 4,
  FLAG_PROXY_BLOCK: 0x1 << 5,
  FLAG_EXT_BROKER_GROUP: 0x1 << 6,
  FLAG_INNER_SQL: 0x1 << 7,
  FLAG_MULTI_TAG: 0x1 << 8,
  FLAG_START_OFFSET: 0x1 << 9,

  buildSysFlag(commitOffset, suspend, subscription, classFilter, litePull = false) {
    let flag = 0;
    if (commitOffset) flag |= PullSysFlag.FLAG_COMMIT_OFFSET;
    if (suspend) flag |= PullSysFlag.FLAG_SUSPEND;
    if (subscription) flag |= PullSysFlag.FLAG_SUBSCRIPTION;
    if (classFilter) flag |= PullSysFlag.FLAG_CLASS_FILTER;
    if (litePull) flag |= PullSysFlag.FLAG_LITE_PULL_MESSAGE;
    return flag;
  },
  clearCommitOffsetFlag(sysFlag) { return sysFlag & ~PullSysFlag.FLAG_COMMIT_OFFSET; },
  hasCommitOffsetFlag(sysFlag) { return (sysFlag & PullSysFlag.FLAG_COMMIT_OFFSET) === PullSysFlag.FLAG_COMMIT_OFFSET; },
  hasSuspendFlag(sysFlag) { return (sysFlag & PullSysFlag.FLAG_SUSPEND) === PullSysFlag.FLAG_SUSPEND; },
  clearSuspendFlag(sysFlag) { return sysFlag & ~PullSysFlag.FLAG_SUSPEND; },
  hasSubscriptionFlag(sysFlag) { return (sysFlag & PullSysFlag.FLAG_SUBSCRIPTION) === PullSysFlag.FLAG_SUBSCRIPTION; },
  buildSysFlagWithSubscription(sysFlag) { return sysFlag | PullSysFlag.FLAG_SUBSCRIPTION; },
  hasClassFilterFlag(sysFlag) { return (sysFlag & PullSysFlag.FLAG_CLASS_FILTER) === PullSysFlag.FLAG_CLASS_FILTER; },
  hasLitePullFlag(sysFlag) { return (sysFlag & PullSysFlag.FLAG_LITE_PULL_MESSAGE) === PullSysFlag.FLAG_LITE_PULL_MESSAGE; },
};

export const PermName = {
  PERM_PRIORITY: 0x1 << 3,
  PERM_READ: 0x4,
  PERM_WRITE: 0x2,
  PERM_INHERIT: 0x1,
  PERM_OWNER: 0x1 << 4,
  isValid(perm) { return 0 <= Number(perm) && Number(perm) < PermName.PERM_PRIORITY; },
  permToString(perm) {
    let sb = '';
    sb += (PermName.PERM_READ === (perm & PermName.PERM_READ)) ? 'R' : '-';
    sb += (PermName.PERM_WRITE === (perm & PermName.PERM_WRITE)) ? 'W' : '-';
    sb += (PermName.PERM_INHERIT === (perm & PermName.PERM_INHERIT)) ? 'X' : '-';
    return sb;
  },
  checkPerm(perm, wantedPerm) { return (perm & wantedPerm) === wantedPerm; },
};

export const SubscriptionMode = { GROUP: 0, BROADCASTING: 1 };

export default { MessageSysFlag, PullSysFlag, PermName, SubscriptionMode };
