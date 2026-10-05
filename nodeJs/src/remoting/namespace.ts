// -*- coding: utf-8 -*-
// Namespace utilities (org.apache.rocketmq.remoting.protocol.NamespaceUtil).
// Mirrors python/remoting/protocol/namespace_util.py.
import { MixAll } from '../common/mixAll.ts';

export const NAMESPACE_SEPARATOR = '%';

export class NamespaceUtil {
  // Strips the retry/DLQ prefix (matches Java withOutRetryAndDLQ).
  static withOutRetryAndDLQ(resource: string): string {
    return MixAll.resetRetryAndDlqTopic(resource);
  }

  static isRetryTopic(resource: string): boolean {
    return MixAll.isRetryTopic(resource);
  }

  static isDlqTopic(resource: string): boolean {
    return MixAll.isDlqTopic(resource);
  }

  static isSystemResource(resource: string): boolean {
    if (!resource) return false;
    return MixAll.isSysTopic(resource) || MixAll.isSysConsumerGroup(resource);
  }

  static isAlreadyWithNamespace(resource: string, namespace: string): boolean {
    if (!namespace || !resource || NamespaceUtil.isSystemResource(resource)) return false;
    const plain = NamespaceUtil.withOutRetryAndDLQ(resource);
    return plain.startsWith(namespace + NAMESPACE_SEPARATOR);
  }

  // Strip the namespace prefix. NS%Topic -> Topic; %RETRY%NS%GID -> %RETRY%GID.
  static withoutNamespace(resourceWithNamespace: string, namespace = ''): string {
    if (!resourceWithNamespace) return resourceWithNamespace;
    if (namespace) {
      const plain = NamespaceUtil.withOutRetryAndDLQ(resourceWithNamespace);
      if (!plain.startsWith(namespace + NAMESPACE_SEPARATOR)) return resourceWithNamespace;
    } else if (NamespaceUtil.isSystemResource(resourceWithNamespace)) {
      return resourceWithNamespace;
    }
    let prefix = '';
    if (NamespaceUtil.isRetryTopic(resourceWithNamespace)) prefix = MixAll.RETRY_GROUP_TOPIC_PREFIX;
    if (NamespaceUtil.isDlqTopic(resourceWithNamespace)) prefix = MixAll.DLQ_GROUP_TOPIC_PREFIX;
    const plain = NamespaceUtil.withOutRetryAndDLQ(resourceWithNamespace);
    const index = plain.indexOf(NAMESPACE_SEPARATOR);
    if (index > 0) return prefix + plain.substring(index + 1);
    return resourceWithNamespace;
  }

  // Prepend the namespace prefix (matches Java wrapNamespace).
  static wrapNamespace(namespace: string, resourceWithoutNamespace: string): string {
    if (!namespace || !resourceWithoutNamespace) return resourceWithoutNamespace;
    if (NamespaceUtil.isSystemResource(resourceWithoutNamespace)) return resourceWithoutNamespace;
    if (NamespaceUtil.isAlreadyWithNamespace(resourceWithoutNamespace, namespace)) return resourceWithoutNamespace;
    let prefix = '';
    if (NamespaceUtil.isRetryTopic(resourceWithoutNamespace)) prefix = MixAll.RETRY_GROUP_TOPIC_PREFIX;
    if (NamespaceUtil.isDlqTopic(resourceWithoutNamespace)) prefix = MixAll.DLQ_GROUP_TOPIC_PREFIX;
    const plain = NamespaceUtil.withOutRetryAndDLQ(resourceWithoutNamespace);
    return `${prefix}${namespace}${NAMESPACE_SEPARATOR}${plain}`;
  }

  // %RETRY%<wrapNamespace(namespace, group)>
  static wrapNamespaceAndRetry(namespace: string, consumerGroup: string): string {
    if (!consumerGroup) return consumerGroup;
    return MixAll.RETRY_GROUP_TOPIC_PREFIX + NamespaceUtil.wrapNamespace(namespace, consumerGroup);
  }

  // Extract the namespace embedded in a resource name (Java getNamespaceFromResource).
  static getNamespaceFromResource(resource: string): string {
    if (!resource || NamespaceUtil.isSystemResource(resource)) return '';
    const plain = NamespaceUtil.withOutRetryAndDLQ(resource);
    const index = plain.indexOf(NAMESPACE_SEPARATOR);
    return index > 0 ? plain.substring(0, index) : '';
  }

  // getRV: get the resource value without its namespace (alias used by some callers).
  static getRV(resource: string, namespace: string): string {
    return NamespaceUtil.withoutNamespace(resource, namespace);
  }
}

export default NamespaceUtil;
