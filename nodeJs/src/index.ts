// Public API surface of the RocketMQ Node.js client (the npm package entry).
//
// The repo itself runs TypeScript directly (`node --experimental-strip-types`), so internal
// code imports deep paths with explicit `.ts` extensions. This barrel is the **published**
// entry: `npm run build` compiles it together with the whole src/ tree into dist/, with the
// `.ts` import specifiers rewritten to `.js` (tsconfig.build.json +
// rewriteRelativeImportExtensions), and package.json's main/types/exports point here.
//
// Consumers of the tarball / registry package import it without any flag:
//   import { DefaultMQProducer, Message } from 'rocketmq-client-remoting-nodejs';

// ---- producer ----
export { DefaultMQProducer } from './client/producer.ts';
export { TransactionMQProducer } from './client/producer.ts';
export { TransactionListener } from './client/producer.ts';
export { LocalTransactionState } from './client/producer.ts';
export { MessageQueueSelector } from './client/producer.ts';
export { SelectMessageQueueByHash } from './client/producer.ts';
export { SelectMessageQueueByRandom } from './client/producer.ts';
export type { SendCallback } from './client/producer.ts';

// ---- consumer ----
export { DefaultMQPushConsumer } from './client/consumer.ts';
export { DefaultMQPullConsumer } from './client/pull_consumer.ts';
export type { PullCallback } from './client/pull_consumer.ts';
export { DefaultLitePullConsumer } from './client/lite_pull_consumer.ts';
export type { TopicMessageQueueChangeListener } from './client/lite_pull_consumer.ts';
export { DefaultMQAdminExt } from './client/admin.ts';

// ---- pull schedule service (Java MQPullConsumerScheduleService equivalent) ----
export {
  PullConsumerScheduleService,
  PullTaskContext,
  DEFAULT_PULL_NEXT_DELAY_MILLIS,
  PULL_TASK_EXCEPTION_DELAY_MILLIS,
  DEFAULT_QUEUE_CHECK_INTERVAL_MILLIS,
  DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MILLIS,
} from './client/pull_schedule_service.ts';
export type { PullTaskCallback, PullTaskOptions } from './client/pull_schedule_service.ts';

// ---- consume results / listeners / selectors ----
export {
  ConsumeConcurrentlyStatus,
  ConsumeOrderlyStatus,
  PullStatus,
  PullResult,
  ConsumeConcurrentlyContext,
  ConsumeOrderlyContext,
  MessageSelector,
  MessageQueueListener,
  MessageListener,
  MessageListenerConcurrently,
  MessageListenerOrderly,
} from './client/consumer_result.ts';

// ---- message model ----
export { Message, MessageExt, MessageQueue, MessageBatch, isWaitStoreMsgOK } from './common/message.ts';
export { MessageConst } from './common/messageConst.ts';
export { MixAll } from './common/mixAll.ts';
export { MessageSysFlag, PullSysFlag, PermName, SubscriptionMode } from './common/sysflag.ts';
export { MessageAccessor } from './common/message_accessor.ts';
export { ExpressionType, SubscriptionData, FilterAPI } from './common/subscriptionData.ts';

// ---- remoting-level enums (public knobs) ----
export { MessageModel, ConsumeFromWhere, ConsumeType } from './remoting/heartbeat.ts';
