// -*- coding: utf-8 -*-
// Client-level exception codes (org.apache.rocketmq.client.exception.ClientErrorCode)
// plus re-exports of the shared exception hierarchy from the remoting layer.
//
// NOTE: the foundation remoting/exception.ts is the single source of truth for the exception
// classes; this module only adds the client-specific error codes (mirroring python/rocketmq/
// client/exception.py) and a thin RequestTimeoutException used by the request-reply path.
import {
  MQClientException,
  MQBrokerException,
  RemotingException,
  RemotingConnectException,
  RemotingSendRequestException,
  RemotingTimeoutException,
  RemotingCommandException,
  InterruptedException,
  RuntimeException,
} from '../remoting/exception.ts';

export const ClientErrorCode = {
  CONNECT_BROKER_EXCEPTION: 10001,
  ACCESS_BROKER_TIMEOUT: 10002,
  BROKER_NOT_EXIST_EXCEPTION: 10003,
  NO_NAME_SERVER_EXCEPTION: 10004,
  NOT_FOUND_TOPIC_EXCEPTION: 10005,
  REQUEST_TIMEOUT_EXCEPTION: 10006,
  CREATE_REPLY_MESSAGE_EXCEPTION: 10007,
  SEND_REQUEST_WITH_FUTURE_EXCEPTION: 10008,
};

// Convenience subclass used by the request-reply future path (mirrors python).
export class RequestTimeoutException extends RemotingTimeoutException {
  constructor(addr: string, timeoutMillis: number) {
    super(addr, timeoutMillis);
    this.name = 'RequestTimeoutException';
  }
}

export {
  MQClientException,
  MQBrokerException,
  RemotingException,
  RemotingConnectException,
  RemotingSendRequestException,
  RemotingTimeoutException,
  RemotingCommandException,
  InterruptedException,
  RuntimeException,
};

export default {
  ClientErrorCode,
  RequestTimeoutException,
  MQClientException,
  MQBrokerException,
  RemotingException,
  RemotingConnectException,
  RemotingSendRequestException,
  RemotingTimeoutException,
  RemotingCommandException,
  InterruptedException,
  RuntimeException,
};
