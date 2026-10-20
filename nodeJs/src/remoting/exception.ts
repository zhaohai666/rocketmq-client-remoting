
export class RemotingException extends Error {
  constructor(message) { super(message); this.name = 'RemotingException'; }
}

export class RemotingConnectException extends RemotingException {
  constructor(addr, message = '') {
    super(`connect to ${addr} failed${message ? ': ' + message : ''}`);
    this.name = 'RemotingConnectException';
    this.addr = addr;
  }
}

export class RemotingSendRequestException extends RemotingException {
  constructor(addr, message = '') {
    super(`send request to ${addr} failed${message ? ': ' + message : ''}`);
    this.name = 'RemotingSendRequestException';
    this.addr = addr;
  }
}

export class RemotingTimeoutException extends RemotingException {
  constructor(addr, timeoutMillis) {
    super(`wait response on the channel <${addr}> timeout, ${timeoutMillis}ms`);
    this.name = 'RemotingTimeoutException';
    this.addr = addr;
    this.timeoutMillis = timeoutMillis;
  }
}

export class RemotingCommandException extends RemotingException {
  constructor(message) { super(message); this.name = 'RemotingCommandException'; }
}

export class MQClientException extends Error {
  constructor(message, cause = null) {
    super(message);
    this.name = 'MQClientException';
    this.cause = cause;
  }
}

export class MQBrokerException extends Error {
  // Declared, not just assigned: callers branch on the code (the SYNC retry
  // loop consults it before deciding whether another broker is worth trying),
  // so an implicit-any field would let a typo in that branch pass unseen.
  responseCode: number;
  brokerAddr: string;

  constructor(responseCode: number, errorMessage: string, brokerAddr: string = '') {
    super(errorMessage || `CODE: ${responseCode}`);
    this.name = 'MQBrokerException';
    this.responseCode = responseCode;
    this.brokerAddr = brokerAddr;
  }
}

export class InterruptedException extends Error {
  constructor(message = 'sleep interrupted') { super(message); this.name = 'InterruptedException'; }
}

export class RuntimeException extends Error {
  constructor(message) { super(message); this.name = 'RuntimeException'; }
}

export default { RemotingException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQClientException, MQBrokerException, InterruptedException, RuntimeException };
