# -*- coding: utf-8 -*-
"""Heartbeat V2: the fingerprint-gated heartbeat registration path.

The contract this module implements, split the way the work is split here:

* the heartbeat round and its per-broker decision table, which chooses between the full
  registration body and the light "withoutSub" one,
* the RPC side, which parses ``IS_SUPPORT_HEART_BEAT_V2`` and ``IS_SUB_CHANGE`` out of the
  **response extFields** of a plain ``HEART_BEAT`` frame,
* the heartbeat fingerprint, a hash of the whole registration content,
* the broker side, which switches on ``heartbeatFingerprint != 0``, keeps
  ``consumerGroup -> fingerprint`` and answers with the two flags above.

Two protocol facts that are easy to get wrong:

1. **There is no ``HEART_BEAT_V2`` request code.** V2 reuses ``HEART_BEAT`` (34) and is
   recognised from the two body fields; ``203`` is ``GET_CONSUMER_CONNECTION_LIST``. A client
   that sends an invented request code gets ``UNKNOWN_REQUEST_CODE`` back from a real broker.
2. **V2 is opt-in.** ``use_heartbeat_v2`` defaults to ``False`` in this client, so a client
   that does not ask for it keeps sending ``heartbeatFingerprint = 0`` and the broker keeps
   the V1 registration path.

The per-broker state (the set of brokers known to speak V2 plus the
``broker addr -> fingerprint`` table) lives here instead of inside a facade's send loop,
because this port lets every facade run its own heartbeat loop: one broker address receives
several **independent** heartbeat streams (the producer's ``ProducerData``, each consumer's
``ConsumerData``), so the tables are keyed by ``(owner, broker addr)`` -- ``owner`` being the
facade's group. Keying by address alone would make those streams evict each other's
fingerprint every round and silently degrade the "simple" heartbeat back to the full one forever.
"""
from __future__ import annotations

import threading
from typing import Dict, Optional, Set

from rocketmq_logging import get_logger
from remoting.protocol.heartbeat import HeartbeatData, HeartbeatV2Result

logger = get_logger()


class HeartbeatV2State:
    """The two tables kept per heartbeat stream, plus that stream's heartbeat lock.

    ``support_v2_broker_addrs``
        Brokers that answered ``IS_SUPPORT_HEART_BEAT_V2=true`` at least once. An address that is
        not in it only ever receives the full (with-subscription) heartbeat, which is exactly how a
        pre-V2 broker keeps working.
    ``broker_addr_heartbeat_fingerprint_table``
        The fingerprint this broker is known to hold for this stream. When it still equals the
        freshly computed one, the subscription set is known to be unchanged, so this round can send
        the cheap ``withoutSub`` heartbeat.
    """

    def __init__(self):
        self.support_v2_broker_addrs: Set[str] = set()
        self.broker_addr_heartbeat_fingerprint_table: Dict[str, int] = {}
        self.lock = threading.Lock()

    def reset_fingerprint_table(self) -> None:
        """Drop every remembered fingerprint for this stream.

        Called when the queue allocation changes (an assignment change asks for an
        immediate heartbeat round instead of waiting for the next tick):
        the next round must re-deliver the subscriptions, so the remembered fingerprints are
        dropped. The "this broker speaks V2" knowledge survives -- it is a property of the broker,
        not of this round.
        """
        with self.lock:
            self.broker_addr_heartbeat_fingerprint_table.clear()


class HeartbeatV2Coordinator:
    """Sends one heartbeat stream through the V2 path (the per-broker decision table)."""

    def __init__(self, client):
        self._client = client
        self._states: Dict[str, HeartbeatV2State] = {}
        self._lock = threading.Lock()

    def state(self, owner: str = "") -> HeartbeatV2State:
        key = owner or ""
        with self._lock:
            state = self._states.get(key)
            if state is None:
                state = HeartbeatV2State()
                self._states[key] = state
            return state

    def owners(self) -> Set[str]:
        with self._lock:
            return set(self._states.keys())

    def reset_fingerprint_table(self, owner: Optional[str] = None) -> None:
        """Reset one stream's table, or every stream's when ``owner`` is ``None``."""
        if owner is None:
            with self._lock:
                states = list(self._states.values())
        else:
            states = [self.state(owner)]
        for state in states:
            state.reset_fingerprint_table()

    def send_to_broker(self, owner: str, addr: str,
                       heartbeat_data: HeartbeatData,
                       timeout_millis: int = 5000) -> int:
        """One broker over the V2 path; returns the broker version from the response header.

        The decision table, branch by branch:

        * broker already known to speak V2 **and** still holding our fingerprint -> send the
          ``withoutSub`` body; if it answers ``IS_SUB_CHANGE`` (its per-group table moved under our
          feet, e.g. another instance of the same group registered different subscriptions), forget
          the fingerprint so the next round re-sends everything.
        * anything else -> send the full body. Only when the response says ``IS_SUPPORT_HEART_BEAT_V2``
          do we remember the address as a V2 broker and remember the fingerprint (a ``IS_SUB_CHANGE``
          answer means the broker's table moved, so the fingerprint is dropped instead).

        Exceptions propagate: callers count the successes per broker, so a raised error is
        how a round learns that this broker did not accept the heartbeat.
        """
        fingerprint = heartbeat_data.compute_fingerprint()
        heartbeat_data.heartbeat_fingerprint = fingerprint
        state = self.state(owner)
        with state.lock:
            remembered = state.broker_addr_heartbeat_fingerprint_table.get(addr)
            if (addr in state.support_v2_broker_addrs
                    and remembered is not None and remembered == fingerprint):
                result = self._client.send_heartbeat_v2(addr, heartbeat_data.without_sub_copy(),
                                                        timeout_millis)
                if result.sub_change:
                    state.broker_addr_heartbeat_fingerprint_table.pop(addr, None)
                logger.debug("heartbeatV2 simple to %s owner=%s subChange=%s",
                             addr, owner, result.sub_change)
            else:
                result = self._client.send_heartbeat_v2(addr, heartbeat_data, timeout_millis)
                if result.support_v2:
                    state.support_v2_broker_addrs.add(addr)
                    if result.sub_change:
                        state.broker_addr_heartbeat_fingerprint_table.pop(addr, None)
                    elif state.broker_addr_heartbeat_fingerprint_table.get(addr) != fingerprint:
                        state.broker_addr_heartbeat_fingerprint_table[addr] = fingerprint
                logger.debug("heartbeatV2 normal to %s owner=%s supportV2=%s subChange=%s",
                             addr, owner, result.support_v2, result.sub_change)
        return result.version


__all__ = ["HeartbeatV2Coordinator", "HeartbeatV2State", "HeartbeatV2Result"]
