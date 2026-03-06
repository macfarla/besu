/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.eth.manager.snap;

import org.hyperledger.besu.ethereum.eth.manager.EthPeer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;

public class SnapRequestRateLimiter {

  private final int maxConcurrentRequestsPerPeer;
  private final ConcurrentHashMap<Bytes, PeerConcurrencyState> peerStates;

  public SnapRequestRateLimiter(final int maxConcurrentRequestsPerPeer) {
    this.maxConcurrentRequestsPerPeer = maxConcurrentRequestsPerPeer;
    this.peerStates = new ConcurrentHashMap<>();
  }

  /**
   * Attempts to acquire a permit for the given peer. If successful, the caller MUST call {@link
   * #release(EthPeer)} when the request is complete.
   *
   * @param peer the peer making the request
   * @return true if a permit was acquired, false if at the concurrency limit
   */
  public boolean tryAcquire(final EthPeer peer) {
    if (maxConcurrentRequestsPerPeer <= 0) {
      return true; // disabled
    }
    final PeerConcurrencyState state =
        peerStates.computeIfAbsent(
            peer.getId(), k -> new PeerConcurrencyState(maxConcurrentRequestsPerPeer));
    state.totalRequests.incrementAndGet();
    return state.semaphore.tryAcquire();
  }

  /**
   * Releases a permit for the given peer. Must be called after a successful {@link
   * #tryAcquire(EthPeer)}.
   *
   * @param peer the peer whose permit to release
   */
  public void release(final EthPeer peer) {
    if (maxConcurrentRequestsPerPeer <= 0) {
      return;
    }
    final PeerConcurrencyState state = peerStates.get(peer.getId());
    if (state != null) {
      state.semaphore.release();
    }
  }

  /**
   * Checks whether logging should be performed for a rejected request from this peer. Throttles
   * logging to at most once per 10 seconds per peer.
   *
   * @param peer the peer whose request was rejected
   * @return true if a log message should be emitted
   */
  public boolean shouldLogRejection(final EthPeer peer) {
    final PeerConcurrencyState state = peerStates.get(peer.getId());
    if (state == null) {
      return true;
    }
    state.rejectedRequests.incrementAndGet();
    final long now = System.nanoTime();
    final long lastLogNanos = state.lastLogNanos.get();
    return (now - lastLogNanos) >= 10_000_000_000L
        && state.lastLogNanos.compareAndSet(lastLogNanos, now);
  }

  public void removePeer(final Bytes peerId) {
    peerStates.remove(peerId);
  }

  public int getMaxConcurrentRequestsPerPeer() {
    return maxConcurrentRequestsPerPeer;
  }

  @VisibleForTesting
  int trackedPeerCount() {
    return peerStates.size();
  }

  private static class PeerConcurrencyState {
    final Semaphore semaphore;
    final AtomicLong totalRequests = new AtomicLong();
    final AtomicLong rejectedRequests = new AtomicLong();
    final AtomicLong lastLogNanos = new AtomicLong();

    PeerConcurrencyState(final int maxConcurrent) {
      this.semaphore = new Semaphore(maxConcurrent);
    }
  }
}
