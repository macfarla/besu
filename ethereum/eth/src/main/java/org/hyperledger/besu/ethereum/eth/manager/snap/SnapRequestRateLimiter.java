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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.tuweni.bytes.Bytes;

@SuppressWarnings("UnstableApiUsage")
public class SnapRequestRateLimiter {

  private final boolean enabled;
  private final double permitsPerSecond;
  private final ConcurrentHashMap<Bytes, PeerRateLimitState> peerStates;

  public SnapRequestRateLimiter(final boolean enabled, final double permitsPerSecond) {
    this.enabled = enabled;
    this.permitsPerSecond = permitsPerSecond;
    this.peerStates = new ConcurrentHashMap<>();
  }

  /**
   * Attempts to acquire a permit for the given peer.
   *
   * @param peer the peer making the request
   * @return a {@link RateLimitResult} indicating whether the request was allowed
   */
  public RateLimitResult tryAcquire(final EthPeer peer) {
    if (!enabled) {
      return RateLimitResult.ALLOWED;
    }
    final PeerRateLimitState state =
        peerStates.computeIfAbsent(peer.getId(), k -> new PeerRateLimitState(permitsPerSecond));
    state.totalRequests.incrementAndGet();
    if (state.limiter.tryAcquire()) {
      return RateLimitResult.ALLOWED;
    }
    final long rejected = state.rejectedRequests.incrementAndGet();
    final long total = state.totalRequests.get();
    final long now = System.nanoTime();
    final long lastLogNanos = state.lastLogNanos.get();
    // throttle logging to at most once per 10 seconds per peer
    final boolean shouldLog =
        (now - lastLogNanos) >= 10_000_000_000L
            && state.lastLogNanos.compareAndSet(lastLogNanos, now);
    return new RateLimitResult(false, shouldLog, total, rejected, permitsPerSecond);
  }

  public void removePeer(final Bytes peerId) {
    peerStates.remove(peerId);
  }

  @VisibleForTesting
  int trackedPeerCount() {
    return peerStates.size();
  }

  private static class PeerRateLimitState {
    final RateLimiter limiter;
    final AtomicLong totalRequests = new AtomicLong();
    final AtomicLong rejectedRequests = new AtomicLong();
    final AtomicLong lastLogNanos = new AtomicLong();

    PeerRateLimitState(final double permitsPerSecond) {
      this.limiter = RateLimiter.create(permitsPerSecond);
    }
  }

  /** Result of a rate limit check. */
  public static class RateLimitResult {
    static final RateLimitResult ALLOWED = new RateLimitResult(true, false, 0, 0, 0);

    private final boolean allowed;
    private final boolean shouldLog;
    private final long totalRequests;
    private final long rejectedRequests;
    private final double configuredRate;

    RateLimitResult(
        final boolean allowed,
        final boolean shouldLog,
        final long totalRequests,
        final long rejectedRequests,
        final double configuredRate) {
      this.allowed = allowed;
      this.shouldLog = shouldLog;
      this.totalRequests = totalRequests;
      this.rejectedRequests = rejectedRequests;
      this.configuredRate = configuredRate;
    }

    public boolean isAllowed() {
      return allowed;
    }

    public boolean shouldLog() {
      return shouldLog;
    }

    public long getTotalRequests() {
      return totalRequests;
    }

    public long getRejectedRequests() {
      return rejectedRequests;
    }

    public double getConfiguredRate() {
      return configuredRate;
    }
  }
}
