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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.tuweni.bytes.Bytes;

@SuppressWarnings("UnstableApiUsage")
public class SnapRequestRateLimiter {

  private final boolean enabled;
  private final double permitsPerSecond;
  private final ConcurrentHashMap<Bytes, RateLimiter> peerRateLimiters;

  public SnapRequestRateLimiter(final boolean enabled, final double permitsPerSecond) {
    this.enabled = enabled;
    this.permitsPerSecond = permitsPerSecond;
    this.peerRateLimiters = new ConcurrentHashMap<>();
  }

  public boolean tryAcquire(final EthPeer peer) {
    if (!enabled) {
      return true;
    }
    final RateLimiter limiter =
        peerRateLimiters.computeIfAbsent(peer.getId(), k -> RateLimiter.create(permitsPerSecond));
    return limiter.tryAcquire();
  }

  public void removePeer(final Bytes peerId) {
    peerRateLimiters.remove(peerId);
  }

  @VisibleForTesting
  int trackedPeerCount() {
    return peerRateLimiters.size();
  }
}
