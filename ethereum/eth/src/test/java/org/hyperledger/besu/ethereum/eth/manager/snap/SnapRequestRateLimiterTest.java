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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.eth.manager.EthPeer;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class SnapRequestRateLimiterTest {

  private static final Bytes PEER_ID_1 = Bytes.fromHexString("0x01");
  private static final Bytes PEER_ID_2 = Bytes.fromHexString("0x02");

  private EthPeer mockPeer(final Bytes id) {
    final EthPeer peer = mock(EthPeer.class);
    when(peer.getId()).thenReturn(id);
    return peer;
  }

  @Test
  void disabledLimiterAlwaysAllows() {
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(0);
    final EthPeer peer = mockPeer(PEER_ID_1);

    for (int i = 0; i < 100; i++) {
      assertThat(limiter.tryAcquire(peer)).isTrue();
    }
  }

  @Test
  void acquireUpToMaxConcurrentThenRejects() {
    final int maxConcurrent = 3;
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(maxConcurrent);
    final EthPeer peer = mockPeer(PEER_ID_1);

    // Acquire all permits
    for (int i = 0; i < maxConcurrent; i++) {
      assertThat(limiter.tryAcquire(peer)).isTrue();
    }

    // Next acquire should fail
    assertThat(limiter.tryAcquire(peer)).isFalse();
  }

  @Test
  void releaseAllowsNewAcquire() {
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(1);
    final EthPeer peer = mockPeer(PEER_ID_1);

    assertThat(limiter.tryAcquire(peer)).isTrue();
    assertThat(limiter.tryAcquire(peer)).isFalse();

    limiter.release(peer);
    assertThat(limiter.tryAcquire(peer)).isTrue();
  }

  @Test
  void peersAreLimitedIndependently() {
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(1);
    final EthPeer peer1 = mockPeer(PEER_ID_1);
    final EthPeer peer2 = mockPeer(PEER_ID_2);

    // Exhaust peer1's permit
    assertThat(limiter.tryAcquire(peer1)).isTrue();
    assertThat(limiter.tryAcquire(peer1)).isFalse();

    // peer2 should still get its permit
    assertThat(limiter.tryAcquire(peer2)).isTrue();
  }

  @Test
  void removePeerCleansUpState() {
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(2);
    final EthPeer peer = mockPeer(PEER_ID_1);

    limiter.tryAcquire(peer);
    assertThat(limiter.trackedPeerCount()).isEqualTo(1);

    limiter.removePeer(PEER_ID_1);
    assertThat(limiter.trackedPeerCount()).isEqualTo(0);
  }

  @Test
  void removePeerForUnknownPeerIsNoOp() {
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(2);
    limiter.removePeer(PEER_ID_1);
    assertThat(limiter.trackedPeerCount()).isEqualTo(0);
  }

  @Test
  void releaseWithoutAcquireOnDisabledLimiterIsNoOp() {
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(0);
    final EthPeer peer = mockPeer(PEER_ID_1);
    // Should not throw
    limiter.release(peer);
  }

  @Test
  void releaseForUnknownPeerIsNoOp() {
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(2);
    final EthPeer peer = mockPeer(PEER_ID_1);
    // Should not throw
    limiter.release(peer);
  }

  @Test
  void firstRejectionShouldLog() {
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(1);
    final EthPeer peer = mockPeer(PEER_ID_1);

    // Exhaust permit
    limiter.tryAcquire(peer);

    // Rejection attempt
    limiter.tryAcquire(peer);

    // First rejection log check should return true
    assertThat(limiter.shouldLogRejection(peer)).isTrue();
  }

  @Test
  void getMaxConcurrentRequestsPerPeerReturnsConfiguredValue() {
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(10);
    assertThat(limiter.getMaxConcurrentRequestsPerPeer()).isEqualTo(10);
  }
}
