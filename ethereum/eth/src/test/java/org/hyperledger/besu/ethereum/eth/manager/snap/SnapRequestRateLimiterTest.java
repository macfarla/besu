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
  void disabledRateLimiterAlwaysAllows() {
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(false, 1.0);
    final EthPeer peer = mockPeer(PEER_ID_1);

    for (int i = 0; i < 1000; i++) {
      assertThat(limiter.tryAcquire(peer)).isTrue();
    }
  }

  @Test
  void enabledRateLimiterBlocksBursts() {
    // 1 permit per second — first request should succeed, rapid subsequent ones should fail
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(true, 1.0);
    final EthPeer peer = mockPeer(PEER_ID_1);

    // First call always succeeds (initial permit)
    assertThat(limiter.tryAcquire(peer)).isTrue();

    // Rapid subsequent calls should be rate limited
    boolean anyRejected = false;
    for (int i = 0; i < 10; i++) {
      if (!limiter.tryAcquire(peer)) {
        anyRejected = true;
        break;
      }
    }
    assertThat(anyRejected).isTrue();
  }

  @Test
  void peersAreLimitedIndependently() {
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(true, 1.0);
    final EthPeer peer1 = mockPeer(PEER_ID_1);
    final EthPeer peer2 = mockPeer(PEER_ID_2);

    // Exhaust peer1's permit
    assertThat(limiter.tryAcquire(peer1)).isTrue();

    // peer2 should still get its first permit
    assertThat(limiter.tryAcquire(peer2)).isTrue();
  }

  @Test
  void removePeerCleansUpState() {
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(true, 1.0);
    final EthPeer peer = mockPeer(PEER_ID_1);

    limiter.tryAcquire(peer);
    assertThat(limiter.trackedPeerCount()).isEqualTo(1);

    limiter.removePeer(PEER_ID_1);
    assertThat(limiter.trackedPeerCount()).isEqualTo(0);
  }

  @Test
  void removePeerForUnknownPeerIsNoOp() {
    final SnapRequestRateLimiter limiter = new SnapRequestRateLimiter(true, 1.0);
    limiter.removePeer(PEER_ID_1);
    assertThat(limiter.trackedPeerCount()).isEqualTo(0);
  }
}
