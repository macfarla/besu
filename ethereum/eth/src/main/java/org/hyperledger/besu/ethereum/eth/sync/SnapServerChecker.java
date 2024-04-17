/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.sync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.snap.GetAccountRangeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.task.AbstractPeerTask;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.plugin.services.BesuEvents;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapServerChecker implements BesuEvents.InitialSyncCompletionListener {

  private static final Logger LOG = LoggerFactory.getLogger(SnapServerChecker.class);
  private final SyncMode syncMode;
  private final EthContext ethContext;

  private final ChainHeadTracker tracker;
  private final MetricsSystem metricsSystem;
  private long subscriptionId;

  public SnapServerChecker(
      final SynchronizerConfiguration syncConfig,
      final EthContext ethContext,
      final ChainHeadTracker tracker,
      final MetricsSystem metricsSystem) {
    this.ethContext = ethContext;
    this.tracker = tracker;
    this.metricsSystem = metricsSystem;
    this.syncMode = syncConfig.getSyncMode();
    // TODO remove syncMode check since this class is only instantiated if this is already true
    if (syncMode == SyncMode.X_CHECKPOINT
        || syncMode == SyncMode.X_SNAP
        || syncMode == SyncMode.CHECKPOINT
        || syncMode == SyncMode.X_SNAP) {
      LOG.info("subscribing to peer connect events with snap server check");
      subscriptionId =
          ethContext
              .getEthPeers()
              .subscribeConnect(this::checkBestHeaderAndSetWhetherPeerIsServingSnap);
    }
  }

  public void checkBestHeaderAndSetWhetherPeerIsServingSnap(final EthPeer peer) {

    assert tracker != null : "ChainHeadTracker must be set before SnapServerChecker can be used";
    CompletableFuture<BlockHeader> future = tracker.getBestHeaderFromPeer(peer);

    future.whenComplete(
        (peerHeadBlockHeader, error) -> {
          if (peerHeadBlockHeader == null) {
            LOG.debug(
                "Failed to retrieve chain head info. Disconnecting {}... {}",
                peer.getLoggableId(),
                error);
            peer.disconnect(
                DisconnectMessage.DisconnectReason.USELESS_PEER_FAILED_TO_RETRIEVE_CHAIN_STATE);
          } else {
            peer.chainState().updateHeightEstimate(peerHeadBlockHeader.getNumber());
            try {
              sendSnapRequestToDetermineIfPeerIsServingSnap(peer, peerHeadBlockHeader);
            } catch (Exception e) {
              LOG.debug(
                  "Failed to retrieve chain head info. Disconnecting {}... {}",
                  peer.getLoggableId(),
                  e);
            }
          }
        });
  }

  private void sendSnapRequestToDetermineIfPeerIsServingSnap(
      final EthPeer peer, final BlockHeader peersHeadBlockHeader) {
    if (peer.getAgreedCapabilities().contains(SnapProtocol.SNAP1)) {
      Boolean isServer;
      // we don't know yet if they are serving snap but for the check we have to pass true otherwise
      // exceptions will be thrown
      peer.setIsServingSnap(true);
      try {
        isServer = checkSnapResult(peer, peersHeadBlockHeader).get(10L, TimeUnit.SECONDS);
      } catch (Exception e) {
        // TODO: change LOG to debug?
        LOG.info("XXXXXX Error checking if peer is a snap server.", e);
        peer.setIsServingSnap(false);
        return;
      }
      peer.setIsServingSnap(isServer);
      LOG.info("Peer {} snap server? {}", peer.getLoggableId(), isServer);

      // TODO: remove the following code. Just here for testing
      final boolean simpleCheck = peer.getConnection().getPeerInfo().getClientId().contains("Geth");
      if (simpleCheck && !isServer) {
        LOG.info(
            "YYYYYYYYYY Found a peer {} that is Geth but not a snap server: {}",
            peer.getConnection().getPeerInfo().getClientId(),
            peer.getLoggableId());
      } else if (!simpleCheck && isServer) {
        LOG.info(
            "ZZZZZZZZZZ Found a peer {} that is NOT Geth but is a snap server: {}",
            peer.getConnection().getPeerInfo().getClientId(),
            peer.getLoggableId());
      }
    }
  }

  private CompletableFuture<Boolean> checkSnapResult(final EthPeer peer, final BlockHeader header) {
    LOG.atDebug()
        .setMessage("Checking whether peer {} is a snap server ...")
        .addArgument(peer::getLoggableId)
        .log();
    final CompletableFuture<AbstractPeerTask.PeerTaskResult<AccountRangeMessage.AccountRangeData>>
        snapServerCheckCompletableFuture = getAccountRangeFromPeer(peer, header);
    final CompletableFuture<Boolean> future = new CompletableFuture<>();
    snapServerCheckCompletableFuture.whenComplete(
        (peerResult, error) -> {
          if (peerResult != null) {
            if (!peerResult.getResult().accounts().isEmpty()
                || !peerResult.getResult().proofs().isEmpty()) {
              LOG.atInfo()
                  .setMessage("Peer {} is a snap server! getAccountRangeResult: {}")
                  .addArgument(peer::getLoggableId)
                  .addArgument(peerResult.getResult())
                  .log();
              future.complete(true);
            } else {
              LOG.atDebug()
                  .setMessage("Peer {} is not a snap server ...")
                  .addArgument(peer::getLoggableId)
                  .log();
              future.complete(false);
            }
          }
        });
    return future;
  }

  public CompletableFuture<AbstractPeerTask.PeerTaskResult<AccountRangeMessage.AccountRangeData>>
      getAccountRangeFromPeer(final EthPeer peer, final BlockHeader header) {
    return GetAccountRangeFromPeerTask.forAccountRange(
            ethContext, Hash.ZERO, Hash.ZERO, header, metricsSystem)
        .assignPeer(peer)
        .run();
  }

  @Override
  public void onInitialSyncCompleted() {
    // set comparator back to one that disregards snap
    ethContext.getEthPeers().setBestChainComparator(EthPeers.MOST_USEFUL_PEER);
    // stop checking whether peers serve snap data
    ethContext.getEthPeers().unsubscribeConnect(subscriptionId);
  }

  @Override
  public void onInitialSyncRestart() {
    ethContext.getEthPeers().setBestChainComparator(EthPeers.MOST_USEFUL_PEER_SERVING_SNAP);
    // TODO make sure not already subscribed
    subscriptionId =
        ethContext
            .getEthPeers()
            .subscribeConnect(this::checkBestHeaderAndSetWhetherPeerIsServingSnap);
  }
}
