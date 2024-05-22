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
package org.hyperledger.besu.tests.acceptance.pubsub;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.account.Account;
import org.hyperledger.besu.tests.acceptance.dsl.node.BesuNode;
import org.hyperledger.besu.tests.acceptance.dsl.pubsub.Subscription;
import org.hyperledger.besu.tests.acceptance.dsl.pubsub.WebSocket;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

public class NewPendingTransactionAcceptanceTest extends AcceptanceTestBase {

  private Vertx vertx;
  private Account accountOne;
  private WebSocket minerWebSocket;
  private WebSocket archiveWebSocket;
  private BesuNode minerNode;
  private BesuNode archiveNode;

  @BeforeEach
  public void setUp() throws Exception {
    vertx = Vertx.vertx();
    minerNode = besu.createMinerNode("miner-node1");
    archiveNode = besu.createArchiveNode("full-node1");
    cluster.start(minerNode, archiveNode);

    // verify nodes are fully connected otherwise tx could not be propagated
    minerNode.verify(net.awaitPeerCount(1));
    archiveNode.verify(net.awaitPeerCount(1));

    accountOne = accounts.createAccount("account-one");
    minerWebSocket = new WebSocket(vertx, minerNode.getConfiguration());
    archiveWebSocket = new WebSocket(vertx, archiveNode.getConfiguration());

    // verify that the miner started producing blocks and all other nodes are syncing from it
    waitForBlockHeight(minerNode, 1);
    final var minerChainHead = minerNode.execute(ethTransactions.block());
    archiveNode.verify(blockchain.minimumHeight(minerChainHead.getNumber().longValue()));
  }

  @AfterEach
  public void tearDown() {
    vertx.close();
  }

  @Test
  public void subscriptionToMinerNodeMustReceivePublishEvent() {
    System.out.println("Starting test: subscriptionToMinerNodeMustReceivePublishEvent");

    try {
        System.out.println("Subscribing to miner node WebSocket");
        final Subscription minerSubscription = minerWebSocket.subscribe();
        System.out.println("Subscribed to miner node WebSocket");

        BigInteger initialBalance = minerNode.execute(ethTransactions.getBalance(accountOne));
        System.out.println("Initial Balance: " + initialBalance);

        final Hash event = minerNode.execute(accountTransactions.createTransfer(accountOne, 4));
        System.out.println("Event created: " + event);
        cluster.verify(accountOne.balanceEquals(4));

        minerWebSocket.verifyTotalEventsReceived(1);
        minerSubscription.verifyEventReceived(event);

        BigInteger finalBalance = minerNode.execute(ethTransactions.getBalance(accountOne));
        System.out.println("Final Balance: " + finalBalance);

        System.out.println("Unsubscribing from miner node WebSocket");
        minerWebSocket.unsubscribe(minerSubscription);
        System.out.println("Unsubscribed from miner node WebSocket");
    } catch (Exception e) {
        System.out.println("Exception occurred: " + e.getMessage());
        e.printStackTrace();
    }

    System.out.println("Ending test: subscriptionToMinerNodeMustReceivePublishEvent");
  }

  @Test
  public void subscriptionToArchiveNodeMustReceivePublishEvent() {
    System.out.println("Starting test: subscriptionToArchiveNodeMustReceivePublishEvent");

    try {
        System.out.println("Subscribing to archive node WebSocket");
        final Subscription archiveSubscription = archiveWebSocket.subscribe();
        System.out.println("Subscribed to archive node WebSocket");

        BigInteger initialBalance = minerNode.execute(ethTransactions.getBalance(accountOne));
        System.out.println("Initial Balance: " + initialBalance);

        final Hash event = minerNode.execute(accountTransactions.createTransfer(accountOne, 23));
        System.out.println("Event created: " + event);
        cluster.verify(accountOne.balanceEquals(23));

        archiveWebSocket.verifyTotalEventsReceived(1);
        archiveSubscription.verifyEventReceived(event);

        BigInteger finalBalance = minerNode.execute(ethTransactions.getBalance(accountOne));
        System.out.println("Final Balance: " + finalBalance);

        System.out.println("Unsubscribing from archive node WebSocket");
        archiveWebSocket.unsubscribe(archiveSubscription);
        System.out.println("Unsubscribed from archive node WebSocket");
    } catch (Exception e) {
        System.out.println("Exception occurred: " + e.getMessage());
        e.printStackTrace();
    }

    System.out.println("Ending test: subscriptionToArchiveNodeMustReceivePublishEvent");
  }

  @Test
  public void everySubscriptionMustReceivePublishEvent() {
    System.out.println("Starting test: everySubscriptionMustReceivePublishEvent");

    try {
        System.out.println("Subscribing to miner and archive nodes WebSocket");
        final Subscription minerSubscriptionOne = minerWebSocket.subscribe();
        final Subscription minerSubscriptionTwo = minerWebSocket.subscribe();
        final Subscription archiveSubscriptionOne = archiveWebSocket.subscribe();
        final Subscription archiveSubscriptionTwo = archiveWebSocket.subscribe();
        final Subscription archiveSubscriptionThree = archiveWebSocket.subscribe();
        System.out.println("Subscribed to miner and archive nodes WebSocket");

        BigInteger initialBalance = minerNode.execute(ethTransactions.getBalance(accountOne));
        System.out.println("Initial Balance: " + initialBalance);

        final Hash event = minerNode.execute(accountTransactions.createTransfer(accountOne, 30));
        System.out.println("Event created: " + event);
        cluster.verify(accountOne.balanceEquals(30));

        minerWebSocket.verifyTotalEventsReceived(2);
        minerSubscriptionOne.verifyEventReceived(event);
        minerSubscriptionTwo.verifyEventReceived(event);

        archiveWebSocket.verifyTotalEventsReceived(3);
        archiveSubscriptionOne.verifyEventReceived(event);
        archiveSubscriptionTwo.verifyEventReceived(event);
        archiveSubscriptionThree.verifyEventReceived(event);

        BigInteger finalBalance = minerNode.execute(ethTransactions.getBalance(accountOne));
        System.out.println("Final Balance: " + finalBalance);

        System.out.println("Unsubscribing from miner and archive nodes WebSocket");
        minerWebSocket.unsubscribe(minerSubscriptionOne);
        minerWebSocket.unsubscribe(minerSubscriptionTwo);
        archiveWebSocket.unsubscribe(archiveSubscriptionOne);
        archiveWebSocket.unsubscribe(archiveSubscriptionTwo);
        archiveWebSocket.unsubscribe(archiveSubscriptionThree);
        System.out.println("Unsubscribed from miner and archive nodes WebSocket");
    } catch (Exception e) {
        System.out.println("Exception occurred: " + e.getMessage());
        e.printStackTrace();
    }

    System.out.println("Ending test: everySubscriptionMustReceivePublishEvent");
  }

  @Test
  public void subscriptionToMinerNodeMustReceiveEveryPublishEvent() {
    System.out.println("Starting test: subscriptionToMinerNodeMustReceiveEveryPublishEvent");

    try {
        final Subscription minerSubscription = minerWebSocket.subscribe();
        System.out.println("Subscribed to miner node");

        BigInteger initialBalance = minerNode.execute(ethTransactions.getBalance(accountOne));
        System.out.println("Initial Balance: " + initialBalance);

        final Hash eventOne = minerNode.execute(accountTransactions.createTransfer(accountOne, 1));
        System.out.println("Event one created: " + eventOne);
        cluster.verify(accountOne.balanceEquals(1));

        minerWebSocket.verifyTotalEventsReceived(1);
        minerSubscription.verifyEventReceived(eventOne);

        final Hash eventTwo = minerNode.execute(accountTransactions.createTransfer(accountOne, 4));
        System.out.println("Event two created: " + eventTwo);
        final Hash eventThree = minerNode.execute(accountTransactions.createTransfer(accountOne, 5));
        System.out.println("Event three created: " + eventThree);
        cluster.verify(accountOne.balanceEquals(1 + 4 + 5));

        minerWebSocket.verifyTotalEventsReceived(3);
        minerSubscription.verifyEventReceived(eventTwo);
        minerSubscription.verifyEventReceived(eventThree);

        BigInteger finalBalance = minerNode.execute(ethTransactions.getBalance(accountOne));
        System.out.println("Final Balance: " + finalBalance);

        // Log the current block number
        final var currentBlock = minerNode.execute(ethTransactions.block());
        System.out.println("Current Block Number: " + currentBlock.getNumber());

        // Log the transaction receipt for event one
        final var receiptOne = minerNode.execute(ethTransactions.getTransactionReceipt(eventOne.toString()));
        System.out.println("Transaction Receipt for Event One: " + receiptOne);

        // Log the transaction receipt for event two
        final var receiptTwo = minerNode.execute(ethTransactions.getTransactionReceipt(eventTwo.toString()));
        System.out.println("Transaction Receipt for Event Two: " + receiptTwo);

        // Log the transaction receipt for event three
        final var receiptThree = minerNode.execute(ethTransactions.getTransactionReceipt(eventThree.toString()));
        System.out.println("Transaction Receipt for Event Three: " + receiptThree);

        minerWebSocket.unsubscribe(minerSubscription);
        System.out.println("Unsubscribed from miner node");
    } catch (Exception e) {
        System.out.println("Exception occurred: " + e.getMessage());
        e.printStackTrace();
    }

    System.out.println("Ending test: subscriptionToMinerNodeMustReceiveEveryPublishEvent");
  }

  @Test
  public void subscriptionToArchiveNodeMustReceiveEveryPublishEvent() {
    System.out.println("Starting test: subscriptionToArchiveNodeMustReceiveEveryPublishEvent");

    try {
        System.out.println("Subscribing to archive node WebSocket");
        final Subscription archiveSubscription = archiveWebSocket.subscribe();
        System.out.println("Subscribed to archive node WebSocket");

        BigInteger initialBalance = minerNode.execute(ethTransactions.getBalance(accountOne));
        System.out.println("Initial Balance: " + initialBalance);

        final Hash eventOne = minerNode.execute(accountTransactions.createTransfer(accountOne, 2));
        System.out.println("Event one created: " + eventOne);
        final Hash eventTwo = minerNode.execute(accountTransactions.createTransfer(accountOne, 5));
        System.out.println("Event two created: " + eventTwo);
        cluster.verify(accountOne.balanceEquals(2 + 5));

        archiveWebSocket.verifyTotalEventsReceived(2);
        archiveSubscription.verifyEventReceived(eventOne);
        archiveSubscription.verifyEventReceived(eventTwo);

        final Hash eventThree = minerNode.execute(accountTransactions.createTransfer(accountOne, 8));
        System.out.println("Event three created: " + eventThree);
        cluster.verify(accountOne.balanceEquals(2 + 5 + 8));

        archiveWebSocket.verifyTotalEventsReceived(3);
        archiveSubscription.verifyEventReceived(eventThree);

        BigInteger finalBalance = minerNode.execute(ethTransactions.getBalance(accountOne));
        System.out.println("Final Balance: " + finalBalance);

        System.out.println("Unsubscribing from archive node WebSocket");
        archiveWebSocket.unsubscribe(archiveSubscription);
        System.out.println("Unsubscribed from archive node WebSocket");
    } catch (Exception e) {
        System.out.println("Exception occurred: " + e.getMessage());
        e.printStackTrace();
    }

    System.out.println("Ending test: subscriptionToArchiveNodeMustReceiveEveryPublishEvent");
  }

  @Test
  public void everySubscriptionMustReceiveEveryPublishEvent() {
    System.out.println("Starting test: everySubscriptionMustReceiveEveryPublishEvent");

    try {
        final Subscription minerSubscriptionOne = minerWebSocket.subscribe();
        final Subscription minerSubscriptionTwo = minerWebSocket.subscribe();
        final Subscription archiveSubscriptionOne = archiveWebSocket.subscribe();
        final Subscription archiveSubscriptionTwo = archiveWebSocket.subscribe();
        final Subscription archiveSubscriptionThree = archiveWebSocket.subscribe();
        System.out.println("Subscribed to miner and archive nodes");

        final Hash eventOne = minerNode.execute(accountTransactions.createTransfer(accountOne, 10));
        System.out.println("Event one created: " + eventOne);
        final Hash eventTwo = minerNode.execute(accountTransactions.createTransfer(accountOne, 5));
        System.out.println("Event two created: " + eventTwo);
        cluster.verify(accountOne.balanceEquals(10 + 5));

        minerWebSocket.verifyTotalEventsReceived(4);
        minerSubscriptionOne.verifyEventReceived(eventOne);
        minerSubscriptionOne.verifyEventReceived(eventTwo);
        minerSubscriptionTwo.verifyEventReceived(eventOne);
        minerSubscriptionTwo.verifyEventReceived(eventTwo);

        archiveWebSocket.verifyTotalEventsReceived(6);
        archiveSubscriptionOne.verifyEventReceived(eventOne);
        archiveSubscriptionOne.verifyEventReceived(eventTwo);
        archiveSubscriptionTwo.verifyEventReceived(eventOne);
        archiveSubscriptionTwo.verifyEventReceived(eventTwo);
        archiveSubscriptionThree.verifyEventReceived(eventOne);
        archiveSubscriptionThree.verifyEventReceived(eventTwo);

        minerWebSocket.unsubscribe(minerSubscriptionOne);
        minerWebSocket.unsubscribe(minerSubscriptionTwo);
        archiveWebSocket.unsubscribe(archiveSubscriptionOne);
        archiveWebSocket.unsubscribe(archiveSubscriptionTwo);
        archiveWebSocket.unsubscribe(archiveSubscriptionThree);

        System.out.println("Unsubscribed from miner and archive nodes");
    } catch (Exception e) {
        System.out.println("Exception occurred: " + e.getMessage());
        e.printStackTrace();
    }

    System.out.println("Ending test: everySubscriptionMustReceiveEveryPublishEvent");
  }
}
