/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.mainnet.feemarket;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ZeroBlobFeeMarketTest {

  private static final KeyPair KEY_PAIR1 =
      SignatureAlgorithmFactory.getInstance().generateKeyPair();
  private static final long FORK_BLOCK = 0;
  private ZeroBlobFeeMarket zeroBlobFeeMarket;

  @BeforeEach
  public void setUp() throws Exception {
    zeroBlobFeeMarket = new ZeroBlobFeeMarket(FORK_BLOCK);
  }

  @Test
  public void getBasefeeMaxChangeDenominatorShouldUseLondonDefault() {
    assertThat(zeroBlobFeeMarket.getBasefeeMaxChangeDenominator())
        .isEqualTo(LondonFeeMarket.DEFAULT_BASEFEE_MAX_CHANGE_DENOMINATOR);
  }

  @Test
  public void getInitialBasefeeShouldBeZero() {
    assertThat(zeroBlobFeeMarket.getInitialBasefee()).isEqualTo(Wei.ZERO);
  }

  @Test
  public void getSlackCoefficientShouldUseLondonDefault() {
    assertThat(zeroBlobFeeMarket.getSlackCoefficient())
        .isEqualTo(LondonFeeMarket.DEFAULT_SLACK_COEFFICIENT);
  }

  @Test
  public void getTransactionPriceCalculatorShouldBeEIP1559() {
    // only eip1559 will read the fee per gas values
    final Transaction transaction =
        new TransactionTestFixture()
            .type(TransactionType.EIP1559)
            .maxFeePerGas(Optional.of(Wei.of(8)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(8)))
            .gasPrice(null)
            .createTransaction(KEY_PAIR1);

    assertThat(
            zeroBlobFeeMarket
                .getTransactionPriceCalculator()
                .price(transaction, Optional.of(Wei.ZERO)))
        .isEqualTo(Wei.of(8));
  }

  @Test
  public void satisfiesFloorTxCostWhenGasFeeIsNonZero() {
    final Transaction transaction =
        new TransactionTestFixture()
            .type(TransactionType.FRONTIER)
            .gasPrice(Wei.of(7))
            .createTransaction(KEY_PAIR1);
    assertThat(zeroBlobFeeMarket.satisfiesFloorTxFee(transaction)).isTrue();
  }

  @Test
  public void satisfiesFloorTxCostWhenGasFeeIsZero() {
    final Transaction transaction =
        new TransactionTestFixture()
            .type(TransactionType.EIP1559)
            .maxFeePerGas(Optional.of(Wei.ZERO))
            .maxPriorityFeePerGas(Optional.of(Wei.ZERO))
            .gasPrice(null)
            .createTransaction(KEY_PAIR1);
    assertThat(zeroBlobFeeMarket.satisfiesFloorTxFee(transaction)).isTrue();
  }

  @Test
  public void computeBaseFeeReturnsZero() {
    assertThat(zeroBlobFeeMarket.computeBaseFee(1L, Wei.of(8), 1L, 2L)).isEqualTo(Wei.ZERO);
  }

  @Test
  public void baseFeeValidationModeShouldBeNoneWhenIsForkBlock() {
    assertThat(zeroBlobFeeMarket.baseFeeValidationMode(FORK_BLOCK))
        .isEqualTo(BaseFeeMarket.ValidationMode.NONE);
  }

  @Test
  public void baseFeeValidationModeShouldBeNoneWhenIsNotForkBlock() {
    assertThat(zeroBlobFeeMarket.baseFeeValidationMode(FORK_BLOCK + 1))
        .isEqualTo(BaseFeeMarket.ValidationMode.NONE);
  }

  @Test
  public void gasLimitValidationModeShouldBeInitialWhenIsForkBlock() {
    assertThat(zeroBlobFeeMarket.gasLimitValidationMode(FORK_BLOCK))
        .isEqualTo(BaseFeeMarket.ValidationMode.INITIAL);
  }

  @Test
  public void gasLimitValidationModeShouldBeOngoingWhenIsNotForkBlock() {
    assertThat(zeroBlobFeeMarket.gasLimitValidationMode(FORK_BLOCK + 1))
        .isEqualTo(BaseFeeMarket.ValidationMode.ONGOING);
  }

  @Test
  public void isBeforeForkBlockShouldBeTrue() {
    final ZeroBlobFeeMarket zeroBlobFeeMarket = new ZeroBlobFeeMarket(10);
    assertThat(zeroBlobFeeMarket.isBeforeForkBlock(9)).isTrue();
  }

  @Test
  public void isBeforeForkBlockShouldBeFalse() {
    final ZeroBlobFeeMarket zeroBlobFeeMarket = new ZeroBlobFeeMarket(10);
    assertThat(zeroBlobFeeMarket.isBeforeForkBlock(10)).isFalse();
    assertThat(zeroBlobFeeMarket.isBeforeForkBlock(11)).isFalse();
  }

  @Test
  public void implementsBlobFeeShouldReturnTrue() {
    assertThat(zeroBlobFeeMarket.implementsBlobFee()).isTrue();
  }

  @Test
  public void blobGasPricePerGasShouldReturnZero() {
    assertThat(zeroBlobFeeMarket.blobGasPricePerGas(BlobGas.ONE)).isEqualTo(Wei.ZERO);
  }
}
