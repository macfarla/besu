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
package org.hyperledger.besu.ethereum.privacy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.transaction.CallParameter;
import org.hyperledger.besu.ethereum.transaction.ImmutableCallParameter;
import org.hyperledger.besu.evm.log.Log;

import java.util.ArrayList;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MultiTenancyPrivacyControllerOnchainTest {

  private static final String ENCLAVE_PUBLIC_KEY1 = "Ko2bVqD+nNlNYL5EE7y3IdOnviftjiizpjRt+HTuFBs=";
  private static final String ENCLAVE_PUBLIC_KEY2 = "OnviftjiizpjRt+HTuFBsKo2bVqD+nNlNYL5EE7y3Id=";
  private static final String PRIVACY_GROUP_ID = "nNlNYL5EE7y3IdM=";
  private static final ArrayList<Log> LOGS = new ArrayList<>();

  private final PrivacyController privacyController = mock(PrivacyController.class);

  private MultiTenancyPrivacyController multiTenancyPrivacyController;

  @BeforeEach
  public void setup() {
    multiTenancyPrivacyController = new MultiTenancyPrivacyController(privacyController);
  }

  @Test
  public void simulatePrivateTransactionSucceedsForPresentEnclaveKey() {
    when(privacyController.simulatePrivateTransaction(any(), any(), any(), any(long.class)))
        .thenReturn(
            Optional.of(
                TransactionProcessingResult.successful(
                    LOGS, 0, 0, Bytes.EMPTY, ValidationResult.valid())));
    final Optional<TransactionProcessingResult> result =
        multiTenancyPrivacyController.simulatePrivateTransaction(
            PRIVACY_GROUP_ID, ENCLAVE_PUBLIC_KEY1, ImmutableCallParameter.builder().build(), 1);

    assertThat(result.isPresent()).isTrue();
    assertThat(result.get().getValidationResult().isValid()).isTrue();
  }

  @Test
  public void simulatePrivateTransactionFailsForAbsentEnclaveKey() {
    doThrow(
            new MultiTenancyValidationException(
                "Privacy group must contain the enclave public key"))
        .when(privacyController)
        .verifyPrivacyGroupContainsPrivacyUserId(
            PRIVACY_GROUP_ID, ENCLAVE_PUBLIC_KEY2, Optional.of(1L));
    final CallParameter callParams = ImmutableCallParameter.builder().build();
    assertThatThrownBy(
            () ->
                multiTenancyPrivacyController.simulatePrivateTransaction(
                    PRIVACY_GROUP_ID, ENCLAVE_PUBLIC_KEY2, callParams, 1))
        .isInstanceOf(MultiTenancyValidationException.class);
  }
}
