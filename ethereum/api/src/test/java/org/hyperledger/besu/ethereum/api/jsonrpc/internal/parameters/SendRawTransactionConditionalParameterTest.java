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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class SendRawTransactionConditionalParameterTest {

  public static final String METHOD_NAME = "eth_XsendRawTransactionConditional";

  @Test
  public void maxBlockNumber_shouldSerializeSuccessfully() throws JsonProcessingException {
    final SendRawTransactionConditionalParameter expectedParam =
        parameterWithBlockNumberConditions(90L, 98L);

    final String jsonWithBlockConditions =
        "{\"jsonrpc\":\"2.0\",\"method\":\""
            + METHOD_NAME
            + "\",\"params\":[\"0x00\",{\"blockNumberMin\":\"90\",\"blockNumberMax\":\"98\"}],\"id\":1}";
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new ObjectMapper().readValue(jsonWithBlockConditions, JsonRpcRequest.class));

    final SendRawTransactionConditionalParameter parsedParam =
        request.getRequiredParameter(1, SendRawTransactionConditionalParameter.class);

    assertThat(parsedParam).usingRecursiveComparison().isEqualTo(expectedParam);
  }

  @Test
  public void allConditions_shouldSerializeSuccessfully() throws JsonProcessingException {
    final Map<Address, Hash> knownAccounts = new HashMap<>();
    knownAccounts.put(
        Address.fromHexString("0x000000000000000000000000000000000000abcd"),
        Hash.fromHexString("0x000000000000000000000000000000000000000000000000000000000000beef"));
    final SendRawTransactionConditionalParameter expectedParam =
        parameterWithConditions(90L, 98L, knownAccounts, 7337L, 7447L);

    final String jsonWithBlockConditions =
        "{\"jsonrpc\":\"2.0\",\"method\":\""
            + METHOD_NAME
            + "\",\"params\":[\"0x00\",{\"blockNumberMin\":\"90\",\"blockNumberMax\":\"98\",\"knownAccounts\": {\"0x000000000000000000000000000000000000abcd\": \"0x000000000000000000000000000000000000000000000000000000000000beef\"}, \"timestampMin\":\"7337\",\"timestampMax\":\"7447\"}],\"id\":1}";
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new ObjectMapper().readValue(jsonWithBlockConditions, JsonRpcRequest.class));

    final SendRawTransactionConditionalParameter parsedParam =
        request.getRequiredParameter(1, SendRawTransactionConditionalParameter.class);

    assertThat(parsedParam).usingRecursiveComparison().isEqualTo(expectedParam);
  }

  @Test
  public void noConditionsParamDecodesCorrectly() {
    final SendRawTransactionConditionalParameter param = parameterWithNoConditions();
    assertThat(param.getTimestampMin()).isEmpty();
    assertThat(param.getTimestampMax()).isEmpty();
    assertThat(param.getBlockNumberMin()).isEmpty();
    assertThat(param.getBlockNumberMax()).isEmpty();
    assertThat(param.getKnownAccounts()).isEmpty();
  }

  @Test
  public void blockNumberConditionParamDecodesCorrectly() {
    final SendRawTransactionConditionalParameter param =
        parameterWithBlockNumberConditions(5L, 15L);
    assertThat(param.getTimestampMin()).isEmpty();
    assertThat(param.getTimestampMax()).isEmpty();
    assertThat(param.getBlockNumberMin().get()).isEqualTo(5L);
    assertThat(param.getBlockNumberMax().get()).isEqualTo(15L);
    assertThat(param.getKnownAccounts()).isEmpty();
  }

  @Test
  public void timestampConditionParamDecodesCorrectly() {
    final SendRawTransactionConditionalParameter param =
        parameterWithTimestampConditions(1777L, 1999L);
    assertThat(param.getTimestampMin().get()).isEqualTo(1777L);
    assertThat(param.getTimestampMax().get()).isEqualTo(1999L);
    assertThat(param.getBlockNumberMin()).isEmpty();
    assertThat(param.getBlockNumberMax()).isEmpty();
    assertThat(param.getKnownAccounts()).isEmpty();
  }

  @Test
  public void knownAccountConditionsParamDecodesCorrectly() {
    final Map<Address, Hash> knownAccounts = new HashMap<>();
    knownAccounts.put(Address.ZERO, Hash.ZERO);
    final SendRawTransactionConditionalParameter param =
        parameterWithKnownAccountConditions(knownAccounts);
    assertThat(param.getTimestampMin()).isEmpty();
    assertThat(param.getTimestampMax()).isEmpty();
    assertThat(param.getBlockNumberMin()).isEmpty();
    assertThat(param.getBlockNumberMax()).isEmpty();
    assertThat(param.getKnownAccounts().get()).containsExactly(Map.entry(Address.ZERO, Hash.ZERO));
  }

  private SendRawTransactionConditionalParameter parameterWithNoConditions() {
    return new SendRawTransactionConditionalParameter(null, null, null, null, null);
  }

  private SendRawTransactionConditionalParameter parameterWithBlockNumberConditions(
      final long blockNumberMin, final long blockNumberMax) {
    return new SendRawTransactionConditionalParameter(
        blockNumberMin, blockNumberMax, null, null, null);
  }

  private SendRawTransactionConditionalParameter parameterWithTimestampConditions(
      final long timestampMin, final long timestampMax) {
    return new SendRawTransactionConditionalParameter(null, null, null, timestampMin, timestampMax);
  }

  private SendRawTransactionConditionalParameter parameterWithKnownAccountConditions(
      final Map<Address, Hash> knownAccounts) {
    return new SendRawTransactionConditionalParameter(null, null, knownAccounts, null, null);
  }

  private SendRawTransactionConditionalParameter parameterWithConditions(
      final long blockNumberMin,
      final long blockNumberMax,
      final Map<Address, Hash> knownAccounts,
      final long timestampMin,
      final long timestampMax) {
    return new SendRawTransactionConditionalParameter(
        blockNumberMin, blockNumberMax, knownAccounts, timestampMin, timestampMax);
  }
}
