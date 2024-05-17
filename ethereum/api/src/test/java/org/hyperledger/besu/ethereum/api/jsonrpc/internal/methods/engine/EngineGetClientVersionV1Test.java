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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponseType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetClientVersionResultV1;

import java.util.Optional;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineGetClientVersionV1Test extends AbstractScheduledApiTest {

  protected EngineGetClientVersionV1 method;

  protected static final Vertx vertx = Vertx.vertx();
  private static final String besuClientVersion = "v24.5-develop-7577120";

  @Mock protected ProtocolContext protocolContext;

  @Mock protected EngineCallListener engineCallListener;

  @Override
  @BeforeEach
  public void before() {
    super.before();
    this.method =
        new EngineGetClientVersionV1(besuClientVersion, vertx, protocolContext, engineCallListener);
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getClientVersionV1");
  }

  @Test
  public void shouldReturnExpected() {
    final var resp = resp("engine_getClientVersionV1");
    assertThat(resp).isInstanceOf(JsonRpcSuccessResponse.class);
    verify(engineCallListener, times(1)).executionEngineCalled();

    EngineGetClientVersionResultV1 expectedResult =
        new EngineGetClientVersionResultV1("v24.5-develop-7577120", "");
    assertThat(fromSuccessResp(resp).getVersion()).isEqualTo(expectedResult.getVersion());
    assertThat(fromSuccessResp(resp).getCommit()).isEqualTo(expectedResult.getCommit());
  }

  protected JsonRpcResponse resp(final String methodName) {
    return method.response(
        new JsonRpcRequestContext(new JsonRpcRequest("2.0", methodName, new Object[] {})));
  }

  protected EngineGetClientVersionResultV1 fromSuccessResp(final JsonRpcResponse resp) {
    assertThat(resp.getType()).isEqualTo(JsonRpcResponseType.SUCCESS);
    return Optional.of(resp)
        .map(JsonRpcSuccessResponse.class::cast)
        .map(JsonRpcSuccessResponse::getResult)
        .map(EngineGetClientVersionResultV1.class::cast)
        .get();
  }
}
