package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.AbstractEngineGetPayload;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EngineGetClientVersionV1Test {

    private AbstractEngineGetPayload method;
    private final String version = "besu/v1.5.0-develop-1234abcd/osx-x86_64/openjdk-java-17";

    @BeforeEach
    public void before() {
        method = mock(AbstractEngineGetPayload.class);
        System.setProperty("besu.version", version);
        when(method.engine_getClientVersionV1(any(JsonRpcRequestContext.class)))
            .thenCallRealMethod();
    }

    @AfterEach
    public void after() {
        System.clearProperty("besu.version");
    }

    @Test
    public void shouldReturnClientVersion() {
        final JsonRpcRequestContext requestContext = mock(JsonRpcRequestContext.class);
        final JsonRpcRequest request = mock(JsonRpcRequest.class);
        when(requestContext.getRequest()).thenReturn(request);
        when(request.getId()).thenReturn("1");

        final JsonRpcSuccessResponse expectedResponse = new JsonRpcSuccessResponse("1", version);
        final JsonRpcResponse response = method.engine_getClientVersionV1(requestContext);

        assertEquals(expectedResponse.getResult(), ((JsonRpcSuccessResponse) response).getResult());
    }
}
