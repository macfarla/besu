# Implement `web3_clientVersion` exposure via EngineAPI

This commit introduces the following changes to address issue #6471:
- A new method `engine_getClientVersionV1` has been added to `AbstractEngineGetPayload.java` to expose the `web3_clientVersion` via the EngineAPI.
- The `build.gradle` file has been modified to set the `besu.version` system property during the build process, enabling the Java code to retrieve the project version at runtime.
- The `ethereum:core` module has been added as a dependency in `build.gradle` to resolve import errors in the `ethereum/api` module.

These changes allow Consensus Clients to access the execution client's name and version over the EngineAPI to include it in the proposed block graffiti.
