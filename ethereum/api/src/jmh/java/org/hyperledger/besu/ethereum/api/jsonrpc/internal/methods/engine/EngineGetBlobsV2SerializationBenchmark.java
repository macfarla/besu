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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobAndProofV2;
import org.hyperledger.besu.util.HexUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks the old (collect-then-serialize) vs new (stream-per-entry) approach for
 * engine_getBlobsV2 responses.
 *
 * <p>Each blob is 131072 bytes; each V2 entry also carries 128 × 48-byte cell proofs. At 128 blobs
 * the full response is ~34 MB of hex-encoded JSON. The streaming approach serialises and writes
 * each entry individually rather than building the complete list in memory first, reducing peak
 * heap pressure.
 *
 * <p>Run with:
 *
 * <pre>
 *   ./gradlew :ethereum:api:jmh -Pincludes=EngineGetBlobsV2Serialization --rerun-tasks --no-daemon
 * </pre>
 *
 * <p>To capture allocation rate (key metric for this PR):
 *
 * <pre>
 *   ./gradlew :ethereum:api:jmh -Pincludes=EngineGetBlobsV2Serialization -Pjmh.profilers=gc --rerun-tasks --no-daemon
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
public class EngineGetBlobsV2SerializationBenchmark {

  /** Number of blobs in the request — 128 is the spec maximum for engine_getBlobsV2. */
  @Param({"1", "8", "32", "128"})
  public int blobCount;

  private static final int BLOB_BYTES = 131_072;
  private static final int PROOF_BYTES = 48;
  private static final int CELL_PROOFS_PER_BLOB = 128;

  private List<BlobAndProofV2> entries;
  private ObjectMapper mapper;

  @Setup
  public void setUp() {
    mapper = new ObjectMapper();
    final Random rng = new Random(0xBE5);
    entries = new ArrayList<>(blobCount);

    for (int i = 0; i < blobCount; i++) {
      final byte[] blobData = new byte[BLOB_BYTES];
      rng.nextBytes(blobData);
      final String blobHex = HexUtils.toFastHex(Bytes.wrap(blobData), true);

      final List<String> proofHexes = new ArrayList<>(CELL_PROOFS_PER_BLOB);
      for (int j = 0; j < CELL_PROOFS_PER_BLOB; j++) {
        final byte[] proofData = new byte[PROOF_BYTES];
        rng.nextBytes(proofData);
        proofHexes.add(HexUtils.toFastHex(Bytes.wrap(proofData), true));
      }
      entries.add(new BlobAndProofV2(blobHex, proofHexes));
    }
  }

  /**
   * Old path: collect all entries into a list and hand the whole list to Jackson. Jackson
   * internally builds one large buffer before any bytes reach the output.
   */
  @Benchmark
  public byte[] collectAndSerialize() throws IOException {
    return mapper.writeValueAsBytes(entries);
  }

  /**
   * New path (streaming): write the JSON-RPC envelope once, then serialise and flush each entry
   * individually. Peak in-process allocation is bounded by one entry at a time rather than the full
   * response.
   */
  @Benchmark
  public void streamSerialize(final Blackhole bh) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":".getBytes(StandardCharsets.UTF_8));
    out.write('[');
    boolean first = true;
    for (final BlobAndProofV2 entry : entries) {
      if (!first) out.write(',');
      out.write(mapper.writeValueAsBytes(entry));
      first = false;
    }
    out.write(']');
    out.write('}');
    bh.consume(out);
  }
}
