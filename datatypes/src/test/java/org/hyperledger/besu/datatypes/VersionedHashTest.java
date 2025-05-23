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
package org.hyperledger.besu.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class VersionedHashTest {

  @Test
  public void throwsOnUnsupportedHashType() {
    assertThrows(IllegalArgumentException.class, () -> new VersionedHash((byte) 0, Hash.ZERO));
  }

  @Test
  public void throwsOnParsingUnsupportedHashType() {
    assertThrows(IllegalArgumentException.class, () -> new VersionedHash(Bytes32.ZERO));
  }

  @Test
  public void parseValidVersionedHash() {
    // Valid versioned hash: version byte = 0x01 followed by 31 bytes
    String hex = "0x010657f37554c781402a22917dee2f75def7ab966d7b770905398eba3c444014";
    VersionedHash vh = VersionedHash.fromHexString(hex);
    assertEquals(VersionedHash.SHA256_VERSION_ID, vh.getVersionId(), "Version ID should be 1");
    assertEquals(hex, vh.toString(), "toString should return the original hex string");
  }

  @Test
  public void throwsOnParsingInvalidVersionedHash() {
    // Invalid versioned hash: version byte = 0x00 not supported
    String badHex = "0x000657f37554c781402a22917dee2f75def7ab966d7b770905398eba3c444014";
    assertThrows(
        IllegalArgumentException.class,
        () -> VersionedHash.fromHexString(badHex),
        "Parsing a hash with version byte 0x00 should throw IllegalArgumentException");
  }
}
