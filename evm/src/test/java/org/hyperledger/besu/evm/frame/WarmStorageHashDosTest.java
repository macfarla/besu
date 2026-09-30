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
package org.hyperledger.besu.evm.frame;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.internal.AddressStorageSlotKey;
import org.hyperledger.besu.evm.toy.ToyBlockValues;
import org.hyperledger.besu.evm.toy.ToyWorld;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Regression test asserting that an attacker who grinds many {@link Address}/{@link Bytes32} keys
 * sharing the same non-treeified {@code hashCode()} cannot force O(n) bucket walks per {@code
 * TSTORE}/warm-up insert. {@link MessageFrame}'s warm-address, warm-storage and transient-storage
 * collections are keyed on exactly such colliding values below, and the whole batch of inserts is
 * required to complete well inside a budget that a quadratic blow-up would blow through by orders
 * of magnitude.
 */
class WarmStorageHashDosTest {

  private static final int SLOT_COUNT = 50_000;
  private static final int ADDRESS_COUNT = 50_000;

  /**
   * Two bytes (a, b) contribute {@code 31*a + b} to Tuweni's base-31 polynomial {@code hashCode()}
   * at the position of {@code a}. Each of these three pairs contributes exactly zero, so tiling any
   * combination of them across a key leaves its hashCode unchanged (all keys collide into a single
   * hash bucket) while the underlying bytes - and thus the keys themselves - remain distinct.
   */
  private static void writeZeroSumPair(final byte[] bytes, final int offset, final int digit) {
    switch (digit) {
      case 0 -> {
        bytes[offset] = 0;
        bytes[offset + 1] = 0;
      }
      case 1 -> {
        bytes[offset] = 1;
        bytes[offset + 1] = (byte) -31;
      }
      default -> {
        bytes[offset] = (byte) -1;
        bytes[offset + 1] = 31;
      }
    }
  }

  private static MessageFrame newFrame(final Address address) {
    return MessageFrame.builder()
        .worldUpdater(new ToyWorld())
        .originator(Address.ZERO)
        .gasPrice(Wei.ONE)
        .blobGasPrice(Wei.ONE)
        .blockValues(new ToyBlockValues())
        .miningBeneficiary(Address.ZERO)
        .blockHashLookup((__, ___) -> Hash.ZERO)
        .type(MessageFrame.Type.MESSAGE_CALL)
        .initialGas(1)
        .address(address)
        .contract(Address.ZERO)
        .inputData(Bytes32.ZERO)
        .sender(Address.ZERO)
        .value(Wei.ZERO)
        .apparentValue(Wei.ZERO)
        .code(Code.EMPTY_CODE)
        .completer(messageFrame -> {})
        .build();
  }

  static class TransientStorage {
    private static long[] readSeeds() throws Exception {
      final long[] seeds = new long[7];
      for (int i = 0; i < 7; i++) {
        final Field f = AddressStorageSlotKey.class.getDeclaredField("SEED_" + i);
        f.setAccessible(true);
        seeds[i] = f.getLong(null);
      }
      return seeds;
    }

    /**
     * Inverse mod 2^64 by Newton iteration; exists because the seeded multipliers are forced odd.
     */
    private static long inv(final long x) {
      long y = x;
      for (int i = 0; i < 6; i++) {
        y *= 2 - x * y;
      }
      return y;
    }

    /**
     * Algorithm: hash = s0*A0 + s1*A1 + s2*A2 + s3*A3 + a0*A4 + a1·A5 + a2*A6
     *
     * <p>hashCode = (int)(H >>> 32)
     *
     * <p>sN - slot limbs
     *
     * <p>aN - address limbs
     *
     * <p>AN - seeds
     *
     * <p>if hash collides then its integer shifted version (hashCode) will also collide
     */
    private static Bytes32 collidingSlot(final Address address, final int index) throws Exception {
      final ByteBuffer addrBytes =
          ByteBuffer.wrap(address.getBytes().toArrayUnsafe()).order(ByteOrder.LITTLE_ENDIAN);
      final long[] seeds = readSeeds();
      final long k =
          addrBytes.getLong(0) * seeds[4]
              + addrBytes.getLong(8) * seeds[5]
              + addrBytes.getLong(12) * seeds[6];
      final long invA1 = inv(seeds[1]);

      final ByteBuffer slotBytes = ByteBuffer.wrap(new byte[32]).order(ByteOrder.LITTLE_ENDIAN);
      // s0 is free choice
      slotBytes.putLong(0, index);
      // solves for s1; s2 == s3 == hash = 0
      slotBytes.putLong(8, -invA1 * (k + index * seeds[0]));
      return Bytes32.wrap(slotBytes.array());
    }

    private static Stream<Arguments> addressArgs() {
      final Random rand = new Random(234234L);
      return Stream.concat(
          Stream.of(Arguments.of(Address.ZERO)),
          IntStream.range(0, 10)
              .mapToObj(__ -> Arguments.of(Address.wrap(Bytes.random(20, rand)))));
    }

    @ParameterizedTest
    @MethodSource("addressArgs")
    void generatedAddressStorageSlotKeysActuallyCollide(final Address address) throws Exception {
      for (int i = 0; i < 1_000; i++) {
        for (int j = 0; j < i; j++) {
          final AddressStorageSlotKey key_i =
              new AddressStorageSlotKey(address, collidingSlot(address, i));
          final AddressStorageSlotKey key_j =
              new AddressStorageSlotKey(address, collidingSlot(address, j));

          assertThat(key_i.hashCode()).isEqualTo(key_j.hashCode());
          assertThat(key_i).isNotEqualTo(key_j);
        }
      }
    }

    @Test
    void transientStorageResistsHashCollisionFlood() throws Exception {
      final Address address = Address.fromHexString("0x1234");
      final MessageFrame frame = newFrame(address);
      final List<Bytes32> slots = new ArrayList<>(SLOT_COUNT);
      for (int i = 0; i < SLOT_COUNT; i++) {
        slots.add(collidingSlot(address, i));
      }

      assertTimeoutPreemptively(
          ofSeconds(10),
          () -> {
            for (final Bytes32 slot : slots) {
              frame.setTransientStorageValue(address, slot, slot);
            }
          });

      for (final Bytes32 slot : slots) {
        assertThat(frame.getTransientStorageValue(address, slot)).isEqualTo(slot);
      }
    }
  }

  static class Eip2929Storage {

    private static Bytes32 collidingSlot(final int index) {
      final byte[] bytes = new byte[32];
      long remaining = index;
      for (int pair = 0; pair < 16; pair++) {
        writeZeroSumPair(bytes, pair * 2, (int) (remaining % 3));
        remaining /= 3;
      }
      return Bytes32.wrap(bytes);
    }

    @Test
    void generatedStorageKeysActuallyCollide() {
      for (int i = 0; i < 1_000; i++) {
        for (int j = 0; j < i; j++) {
          final Bytes32 slot_i = collidingSlot(i);
          final Bytes32 slot_j = collidingSlot(j);
          assertThat(slot_i.hashCode()).isEqualTo(slot_j.hashCode());
          assertThat(slot_i).isNotEqualTo(slot_j);
        }
      }
    }

    @Test
    void warmedUpStorageResistsHashCollisionFlood() {
      final MessageFrame frame = newFrame(Address.ZERO);
      final List<Bytes32> slots = new ArrayList<>(SLOT_COUNT);
      for (int i = 0; i < SLOT_COUNT; i++) {
        slots.add(collidingSlot(i));
      }

      assertTimeoutPreemptively(
          ofSeconds(10),
          () -> {
            for (final Bytes32 slot : slots) {
              frame.warmUpStorage(Address.ZERO, slot);
            }
          });

      for (final Bytes32 slot : slots) {
        assertThat(frame.getWarmedUpStorage().contains(Address.ZERO, slot)).isTrue();
      }
    }
  }

  static class Addresses {
    private static Address collidingAddress(final long index) {
      final byte[] bytes = new byte[Address.SIZE];
      long remaining = index;
      for (int pair = 0; pair < Address.SIZE / 2; pair++) {
        writeZeroSumPair(bytes, pair * 2, (int) (remaining % 3));
        remaining /= 3;
      }
      return Address.wrap(Bytes.wrap(bytes));
    }

    @Test
    void generatedAddressesActuallyCollide() {
      for (int i = 0; i < 1_000; i++) {
        for (int j = 0; j < i; j++) {
          final Address address_i = collidingAddress(i);
          final Address address_j = collidingAddress(j);
          assertThat(address_i.getBytes().hashCode()).isEqualTo(address_j.getBytes().hashCode());
          assertThat(address_i.getBytes()).isNotEqualTo(address_j.getBytes());
        }
      }
    }

    @Test
    void warmedUpAddressesResistHashCollisionFlood() {
      final MessageFrame frame = newFrame(Address.ZERO);
      final List<Address> addresses = new ArrayList<>(ADDRESS_COUNT);
      for (long i = 0; i < ADDRESS_COUNT; i++) {
        addresses.add(collidingAddress(i));
      }

      assertTimeoutPreemptively(
          ofSeconds(10),
          () -> {
            for (final Address address : addresses) {
              frame.warmUpAddress(address);
            }
          });

      for (final Address address : addresses) {
        assertThat(frame.isAddressWarm(address)).isTrue();
      }
    }
  }
}
