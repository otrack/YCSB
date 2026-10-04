/**
 * Copyright (c) 2026 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.workloads;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.LongFunction;
import java.util.function.ToIntFunction;

/**
 * Splits the key numbers {@code [0, recordcount)} of the Calvin micro-benchmark into {@code N} partitions,
 * mirroring how the evaluated system places the records on its nodes.
 * The records of a partition are numbered {@code 0 .. size(p) - 1} in increasing key number order;
 * the first ones form the hot pool of the partition, the others its cold pool.
 */
public abstract class CalvinPartitioner {

  /** Name of the partitioner placing key number {@code k} on partition {@code k mod N} (Calvin, Tiga). */
  public static final String MOD = "mod";
  /** Name of the partitioner splitting the key numbers into {@code N} contiguous blocks (range-sharded stores). */
  public static final String RANGE = "range";
  /** Name of the partitioner cutting Cassandra's Murmur3 token ring into {@code N} equal slices. */
  public static final String MURMUR3 = "murmur3";
  /** Name of the partitioner placing the keys on the shards as Tiga's YCSB client does. */
  public static final String TIGA = "tiga";

  /** Tiga's YCSB client maps the keys to integers in {@code [0, TIGA_KEY_SPACE)}. */
  static final int TIGA_KEY_SPACE = 2000005;

  protected final int partitions;
  protected final long recordCount;

  protected CalvinPartitioner(int partitions, long recordCount) {
    this.partitions = partitions;
    this.recordCount = recordCount;
  }

  public int getPartitions() {
    return partitions;
  }

  /** Number of records of partition {@code p}. */
  public abstract long size(int p);

  /** Key number of the {@code i}-th record of partition {@code p}. */
  public abstract long keyNum(int p, long i);

  /** Partition of key number {@code k}. */
  public abstract int partitionOf(long keyNum);

  /**
   * Creates the partitioner {@code name}.
   *
   * @param keyName maps a key number to the key stored in the database (used by {@link #MURMUR3})
   */
  public static CalvinPartitioner create(String name, int partitions, long recordCount,
                                         LongFunction<String> keyName) {
    switch (name) {
    case MOD:
      return new Mod(partitions, recordCount);
    case RANGE:
      return new Range(partitions, recordCount);
    case MURMUR3:
      return new Hashed(partitions, recordCount, keyName, key -> slice(token(key), partitions));
    case TIGA:
      return new Hashed(partitions, recordCount, keyName, key -> tigaKey(key) % partitions);
    default:
      throw new IllegalArgumentException("unknown partitioner: " + name
          + " (expected " + MOD + ", " + RANGE + ", " + MURMUR3 + " or " + TIGA + ")");
    }
  }

  /**
   * Key number {@code k} belongs to partition {@code k mod N}.
   */
  static final class Mod extends CalvinPartitioner {
    Mod(int partitions, long recordCount) {
      super(partitions, recordCount);
    }

    @Override
    public long size(int p) {
      return (recordCount - p + partitions - 1) / partitions;
    }

    @Override
    public long keyNum(int p, long i) {
      return p + i * partitions;
    }

    @Override
    public int partitionOf(long keyNum) {
      return (int) (keyNum % partitions);
    }
  }

  /**
   * Partition {@code p} holds key numbers {@code [p * R / N, (p + 1) * R / N)}.
   */
  static final class Range extends CalvinPartitioner {
    Range(int partitions, long recordCount) {
      super(partitions, recordCount);
    }

    private long start(int p) {
      return p * recordCount / partitions;
    }

    @Override
    public long size(int p) {
      return start(p + 1) - start(p);
    }

    @Override
    public long keyNum(int p, long i) {
      return start(p) + i;
    }

    @Override
    public int partitionOf(long keyNum) {
      int p = (int) (keyNum * partitions / recordCount);
      // Fix the rounding of the integer division
      while (keyNum < start(p)) {
        p--;
      }
      while (keyNum >= start(p + 1)) {
        p++;
      }
      return p;
    }
  }

  /**
   * The partition of a key is a function of its name, given by {@code partitionOfKey}:
   * <ul>
   *   <li>{@link #MURMUR3}: the keys whose Cassandra token (Murmur3Partitioner) falls in the {@code p}-th of
   *   {@code N} equal slices of the token ring, i.e. on node {@code p + 1} of a data center when it owns the single
   *   token {@link #initialToken(int, int)};</li>
   *   <li>{@link #TIGA}: the keys that Tiga's YCSB client sends to shard {@code p}, i.e.
   *   {@code tigaKey(key) mod N}.</li>
   * </ul>
   */
  static final class Hashed extends CalvinPartitioner {
    private final long[][] keys;

    Hashed(int partitions, long recordCount, LongFunction<String> keyName, ToIntFunction<String> partitionOfKey) {
      super(partitions, recordCount);
      if (recordCount > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("too many records for a hash partitioner");
      }
      if (partitions > Byte.MAX_VALUE) {
        throw new IllegalArgumentException("too many partitions for a hash partitioner");
      }
      int[] counts = new int[partitions];
      byte[] owner = new byte[(int) recordCount];
      for (int k = 0; k < recordCount; k++) {
        int p = partitionOfKey.applyAsInt(keyName.apply(k));
        owner[k] = (byte) p;
        counts[p]++;
      }
      keys = new long[partitions][];
      for (int p = 0; p < partitions; p++) {
        keys[p] = new long[counts[p]];
        counts[p] = 0;
      }
      for (int k = 0; k < recordCount; k++) {
        int p = owner[k];
        keys[p][counts[p]++] = k;
      }
    }

    @Override
    public long size(int p) {
      return keys[p].length;
    }

    @Override
    public long keyNum(int p, long i) {
      return keys[p][(int) i];
    }

    @Override
    public int partitionOf(long keyNum) {
      // Not on the hot path: only used for checks and tests
      for (int p = 0; p < partitions; p++) {
        if (Arrays.binarySearch(keys[p], keyNum) >= 0) {
          return p;
        }
      }
      throw new IllegalArgumentException("unknown key number: " + keyNum);
    }
  }

  private static final BigInteger TWO_TO_64 = BigInteger.ONE.shiftLeft(64);

  /** Width of the token slices: {@code floor(2^64 / N)}. */
  static long sliceWidth(int partitions) {
    return TWO_TO_64.divide(BigInteger.valueOf(partitions)).longValue();
  }

  /**
   * Slice of the token ring holding {@code token}: slice {@code p} spans the tokens
   * {@code (initialToken(p - 1, N), initialToken(p, N)]}, wrapping around for the first one.
   */
  static int slice(long token, int partitions) {
    if (partitions == 1) {
      return 0;
    }
    long offset = token - Long.MIN_VALUE; // unsigned distance from the start of the ring
    long p = Long.divideUnsigned(offset, sliceWidth(partitions));
    return (int) Math.min(p, partitions - 1);
  }

  /**
   * The token of the node owning slice {@code p} (the last token of the slice).
   * In data center {@code d} (counting from 0), the nodes use this token minus {@code d}, as tokens must be
   * unique in a Cassandra cluster.
   */
  public static long initialToken(int p, int partitions) {
    if (p == partitions - 1) {
      return Long.MAX_VALUE;
    }
    return Long.MIN_VALUE + (p + 1) * sliceWidth(partitions) - 1;
  }

  /**
   * The token Cassandra's Murmur3Partitioner gives to a text partition key.
   */
  public static long token(String key) {
    long h = murmur3X64Hash(key.getBytes(StandardCharsets.UTF_8));
    return h == Long.MIN_VALUE ? Long.MAX_VALUE : h;
  }

  /**
   * First half of MurmurHash3_x64_128 with seed 0, as computed by Cassandra (org.apache.cassandra.utils.MurmurHash,
   * including the sign extension of the bytes of the tail).
   */
  static long murmur3X64Hash(byte[] key) {
    final long c1 = 0x87c37b91114253d5L;
    final long c2 = 0x4cf5ad432745937fL;
    final int length = key.length;
    final int nblocks = length >> 4;
    long h1 = 0;
    long h2 = 0;

    for (int i = 0; i < nblocks; i++) {
      long k1 = getBlock(key, i * 16);
      long k2 = getBlock(key, i * 16 + 8);

      k1 *= c1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= c2;
      h1 ^= k1;
      h1 = Long.rotateLeft(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729;

      k2 *= c2;
      k2 = Long.rotateLeft(k2, 33);
      k2 *= c1;
      h2 ^= k2;
      h2 = Long.rotateLeft(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5;
    }

    int offset = nblocks * 16;
    long k1 = 0;
    long k2 = 0;
    switch (length & 15) {
    case 15: k2 ^= ((long) key[offset + 14]) << 48;
    case 14: k2 ^= ((long) key[offset + 13]) << 40;
    case 13: k2 ^= ((long) key[offset + 12]) << 32;
    case 12: k2 ^= ((long) key[offset + 11]) << 24;
    case 11: k2 ^= ((long) key[offset + 10]) << 16;
    case 10: k2 ^= ((long) key[offset + 9]) << 8;
    case 9:
      k2 ^= key[offset + 8];
      k2 *= c2;
      k2 = Long.rotateLeft(k2, 33);
      k2 *= c1;
      h2 ^= k2;
    case 8: k1 ^= ((long) key[offset + 7]) << 56;
    case 7: k1 ^= ((long) key[offset + 6]) << 48;
    case 6: k1 ^= ((long) key[offset + 5]) << 40;
    case 5: k1 ^= ((long) key[offset + 4]) << 32;
    case 4: k1 ^= ((long) key[offset + 3]) << 24;
    case 3: k1 ^= ((long) key[offset + 2]) << 16;
    case 2: k1 ^= ((long) key[offset + 1]) << 8;
    case 1:
      k1 ^= key[offset];
      k1 *= c1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= c2;
      h1 ^= k1;
    default:
      break;
    }

    h1 ^= length;
    h2 ^= length;
    h1 += h2;
    h2 += h1;
    h1 = fmix(h1);
    h2 = fmix(h2);
    h1 += h2;
    return h1;
  }

  private static long getBlock(byte[] key, int offset) {
    return (key[offset] & 0xffL)
        | (key[offset + 1] & 0xffL) << 8
        | (key[offset + 2] & 0xffL) << 16
        | (key[offset + 3] & 0xffL) << 24
        | (key[offset + 4] & 0xffL) << 32
        | (key[offset + 5] & 0xffL) << 40
        | (key[offset + 6] & 0xffL) << 48
        | (key[offset + 7] & 0xffL) << 56;
  }

  private static long fmix(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;
    return k;
  }

  /**
   * The integer key of Tiga's YCSB client ({@code hashKey} in ycsb_jni.cc):
   * {@code (std::hash<std::string>(key) & 0x7FFFFFFF) % 2000005}.
   */
  public static int tigaKey(String key) {
    return (int) ((stdHash(key.getBytes(StandardCharsets.UTF_8)) & 0x7FFFFFFFL) % TIGA_KEY_SPACE);
  }

  /**
   * {@code std::hash<std::string>} of libstdc++ on a 64-bit platform, i.e. {@code _Hash_bytes} with seed
   * {@code 0xc70f6907}.
   */
  static long stdHash(byte[] buf) {
    final long mul = (0xc6a4a793L << 32) + 0x5bd1e995L;
    final int len = buf.length;
    final int lenAligned = len & ~7;
    long hash = 0xc70f6907L ^ (len * mul);
    for (int p = 0; p < lenAligned; p += 8) {
      long data = shiftMix(getBlock(buf, p) * mul) * mul;
      hash ^= data;
      hash *= mul;
    }
    if ((len & 7) != 0) {
      long data = 0;
      for (int n = (len & 7) - 1; n >= 0; n--) {
        data = (data << 8) + (buf[lenAligned + n] & 0xffL);
      }
      hash ^= data;
      hash *= mul;
    }
    hash = shiftMix(hash) * mul;
    return shiftMix(hash);
  }

  private static long shiftMix(long v) {
    return v ^ (v >>> 47);
  }
}
