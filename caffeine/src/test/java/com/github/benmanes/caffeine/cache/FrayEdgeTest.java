/*
 * Copyright 2025 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.benmanes.caffeine.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.errorprone.annotations.Var;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/**
 * Edge case race condition tests using Fray. Targets less common code paths
 * and tight invariant checks to find subtle concurrency bugs.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@ExtendWith(FrayTestExtension.class)
@SuppressWarnings({"IdentityConversion", "PMD.AvoidAccessibilityAlteration"})
final class FrayEdgeTest {

  /* --------------- maximumSize(1) single slot contention --------------- */

  /** Extreme contention on a single-entry cache. Every put evicts the prior entry. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void singleEntryCacheContention() throws InterruptedException {
    var evictions = new AtomicInteger();
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(1)
        .evictionListener(
            (@Nullable Integer key, @Nullable Integer value, RemovalCause cause) -> {
              evictions.incrementAndGet();
            })
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var t1 = new Thread(() -> {
      cache.put(1, 100);
      cache.put(2, 200);
    });
    var t2 = new Thread(() -> {
      cache.put(3, 300);
      cache.put(4, 400);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    long size = cache.estimatedSize();
    assertEquals(1, size, "Single-entry cache should have exactly 1 entry, got: " + size);
  }

  /* --------------- maximumSize(0) zero-capacity cache --------------- */

  /** A zero-capacity cache should evict everything immediately. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void zeroCapacityCacheContention() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(0)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var t1 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.put(i, i);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 5; i < 10; i++) {
        cache.put(i, i);
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    assertEquals(0, cache.estimatedSize(),
        "Zero-capacity cache should be empty after cleanUp");
  }

  /* --------------- Weighted size consistency after concurrent updates --------------- */

  /**
   * After cleanUp, the tracked weighted size should exactly match the sum of all
   * entry weights. Concurrent weight changes could cause drift if not properly tracked.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void weightedSizeConsistency() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumWeight(100)
        .weigher((Integer key, Integer value) -> value)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 5);
    cache.put(2, 5);
    cache.put(3, 5);

    var t1 = new Thread(() -> {
      // Change weight of key 1 from 5 to 10
      cache.put(1, 10);
    });
    var t2 = new Thread(() -> {
      // Change weight of key 2 from 5 to 3
      cache.put(2, 3);
    });
    var t3 = new Thread(() -> {
      // Remove key 3
      cache.invalidate(3);
    });

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    cache.cleanUp();

    // Calculate actual weighted size
    @Var int actualWeightedSize = 0;
    for (var entry : cache.asMap().entrySet()) {
      actualWeightedSize += entry.getValue(); // weight = value
    }

    long reportedSize = cache.policy().eviction()
        .orElseThrow().weightedSize().orElseThrow();
    assertEquals(actualWeightedSize, reportedSize,
        "Weighted size mismatch: actual=" + actualWeightedSize
        + " reported=" + reportedSize);
  }

  /**
   * Stress test: many concurrent weight changes on the same key.
   * The final weighted size must be consistent.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void weightedSizeConcurrentSameKey() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumWeight(100)
        .weigher((Integer key, Integer value) -> value)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 5);

    var t1 = new Thread(() -> cache.put(1, 10));
    var t2 = new Thread(() -> cache.put(1, 1));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();

    Integer finalValue = cache.getIfPresent(1);
    assertNotNull(finalValue);

    long reportedSize = cache.policy().eviction()
        .orElseThrow().weightedSize().orElseThrow();
    assertEquals((long) finalValue, reportedSize,
        "Weighted size should equal the final value/weight");
  }

  /* --------------- removeIf + concurrent put --------------- */

  /**
   * Tests keySet().removeIf() racing with concurrent puts.
   * The removeIf iterates keys, then removes matching ones individually.
   * A concurrent put could add entries that pass the filter but are missed.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void removeIfConcurrentPut() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(20)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 10; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> {
      // Remove all even keys
      cache.asMap().keySet().removeIf(k -> k % 2 == 0);
    });
    var t2 = new Thread(() -> {
      // Add more entries concurrently
      cache.put(10, 10);
      cache.put(11, 11);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    // No crash or inconsistency
    long actualCount = cache.asMap().size();
    long estimated = cache.estimatedSize();
    assertEquals(actualCount, estimated,
        "estimatedSize should match actual: actual=" + actualCount + " estimated=" + estimated);
  }

  /* --------------- entrySet().removeIf + concurrent compute --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void entrySetRemoveIfConcurrentCompute() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(20)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> {
      // Remove entries where value < 3
      cache.asMap().entrySet().removeIf(e -> e.getValue() < 3);
    });
    var t2 = new Thread(() -> {
      // Compute changes value of key 1 (which is < 3)
      cache.asMap().compute(1, (k, v) -> 100);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    long actualCount = cache.asMap().size();
    long estimated = cache.estimatedSize();
    assertEquals(actualCount, estimated,
        "estimatedSize mismatch: actual=" + actualCount + " estimated=" + estimated);
  }

  /* --------------- merge returning null (removal) + concurrent get --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void mergeNullReturnConcurrentGet() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 10);

    var result = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> {
      // merge that removes the entry by returning null
      cache.asMap().merge(1, 5, (oldVal, newVal) -> null);
    });
    var t2 = new Thread(() -> result.set(cache.getIfPresent(1)));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // get should return either 10 (before merge) or null (after merge)
    Integer val = result.get();
    assertTrue(val == null || val == 10,
        "Should be null or 10, got: " + val);
  }

  /* --------------- compute throwing exception + concurrent access --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void computeExceptionConcurrentGet() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 10);

    var gotException = new AtomicBoolean();
    var result = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> {
      try {
        cache.asMap().compute(1, (k, v) -> {
          throw new RuntimeException("test exception");
        });
      } catch (RuntimeException expected) {
        gotException.set(true);
      }
    });
    var t2 = new Thread(() -> result.set(cache.getIfPresent(1)));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // After exception, the entry should still exist with original value
    Integer val = cache.getIfPresent(1);
    assertEquals(Integer.valueOf(10), val,
        "Entry should survive compute exception");
  }

  /* --------------- computeIfAbsent exception on new key + concurrent put --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void computeIfAbsentExceptionConcurrentPut() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var gotException = new AtomicBoolean();

    var t1 = new Thread(() -> {
      try {
        cache.asMap().computeIfAbsent(1, k -> {
          throw new RuntimeException("test exception");
        });
      } catch (RuntimeException expected) {
        gotException.set(true);
      }
    });
    var t2 = new Thread(() -> cache.put(1, 42));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    // Cache should be consistent - either has entry 42 or is empty (if put ran first
    // and compute threw before seeing it, or compute threw and put succeeded after)
    long actualCount = cache.asMap().size();
    long estimated = cache.estimatedSize();
    assertEquals(actualCount, estimated,
        "estimatedSize mismatch after exception: actual=" + actualCount
        + " estimated=" + estimated);
  }

  /* --------------- putAll racing with eviction --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void putAllConcurrentEviction() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(5)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> {
      cache.asMap().putAll(Map.of(10, 10, 11, 11, 12, 12));
    });
    var t2 = new Thread(() -> {
      cache.asMap().putAll(Map.of(20, 20, 21, 21, 22, 22));
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    assertTrue(cache.estimatedSize() <= 5,
        "Size should be <= 5, got: " + cache.estimatedSize());

    // Verify size consistency
    long actualCount = cache.asMap().size();
    assertEquals(actualCount, cache.estimatedSize(),
        "estimatedSize mismatch: actual=" + actualCount);
  }

  /* --------------- LoadingCache getAll with slow loader + concurrent invalidate --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void loadingGetAllConcurrentInvalidate() throws InterruptedException {
    var loaded = ConcurrentHashMap.<Integer>newKeySet();
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .build(key -> {
          loaded.add(key);
          return key * 10;
        });

    var result = new AtomicReference<Map<Integer, Integer>>();

    var t1 = new Thread(() -> result.set(cache.getAll(List.of(1, 2, 3))));
    var t2 = new Thread(() -> {
      cache.invalidate(2);
      cache.invalidate(3);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Map<Integer, Integer> map = result.get();
    assertNotNull(map);
    // getAll should return consistent values for all requested keys
    assertEquals(Integer.valueOf(10), map.get(1));
    assertEquals(Integer.valueOf(20), map.get(2));
    assertEquals(Integer.valueOf(30), map.get(3));
  }

  /* --------------- Async cache getAll overlapping from 3 threads --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void asyncGetAllThreeWayOverlap() throws InterruptedException {
    AsyncCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(20)
        .executor(Runnable::run)
        .buildAsync();

    var r1 = new AtomicReference<Map<Integer, Integer>>();
    var r2 = new AtomicReference<Map<Integer, Integer>>();
    var r3 = new AtomicReference<Map<Integer, Integer>>();

    var t1 = new Thread(() -> {
      var map = cache.getAll(List.of(1, 2, 3), keys -> {
        var m = new LinkedHashMap<Integer, Integer>();
        for (var k : keys) {
          m.put(k, k * 10);
        }
        return m;
      }).join();
      r1.set(map);
    });
    var t2 = new Thread(() -> {
      var map = cache.getAll(List.of(2, 3, 4), keys -> {
        var m = new LinkedHashMap<Integer, Integer>();
        for (var k : keys) {
          m.put(k, k * 10);
        }
        return m;
      }).join();
      r2.set(map);
    });
    var t3 = new Thread(() -> {
      var map = cache.getAll(List.of(3, 4, 5), keys -> {
        var m = new LinkedHashMap<Integer, Integer>();
        for (var k : keys) {
          m.put(k, k * 10);
        }
        return m;
      }).join();
      r3.set(map);
    });

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    // All threads should get correct values
    assertNotNull(r1.get());
    assertNotNull(r2.get());
    assertNotNull(r3.get());
    assertEquals(Integer.valueOf(10), r1.get().get(1));
    assertEquals(Integer.valueOf(20), r1.get().get(2));
    assertEquals(Integer.valueOf(30), r1.get().get(3));
    assertEquals(Integer.valueOf(20), r2.get().get(2));
    assertEquals(Integer.valueOf(30), r2.get().get(3));
    assertEquals(Integer.valueOf(40), r2.get().get(4));
    assertEquals(Integer.valueOf(30), r3.get().get(3));
    assertEquals(Integer.valueOf(40), r3.get().get(4));
    assertEquals(Integer.valueOf(50), r3.get().get(5));
  }

  /* --------------- retainAll + concurrent put --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void retainAllConcurrentPut() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(20)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 10; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> {
      // Retain only keys 0, 1, 2
      cache.asMap().keySet().retainAll(List.of(0, 1, 2));
    });
    var t2 = new Thread(() -> {
      cache.put(20, 20);
      cache.put(21, 21);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    long actualCount = cache.asMap().size();
    long estimated = cache.estimatedSize();
    assertEquals(actualCount, estimated,
        "estimatedSize mismatch: actual=" + actualCount + " estimated=" + estimated);
  }

  /* --------------- Concurrent invalidateAll(keys) with overlapping sets --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void invalidateAllKeysConcurrent() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(20)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 10; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> cache.invalidateAll(List.of(0, 1, 2, 3, 4)));
    var t2 = new Thread(() -> cache.invalidateAll(List.of(3, 4, 5, 6, 7)));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    long actualCount = cache.asMap().size();
    long estimated = cache.estimatedSize();
    assertEquals(actualCount, estimated,
        "estimatedSize mismatch: actual=" + actualCount + " estimated=" + estimated);

    // Keys 0-7 should all be invalidated, 8 and 9 should remain
    for (int i = 0; i <= 7; i++) {
      assertTrue(cache.getIfPresent(i) == null,
          "Key " + i + " should be invalidated");
    }
  }

  /* --------------- Weighted eviction: weight to 0 and back --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void weightedEvictionZeroWeight() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumWeight(10)
        .weigher((Integer key, Integer value) -> value)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 5);
    cache.put(2, 5);

    var t1 = new Thread(() -> {
      // Set weight to 0 (should be treated as zero-weight entry)
      cache.put(1, 0);
    });
    var t2 = new Thread(() -> {
      // Add entry that pushes over the limit
      cache.put(3, 8);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    long reportedWeight = cache.policy().eviction()
        .orElseThrow().weightedSize().orElseThrow();
    assertTrue(reportedWeight <= 10,
        "Weighted size should be <= 10, got: " + reportedWeight);

    // Verify consistency
    @Var int actualWeight = 0;
    for (var entry : cache.asMap().entrySet()) {
      actualWeight += entry.getValue();
    }
    assertEquals(actualWeight, reportedWeight,
        "Weighted size mismatch: actual=" + actualWeight + " reported=" + reportedWeight);
  }

  /* --------------- Variable expiry: expireAfterCreate returns 0 --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void variableExpiryZeroDuration() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .expireAfter(new Expiry<Integer, Integer>() {
          @Override public long expireAfterCreate(Integer key, Integer value, long currentTime) {
            // Key 1 expires immediately, others live long
            return (key == 1) ? 0L : Duration.ofMinutes(5).toNanos();
          }
          @Override public long expireAfterUpdate(Integer key, Integer value,
              long currentTime, long currentDuration) {
            return currentDuration;
          }
          @Override public long expireAfterRead(Integer key, Integer value,
              long currentTime, long currentDuration) {
            return currentDuration;
          }
        })
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var t1 = new Thread(() -> cache.put(1, 100));
    var t2 = new Thread(() -> cache.put(2, 200));
    var t3 = new Thread(() -> cache.cleanUp());

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    cache.cleanUp();
    // Key 2 should survive, key 1 should expire
    assertNotNull(cache.getIfPresent(2), "Key 2 should be present");
  }

  /* --------------- put + compute + invalidate three-way same key --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void threeWaySameKeyPutComputeInvalidate() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 10);

    var t1 = new Thread(() -> cache.put(1, 20));
    var t2 = new Thread(() -> {
      cache.asMap().compute(1, (k, v) -> (v == null ? 0 : v) + 100);
    });
    var t3 = new Thread(() -> cache.invalidate(1));

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    cache.cleanUp();
    long actualCount = cache.asMap().size();
    long estimated = cache.estimatedSize();
    assertEquals(actualCount, estimated,
        "estimatedSize mismatch: actual=" + actualCount + " estimated=" + estimated);
  }

  /* --------------- Async cache: future completes exceptionally --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void asyncExceptionalCompletionConcurrentGet() throws InterruptedException {
    AsyncCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .buildAsync();

    var future = new CompletableFuture<Integer>();
    cache.put(1, future);

    var t1 = new Thread(() -> future.completeExceptionally(new RuntimeException("fail")));
    var t2 = new Thread(() -> {
      // Try to get the same key - should not block forever
      CompletableFuture<Integer> f = cache.get(1, k -> 99);
      try {
        var unused = f.join();
      } catch (RuntimeException ignored) {
        // Expected if the future failed
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // After exceptional completion, the key should have been removed
    // by handleCompletion (cache.remove(key, valueFuture))
    cache.synchronous().cleanUp();
  }

  /* --------------- values().removeAll + concurrent modification --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void valuesRemoveAllConcurrentPut() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(20)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> {
      cache.asMap().values().removeAll(List.of(0, 1, 2));
    });
    var t2 = new Thread(() -> {
      cache.put(10, 10);
      cache.put(0, 999); // Re-add key 0 with different value
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    long actualCount = cache.asMap().size();
    long estimated = cache.estimatedSize();
    assertEquals(actualCount, estimated,
        "estimatedSize mismatch: actual=" + actualCount + " estimated=" + estimated);
  }

  /* --------------- expireAfterAccess: read extends lifetime racing with cleanup --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void expireAfterAccessReadExtendRace() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .expireAfterAccess(Duration.ofNanos(1))
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 100);

    var result = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> {
      // Read should either extend lifetime or see expired entry
      result.set(cache.getIfPresent(1));
    });
    var t2 = new Thread(() -> cache.cleanUp());

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // Result is either 100 (read before/during expiry) or null (expired)
    Integer val = result.get();
    assertTrue(val == null || val == 100,
        "Should be null or 100, got: " + val);
  }

  /* --------------- Concurrent getAll on loading cache with compute on same keys --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void loadingGetAllWithConcurrentCompute() throws InterruptedException {
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .build(key -> key * 10);

    // Pre-populate some entries
    var unused = cache.get(1);

    var result = new AtomicReference<Map<Integer, Integer>>();

    var t1 = new Thread(() -> result.set(cache.getAll(List.of(1, 2, 3))));
    var t2 = new Thread(() -> {
      cache.asMap().compute(2, (k, v) -> 999);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Map<Integer, Integer> map = result.get();
    assertNotNull(map);
    // Key 1 should always be 10 (pre-loaded, not modified)
    assertEquals(Integer.valueOf(10), map.get(1));
    // Key 2 might be 20 (loaded by getAll) or 999 (computed by t2)
    Integer v2 = map.get(2);
    assertNotNull(v2);
    assertTrue(v2 == 20 || v2 == 999, "Key 2 should be 20 or 999, got: " + v2);
  }

  /* --------------- estimatedSize exact consistency --------------- */

  /**
   * After cleanUp with synchronous executor, estimatedSize must exactly equal
   * the number of non-expired entries in the map.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void estimatedSizeExactAfterConcurrentOps() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(5)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var t1 = new Thread(() -> {
      for (int i = 0; i < 8; i++) {
        cache.put(i, i);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 3; i < 6; i++) {
        cache.invalidate(i);
      }
    });
    var t3 = new Thread(() -> {
      cache.asMap().compute(2, (k, v) -> null); // remove via compute
      cache.asMap().merge(7, 77, Integer::sum); // add via merge
    });

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    cache.cleanUp();
    long actualCount = cache.asMap().size();
    long estimated = cache.estimatedSize();
    assertEquals(actualCount, estimated,
        "estimatedSize should exactly match asMap().size(): actual="
        + actualCount + " estimated=" + estimated);
  }
}
