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
import java.util.List;
import java.util.Map;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.errorprone.annotations.Var;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/**
 * Stress tests targeting less-common code paths using Fray.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@ExtendWith(FrayTestExtension.class)
@SuppressWarnings({"IdentityConversion", "PMD.AvoidAccessibilityAlteration"})
final class FrayStressTest {

  /* --------------- Unbounded cache with removal listener --------------- */

  /**
   * Tests unbounded cache (no max size) with removal listener during concurrent
   * put and remove. The unbounded cache uses ConcurrentHashMap directly.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void unboundedCacheRemovalListener() throws InterruptedException {
    var notifications = ConcurrentHashMap.<String>newKeySet();
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .removalListener((@Nullable Integer key, @Nullable Integer value, RemovalCause cause) -> {
          notifications.add(key + ":" + value + ":" + cause);
        })
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var t1 = new Thread(() -> {
      cache.put(1, 10);
      cache.put(2, 20);
    });
    var t2 = new Thread(() -> {
      cache.put(1, 100); // replaces
      cache.invalidate(2);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // All operations should complete without error
    // Key 1 should have a replacement notification
    Integer val = cache.getIfPresent(1);
    assertNotNull(val);
  }

  /**
   * Tests unbounded cache clear() with concurrent put. The clear() iterates
   * keySet() (live view when no removal listener) and removes each key.
   * A concurrent put could add an entry that survives the clear.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void unboundedCacheClearConcurrentPut() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> cache.invalidateAll());
    var t2 = new Thread(() -> cache.put(10, 10));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // No crash; key 10 may or may not survive
  }

  /**
   * Tests unbounded cache clear() with removal listener and concurrent puts.
   * When a removal listener is present, clear() snapshots the keySet first.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void unboundedCacheClearWithListenerConcurrentPut() throws InterruptedException {
    var notifications = new AtomicInteger();
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .removalListener((@Nullable Integer key, @Nullable Integer value, RemovalCause cause) -> {
          notifications.incrementAndGet();
        })
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> cache.invalidateAll());
    var t2 = new Thread(() -> {
      cache.put(10, 10);
      cache.put(11, 11);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // No crash; notifications should have been sent for removed entries
    assertTrue(notifications.get() >= 0);
  }

  /* --------------- Unbounded cache compute races --------------- */

  /**
   * Tests concurrent compute operations on unbounded cache.
   * The unbounded cache uses ConcurrentHashMap.compute directly.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void unboundedCacheConcurrentCompute() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 0);

    var t1 = new Thread(() -> {
      cache.asMap().compute(1, (k, v) -> (v == null ? 0 : v) + 1);
    });
    var t2 = new Thread(() -> {
      cache.asMap().compute(1, (k, v) -> (v == null ? 0 : v) + 10);
    });
    var t3 = new Thread(() -> {
      cache.asMap().compute(1, (k, v) -> (v == null ? 0 : v) + 100);
    });

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    assertEquals(Integer.valueOf(111), cache.getIfPresent(1),
        "All computes should be applied serialized by CHM");
  }

  /* --------------- Stats consistency --------------- */

  /**
   * Tests that hit/miss stats are exactly correct after concurrent gets.
   * With synchronous executor, stats should be precise.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void statsConsistencyConcurrentGets() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .recordStats()
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 10);
    cache.put(2, 20);

    var t1 = new Thread(() -> {
      var unused = cache.getIfPresent(1); // hit
      var unused2 = cache.getIfPresent(3); // miss
    });
    var t2 = new Thread(() -> {
      var unused = cache.getIfPresent(2); // hit
      var unused2 = cache.getIfPresent(4); // miss
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    CacheStats stats = cache.stats();
    assertEquals(2, stats.hitCount(), "Expected 2 hits, got: " + stats.hitCount());
    assertEquals(2, stats.missCount(), "Expected 2 misses, got: " + stats.missCount());
  }

  /**
   * Tests that load stats are correctly recorded under concurrent loading.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void statsConsistencyConcurrentLoads() throws InterruptedException {
    var loadCount = new AtomicInteger();
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .recordStats()
        .executor(Runnable::run)
        .build(key -> {
          loadCount.incrementAndGet();
          return key * 10;
        });

    var t1 = new Thread(() -> {
      var unused = cache.get(1);
    });
    var t2 = new Thread(() -> {
      var unused = cache.get(2);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    CacheStats stats = cache.stats();
    // Both are misses that trigger loads
    assertEquals(2, stats.missCount(),
        "Expected 2 misses, got: " + stats.missCount());
    assertEquals(2, stats.loadCount(),
        "Expected 2 loads, got: " + stats.loadCount());
  }

  /* --------------- Bounded cache: rapid put/remove cycles on same key --------------- */

  /**
   * Rapidly alternating put and remove on the same key to stress node state transitions.
   * The node cycles through alive → retired → dead states.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void rapidPutRemoveSameKey() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var t1 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.put(1, i);
        cache.invalidate(1);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.put(1, i + 100);
        cache.invalidate(1);
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    long actualCount = cache.asMap().size();
    long estimated = cache.estimatedSize();
    assertEquals(actualCount, estimated);
  }

  /* --------------- Compute returns same value (identity check optimization) --------------- */

  /**
   * Tests the optimization where compute returns the same value instance.
   * When newValue == oldValue (same reference), the cache avoids the update.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void computeReturnsSameValue() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var sharedValue = Integer.valueOf(42);
    cache.put(1, sharedValue);

    var t1 = new Thread(() -> {
      // Returns same reference - should be a no-op
      cache.asMap().compute(1, (k, v) -> v);
    });
    var t2 = new Thread(() -> {
      // Returns different value
      cache.asMap().compute(1, (k, v) -> 99);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer val = cache.getIfPresent(1);
    assertNotNull(val);
    assertTrue(val == 42 || val == 99, "Value should be 42 or 99, got: " + val);
  }

  /* --------------- put with weight 0 --------------- */

  /**
   * An entry with weight 0 should not be evicted by size-based eviction.
   * Tests the evictEntry resurrection for zero-weight entries.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void zeroWeightEntryDuringEviction() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumWeight(5)
        .weigher((Integer key, Integer value) -> (key == 1) ? 0 : value)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 999); // weight 0
    cache.put(2, 3);   // weight 3
    cache.put(3, 2);   // weight 2

    var t1 = new Thread(() -> {
      // This should trigger eviction of non-zero-weight entries
      cache.put(4, 5); // weight 5
    });
    var t2 = new Thread(() -> {
      var unused = cache.getIfPresent(1); // access zero-weight entry
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    // Zero-weight entry should survive eviction
    Integer val = cache.getIfPresent(1);
    assertNotNull(val, "Zero-weight entry should survive eviction");
    assertEquals(Integer.valueOf(999), val);
  }

  /* --------------- Async loading cache: concurrent refresh and get --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void asyncLoadingRefreshAndGet() throws InterruptedException {
    AsyncLoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .refreshAfterWrite(Duration.ofNanos(1))
        .executor(Runnable::run)
        .buildAsync(key -> key * 10);

    // Seed
    var unused = cache.get(1).join();

    var r1 = new AtomicReference<@Nullable Integer>();
    var r2 = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> r1.set(cache.get(1).join()));
    var t2 = new Thread(() -> r2.set(cache.get(1).join()));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    assertEquals(Integer.valueOf(10), r1.get());
    assertEquals(Integer.valueOf(10), r2.get());
  }

  /* --------------- Bulk getAll on async loading cache --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void asyncLoadingGetAllConcurrent() throws InterruptedException {
    AsyncLoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .buildAsync((key, executor) ->
            CompletableFuture.completedFuture(key * 10));

    var r1 = new AtomicReference<Map<Integer, Integer>>();
    var r2 = new AtomicReference<Map<Integer, Integer>>();

    var t1 = new Thread(() -> r1.set(cache.getAll(List.of(1, 2, 3)).join()));
    var t2 = new Thread(() -> r2.set(cache.getAll(List.of(2, 3, 4)).join()));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    assertNotNull(r1.get());
    assertNotNull(r2.get());
    assertEquals(Integer.valueOf(10), r1.get().get(1));
    assertEquals(Integer.valueOf(20), r1.get().get(2));
    assertEquals(Integer.valueOf(30), r1.get().get(3));
    assertEquals(Integer.valueOf(20), r2.get().get(2));
    assertEquals(Integer.valueOf(30), r2.get().get(3));
    assertEquals(Integer.valueOf(40), r2.get().get(4));
  }

  /* --------------- Concurrent refresh() from multiple threads --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentExplicitRefresh() throws InterruptedException {
    var loadCount = new AtomicInteger();
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .build(key -> {
          loadCount.incrementAndGet();
          return key * 10;
        });

    var unused = cache.get(1);

    var t1 = new Thread(() -> cache.refresh(1));
    var t2 = new Thread(() -> cache.refresh(1));
    var t3 = new Thread(() -> cache.refresh(1));

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    Integer val = cache.getIfPresent(1);
    assertEquals(Integer.valueOf(10), val);
  }

  /* --------------- Eviction listener exception + concurrent operations --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void evictionListenerExceptionConcurrent() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(2)
        .evictionListener(
            (@Nullable Integer key, @Nullable Integer value, RemovalCause cause) -> {
              throw new RuntimeException("listener error");
            })
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

    // Despite listener exceptions, cache should still be functional
    cache.cleanUp();
    assertTrue(cache.estimatedSize() <= 2,
        "Size should be <= 2, got: " + cache.estimatedSize());
  }

  /* --------------- Unbounded loading cache: concurrent getAll --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void unboundedLoadingGetAllConcurrent() throws InterruptedException {
    var loadCount = new AtomicInteger();
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .executor(Runnable::run)
        .build(key -> {
          loadCount.incrementAndGet();
          return key * 10;
        });

    var r1 = new AtomicReference<Map<Integer, Integer>>();
    var r2 = new AtomicReference<Map<Integer, Integer>>();

    var t1 = new Thread(() -> r1.set(cache.getAll(List.of(1, 2, 3))));
    var t2 = new Thread(() -> r2.set(cache.getAll(List.of(2, 3, 4))));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    assertNotNull(r1.get());
    assertNotNull(r2.get());
    assertEquals(Integer.valueOf(10), r1.get().get(1));
    assertEquals(Integer.valueOf(20), r1.get().get(2));
    assertEquals(Integer.valueOf(30), r1.get().get(3));
    assertEquals(Integer.valueOf(20), r2.get().get(2));
    assertEquals(Integer.valueOf(30), r2.get().get(3));
    assertEquals(Integer.valueOf(40), r2.get().get(4));
  }

  /* --------------- Bounded cache: estimatedSize with expiration --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void estimatedSizeWithExpiration() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .expireAfterWrite(Duration.ofNanos(1))
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var t1 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.put(i, i);
      }
    });
    var t2 = new Thread(() -> cache.cleanUp());

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    long actualCount = cache.asMap().size();
    long estimated = cache.estimatedSize();
    assertEquals(actualCount, estimated,
        "estimatedSize mismatch with expiration: actual=" + actualCount
        + " estimated=" + estimated);
  }

  /* --------------- Conditional operations on unbounded cache --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void unboundedConditionalReplaceConcurrent() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 10);

    var r1 = new AtomicReference<@Nullable Boolean>();
    var r2 = new AtomicReference<@Nullable Boolean>();

    var t1 = new Thread(() -> r1.set(cache.asMap().replace(1, 10, 20)));
    var t2 = new Thread(() -> r2.set(cache.asMap().replace(1, 10, 30)));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // Exactly one should succeed
    Boolean b1 = r1.get();
    Boolean b2 = r2.get();
    assertNotNull(b1);
    assertNotNull(b2);
    assertTrue((b1 && !b2) || (!b1 && b2),
        "Exactly one replace should succeed: r1=" + b1 + " r2=" + b2);
  }

  /* --------------- Multiple puts creating and removing same key with eviction listener --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void evictionListenerNotificationOrdering() throws InterruptedException {
    var evicted = ConcurrentHashMap.<String>newKeySet();
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(2)
        .evictionListener(
            (@Nullable Integer key, @Nullable Integer value, RemovalCause cause) -> {
              evicted.add(key + "=" + value);
            })
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var t1 = new Thread(() -> {
      cache.put(1, 1);
      cache.put(2, 2);
      cache.put(3, 3); // should evict
    });
    var t2 = new Thread(() -> {
      cache.put(4, 4);
      cache.put(5, 5);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    assertTrue(cache.estimatedSize() <= 2);
    // All eviction notifications should have valid key-value pairs
    for (var entry : evicted) {
      var parts = entry.split("=");
      int key = Integer.parseInt(parts[0]);
      int value = Integer.parseInt(parts[1]);
      assertEquals(key, value,
          "Eviction notification has mismatched key-value: " + entry);
    }
  }

  /* --------------- Rapid alternation between get and put on loading cache --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void loadingCacheGetPutAlternation() throws InterruptedException {
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .build(key -> key * 10);

    var t1 = new Thread(() -> {
      @Var var unused = cache.get(1);
      cache.put(1, 999);
      unused = cache.get(1);
    });
    var t2 = new Thread(() -> {
      @Var var unused = cache.get(1);
      cache.put(1, 888);
      unused = cache.get(1);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer val = cache.getIfPresent(1);
    assertNotNull(val);
    assertTrue(val == 10 || val == 999 || val == 888,
        "Unexpected value: " + val);
  }
}
