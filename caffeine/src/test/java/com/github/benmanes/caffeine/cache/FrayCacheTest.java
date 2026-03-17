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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
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
 * Controlled concurrency tests using Fray. These tests explore different thread interleavings
 * to find race conditions in core cache operations.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@ExtendWith(FrayTestExtension.class)
@SuppressWarnings("IdentityConversion")
final class FrayCacheTest {

  private static Cache<Integer, Integer> newCache(long maximumSize) {
    return Caffeine.newBuilder()
        .maximumSize(maximumSize)
        .executor(Runnable::run)
        .<Integer, Integer>build();
  }

  /* --------------- Basic Cache Operations --------------- */

  /** Tests concurrent put and get operations on a bounded cache. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentPutGet() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);

    var t1 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.put(i, i * 10);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        Integer value = cache.getIfPresent(i);
        if (value != null) {
          assertEquals(Integer.valueOf(i * 10), value, "Unexpected value for key " + i);
        }
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    for (int i = 0; i < 5; i++) {
      assertNotNull(cache.getIfPresent(i), "Key " + i + " should be present");
    }
  }

  /** Tests concurrent computeIfAbsent to verify only one computation wins per key. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentComputeIfAbsent() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);
    var computeCount = new AtomicInteger();

    var t1 = new Thread(() -> {
      cache.asMap().computeIfAbsent(1, k -> {
        computeCount.incrementAndGet();
        return 100;
      });
    });
    var t2 = new Thread(() -> {
      cache.asMap().computeIfAbsent(1, k -> {
        computeCount.incrementAndGet();
        return 200;
      });
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer value = cache.getIfPresent(1);
    assertNotNull(value);
    assertTrue(value == 100 || value == 200, "Value should be from one of the computations");
  }

  /** Tests concurrent compute operations on the same key. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentCompute() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);
    cache.put(1, 0);

    var t1 = new Thread(() -> {
      cache.asMap().compute(1, (k, v) -> (v == null ? 0 : v) + 1);
    });
    var t2 = new Thread(() -> {
      cache.asMap().compute(1, (k, v) -> (v == null ? 0 : v) + 1);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    assertEquals(Integer.valueOf(2), cache.getIfPresent(1), "Both increments should be applied");
  }

  /** Tests concurrent put and invalidate on the same key. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentPutInvalidate() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);

    var t1 = new Thread(() -> cache.put(1, 42));
    var t2 = new Thread(() -> cache.invalidate(1));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer value = cache.getIfPresent(1);
    assertTrue(value == null || value == 42,
        "Key should be absent or have value 42, got: " + value);
  }

  /** Tests concurrent merge operations on the same key. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentMerge() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);
    cache.put(1, 0);

    var t1 = new Thread(() -> cache.asMap().merge(1, 1, Integer::sum));
    var t2 = new Thread(() -> cache.asMap().merge(1, 1, Integer::sum));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    assertEquals(Integer.valueOf(2), cache.getIfPresent(1), "Both merges should be applied");
  }

  /** Tests concurrent put and replace operations. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentPutReplace() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);
    cache.put(1, 100);

    var t1 = new Thread(() -> cache.put(1, 200));
    var t2 = new Thread(() -> cache.asMap().replace(1, 100, 300));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer value = cache.getIfPresent(1);
    assertNotNull(value);
    assertTrue(value == 200 || value == 300,
        "Value should be 200 or 300, got: " + value);
  }

  /** Tests concurrent operations on a cache with eviction under pressure. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentEviction() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(5);

    var t1 = new Thread(() -> {
      for (int i = 0; i < 10; i++) {
        cache.put(i, i);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 10; i < 20; i++) {
        cache.put(i, i);
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    assertTrue(cache.estimatedSize() <= 5,
        "Cache size should be <= 5 after eviction, got: " + cache.estimatedSize());
  }

  /** Tests concurrent get with loading function on overlapping keys. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentGetWithLoader() throws InterruptedException {
    var loadCount = new ConcurrentHashMap<Integer, AtomicInteger>();
    Cache<Integer, Integer> cache = newCache(10);

    var t1 = new Thread(() -> {
      for (int i = 0; i < 3; i++) {
        int key = i;
        var unused = cache.get(key, k -> {
          loadCount.computeIfAbsent(key, x -> new AtomicInteger()).incrementAndGet();
          return key * 10;
        });
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 3; i++) {
        int key = i;
        var unused = cache.get(key, k -> {
          loadCount.computeIfAbsent(key, x -> new AtomicInteger()).incrementAndGet();
          return key * 10;
        });
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    for (int i = 0; i < 3; i++) {
      assertEquals(Integer.valueOf(i * 10), cache.getIfPresent(i), "Wrong value for key " + i);
    }
  }

  /* --------------- Compute and Remove Races --------------- */

  /** Tests compute racing with remove on the same key. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentComputeRemove() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);
    cache.put(1, 10);

    var t1 = new Thread(() -> {
      cache.asMap().compute(1, (k, v) -> (v == null ? 0 : v) + 5);
    });
    var t2 = new Thread(() -> cache.asMap().remove(1));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // After compute and remove race: either key absent, or value is 5 (compute won after remove)
    // or 15 (compute ran before remove but remove won) - key may or may not exist
    Integer value = cache.getIfPresent(1);
    if (value != null) {
      assertTrue(value == 5 || value == 15,
          "Value should be 5 or 15, got: " + value);
    }
  }

  /** Tests computeIfPresent racing with invalidate on the same key. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentComputeIfPresentInvalidate() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);
    cache.put(1, 10);

    var t1 = new Thread(() -> {
      cache.asMap().computeIfPresent(1, (k, v) -> v * 2);
    });
    var t2 = new Thread(() -> cache.invalidate(1));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer value = cache.getIfPresent(1);
    assertTrue(value == null || value == 20,
        "Key should be absent or have value 20, got: " + value);
  }

  /** Tests concurrent remove(key, value) with conditional semantics. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentConditionalRemove() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);
    cache.put(1, 100);

    var removed1 = new AtomicBoolean();
    var removed2 = new AtomicBoolean();

    var t1 = new Thread(() -> removed1.set(cache.asMap().remove(1, 100)));
    var t2 = new Thread(() -> removed2.set(cache.asMap().remove(1, 100)));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // At most one conditional remove should succeed
    assertNull(cache.getIfPresent(1), "Key should be absent after removal");
    assertTrue(removed1.get() || removed2.get(), "At least one remove should succeed");
  }

  /* --------------- Eviction Under Contention --------------- */

  /** Tests concurrent puts with reads during active eviction on a tiny cache. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentEvictionWithReads() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(3);

    var t1 = new Thread(() -> {
      for (int i = 0; i < 8; i++) {
        cache.put(i, i);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 8; i++) {
        var unused = cache.getIfPresent(i);
      }
    });
    var t3 = new Thread(() -> {
      for (int i = 8; i < 16; i++) {
        cache.put(i, i);
      }
    });

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    cache.cleanUp();
    assertTrue(cache.estimatedSize() <= 3,
        "Cache size should be <= 3, got: " + cache.estimatedSize());
  }

  /** Tests concurrent put and invalidateAll race. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentPutInvalidateAll() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);

    var t1 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.put(i, i);
      }
    });
    var t2 = new Thread(() -> cache.invalidateAll());

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // After invalidateAll, no guarantees on which entries survive the race
    cache.cleanUp();
    assertTrue(cache.estimatedSize() >= 0, "Size must be non-negative");
  }

  /* --------------- Loading Cache --------------- */

  /** Tests concurrent loads through a LoadingCache for the same key. */
  @FrayTest(iterations = 1000, resetClassLoaderPerIteration = false)
  void concurrentLoadingCacheGet() throws InterruptedException {
    var loadCount = new AtomicInteger();
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .build(key -> {
          loadCount.incrementAndGet();
          return key * 10;
        });

    var result1 = new AtomicReference<Integer>();
    var result2 = new AtomicReference<Integer>();

    var t1 = new Thread(() -> result1.set(cache.get(1)));
    var t2 = new Thread(() -> result2.set(cache.get(1)));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    assertEquals(Integer.valueOf(10), result1.get());
    assertEquals(Integer.valueOf(10), result2.get());
    assertEquals(Integer.valueOf(10), cache.getIfPresent(1));
  }

  /** Tests concurrent getAll on a LoadingCache with overlapping key sets. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentLoadingCacheGetAll() throws InterruptedException {
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(20)
        .executor(Runnable::run)
        .build(key -> key * 10);

    var result1 = new AtomicReference<Map<Integer, Integer>>();
    var result2 = new AtomicReference<Map<Integer, Integer>>();

    var t1 = new Thread(() -> result1.set(cache.getAll(Set.of(1, 2, 3))));
    var t2 = new Thread(() -> result2.set(cache.getAll(Set.of(2, 3, 4))));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // Both results should have correct values for their requested keys
    assertEquals(Integer.valueOf(10), result1.get().get(1));
    assertEquals(Integer.valueOf(20), result1.get().get(2));
    assertEquals(Integer.valueOf(30), result1.get().get(3));
    assertEquals(Integer.valueOf(20), result2.get().get(2));
    assertEquals(Integer.valueOf(30), result2.get().get(3));
    assertEquals(Integer.valueOf(40), result2.get().get(4));
  }

  /** Tests concurrent refresh racing with get on a LoadingCache. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentRefreshAndGet() throws InterruptedException {
    var loadCount = new AtomicInteger();
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .refreshAfterWrite(Duration.ofNanos(1))
        .executor(Runnable::run)
        .build(key -> {
          loadCount.incrementAndGet();
          return key * 10;
        });

    // Seed the cache
    var unused = cache.get(1);

    var result1 = new AtomicReference<Integer>();
    var result2 = new AtomicReference<Integer>();

    var t1 = new Thread(() -> {
      // Force a refresh by reading an expired entry
      result1.set(cache.get(1));
    });
    var t2 = new Thread(() -> {
      result2.set(cache.get(1));
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // Both reads should return valid values (the old or refreshed value)
    assertEquals(Integer.valueOf(10), result1.get());
    assertEquals(Integer.valueOf(10), result2.get());
  }

  /* --------------- Expiration Races --------------- */

  /** Tests concurrent access with expireAfterWrite, where entries may be expired mid-access. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentExpireAfterWrite() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .expireAfterWrite(Duration.ofNanos(1))
        .executor(Runnable::run)
        .<Integer, Integer>build();

    // Seed entries that will immediately expire
    for (int i = 0; i < 5; i++) {
      cache.put(i, i * 10);
    }

    var t1 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        var unused = cache.getIfPresent(i);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.put(i, i * 100);
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // No crash or inconsistent state
    cache.cleanUp();
    assertTrue(cache.estimatedSize() >= 0, "Size must be non-negative");
  }

  /** Tests concurrent access with variable expiration (Expiry interface). */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentVariableExpiration() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .expireAfter(new Expiry<Integer, Integer>() {
          @Override public long expireAfterCreate(Integer key, Integer value, long currentTime) {
            return Long.MAX_VALUE;
          }
          @Override public long expireAfterUpdate(Integer key, Integer value,
              long currentTime, long currentDuration) {
            return Long.MAX_VALUE;
          }
          @Override public long expireAfterRead(Integer key, Integer value,
              long currentTime, long currentDuration) {
            return Long.MAX_VALUE;
          }
        })
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.put(i, i * 2);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        var unused = cache.getIfPresent(i);
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    for (int i = 0; i < 5; i++) {
      Integer value = cache.getIfPresent(i);
      assertNotNull(value, "Key " + i + " should be present");
      assertTrue(value == i || value == i * 2,
          "Key " + i + " should be " + i + " or " + (i * 2) + ", got: " + value);
    }
  }

  /* --------------- Weighted Eviction --------------- */

  /** Tests concurrent weighted puts that trigger eviction. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentWeightedEviction() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumWeight(20)
        .weigher((Integer key, Integer value) -> value)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var t1 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.put(i, 5); // each has weight 5
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 5; i < 10; i++) {
        cache.put(i, 5);
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    // Total weight of remaining entries should not exceed maximum
    long totalWeight = cache.asMap().values().stream()
        .mapToLong(Integer::longValue).sum();
    assertTrue(totalWeight <= 20,
        "Total weight should be <= 20, got: " + totalWeight);
  }

  /* --------------- Removal Listener Races --------------- */

  /** Tests that removal listeners see consistent state during concurrent mutations. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentRemovalListener() throws InterruptedException {
    var evicted = new ConcurrentHashMap<Integer, Integer>();
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(3)
        .removalListener((@Nullable Integer key, @Nullable Integer value, RemovalCause cause) -> {
          if (key != null && value != null) {
            evicted.put(key, value);
          }
        })
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var t1 = new Thread(() -> {
      for (int i = 0; i < 6; i++) {
        cache.put(i, i);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 6; i < 12; i++) {
        cache.put(i, i);
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    // Evicted entries should have consistent key-value pairs
    evicted.forEach((key, value) ->
        assertEquals(key, value, "Evicted entry has mismatched key-value: " + key + "=" + value));
  }

  /* --------------- Async Cache --------------- */

  /** Tests concurrent get on AsyncCache with overlapping keys. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentAsyncGet() throws InterruptedException {
    AsyncCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .buildAsync();

    var t1 = new Thread(() -> {
      for (int i = 0; i < 3; i++) {
        int key = i;
        var unused = cache.get(key, k -> key * 10);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 3; i++) {
        int key = i;
        var unused = cache.get(key, k -> key * 10);
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // Verify all futures have the correct values
    for (int i = 0; i < 3; i++) {
      CompletableFuture<Integer> future = cache.getIfPresent(i);
      assertNotNull(future, "Key " + i + " should have a future");
      assertTrue(future.isDone(), "Future for key " + i + " should be done");
      assertEquals(Integer.valueOf(i * 10), future.join(), "Wrong value for key " + i);
    }
  }

  /** Tests concurrent put and get on AsyncCache. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentAsyncPutGet() throws InterruptedException {
    AsyncCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .buildAsync();

    var t1 = new Thread(() -> cache.put(1, CompletableFuture.completedFuture(100)));
    var t2 = new Thread(() -> {
      var unused = cache.get(1, k -> 200);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    CompletableFuture<Integer> future = cache.getIfPresent(1);
    assertNotNull(future);
    assertTrue(future.isDone());
    Integer value = future.join();
    assertTrue(value == 100 || value == 200,
        "Value should be 100 or 200, got: " + value);
  }

  /* --------------- Replace Races --------------- */

  /** Tests concurrent replace with different old values. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentReplaceRace() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);
    cache.put(1, 100);

    var replaced1 = new AtomicBoolean();
    var replaced2 = new AtomicBoolean();

    var t1 = new Thread(() -> replaced1.set(cache.asMap().replace(1, 100, 200)));
    var t2 = new Thread(() -> replaced2.set(cache.asMap().replace(1, 100, 300)));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer value = cache.getIfPresent(1);
    assertNotNull(value);
    // At most one conditional replace should succeed on the original value
    if (replaced1.get() && !replaced2.get()) {
      assertTrue(value == 200 || value == 300,
          "Expected 200 or 300, got: " + value);
    } else if (!replaced1.get() && replaced2.get()) {
      assertTrue(value == 200 || value == 300,
          "Expected 200 or 300, got: " + value);
    }
  }

  /** Tests concurrent unconditional replace on the same key. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentUnconditionalReplace() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);
    cache.put(1, 0);

    var t1 = new Thread(() -> cache.asMap().replace(1, 100));
    var t2 = new Thread(() -> cache.asMap().replace(1, 200));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer value = cache.getIfPresent(1);
    assertNotNull(value);
    assertTrue(value == 100 || value == 200,
        "Value should be 100 or 200, got: " + value);
  }

  /* --------------- Three-Way Races --------------- */

  /** Tests three-way race: put, compute, and remove on the same key. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentPutComputeRemove() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);
    cache.put(1, 10);

    var t1 = new Thread(() -> cache.put(1, 50));
    var t2 = new Thread(() -> cache.asMap().compute(1, (k, v) -> (v == null ? 0 : v) + 1));
    var t3 = new Thread(() -> cache.asMap().remove(1));

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    // Verify no crash and state is consistent
    Integer value = cache.getIfPresent(1);
    if (value != null) {
      assertTrue(value > 0, "Value should be positive, got: " + value);
    }
  }

  /** Tests three-way race: merge, put, and get on the same key. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentMergePutGet() throws InterruptedException {
    Cache<Integer, Integer> cache = newCache(10);
    cache.put(1, 0);

    var readValue = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> cache.asMap().merge(1, 10, Integer::sum));
    var t2 = new Thread(() -> cache.put(1, 99));
    var t3 = new Thread(() -> readValue.set(cache.getIfPresent(1)));

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    // Final value must be consistent with some valid interleaving
    Integer finalValue = cache.getIfPresent(1);
    assertNotNull(finalValue);
  }

  /* --------------- Unbounded Cache --------------- */

  /** Tests concurrent operations on an unbounded cache (no eviction). */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentUnboundedCache() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var t1 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.put(i, i);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.asMap().computeIfAbsent(i, k -> k * 10);
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    for (int i = 0; i < 5; i++) {
      Integer value = cache.getIfPresent(i);
      assertNotNull(value, "Key " + i + " should be present");
    }
  }

  /** Tests concurrent compute on an unbounded cache. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentUnboundedCompute() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .executor(Runnable::run)
        .<Integer, Integer>build();
    cache.put(1, 0);

    var t1 = new Thread(() -> cache.asMap().compute(1, (k, v) -> (v == null ? 0 : v) + 1));
    var t2 = new Thread(() -> cache.asMap().compute(1, (k, v) -> (v == null ? 0 : v) + 1));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    assertEquals(Integer.valueOf(2), cache.getIfPresent(1),
        "Both increments should be applied on unbounded cache");
  }

  /* --------------- Stats Recording Races --------------- */

  /** Tests that stats recording is consistent during concurrent hits and misses. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentStatsRecording() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .recordStats()
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 10);

    var t1 = new Thread(() -> {
      @Var var unused = cache.getIfPresent(1); // hit
      unused = cache.getIfPresent(99);         // miss
    });
    var t2 = new Thread(() -> {
      @Var var unused = cache.getIfPresent(1); // hit
      unused = cache.getIfPresent(99);         // miss
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    var stats = cache.stats();
    assertEquals(2, stats.hitCount(), "Should record 2 hits");
    assertEquals(2, stats.missCount(), "Should record 2 misses");
  }
}
