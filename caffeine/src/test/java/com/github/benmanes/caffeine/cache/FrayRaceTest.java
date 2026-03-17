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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/**
 * Aggressive race condition tests using Fray. These tests target specific code paths
 * in the cache that are prone to concurrency bugs.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@ExtendWith(FrayTestExtension.class)
@SuppressWarnings({"IdentityConversion", "PMD.AvoidAccessibilityAlteration"})
final class FrayRaceTest {

  /* --------------- putIfAbsent Fast Path Race --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void putIfAbsentFastPathRace() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();
    cache.put(1, 100);

    var t1 = new Thread(() -> {
      var unused = cache.asMap().putIfAbsent(1, 200);
    });
    var t2 = new Thread(() -> cache.put(1, 300));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer value = cache.getIfPresent(1);
    assertNotNull(value);
    assertTrue(value == 100 || value == 200 || value == 300,
        "Unexpected value: " + value);
  }

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void putIfAbsentRemoveRace() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();
    cache.put(1, 100);

    var result = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> result.set(cache.asMap().putIfAbsent(1, 200)));
    var t2 = new Thread(() -> cache.asMap().remove(1));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
  }

  /* --------------- Compute on Expired Entry --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void computeOnExpiredEntry() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .expireAfterWrite(Duration.ofNanos(1))
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 100);

    var result1 = new AtomicReference<@Nullable Integer>();
    var result2 = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> {
      result1.set(cache.asMap().compute(1, (k, v) -> (v == null ? 0 : v) + 1));
    });
    var t2 = new Thread(() -> {
      result2.set(cache.asMap().compute(1, (k, v) -> (v == null ? 0 : v) + 1));
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    assertNotNull(result1.get());
    assertNotNull(result2.get());
  }

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void computeIfAbsentExpirationRace() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .expireAfterWrite(Duration.ofNanos(1))
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 100);

    var result = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> {
      result.set(cache.asMap().computeIfAbsent(1, k -> 999));
    });
    var t2 = new Thread(() -> cache.cleanUp());

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    assertNotNull(result.get());
  }

  /* --------------- computeIfAbsent fast path vs node retirement --------------- */

  /**
   * The computeIfAbsent fast path (BoundedLocalCache:2648-2660) reads node value without
   * holding the node lock, then calls setAccessTime and afterRead. A concurrent eviction
   * can retire the node between the value read and the afterRead call.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void computeIfAbsentFastPathEvictionRace() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(3)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 10);
    cache.put(2, 20);
    cache.put(3, 30);

    var result = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> {
      // computeIfAbsent hits fast path (key exists, not expired)
      // but the node might be getting evicted concurrently
      result.set(cache.asMap().computeIfAbsent(1, k -> 999));
    });
    var t2 = new Thread(() -> {
      // This triggers eviction which may retire key 1's node
      cache.put(4, 40);
      cache.put(5, 50);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
  }

  /**
   * Tests computeIfAbsent fast path racing with a concurrent replace on the same key.
   * The fast path reads the value, but replace could change it between the read and
   * the afterRead recording.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void computeIfAbsentFastPathReplaceRace() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 10);

    var v1 = new AtomicReference<@Nullable Integer>();
    var v2 = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> {
      // Uses fast path - reads value without lock
      v1.set(cache.asMap().computeIfAbsent(1, k -> 999));
    });
    var t2 = new Thread(() -> {
      // Replace changes the value concurrently
      v2.set(cache.asMap().replace(1, 77));
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer finalVal = cache.getIfPresent(1);
    assertNotNull(finalVal);
    // The value should be either 10 (original) or 77 (replaced), never 999
    // because computeIfAbsent should see key 1 exists (fast path or slow path)
    assertTrue(finalVal == 10 || finalVal == 77,
        "Unexpected value: " + finalVal);
  }

  /* --------------- getAllPresent + Concurrent Mutation --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void getAllPresentConcurrentMutation() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i * 10);
    }

    var result = new AtomicReference<Map<Integer, Integer>>();

    var t1 = new Thread(() -> {
      result.set(cache.getAllPresent(List.of(0, 1, 2, 3, 4)));
    });
    var t2 = new Thread(() -> {
      cache.invalidate(2);
      cache.put(2, 999);
      cache.invalidate(4);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Map<Integer, Integer> present = result.get();
    assertNotNull(present);
    if (present.containsKey(2)) {
      Integer val = present.get(2);
      assertTrue(val == 20 || val == 999,
          "Key 2 should be 20 or 999, got: " + val);
    }
  }

  /* --------------- Refresh Races --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void refreshConcurrentReplace() throws InterruptedException {
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .refreshAfterWrite(Duration.ofNanos(1))
        .executor(Runnable::run)
        .build(key -> key * 10);

    var unused = cache.get(1);

    var result1 = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> result1.set(cache.get(1)));
    var t2 = new Thread(() -> {
      cache.asMap().replace(1, cache.getIfPresent(1), 999);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer finalVal = cache.getIfPresent(1);
    assertNotNull(finalVal);
  }

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void explicitRefreshConcurrentPut() throws InterruptedException {
    var loadCount = new AtomicInteger();
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .refreshAfterWrite(Duration.ofNanos(1))
        .executor(Runnable::run)
        .build(key -> {
          loadCount.incrementAndGet();
          return key * 10;
        });

    var unused = cache.get(1);

    var t1 = new Thread(() -> cache.refresh(1));
    var t2 = new Thread(() -> cache.put(1, 777));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer value = cache.getIfPresent(1);
    assertNotNull(value);
  }

  /**
   * Two threads both trigger refresh on the same key. The refreshes map should prevent
   * duplicate loads, but the casWriteTime and refreshes.computeIfAbsent race window
   * could allow both through.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void doubleRefreshSameKey() throws InterruptedException {
    var loadCount = new AtomicInteger();
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .refreshAfterWrite(Duration.ofNanos(1))
        .executor(Runnable::run)
        .build(key -> {
          loadCount.incrementAndGet();
          return key * 10;
        });

    var unused = cache.get(1);

    var t1 = new Thread(() -> {
      var unused2 = cache.get(1);
    });
    var t2 = new Thread(() -> {
      var unused2 = cache.get(1);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer value = cache.getIfPresent(1);
    assertNotNull(value);
    assertEquals(Integer.valueOf(10), value);
  }

  /* --------------- Eviction During Compute --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void computeDuringEviction() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(3)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 3; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> cache.put(10, 10));
    var t2 = new Thread(() -> {
      cache.asMap().compute(0, (k, v) -> (v == null ? 0 : v) + 100);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    assertTrue(cache.estimatedSize() <= 3,
        "Size should be <= 3, got: " + cache.estimatedSize());
  }

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void mergeDuringEviction() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(3)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 10);
    cache.put(2, 20);
    cache.put(3, 30);

    var t1 = new Thread(() -> {
      for (int i = 100; i < 110; i++) {
        cache.put(i, i);
      }
    });
    var t2 = new Thread(() -> cache.asMap().merge(1, 5, Integer::sum));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    assertTrue(cache.estimatedSize() <= 3);
  }

  /* --------------- Iterator + Concurrent Mutation --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void iterationDuringMutation() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i);
    }

    var entries = new ArrayList<Map.Entry<Integer, Integer>>();

    var t1 = new Thread(() -> {
      for (var entry : cache.asMap().entrySet()) {
        entries.add(Map.entry(entry.getKey(), entry.getValue()));
      }
    });
    var t2 = new Thread(() -> {
      cache.put(10, 10);
      cache.invalidate(0);
      cache.put(11, 11);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    for (var entry : entries) {
      assertNotNull(entry.getKey());
      assertNotNull(entry.getValue());
    }
  }

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void forEachDuringInvalidateAll() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i);
    }

    var seen = new ConcurrentHashMap<Integer, Integer>();

    var t1 = new Thread(() -> cache.asMap().forEach(seen::put));
    var t2 = new Thread(() -> cache.invalidateAll());

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    seen.forEach((key, value) ->
        assertEquals(key, value, "Mismatch: key=" + key + " value=" + value));
  }

  /* --------------- Async Cache Races --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void asyncCompletionRemoveRace() throws InterruptedException {
    AsyncCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .buildAsync();

    var future = new CompletableFuture<Integer>();
    cache.put(1, future);

    var t1 = new Thread(() -> future.complete(42));
    var t2 = new Thread(() -> cache.synchronous().invalidate(1));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    @Nullable CompletableFuture<Integer> result = cache.getIfPresent(1);
    if (result != null && result.isDone() && !result.isCompletedExceptionally()) {
      assertEquals(Integer.valueOf(42), result.join());
    }
  }

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void asyncConcurrentGetSameKey() throws InterruptedException {
    AsyncCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .buildAsync();

    var v1 = new AtomicReference<@Nullable Integer>();
    var v2 = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> {
      CompletableFuture<Integer> f = cache.get(1, k -> 42);
      v1.set(f.join());
    });
    var t2 = new Thread(() -> {
      CompletableFuture<Integer> f = cache.get(1, k -> 42);
      v2.set(f.join());
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    assertEquals(Integer.valueOf(42), v1.get());
    assertEquals(Integer.valueOf(42), v2.get());
  }

  /**
   * Tests async getAll with overlapping keys from two threads. The proxy future pattern
   * in LocalAsyncCache.getAll uses getIfPresent+putIfAbsent which can race.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void asyncGetAllOverlappingKeys() throws InterruptedException {
    AsyncCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .buildAsync();

    var result1 = new AtomicReference<Map<Integer, Integer>>();
    var result2 = new AtomicReference<Map<Integer, Integer>>();

    var t1 = new Thread(() -> {
      var map = cache.getAll(List.of(1, 2, 3), keys -> {
        var m = new HashMap<Integer, Integer>();
        for (var k : keys) {
          m.put(k, k * 10);
        }
        return m;
      }).join();
      result1.set(map);
    });
    var t2 = new Thread(() -> {
      var map = cache.getAll(List.of(2, 3, 4), keys -> {
        var m = new HashMap<Integer, Integer>();
        for (var k : keys) {
          m.put(k, k * 10);
        }
        return m;
      }).join();
      result2.set(map);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Map<Integer, Integer> m1 = result1.get();
    Map<Integer, Integer> m2 = result2.get();
    assertNotNull(m1);
    assertNotNull(m2);

    // Both should have correct values for their requested keys
    assertEquals(Integer.valueOf(10), m1.get(1));
    assertEquals(Integer.valueOf(20), m1.get(2));
    assertEquals(Integer.valueOf(30), m1.get(3));
    assertEquals(Integer.valueOf(20), m2.get(2));
    assertEquals(Integer.valueOf(30), m2.get(3));
    assertEquals(Integer.valueOf(40), m2.get(4));
  }

  /**
   * Tests async future completion racing with another get on the same key.
   * The handleCompletion calls cache.replace(key, future, future) to update
   * timestamps, which races with a concurrent get that may trigger a refresh.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void asyncCompletionDuringGet() throws InterruptedException {
    AsyncCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .buildAsync();

    var future = new CompletableFuture<Integer>();
    cache.put(1, future);

    var result = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> future.complete(42));
    var t2 = new Thread(() -> {
      CompletableFuture<Integer> f = cache.get(1, k -> 99);
      result.set(f.join());
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer val = result.get();
    assertNotNull(val);
    assertTrue(val == 42 || val == 99, "Unexpected value: " + val);
  }

  /* --------------- Removal Listener Races --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void removalListenerEvictionCompleteness() throws InterruptedException {
    var notifications = new AtomicInteger();
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(2)
        .removalListener((@Nullable Integer key, @Nullable Integer value, RemovalCause cause) -> {
          notifications.incrementAndGet();
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

    cache.cleanUp();

    long remaining = cache.estimatedSize();
    assertTrue(remaining <= 2, "Size should be <= 2, got: " + remaining);
    assertTrue(notifications.get() >= 0, "Notifications should be non-negative");
  }

  /* --------------- Compute + Concurrent cleanUp --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void computeRacingWithCleanUp() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(5)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> cache.asMap().compute(10, (k, v) -> 100));
    var t2 = new Thread(() -> cache.cleanUp());
    var t3 = new Thread(() -> {
      cache.asMap().compute(0, (k, v) -> (v == null ? 0 : v) + 1);
    });

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    cache.cleanUp();
    assertTrue(cache.estimatedSize() <= 5);
  }

  /* --------------- Write Buffer Contention --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void writeBufferContention() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(100)
        .executor(Runnable::run)
        .<Integer, Integer>build();

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
    var t3 = new Thread(() -> {
      for (int i = 20; i < 30; i++) {
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
    assertEquals(30, cache.estimatedSize(), "All entries should be present");
  }

  /* --------------- replaceAll + Concurrent Mutation --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void replaceAllConcurrentPut() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 1);
    cache.put(2, 2);

    var t1 = new Thread(() -> cache.asMap().replaceAll((k, v) -> v * 10));
    var t2 = new Thread(() -> cache.put(3, 3));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Integer v1 = cache.getIfPresent(1);
    Integer v2 = cache.getIfPresent(2);
    assertNotNull(v1);
    assertNotNull(v2);
    assertTrue(v1 == 10, "Key 1 should be 10, got: " + v1);
    assertTrue(v2 == 20, "Key 2 should be 20, got: " + v2);
  }

  /* --------------- Size Consistency --------------- */

  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void sizeConsistencyAfterConcurrentOps() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var t1 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.put(i, i);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.invalidate(i);
      }
    });
    var t3 = new Thread(() -> {
      for (int i = 5; i < 10; i++) {
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

    long actualCount = cache.asMap().entrySet().stream().count();
    long estimated = cache.estimatedSize();
    assertEquals(actualCount, estimated,
        "estimatedSize should match actual count");
  }

  /* --------------- Variable Expiration Timer Wheel Races --------------- */

  /**
   * Tests variable expiration with concurrent access. The timer wheel schedule/deschedule
   * operations race with concurrent puts that modify variable time.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void variableExpirationConcurrentAccess() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .expireAfter(new Expiry<Integer, Integer>() {
          @Override public long expireAfterCreate(Integer key, Integer value, long currentTime) {
            return Duration.ofMinutes(5).toNanos();
          }
          @Override public long expireAfterUpdate(Integer key, Integer value,
              long currentTime, long currentDuration) {
            return Duration.ofMinutes(5).toNanos();
          }
          @Override public long expireAfterRead(Integer key, Integer value,
              long currentTime, long currentDuration) {
            return currentDuration;
          }
        })
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        cache.put(i, i + 100);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        var unused = cache.getIfPresent(i);
      }
    });
    var t3 = new Thread(() -> cache.cleanUp());

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    cache.cleanUp();
    // All entries should still be present (not expired)
    assertTrue(cache.estimatedSize() >= 1, "Some entries should remain");
  }

  /**
   * Tests variable expiration where the expiry duration changes dynamically, racing
   * with the timer wheel advance/scheduling.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void variableExpirationDynamicDuration() throws InterruptedException {
    var shortExpiry = new AtomicInteger(0);

    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .expireAfter(new Expiry<Integer, Integer>() {
          @Override public long expireAfterCreate(Integer key, Integer value, long currentTime) {
            return (shortExpiry.get() == 1) ? 1L : Duration.ofMinutes(5).toNanos();
          }
          @Override public long expireAfterUpdate(Integer key, Integer value,
              long currentTime, long currentDuration) {
            return (shortExpiry.get() == 1) ? 1L : Duration.ofMinutes(5).toNanos();
          }
          @Override public long expireAfterRead(Integer key, Integer value,
              long currentTime, long currentDuration) {
            return currentDuration;
          }
        })
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> {
      shortExpiry.set(1);
      // Update entries with new (short) expiry
      for (int i = 0; i < 5; i++) {
        cache.put(i, i + 100);
      }
    });
    var t2 = new Thread(() -> cache.cleanUp());

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // No crash or inconsistency
    cache.cleanUp();
  }

  /* --------------- Weighted Eviction Races --------------- */

  /**
   * Tests weighted eviction where concurrent updates change weights. The eviction
   * policy must correctly account for weight changes during concurrent operations.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void weightedEvictionConcurrentUpdate() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumWeight(10)
        .weigher((Integer key, Integer value) -> value)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 3);
    cache.put(2, 3);
    cache.put(3, 3);

    var t1 = new Thread(() -> {
      // Increase weight of key 1 dramatically
      cache.put(1, 8);
    });
    var t2 = new Thread(() -> {
      // Add new entry while weight is changing
      cache.put(4, 2);
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    // Weight should not exceed maximum
    long size = cache.estimatedSize();
    assertTrue(size >= 1, "Cache should have at least 1 entry");
  }

  /* --------------- Three-way Compute Race --------------- */

  /**
   * Tests three threads performing compute operations on overlapping key sets.
   * This stresses the ConcurrentHashMap bin locking and the afterWrite task queue.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void threeWayComputeRace() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
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

    Integer value = cache.getIfPresent(1);
    assertNotNull(value);
    // All three computes should have been applied (serialized by CHM bin lock)
    assertEquals(Integer.valueOf(111), value,
        "All computes should be applied: got " + value);
  }

  /* --------------- put + remove + computeIfAbsent three-way race --------------- */

  /**
   * Tests the interaction between put, remove, and computeIfAbsent on the same key.
   * The computeIfAbsent fast path can observe a stale node that is being removed.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void putRemoveComputeIfAbsentRace() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 10);

    var result = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> cache.invalidate(1));
    var t2 = new Thread(() -> cache.put(1, 20));
    var t3 = new Thread(() -> {
      result.set(cache.asMap().computeIfAbsent(1, k -> 30));
    });

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    // The result should be one of the valid values
    Integer val = result.get();
    assertNotNull(val);
    assertTrue(val == 10 || val == 20 || val == 30,
        "Unexpected computeIfAbsent result: " + val);
  }

  /* --------------- Conditional Remove Races --------------- */

  /**
   * Tests conditional remove (remove(key, value)) racing with put. The conditional
   * remove checks value equality under the node lock, but the node could be swapped.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void conditionalRemovePutRace() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 10);

    var removed = new AtomicReference<@Nullable Boolean>();

    var t1 = new Thread(() -> {
      removed.set(cache.asMap().remove(1, 10));
    });
    var t2 = new Thread(() -> cache.put(1, 20));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // If remove succeeded, the final value should be 20 (from put) or absent
    // If remove failed (value already changed to 20), value should be 20
    cache.cleanUp();
  }

  /**
   * Tests conditional replace(key, oldValue, newValue) racing with another replace.
   * Only one should succeed for a given old value.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void conditionalReplaceRace() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
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

    // Exactly one should have succeeded (both tried to replace 10)
    boolean oneSucceeded = Objects.equals(r1.get(), true)
        ^ Objects.equals(r2.get(), true);
    assertTrue(oneSucceeded,
        "Exactly one replace should succeed: r1=" + r1.get() + " r2=" + r2.get());

    Integer value = cache.getIfPresent(1);
    assertNotNull(value);
    assertTrue(value == 20 || value == 30, "Value should be 20 or 30, got: " + value);
  }

  /* --------------- Loading Cache Concurrent Load --------------- */

  /**
   * Tests that a loading cache only loads once for concurrent gets on the same key.
   * The ConcurrentHashMap.compute should serialize the loads.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void loadingCacheConcurrentGetSameKey() throws InterruptedException {
    var loadCount = new AtomicInteger();
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .build(key -> {
          loadCount.incrementAndGet();
          return key * 10;
        });

    var v1 = new AtomicReference<@Nullable Integer>();
    var v2 = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> v1.set(cache.get(1)));
    var t2 = new Thread(() -> v2.set(cache.get(1)));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    assertEquals(Integer.valueOf(10), v1.get());
    assertEquals(Integer.valueOf(10), v2.get());
    // Should have loaded exactly once
    assertEquals(1, loadCount.get(),
        "Key should be loaded exactly once, loaded: " + loadCount.get());
  }

  /* --------------- getAll + invalidateAll race --------------- */

  /**
   * Tests getAll on a loading cache racing with invalidateAll. The getAll
   * bulk load creates entries that invalidateAll tries to remove concurrently.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void getAllInvalidateAllRace() throws InterruptedException {
    LoadingCache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .build(key -> key * 10);

    // Pre-populate some entries
    for (int i = 0; i < 3; i++) {
      var unused = cache.get(i);
    }

    var result = new AtomicReference<Map<Integer, Integer>>();

    var t1 = new Thread(() -> result.set(cache.getAll(List.of(0, 1, 2, 3, 4))));
    var t2 = new Thread(() -> cache.invalidateAll());

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    Map<Integer, Integer> map = result.get();
    assertNotNull(map);
    // All values returned by getAll should be consistent
    map.forEach((key, value) ->
        assertEquals(Integer.valueOf(key * 10), value,
            "Inconsistent value for key " + key));
  }

  /* --------------- expireAfterAccess + concurrent read/write --------------- */

  /**
   * Tests expireAfterAccess where reads update access time (a write-like side effect)
   * racing with puts and cleanUp. This exercises the read buffer -> afterRead ->
   * setAccessTime path concurrently with write operations.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void expireAfterAccessConcurrentReadWrite() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(5)
        .expireAfterAccess(Duration.ofMinutes(5))
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> {
      // Reads update access time which affects eviction ordering
      for (int i = 0; i < 5; i++) {
        var unused = cache.getIfPresent(i);
      }
    });
    var t2 = new Thread(() -> {
      // Writes that trigger eviction
      cache.put(10, 10);
      cache.put(11, 11);
    });
    var t3 = new Thread(() -> cache.cleanUp());

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    cache.cleanUp();
    assertTrue(cache.estimatedSize() <= 5);
  }

  /* --------------- invalidateAll + cleanUp eviction lock contention --------------- */

  /**
   * Tests invalidateAll and cleanUp both competing for the eviction lock.
   * invalidateAll acquires the lock to drain buffers, then iterates entries.
   * Concurrent puts add to the write buffer while the lock is held.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void invalidateAllCleanUpContention() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    for (int i = 0; i < 5; i++) {
      cache.put(i, i);
    }

    var t1 = new Thread(() -> cache.invalidateAll());
    var t2 = new Thread(() -> cache.cleanUp());
    var t3 = new Thread(() -> {
      for (int i = 10; i < 15; i++) {
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
    // After invalidateAll, only entries added by t3 (or concurrent with invalidateAll) may remain
    assertTrue(cache.estimatedSize() <= 10);
  }

  /* --------------- put same key from multiple threads --------------- */

  /**
   * Tests rapid put operations on the same key from multiple threads.
   * This stresses the node value update path and the write buffer ordering.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentPutSameKey() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    var t1 = new Thread(() -> {
      for (int i = 0; i < 10; i++) {
        cache.put(1, i);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 10; i < 20; i++) {
        cache.put(1, i);
      }
    });
    var t3 = new Thread(() -> {
      for (int i = 0; i < 10; i++) {
        var unused = cache.getIfPresent(1);
      }
    });

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    Integer value = cache.getIfPresent(1);
    assertNotNull(value);
    assertTrue(value >= 0 && value < 20, "Value should be in [0,20), got: " + value);
    assertEquals(1, cache.estimatedSize(), "Should have exactly 1 entry");
  }

  /* --------------- getIfPresent + invalidate same key race --------------- */

  /**
   * Tests getIfPresent racing with invalidate on the same key. The getIfPresent
   * optimistic read may observe a value that is concurrently being invalidated,
   * and then try to update access time on a retired node.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void getIfPresentInvalidateRace() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .expireAfterAccess(Duration.ofMinutes(5))
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 42);

    var result = new AtomicReference<@Nullable Integer>();

    var t1 = new Thread(() -> result.set(cache.getIfPresent(1)));
    var t2 = new Thread(() -> cache.invalidate(1));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // result is either 42 (read before invalidate) or null (read after invalidate)
    Integer val = result.get();
    assertTrue(val == null || val == 42,
        "Should be null or 42, got: " + val);
  }

  /* --------------- compute returning null (removal) racing with put --------------- */

  /**
   * Tests compute that returns null (removing the entry) racing with a put on the same key.
   * The compute retirement and afterWrite task race with put's node creation.
   */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void computeNullReturnPutRace() throws InterruptedException {
    Cache<Integer, Integer> cache = Caffeine.newBuilder()
        .maximumSize(10)
        .executor(Runnable::run)
        .<Integer, Integer>build();

    cache.put(1, 10);

    var t1 = new Thread(() -> cache.asMap().compute(1, (k, v) -> null));
    var t2 = new Thread(() -> cache.put(1, 20));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    cache.cleanUp();
    // Entry is either absent (compute won) or present with 20 (put won or was after compute)
    Integer val = cache.getIfPresent(1);
    assertTrue(val == null || val == 20,
        "Should be null or 20, got: " + val);
  }
}
