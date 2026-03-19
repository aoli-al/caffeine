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
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/**
 * Controlled concurrency tests using Fray. These tests explore different thread interleavings
 * to find race conditions in core cache operations.
 *
 * @author github.com/aoli-al (Ao Li)
 */
@ExtendWith(FrayTestExtension.class)
@SuppressWarnings("IdentityConversion")
final class CacheTest {

  private static Cache<Integer, Integer> newCache(long maximumSize) {
    return Caffeine.newBuilder()
      .maximumSize(maximumSize)
      .executor(Runnable::run)
      .<Integer, Integer>build();
  }

  /* --------------- Basic Cache Operations --------------- */

  /** Tests concurrent put and get operations on a bounded cache. */
  @FrayTest(
    iterations = 10000,
    // If JaCoCo is enabled, disable class reloading across iterations.
    // JaCoCo attaches its instrumentation agent through static class constructor
    // and may reinstrument classes, which introduces significant overhead.
    resetClassLoaderPerIteration = false
  )
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
          assertEquals(i * 10, value, "Unexpected value for key " + i);
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
}
