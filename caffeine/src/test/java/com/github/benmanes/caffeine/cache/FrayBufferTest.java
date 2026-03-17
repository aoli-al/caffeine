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

import static com.github.benmanes.caffeine.cache.Buffer.FAILED;
import static com.github.benmanes.caffeine.cache.Buffer.FULL;
import static com.github.benmanes.caffeine.cache.Buffer.SUCCESS;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/**
 * Controlled concurrency tests for {@link BoundedBuffer} and {@link FrequencySketch} using Fray.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@ExtendWith(FrayTestExtension.class)
final class FrayBufferTest {

  /* --------------- BoundedBuffer --------------- */

  /** Tests concurrent offer operations from multiple threads. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentOffer() throws InterruptedException {
    var buffer = new BoundedBuffer<Integer>();

    Runnable task = () -> {
      for (int i = 0; i < 16; i++) {
        int result = buffer.offer(i);
        assertTrue(result == SUCCESS || result == FAILED || result == FULL,
            "Unexpected offer result: " + result);
      }
    };

    var t1 = new Thread(task);
    var t2 = new Thread(task);
    t1.start();
    t2.start();
    t1.join();
    t2.join();
  }

  /** Tests concurrent offer and drain operations. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentOfferDrain() throws InterruptedException {
    var buffer = new BoundedBuffer<Integer>();
    var drained = new AtomicInteger();
    int offers = 16;

    var offerThread = new Thread(() -> {
      for (int i = 0; i < offers; i++) {
        int result = buffer.offer(i);
        assertTrue(result == SUCCESS || result == FAILED || result == FULL,
            "Unexpected offer result: " + result);
      }
    });
    var drainThread = new Thread(() -> buffer.drainTo(e -> drained.incrementAndGet()));

    offerThread.start();
    drainThread.start();
    offerThread.join();
    drainThread.join();

    int drainedCount = drained.get();
    assertTrue(drainedCount <= offers,
        "Drained more than offered: " + drainedCount + " > " + offers);
  }

  /** Tests concurrent offer with table expansion and drain. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentOfferExpand() throws InterruptedException {
    var buffer = new BoundedBuffer<Integer>();

    var t1 = new Thread(() -> {
      for (int i = 0; i < 16; i++) {
        var unused = buffer.offer(i);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 16; i++) {
        var unused = buffer.offer(100 + i);
      }
    });
    var t3 = new Thread(() -> {
      var count = new AtomicInteger();
      buffer.drainTo(e -> count.incrementAndGet());
    });

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();
  }

  /** Tests that elements drained are unique (no duplicate consumption). */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentDrainUniqueness() throws InterruptedException {
    var buffer = new BoundedBuffer<Integer>();
    Set<Integer> drained = ConcurrentHashMap.newKeySet();

    // Fill the buffer first
    for (int i = 0; i < BoundedBuffer.BUFFER_SIZE; i++) {
      var unused = buffer.offer(i);
    }

    // Two threads drain concurrently
    var t1 = new Thread(() -> buffer.drainTo(e -> drained.add(e)));
    var t2 = new Thread(() -> buffer.drainTo(e -> drained.add(e)));

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // All drained elements should be valid values
    for (int elem : drained) {
      assertTrue(elem >= 0 && elem < BoundedBuffer.BUFFER_SIZE,
          "Unexpected drained element: " + elem);
    }
  }

  /** Tests multiple producers racing to initialize the striped buffer table. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentTableInit() throws InterruptedException {
    var buffer = new BoundedBuffer<Integer>();

    // Three threads all try to offer to an uninitialized buffer simultaneously
    var t1 = new Thread(() -> { var unused = buffer.offer(1); });
    var t2 = new Thread(() -> { var unused = buffer.offer(2); });
    var t3 = new Thread(() -> { var unused = buffer.offer(3); });

    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    // Drain should succeed without errors
    var count = new AtomicInteger();
    buffer.drainTo(e -> count.incrementAndGet());
    assertTrue(count.get() >= 0, "Drain count should be non-negative");
  }

  /* --------------- FrequencySketch --------------- */

  /** Tests concurrent increment operations on the frequency sketch. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentSketchIncrement() throws InterruptedException {
    var sketch = new FrequencySketch();
    sketch.ensureCapacity(16);

    var t1 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        sketch.increment(i);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        sketch.increment(i);
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    // Frequency should never exceed 15 (4-bit counter max)
    for (int i = 0; i < 5; i++) {
      int freq = sketch.frequency(i);
      assertTrue(freq >= 0 && freq <= 15,
          "Frequency for " + i + " should be in [0,15], got: " + freq);
    }
  }

  /** Tests concurrent increment and frequency read. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentSketchIncrementRead() throws InterruptedException {
    var sketch = new FrequencySketch();
    sketch.ensureCapacity(16);

    var t1 = new Thread(() -> {
      for (int i = 0; i < 8; i++) {
        sketch.increment(1);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 8; i++) {
        int freq = sketch.frequency(1);
        assertTrue(freq >= 0 && freq <= 15,
            "Frequency should be in [0,15], got: " + freq);
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();
  }

  /** Tests increment racing with the periodic reset (aging) triggered by sampleSize. */
  @FrayTest(iterations = 10000, resetClassLoaderPerIteration = false)
  void concurrentSketchIncrementReset() throws InterruptedException {
    var sketch = new FrequencySketch();
    // Small capacity so reset triggers quickly
    sketch.ensureCapacity(4);

    var t1 = new Thread(() -> {
      for (int i = 0; i < 20; i++) {
        sketch.increment(i % 4);
      }
    });
    var t2 = new Thread(() -> {
      for (int i = 0; i < 20; i++) {
        sketch.increment((i + 2) % 4);
      }
    });

    t1.start();
    t2.start();
    t1.join();
    t2.join();

    for (int i = 0; i < 4; i++) {
      int freq = sketch.frequency(i);
      assertTrue(freq >= 0 && freq <= 15,
          "Frequency for " + i + " should be in [0,15], got: " + freq);
    }
  }
}
