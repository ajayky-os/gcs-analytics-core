/*
 * Copyright 2026 Google LLC
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

package com.google.cloud.gcs.analyticscore.core.optimizer;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An optimizer that observes read patterns, updates the global heuristic registry, and performs
 * asynchronous predictive prefetching to eliminate network latency.
 */
public class PredictiveReadOptimizer implements FormatOptimizer {

  private final GlobalReadPatternRegistry registry = GlobalReadPatternRegistry.getInstance();
  private long lastOffset = -1;
  private GcsItemId currentItemId;

  // Stream-local buffer to hold async prefetched data
  private final ConcurrentHashMap<Long, ByteBuffer> prefetchBuffer = new ConcurrentHashMap<>();

  @Override
  public boolean isApplicable(GcsItemId itemId) {
    return true;
  }

  @Override
  public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager) {
    this.currentItemId = itemId;
  }

  @Override
  public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel delegate)
      throws IOException {
    // 1. Phase 4: Cache Interception (Zero-Latency Hit)
    ByteBuffer cached = prefetchBuffer.remove(position);
    if (cached != null) {
      int size = Math.min(dst.remaining(), cached.remaining());
      byte[] slice = new byte[size];
      cached.get(slice);
      dst.put(slice);
      lastOffset = position;
      return size;
    }

    // 2. Phase 2: Observation (Record what we are about to read)
    if (lastOffset != -1) {
      registry.recordTransition(currentItemId, lastOffset, position);
    }
    lastOffset = position;

    // 3. Phase 3: Prediction & Asynchronous Prefetching
    Long predictedNext = registry.predictNext(currentItemId, position);
    if (predictedNext != null && !prefetchBuffer.containsKey(predictedNext)) {
      // Fire an async fetch using a background thread.
      @SuppressWarnings("FutureReturnValueIgnored")
      var unused =
          CompletableFuture.runAsync(
              () -> {
                try {
                  // POC: Allocate 1MB for the prefetch
                  ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
                  delegate.position(predictedNext);
                  delegate.read(buf);
                  buf.flip();
                  prefetchBuffer.put(predictedNext, buf);
                } catch (Exception e) {
                  // Ignore errors in async prefetch for POC
                }
              });
    }

    // 4. Return 0 to let the delegate perform the actual network read for the current request
    return 0;
  }

  @Override
  public java.util.List<com.google.cloud.gcs.analyticscore.client.GcsObjectRange> readVectored(
      java.util.List<com.google.cloud.gcs.analyticscore.client.GcsObjectRange> ranges,
      java.util.function.IntFunction<ByteBuffer> allocate)
      throws IOException {

    java.util.List<com.google.cloud.gcs.analyticscore.client.GcsObjectRange> unfulfilled =
        new java.util.ArrayList<>();

    for (com.google.cloud.gcs.analyticscore.client.GcsObjectRange range : ranges) {
      long position = range.getOffset();

      // 1. Cache Interception
      ByteBuffer cached = prefetchBuffer.remove(position);
      if (cached != null && cached.remaining() >= range.getLength()) {
        ByteBuffer resultBuf = allocate.apply(range.getLength());
        byte[] slice = new byte[range.getLength()];
        cached.get(slice);
        resultBuf.put(slice);
        resultBuf.flip();
        range.getByteBufferFuture().complete(resultBuf);
      } else {
        unfulfilled.add(range);
      }

      // 2. Observation (Update global heuristic for vectored reads)
      if (lastOffset != -1) {
        registry.recordTransition(currentItemId, lastOffset, position);
      }
      lastOffset = position;
    }

    return unfulfilled;
  }
}
