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
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An optimizer that observes read patterns, updates the global heuristic registry, and performs
 * asynchronous predictive prefetching to eliminate network latency.
 */
public class PredictiveReadOptimizer implements FormatOptimizer {

  private final GlobalReadPatternRegistry registry = GlobalReadPatternRegistry.getInstance();
  private final Telemetry telemetry;
  private long lastOpIdentifier = -1;
  private GcsItemId currentItemId;

  // Stream-local buffer to hold async prefetched data futures
  private final ConcurrentHashMap<Long, CompletableFuture<ByteBuffer>> prefetchBuffer =
      new ConcurrentHashMap<>();

  public PredictiveReadOptimizer(Telemetry telemetry) {
    this.telemetry = telemetry;
  }

  @Override
  public boolean isApplicable(GcsItemId itemId) {
    return true;
  }

  @Override
  public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager) {
    this.currentItemId = itemId;
  }

  @Override
  public void onOpen(GcsFileInfo fileInfo, AnalyticsCacheManager cacheManager) {
    this.currentItemId = fileInfo.getItemInfo().getItemId();
  }

  @Override
  public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel delegate)
      throws IOException {
    // 1. Phase 4: Cache Interception (Zero-Latency Hit or wait for in-progress fetch)
    CompletableFuture<ByteBuffer> futureCached = prefetchBuffer.remove(position);
    if (futureCached != null) {
      try {
        ByteBuffer cached = futureCached.join();
        int size = Math.min(dst.remaining(), cached.remaining());
        byte[] slice = new byte[size];
        cached.get(slice);
        dst.put(slice);
        lastOpIdentifier = position;
        telemetry.recordMetric(Metric.PREDICTIVE_PREFETCH_HIT, 1L, Collections.emptyMap());
        return size;
      } catch (Exception e) {
        // Fallback to network read if prefetch failed
      }
    }

    telemetry.recordMetric(Metric.PREDICTIVE_PREFETCH_MISS, 1L, Collections.emptyMap());

    // 2. Phase 2: Observation (Record what we are about to read)
    if (lastOpIdentifier != -1) {
      registry.recordVectorTransition(
          currentItemId,
          lastOpIdentifier,
          Collections.singletonList(
              new GlobalReadPatternRegistry.PredictedRange(position, dst.remaining())));
    }
    lastOpIdentifier = position;

    // Note: We safely execute predictive prefetching in the background during scalar reads
    // by using delegate.readVectored(), which does not corrupt the channel's position state.
    if (lastOpIdentifier != -1) {
      List<GlobalReadPatternRegistry.PredictedRange> predictedNextVector =
          registry.predictNextVector(currentItemId, lastOpIdentifier);

      if (predictedNextVector != null) {
        List<GcsObjectRange> unfulfilled = new ArrayList<>();
        long totalPrefetchBytes = 0;
        long prefetchRangesCount = 0;

        for (GlobalReadPatternRegistry.PredictedRange p : predictedNextVector) {
          if (!prefetchBuffer.containsKey(p.offset)) {
            CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
            prefetchBuffer.put(p.offset, future);
            GcsObjectRange predictedRange =
                GcsObjectRange.builder()
                    .setOffset(p.offset)
                    .setLength(p.length)
                    .setByteBufferFuture(future)
                    .build();
            unfulfilled.add(predictedRange);
            totalPrefetchBytes += p.length;
            prefetchRangesCount++;
          }
        }

        if (!unfulfilled.isEmpty()) {
          if (prefetchRangesCount > 0) {
            telemetry.recordMetric(
                Metric.PREDICTIVE_PREFETCH_RANGES_COUNT,
                prefetchRangesCount,
                Collections.emptyMap());
            telemetry.recordMetric(
                Metric.PREDICTIVE_PREFETCH_BYTES, totalPrefetchBytes, Collections.emptyMap());
          }
          // Launch safely in the background
          CompletableFuture.runAsync(
              () -> {
                try {
                  delegate.readVectored(unfulfilled, ByteBuffer::allocate);
                } catch (Exception e) {
                  // Ignore background fetch failure
                }
              });
        }
      }
    }

    return 0;
  }

  @Override
  public List<GcsObjectRange> readVectored(
      List<GcsObjectRange> ranges, java.util.function.IntFunction<ByteBuffer> allocate)
      throws IOException {

    if (ranges.isEmpty()) {
      return ranges;
    }

    List<GcsObjectRange> unfulfilled = new ArrayList<>();
    long currentOpIdentifier = ranges.get(0).getOffset();

    for (GcsObjectRange range : ranges) {
      long position = range.getOffset();

      // 1. Cache Interception (Zero-Latency Hit or wait for in-progress fetch)
      CompletableFuture<ByteBuffer> futureCached = prefetchBuffer.remove(position);
      if (futureCached != null) {
        try {
          ByteBuffer cached = futureCached.join();
          if (cached.remaining() >= range.getLength()) {
            ByteBuffer resultBuf = allocate.apply(range.getLength());
            byte[] slice = new byte[range.getLength()];
            cached.get(slice);
            resultBuf.put(slice);
            resultBuf.flip();
            range.getByteBufferFuture().complete(resultBuf);
            telemetry.recordMetric(Metric.PREDICTIVE_PREFETCH_HIT, 1L, Collections.emptyMap());
          } else {
            telemetry.recordMetric(Metric.PREDICTIVE_PREFETCH_MISS, 1L, Collections.emptyMap());
            unfulfilled.add(range);
          }
        } catch (Exception e) {
          telemetry.recordMetric(Metric.PREDICTIVE_PREFETCH_MISS, 1L, Collections.emptyMap());
          unfulfilled.add(range);
        }
      } else {
        telemetry.recordMetric(Metric.PREDICTIVE_PREFETCH_MISS, 1L, Collections.emptyMap());
        unfulfilled.add(range);
      }
    }

    // 2. Observation (Update global heuristic for vectored reads)
    if (lastOpIdentifier != -1) {
      List<GlobalReadPatternRegistry.PredictedRange> observedRanges = new ArrayList<>();
      for (GcsObjectRange range : ranges) {
        observedRanges.add(
            new GlobalReadPatternRegistry.PredictedRange(range.getOffset(), range.getLength()));
      }
      registry.recordVectorTransition(currentItemId, lastOpIdentifier, observedRanges);
    }
    lastOpIdentifier = currentOpIdentifier;

    // 3. Prediction & Piggyback Prefetching
    List<GlobalReadPatternRegistry.PredictedRange> predictedNextVector =
        registry.predictNextVector(currentItemId, lastOpIdentifier);

    if (predictedNextVector != null) {
      long totalPrefetchBytes = 0;
      long prefetchRangesCount = 0;
      for (GlobalReadPatternRegistry.PredictedRange p : predictedNextVector) {
        if (!prefetchBuffer.containsKey(p.offset)) {
          CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
          prefetchBuffer.put(p.offset, future);
          GcsObjectRange predictedRange =
              GcsObjectRange.builder()
                  .setOffset(p.offset)
                  .setLength(p.length)
                  .setByteBufferFuture(future)
                  .build();
          unfulfilled.add(predictedRange);
          totalPrefetchBytes += p.length;
          prefetchRangesCount++;
        }
      }
      if (prefetchRangesCount > 0) {
        telemetry.recordMetric(
            Metric.PREDICTIVE_PREFETCH_RANGES_COUNT, prefetchRangesCount, Collections.emptyMap());
        telemetry.recordMetric(
            Metric.PREDICTIVE_PREFETCH_BYTES, totalPrefetchBytes, Collections.emptyMap());
      }
    }

    return unfulfilled;
  }
}
