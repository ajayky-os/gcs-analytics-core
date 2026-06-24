/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.core.optimizer;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsCacheOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.IntFunction;

/** A {@link FormatOptimizer} that caches and serves small objects in a private buffer. */
public class SmallObjectOptimizer implements FormatOptimizer {

  private static final Set<String> DATA_FILE_EXTENSIONS = Set.of(".parquet", ".orc");

  private final GcsCacheOptions cacheOptions;
  private final Telemetry telemetry;

  private AnalyticsCacheManager cacheManager;
  private long fileSize = -1;
  private GcsItemId currentItemId;

  public SmallObjectOptimizer(GcsCacheOptions cacheOptions, Telemetry telemetry) {
    this.cacheOptions = checkNotNull(cacheOptions, "cacheOptions cannot be null");
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
  }

  @Override
  public boolean isApplicable(GcsItemId itemId) {
    return cacheOptions.getSmallObjectCacheMaxSizeBytes() > 0
        && itemId
            .getObjectName()
            .map(
                name ->
                    DATA_FILE_EXTENSIONS.stream().anyMatch(ext -> name.toLowerCase().endsWith(ext)))
            .orElse(false);
  }

  @Override
  public boolean isApplicable(GcsFileInfo fileInfo) {
    return isApplicable(fileInfo.getItemInfo().getItemId())
        && fileInfo.getItemInfo().getSize() <= cacheOptions.getSmallObjectCacheMaxSizeBytes();
  }

  @Override
  public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager) {
    this.currentItemId = itemId;
    this.cacheManager = cacheManager;
  }

  @Override
  public void onOpen(GcsFileInfo fileInfo, AnalyticsCacheManager cacheManager) {
    this.currentItemId = fileInfo.getItemInfo().getItemId();
    this.cacheManager = cacheManager;
    this.fileSize = fileInfo.getItemInfo().getSize();
  }

  @Override
  public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel source)
      throws IOException {
    if (fileSize == -1) {
      fileSize = source.size();
    }

    if (fileSize > cacheOptions.getSmallObjectCacheMaxSizeBytes()) {
      return 0;
    }

    if (position >= fileSize) {
      return -1;
    }

    ByteBuffer prefetchBuffer;
    try {
      prefetchBuffer = cacheManager.getSmallObject(currentItemId, id -> ensurePrefetched(source));
    } catch (IOException e) {
      telemetry.recordMetric(Metric.SMALL_OBJECT_CACHE_MISS, 1L, Collections.emptyMap());
      throw e;
    }

    telemetry.recordMetric(Metric.SMALL_OBJECT_CACHE_HIT, 1L, Collections.emptyMap());
    return serveFromCache(position, dst, prefetchBuffer);
  }

  @Override
  public List<GcsObjectRange> readVectored(
      List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate) throws IOException {
    if (fileSize == -1 || fileSize > cacheOptions.getSmallObjectCacheMaxSizeBytes()) {
      return ranges;
    }

    ByteBuffer prefetchBuffer;
    try {
      // Don't force a load if it isn't cached during vectored read, just fail fast and return
      // ranges
      // Since vectored read is async, forcing a sequential read here might defeat the purpose
      // For now, let's trigger it.
      prefetchBuffer =
          cacheManager.getSmallObject(
              currentItemId,
              id -> {
                throw new IOException("Cache miss during vectored read");
              });
    } catch (IOException e) {
      return ranges; // Cannot satisfy yet
    }

    telemetry.recordMetric(Metric.SMALL_OBJECT_CACHE_HIT, ranges.size(), Collections.emptyMap());
    for (GcsObjectRange range : ranges) {
      ByteBuffer dest = allocate.apply(range.getLength());
      int bytesRead = serveFromCache(range.getOffset(), dest, prefetchBuffer);
      if (bytesRead < range.getLength()) {
        range
            .getByteBufferFuture()
            .completeExceptionally(
                new EOFException(
                    String.format("Error while populating range: %s, unexpected EOF", range)));
      } else {
        dest.flip();
        range.getByteBufferFuture().complete(dest);
      }
    }
    return Collections.emptyList();
  }

  private ByteBuffer ensurePrefetched(VectoredSeekableByteChannel source) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
    long originalPosition = source.position();
    try {
      source.position(0);
      while (buffer.hasRemaining()) {
        if (source.read(buffer) == -1) {
          throw new IOException("Unexpected EOF encountered while reading small object.");
        }
      }
      buffer.flip();
      return buffer;
    } finally {
      source.position(originalPosition);
    }
  }

  private int serveFromCache(long currPosition, ByteBuffer dst, ByteBuffer prefetchBuffer) {
    if (currPosition >= fileSize) {
      return -1;
    }

    ByteBuffer view = prefetchBuffer.duplicate();
    view.position((int) currPosition);

    int bytesToRead = Math.min(dst.remaining(), view.remaining());
    view.limit(view.position() + bytesToRead);
    dst.put(view);

    return bytesToRead;
  }
}
