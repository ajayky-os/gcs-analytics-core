/*
 * Copyright 2025 Google LLC
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
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableList;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.IntFunction;

/** A {@link FormatOptimizer} that caches and serves GCS object footers (e.g., for Parquet). */
public class GcsFooterOptimizer implements FormatOptimizer {

  private static final int LARGE_FILE_SIZE_THRESHOLD = 1024 * 1024 * 1024; // 1 GB

  private final GcsReadOptions readOptions;
  private final Telemetry telemetry;
  private final GcsFileSystem gcsFileSystem;

  private AnalyticsCacheManager cacheManager;
  private GcsItemId gcsItemId;
  private long fileSize = -1;
  private long prefetchSize = -1;

  private volatile CompletableFuture<ByteBuffer> footerFuture;

  public GcsFooterOptimizer(
      GcsReadOptions readOptions, Telemetry telemetry, GcsFileSystem gcsFileSystem) {
    this.readOptions = checkNotNull(readOptions, "readOptions cannot be null");
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
    this.gcsFileSystem = checkNotNull(gcsFileSystem, "gcsFileSystem cannot be null");
  }

  @Override
  public boolean isApplicable(GcsItemId itemId) {
    return readOptions.isFooterPrefetchEnabled();
  }

  @Override
  public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager) {
    this.gcsItemId = itemId;
    this.cacheManager = cacheManager;
  }

  @Override
  public void onOpen(GcsFileInfo fileInfo, AnalyticsCacheManager cacheManager) {
    this.gcsItemId = fileInfo.getItemInfo().getItemId();
    this.cacheManager = cacheManager;
    this.fileSize = fileInfo.getItemInfo().getSize();
    this.prefetchSize = calculatePrefetchSize(fileSize, readOptions);

    if (prefetchSize > 0) {
      this.footerFuture =
          cacheManager.getFooterFuture(
              gcsItemId,
              itemId ->
                  CompletableFuture.supplyAsync(
                      () -> {
                        try (VectoredSeekableByteChannel channel =
                            gcsFileSystem.open(itemId, readOptions)) {
                          return loadFooter(channel, fileSize, (int) prefetchSize);
                        } catch (IOException e) {
                          throw new CompletionException(e);
                        }
                      },
                      gcsFileSystem.getExecutorService()));
    }
  }

  @Override
  public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel source)
      throws IOException {
    if (fileSize == -1) {
      fileSize = source.size();
      prefetchSize = calculatePrefetchSize(fileSize, readOptions);
    }

    if (prefetchSize <= 0 || position < (fileSize - prefetchSize)) {
      return 0;
    }

    if (position >= fileSize) {
      return -1;
    }

    ByteBuffer footer = AnalyticsCacheManager.join(getOrInitFooterFuture(source));
    telemetry.recordMetric(Metric.FOOTER_CACHE_HIT, 1L, Collections.emptyMap());

    return serveFromFooter(footer, position, dst);
  }

  @Override
  public List<GcsObjectRange> readVectored(
      List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate) throws IOException {
    if (prefetchSize <= 0 || fileSize == -1) {
      return ranges;
    }

    CompletableFuture<ByteBuffer> future = footerFuture;
    if (future == null) {
      future = cacheManager.getFooterFutureIfPresent(gcsItemId);
      if (future == null) {
        return ranges;
      }
    }

    ByteBuffer footer;
    try {
      footer = future.join();
    } catch (CompletionException e) {
      return ranges;
    }

    if (footer == null) {
      return ranges;
    }

    ImmutableList.Builder<GcsObjectRange> remaining = ImmutableList.builder();
    for (GcsObjectRange range : ranges) {
      long offset = range.getOffset();

      if (offset >= fileSize) {
        range
            .getByteBufferFuture()
            .completeExceptionally(
                new EOFException(
                    String.format(
                        "Offset %d is beyond file size %d for range: %s",
                        offset, fileSize, range)));
        continue;
      }

      if (offset >= (fileSize - prefetchSize)) {
        telemetry.recordMetric(Metric.FOOTER_CACHE_HIT, 1L, Collections.emptyMap());
        ByteBuffer dest = allocate.apply(range.getLength());
        int bytesRead = serveFromFooter(footer, offset, dest);
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
      } else {
        remaining.add(range);
      }
    }

    return remaining.build();
  }

  private CompletableFuture<ByteBuffer> getOrInitFooterFuture(VectoredSeekableByteChannel source) {
    CompletableFuture<ByteBuffer> future = footerFuture;
    if (future == null) {
      synchronized (this) {
        future = footerFuture;
        if (future == null) {
          this.footerFuture =
              future =
                  cacheManager.getFooterFuture(
                      gcsItemId,
                      itemId -> {
                        telemetry.recordMetric(
                            Metric.FOOTER_CACHE_MISS, 1L, Collections.emptyMap());
                        try {
                          return CompletableFuture.completedFuture(
                              loadFooter(source, fileSize, (int) prefetchSize));
                        } catch (IOException e) {
                          CompletableFuture<ByteBuffer> failed = new CompletableFuture<>();
                          failed.completeExceptionally(e);
                          return failed;
                        }
                      });
        }
      }
    }
    return future;
  }

  private static ByteBuffer loadFooter(
      VectoredSeekableByteChannel channel, long fileSize, int prefetchSize) throws IOException {
    long startPosition = fileSize - prefetchSize;
    int bufferSize = (int) (fileSize - startPosition);
    ByteBuffer cacheBuffer = ByteBuffer.allocate(bufferSize);
    long originalPosition = channel.position();
    try {
      channel.position(startPosition);
      while (cacheBuffer.hasRemaining()) {
        int read = channel.read(cacheBuffer);
        if (read == -1) {
          throw new EOFException("Unexpected EOF encountered while reading footer.");
        }
        if (read == 0) {
          throw new IOException("Infinite loop detected: channel.read() returned 0 bytes.");
        }
      }
      cacheBuffer.flip();
      return cacheBuffer;
    } finally {
      channel.position(originalPosition);
    }
  }

  private int serveFromFooter(ByteBuffer footer, long position, ByteBuffer dst) {
    ByteBuffer footerLean = footer.duplicate();
    int readStartPosition = (int) (position - (fileSize - prefetchSize));
    if (readStartPosition < 0 || readStartPosition >= footerLean.capacity()) {
      return 0;
    }
    footerLean.position(readStartPosition);

    if (footerLean.remaining() == 0) {
      return -1;
    }

    int bytesToRead = Math.min(dst.remaining(), footerLean.remaining());
    footerLean.limit(footerLean.position() + bytesToRead);
    dst.put(footerLean);
    return bytesToRead;
  }

  private static long calculatePrefetchSize(long fileSize, GcsReadOptions readOptions) {
    if (!readOptions.isFooterPrefetchEnabled()
        && readOptions.getSmallObjectCacheSize() < fileSize) {
      return 0;
    }
    if (readOptions.getSmallObjectCacheSize() >= fileSize) {
      return fileSize;
    }
    return fileSize > LARGE_FILE_SIZE_THRESHOLD
        ? Math.min(readOptions.getFooterPrefetchSizeLargeFile(), fileSize)
        : Math.min(readOptions.getFooterPrefetchSizeSmallFile(), fileSize);
  }
}
