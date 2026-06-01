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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GcsFooterOptimizerTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("b").setObjectName("test.parquet").build();
  private static final GcsItemInfo ITEM_INFO =
      GcsItemInfo.builder().setItemId(ITEM_ID).setSize(1000).build();
  private static final GcsFileInfo FILE_INFO =
      GcsFileInfo.builder()
          .setItemInfo(ITEM_INFO)
          .setUri(URI.create("gs://b/test.parquet"))
          .setAttributes(ImmutableMap.of())
          .build();

  private GcsReadOptions readOptions;
  private Telemetry mockTelemetry;
  private AnalyticsCacheManager mockCacheManager;
  private VectoredSeekableByteChannel mockSource;
  private GcsFileSystem mockFileSystem;
  private GcsFooterOptimizer optimizer;

  @BeforeEach
  @SuppressWarnings("resource")
  void setUp() throws IOException {
    readOptions =
        GcsReadOptions.builder()
            .setFooterPrefetchEnabled(true)
            .setFooterPrefetchSizeSmallFile(100)
            .setFooterPrefetchSizeLargeFile(500)
            .setSmallObjectCacheSize(0)
            .build();
    mockTelemetry = mock(Telemetry.class);
    mockCacheManager = mock(AnalyticsCacheManager.class);
    mockSource = mock(VectoredSeekableByteChannel.class);
    mockFileSystem = mock(GcsFileSystem.class);
    when(mockFileSystem.getExecutorService()).thenReturn(MoreExecutors.newDirectExecutorService());
    when(mockFileSystem.open(any(GcsItemId.class), any())).thenReturn(mockSource);

    // Default read behavior to avoid infinite loop or NPE in prefetch
    when(mockSource.read(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer bb = invocation.getArgument(0);
              int remaining = bb.remaining();
              bb.position(bb.position() + remaining);
              return remaining;
            });

    optimizer = new GcsFooterOptimizer(readOptions, mockTelemetry, mockFileSystem);
  }

  @Test
  void isApplicable_footerPrefetchEnabled_returnsTrue() {
    assertThat(optimizer.isApplicable(ITEM_ID)).isTrue();
  }

  @Test
  void isApplicable_footerPrefetchDisabled_returnsFalse() {
    readOptions = GcsReadOptions.builder().setFooterPrefetchEnabled(false).build();
    optimizer = new GcsFooterOptimizer(readOptions, mockTelemetry, mockFileSystem);
    assertThat(optimizer.isApplicable(ITEM_ID)).isFalse();
  }

  @Test
  void onOpen_itemIdOnly_setsItemIdAndCacheManager() {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    // Verified via read() call
  }

  @Test
  void onOpen_fileInfo_initiatesPrefetch() {
    when(mockCacheManager.getFooterFuture(eq(ITEM_ID), any()))
        .thenReturn(new CompletableFuture<>());

    optimizer.onOpen(FILE_INFO, mockCacheManager);

    verify(mockCacheManager).getFooterFuture(eq(ITEM_ID), any());
  }

  @Test
  void read_footerHit_servesFromCache() throws IOException {
    CompletableFuture<ByteBuffer> cachedFuture =
        CompletableFuture.completedFuture(ByteBuffer.wrap(new byte[100]));
    when(mockCacheManager.getFooterFuture(eq(ITEM_ID), any())).thenReturn(cachedFuture);

    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    // Read last 10 bytes (position 990 to 1000)
    int bytesRead = optimizer.read(990, dst, mockSource);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(dst.position()).isEqualTo(10);
  }

  @Test
  void read_outsideFooterRange_returnsZero() throws IOException {
    when(mockCacheManager.getFooterFuture(eq(ITEM_ID), any()))
        .thenReturn(new CompletableFuture<>());
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    // Read at position 0, which is far from the 100-byte footer at 900-1000
    int bytesRead = optimizer.read(0, dst, mockSource);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void read_pastEOF_returnsMinusOne() throws IOException {
    when(mockCacheManager.getFooterFuture(eq(ITEM_ID), any()))
        .thenReturn(new CompletableFuture<>());
    optimizer.onOpen(FILE_INFO, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    // Read at position 1000 (EOF)
    int bytesReadEof = optimizer.read(1000, dst, mockSource);
    assertThat(bytesReadEof).isEqualTo(-1);

    // Read past position 1000
    int bytesReadPastEof = optimizer.read(1010, dst, mockSource);
    assertThat(bytesReadPastEof).isEqualTo(-1);
  }

  @Test
  void read_footerMiss_callsLoaderAndCaches() throws IOException {
    when(mockCacheManager.getFooterFuture(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation -> {
              AnalyticsCacheManager.FooterLoader loader = invocation.getArgument(1);
              return loader.load(ITEM_ID);
            });

    // Use ItemId only to avoid background prefetch in onOpen
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockSource.size()).thenReturn(1000L);
    when(mockSource.position()).thenReturn(500L);

    ByteBuffer dst = ByteBuffer.allocate(10);
    optimizer.read(990, dst, mockSource);

    verify(mockSource).position(900L); // Start of 100-byte footer
    verify(mockSource).position(500L); // Restored original position
  }

  @Test
  void read_loaderEncountersUnexpectedEof_throwsIOException() throws IOException {
    when(mockCacheManager.getFooterFuture(eq(ITEM_ID), any()))
        .thenAnswer(
            invocation -> {
              AnalyticsCacheManager.FooterLoader loader = invocation.getArgument(1);
              return loader.load(ITEM_ID);
            });

    // Use ItemId only to avoid background prefetch in onOpen
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockSource.size()).thenReturn(1000L);
    when(mockSource.read(any(ByteBuffer.class))).thenReturn(-1); // Unexpected EOF

    ByteBuffer dst = ByteBuffer.allocate(10);
    assertThrows(IOException.class, () -> optimizer.read(990, dst, mockSource));
  }

  @Test
  void read_largeFile_usesLargeFilePrefetchSize() throws IOException {
    long largeSize = 2L * 1024 * 1024 * 1024; // 2 GB
    GcsItemInfo largeInfo = GcsItemInfo.builder().setItemId(ITEM_ID).setSize(largeSize).build();
    GcsFileInfo largeFile = FILE_INFO.toBuilder().setItemInfo(largeInfo).build();

    CompletableFuture<ByteBuffer> future =
        CompletableFuture.completedFuture(ByteBuffer.allocate(500));
    when(mockCacheManager.getFooterFuture(eq(ITEM_ID), any())).thenReturn(future);

    optimizer.onOpen(largeFile, mockCacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    // Read at (largeSize - 500)
    int bytesRead = optimizer.read(largeSize - 500, dst, mockSource);

    assertThat(bytesRead).isEqualTo(10);
    verify(mockCacheManager).getFooterFuture(eq(ITEM_ID), any());
  }

  @Test
  void read_lazyInitPrefetchSize_whenOnOpenWithItemIdUsed() throws IOException {
    optimizer.onOpen(ITEM_ID, mockCacheManager);
    when(mockSource.size()).thenReturn(1000L);

    CompletableFuture<ByteBuffer> future =
        CompletableFuture.completedFuture(ByteBuffer.allocate(100));
    when(mockCacheManager.getFooterFuture(eq(ITEM_ID), any())).thenReturn(future);

    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(990, dst, mockSource);

    assertThat(bytesRead).isEqualTo(10);
    verify(mockSource, times(1)).size();
  }

  @Test
  void readVectored_footerHit_completesFutures()
      throws IOException, ExecutionException, InterruptedException {
    ByteBuffer cachedFooter = ByteBuffer.wrap(new byte[100]);
    for (int i = 0; i < 100; i++) cachedFooter.put(i, (byte) i);
    CompletableFuture<ByteBuffer> future = CompletableFuture.completedFuture(cachedFooter);

    when(mockCacheManager.getFooterFuture(eq(ITEM_ID), any())).thenReturn(future);
    when(mockCacheManager.getFooterFutureIfPresent(eq(ITEM_ID))).thenReturn(future);

    optimizer.onOpen(FILE_INFO, mockCacheManager);

    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(950)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    List<GcsObjectRange> remaining = optimizer.readVectored(List.of(range), ByteBuffer::allocate);

    assertThat(remaining).isEmpty();
    ByteBuffer result = range.getByteBufferFuture().get();
    assertThat(result.remaining()).isEqualTo(10);
    assertThat(result.get(0)).isEqualTo((byte) 50); // 950 is at index 50 in 100-byte footer
  }

  @Test
  void readVectored_footerMiss_returnsOriginalRanges() throws IOException {
    when(mockCacheManager.getFooterFuture(eq(ITEM_ID), any())).thenReturn(null);
    when(mockCacheManager.getFooterFutureIfPresent(eq(ITEM_ID))).thenReturn(null);

    optimizer.onOpen(FILE_INFO, mockCacheManager);

    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(950)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    List<GcsObjectRange> ranges = List.of(range);

    List<GcsObjectRange> remaining = optimizer.readVectored(ranges, ByteBuffer::allocate);

    assertThat(remaining).containsExactly(range);
  }

  @Test
  void readVectored_pastEOF_completesWithEOFException() throws IOException {
    ByteBuffer cachedFooter = ByteBuffer.wrap(new byte[100]);
    CompletableFuture<ByteBuffer> future = CompletableFuture.completedFuture(cachedFooter);
    when(mockCacheManager.getFooterFuture(eq(ITEM_ID), any())).thenReturn(future);
    when(mockCacheManager.getFooterFutureIfPresent(eq(ITEM_ID))).thenReturn(future);

    optimizer.onOpen(FILE_INFO, mockCacheManager);

    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(1010)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    List<GcsObjectRange> remaining = optimizer.readVectored(List.of(range), ByteBuffer::allocate);

    assertThat(remaining).isEmpty();
    var exception = assertThrows(ExecutionException.class, () -> range.getByteBufferFuture().get());
    assertThat(exception.getCause()).isInstanceOf(EOFException.class);
  }
}
