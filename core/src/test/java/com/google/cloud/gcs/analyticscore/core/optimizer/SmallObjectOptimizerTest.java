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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.FakeGcsClientImpl;
import com.google.cloud.gcs.analyticscore.client.FakeGcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsCacheOptions;
import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SmallObjectOptimizerTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("b").setObjectName("test.parquet").build();
  private static final GcsItemInfo ITEM_INFO =
      GcsItemInfo.builder().setItemId(ITEM_ID).setSize(100).build();
  private static final GcsFileInfo FILE_INFO =
      GcsFileInfo.builder()
          .setItemInfo(ITEM_INFO)
          .setUri(URI.create("gs://b/test.parquet"))
          .setAttributes(ImmutableMap.of())
          .build();

  private GcsReadOptions readOptions;
  private Telemetry telemetry;
  private VectoredSeekableByteChannel realSource;
  private SmallObjectOptimizer optimizer;
  private AnalyticsCacheManager cacheManager;

  @BeforeEach
  void initializeOptimizerAndFakeStorage() throws IOException {
    readOptions = GcsReadOptions.builder().build();
    telemetry = new Telemetry(ImmutableList.of());
    GcsCacheOptions cacheOptions =
        GcsCacheOptions.builder().setSmallObjectCacheMaxSizeBytes(200).build();
    cacheManager = new AnalyticsCacheManager(cacheOptions);
    optimizer = new SmallObjectOptimizer(cacheOptions, telemetry);

    GcsClientOptions clientOptions =
        GcsClientOptions.builder().setGcsReadOptions(readOptions).build();
    GcsFileSystemOptions fileSystemOptions =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(clientOptions)
            .setGcsCacheOptions(GcsCacheOptions.builder().build())
            .build();
    FakeGcsFileSystemImpl fakeFileSystem = new FakeGcsFileSystemImpl(fileSystemOptions);

    byte[] testData = new byte[100];
    for (int i = 0; i < 100; i++) {
      testData[i] = (byte) i;
    }
    FakeGcsClientImpl.storage.create(
        BlobInfo.newBuilder(ITEM_ID.getBucketName(), ITEM_ID.getObjectName().get(), 1L).build(),
        testData);

    realSource = fakeFileSystem.open(FILE_INFO, readOptions);
  }

  @Test
  void isApplicable_fileInfo_smallFile_returnsTrue() {
    assertThat(optimizer.isApplicable(FILE_INFO)).isTrue();
  }

  @Test
  void isApplicable_orcFile_returnsTrue() {
    GcsItemId orcItemId = GcsItemId.builder().setBucketName("b").setObjectName("test.orc").build();
    assertThat(optimizer.isApplicable(orcItemId)).isTrue();
  }

  @Test
  void isApplicable_nonDataFile_returnsFalse() {
    GcsItemId csvItemId = GcsItemId.builder().setBucketName("b").setObjectName("test.csv").build();
    assertThat(optimizer.isApplicable(csvItemId)).isFalse();
  }

  @Test
  void isApplicable_fileInfo_nonDataFile_returnsFalse() {
    GcsItemId csvItemId = GcsItemId.builder().setBucketName("b").setObjectName("test.csv").build();
    GcsItemInfo csvInfo = GcsItemInfo.builder().setItemId(csvItemId).setSize(100).build();
    GcsFileInfo csvFile = FILE_INFO.toBuilder().setItemInfo(csvInfo).build();
    assertThat(optimizer.isApplicable(csvFile)).isFalse();
  }

  @Test
  void isApplicable_fileInfo_largeFile_returnsFalse() {
    GcsItemInfo largeInfo = GcsItemInfo.builder().setItemId(ITEM_ID).setSize(300).build();
    GcsFileInfo largeFile =
        GcsFileInfo.builder()
            .setItemInfo(largeInfo)
            .setUri(FILE_INFO.getUri())
            .setAttributes(FILE_INFO.getAttributes())
            .build();

    assertThat(optimizer.isApplicable(largeFile)).isFalse();
  }

  @Test
  void isApplicable_itemId_returnsTrueIfCacheEnabled() {
    assertThat(optimizer.isApplicable(ITEM_ID)).isTrue();
    GcsCacheOptions cacheOptions =
        GcsCacheOptions.builder().setSmallObjectCacheMaxSizeBytes(0).build();
    optimizer = new SmallObjectOptimizer(cacheOptions, telemetry);
    assertThat(optimizer.isApplicable(ITEM_ID)).isFalse();
  }

  @Test
  void onOpen_itemIdOnly_isNoOp() {
    optimizer.onOpen(ITEM_ID, cacheManager);
  }

  @Test
  void read_smallFile_cachesAndServes() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);
    realSource.position(50L);

    int bytesRead = optimizer.read(10, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(dst.array()).isEqualTo(new byte[] {10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
    assertThat(realSource.position()).isEqualTo(50L); // Position restored
    // Delete the file from storage to prove second read comes from cache
    FakeGcsClientImpl.storage.delete(ITEM_ID.getBucketName(), ITEM_ID.getObjectName().get());
    dst.clear();
    int secondBytesRead = optimizer.read(20, dst, realSource);
    assertThat(secondBytesRead).isEqualTo(10);
    assertThat(dst.array()).isEqualTo(new byte[] {20, 21, 22, 23, 24, 25, 26, 27, 28, 29});
  }

  @Test
  void read_largeFile_returnsZero() throws IOException {
    GcsCacheOptions cacheOptions =
        GcsCacheOptions.builder().setSmallObjectCacheMaxSizeBytes(50).build();
    optimizer = new SmallObjectOptimizer(cacheOptions, telemetry);
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(0, dst, realSource);

    assertThat(bytesRead).isEqualTo(0);
  }

  @Test
  void read_unexpectedEofDuringPrefetch_throwsIOException() throws IOException {
    // Modify file to be shorter than ITEM_INFO claims (100)
    FakeGcsClientImpl.storage.create(
        BlobInfo.newBuilder(ITEM_ID.getBucketName(), ITEM_ID.getObjectName().get(), 1L).build(),
        new byte[50]);
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);
    IOException exception =
        assertThrows(IOException.class, () -> optimizer.read(0, dst, realSource));

    assertThat(exception).hasMessageThat().contains("Received end of stream signal");
  }

  @Test
  void readVectored_uninitializedFileSize_returnsOriginalRanges() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    List<GcsObjectRange> ranges = List.of(range);
    List<GcsObjectRange> remaining =
        optimizer.readVectored(ranges, (size) -> ByteBuffer.allocate(size));

    assertThat(remaining).isSameInstanceAs(ranges);
  }

  @Test
  void read_pastEOF_returnsMinusOne() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesReadEof = optimizer.read(100, dst, realSource);

    assertThat(bytesReadEof).isEqualTo(-1);
    int bytesReadPastEof = optimizer.read(110, dst, realSource);
    assertThat(bytesReadPastEof).isEqualTo(-1);
  }

  @Test
  void read_lazyInitFileSize_whenOnOpenWithItemIdUsed() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = optimizer.read(10, dst, realSource);

    assertThat(bytesRead).isEqualTo(10);
    assertThat(dst.array()).isEqualTo(new byte[] {10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
  }

  @Test
  void serveFromCache_pastEOF_returnsMinusOne() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);

    optimizer.read(0, ByteBuffer.allocate(10), realSource); // Trigger prefetch
    ByteBuffer dst = ByteBuffer.allocate(10);
    int bytesRead = optimizer.read(150, dst, realSource);

    assertThat(bytesRead).isEqualTo(-1);
  }

  @Test
  void readVectored_pastEOF_completesWithEOFException() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);

    optimizer.read(0, ByteBuffer.allocate(10), realSource); // Trigger prefetch
    GcsObjectRange pastEofRange =
        GcsObjectRange.builder()
            .setOffset(110)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    optimizer.readVectored(List.of(pastEofRange), (size) -> ByteBuffer.allocate(size));
    var exception =
        assertThrows(ExecutionException.class, () -> pastEofRange.getByteBufferFuture().get());

    assertThat(exception.getCause()).isInstanceOf(java.io.EOFException.class);
  }

  @Test
  void readVectored_notApplicable_returnsOriginalRanges() throws IOException {
    readOptions = GcsReadOptions.builder().build();
    GcsCacheOptions cacheOptions =
        GcsCacheOptions.builder().setSmallObjectCacheMaxSizeBytes(200).build();
    optimizer = new SmallObjectOptimizer(cacheOptions, telemetry);
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    List<GcsObjectRange> ranges = List.of(range);
    List<GcsObjectRange> remaining =
        optimizer.readVectored(ranges, (size) -> ByteBuffer.allocate(size));

    assertThat(remaining).isSameInstanceAs(ranges);
  }

  @Test
  void readVectored_notYetPrefetched_returnsOriginalRanges() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    List<GcsObjectRange> ranges = List.of(range);
    List<GcsObjectRange> remaining =
        optimizer.readVectored(ranges, (size) -> ByteBuffer.allocate(size));

    assertThat(remaining).isSameInstanceAs(ranges);
  }

  @Test
  void readVectored_partialRead_completesExceptionally() throws IOException {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);

    optimizer.read(0, ByteBuffer.allocate(10), realSource); // Trigger prefetch
    GcsObjectRange partialRange =
        GcsObjectRange.builder()
            .setOffset(90)
            .setLength(20) // Only 10 bytes available before EOF
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    optimizer.readVectored(List.of(partialRange), (size) -> ByteBuffer.allocate(size));
    var exception =
        assertThrows(ExecutionException.class, () -> partialRange.getByteBufferFuture().get());

    assertThat(exception.getCause()).isInstanceOf(java.io.EOFException.class);
  }

  @Test
  void readVectored_success_returnsEmptyList() throws Exception {
    optimizer.onOpen(ITEM_ID, cacheManager);
    optimizer.onOpen(FILE_INFO, cacheManager);

    optimizer.read(0, ByteBuffer.allocate(10), realSource); // Trigger prefetch
    GcsObjectRange validRange =
        GcsObjectRange.builder()
            .setOffset(10)
            .setLength(20)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    List<GcsObjectRange> remaining =
        optimizer.readVectored(List.of(validRange), ByteBuffer::allocate);

    assertThat(remaining).isEmpty();
    ByteBuffer result = validRange.getByteBufferFuture().get();
    assertThat(result.remaining()).isEqualTo(20);
    assertThat(result.get()).isEqualTo((byte) 10);
    assertThat(result.get(19)).isEqualTo((byte) 29);
  }
}
