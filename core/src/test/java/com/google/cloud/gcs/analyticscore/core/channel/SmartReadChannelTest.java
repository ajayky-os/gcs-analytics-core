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

package com.google.cloud.gcs.analyticscore.core.channel;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.core.optimizer.FormatOptimizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SmartReadChannelTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("b").setObjectName("o").build();
  private static final GcsItemInfo ITEM_INFO = GcsItemInfo.builder().setItemId(ITEM_ID).build();
  private static final GcsFileInfo FILE_INFO =
      GcsFileInfo.builder()
          .setItemInfo(ITEM_INFO)
          .setUri(URI.create("gs://b/o"))
          .setAttributes(ImmutableMap.of())
          .build();

  private VectoredSeekableByteChannel mockSource;
  private AnalyticsCacheManager mockCacheManager;
  private FormatOptimizer mockOptimizer;

  @BeforeEach
  void setUp() throws IOException {
    mockSource = mock(VectoredSeekableByteChannel.class);
    mockCacheManager = mock(AnalyticsCacheManager.class);
    mockOptimizer = mock(FormatOptimizer.class);

    when(mockSource.position()).thenReturn(0L);
    when(mockOptimizer.isApplicable(any(GcsItemId.class))).thenReturn(true);
    when(mockOptimizer.isApplicable(any(GcsFileInfo.class))).thenReturn(true);
  }

  @Test
  void constructor_withFileInfo_applicableOptimizer_callsOnOpenWithFileInfo() {
    SmartReadChannel.builder()
        .setSource(mockSource)
        .setItemId(ITEM_ID)
        .setFileInfo(FILE_INFO)
        .setCacheManager(mockCacheManager)
        .addOptimizer(mockOptimizer)
        .build();

    verify(mockOptimizer).onOpen(FILE_INFO, mockCacheManager);
    verify(mockOptimizer, never()).onOpen(eq(ITEM_ID), any());
  }

  @Test
  void constructor_itemIdOnly_applicableOptimizer_callsOnOpenWithItemId() {
    SmartReadChannel.builder()
        .setSource(mockSource)
        .setItemId(ITEM_ID)
        .setCacheManager(mockCacheManager)
        .addOptimizer(mockOptimizer)
        .build();

    verify(mockOptimizer).onOpen(ITEM_ID, mockCacheManager);
    verify(mockOptimizer, never()).onOpen(any(GcsFileInfo.class), any());
  }

  @Test
  void constructor_inapplicableOptimizer_skipsOnOpen() {
    when(mockOptimizer.isApplicable(any(GcsItemId.class))).thenReturn(false);
    when(mockOptimizer.isApplicable(any(GcsFileInfo.class))).thenReturn(false);

    SmartReadChannel.builder()
        .setSource(mockSource)
        .setItemId(ITEM_ID)
        .setCacheManager(mockCacheManager)
        .addOptimizer(mockOptimizer)
        .build();

    verify(mockOptimizer, never()).onOpen(any(GcsItemId.class), any());
    verify(mockOptimizer, never()).onOpen(any(GcsFileInfo.class), any());
  }

  @Test
  void read_optimizerHit_returnsBytesAndUpdatesPosition() throws IOException {
    when(mockOptimizer.read(eq(0L), any(ByteBuffer.class), eq(mockSource))).thenReturn(5);
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setSource(mockSource)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = smartChannel.read(dst);

    assertThat(bytesRead).isEqualTo(5);
    verify(mockSource).position(5L);
  }

  @Test
  void read_optimizerMiss_delegatesToSource() throws IOException {
    when(mockOptimizer.read(eq(0L), any(ByteBuffer.class), eq(mockSource))).thenReturn(0);
    when(mockSource.read(any(ByteBuffer.class))).thenReturn(10);
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setSource(mockSource)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = smartChannel.read(dst);

    assertThat(bytesRead).isEqualTo(10);
    verify(mockSource).read(dst);
  }

  @Test
  void read_optimizerEof_returnsMinusOne() throws IOException {
    when(mockOptimizer.read(eq(0L), any(ByteBuffer.class), eq(mockSource))).thenReturn(-1);
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setSource(mockSource)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();
    ByteBuffer dst = ByteBuffer.allocate(10);

    int bytesRead = smartChannel.read(dst);

    assertThat(bytesRead).isEqualTo(-1);
    verify(mockSource, never()).position(anyLong());
  }

  @Test
  void readVectored_delegatesToSource() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setSource(mockSource)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();
    List<GcsObjectRange> ranges = ImmutableList.of();
    IntFunction<ByteBuffer> allocate = (i) -> ByteBuffer.allocate(i);

    smartChannel.readVectored(ranges, allocate);

    verify(mockSource).readVectored(ranges, allocate);
  }

  @Test
  void write_delegatesToSource() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setSource(mockSource)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();
    ByteBuffer src = ByteBuffer.allocate(10);
    when(mockSource.write(src)).thenReturn(10);

    int written = smartChannel.write(src);

    assertThat(written).isEqualTo(10);
    verify(mockSource).write(src);
  }

  @Test
  void position_get_delegatesToSource() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setSource(mockSource)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();
    when(mockSource.position()).thenReturn(100L);

    assertThat(smartChannel.position()).isEqualTo(100L);
  }

  @Test
  void position_set_delegatesToSource() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setSource(mockSource)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();

    smartChannel.position(200L);

    verify(mockSource).position(200L);
  }

  @Test
  void size_delegatesToSource() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setSource(mockSource)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();
    when(mockSource.size()).thenReturn(500L);

    assertThat(smartChannel.size()).isEqualTo(500L);
  }

  @Test
  void truncate_delegatesToSource() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setSource(mockSource)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();

    smartChannel.truncate(100L);

    verify(mockSource).truncate(100L);
  }

  @Test
  void isOpen_delegatesToSource() {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setSource(mockSource)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .build();
    when(mockSource.isOpen()).thenReturn(true);

    assertThat(smartChannel.isOpen()).isTrue();
  }

  @Test
  void close_default_callsOnCloseAndSourceClose() throws IOException {
    SmartReadChannel smartChannel =
        SmartReadChannel.builder()
            .setSource(mockSource)
            .setItemId(ITEM_ID)
            .setCacheManager(mockCacheManager)
            .addOptimizer(mockOptimizer)
            .build();

    smartChannel.close();

    verify(mockOptimizer).onClose();
    verify(mockSource).close();
  }

  @Test
  void builder_missingSource_throwsException() {
    SmartReadChannel.Builder builder =
        SmartReadChannel.builder().setItemId(ITEM_ID).setCacheManager(mockCacheManager);

    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  void builder_missingCacheManager_throwsException() {
    SmartReadChannel.Builder builder =
        SmartReadChannel.builder().setSource(mockSource).setItemId(ITEM_ID);

    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  void builder_missingItemIdAndFileInfo_throwsException() {
    SmartReadChannel.Builder builder =
        SmartReadChannel.builder().setSource(mockSource).setCacheManager(mockCacheManager);

    assertThrows(NullPointerException.class, builder::build);
  }
}
