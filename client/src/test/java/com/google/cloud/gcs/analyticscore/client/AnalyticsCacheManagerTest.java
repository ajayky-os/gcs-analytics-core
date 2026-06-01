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

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnalyticsCacheManagerTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("b").setObjectName("o").build();
  private static final ByteBuffer FOOTER = ByteBuffer.wrap(new byte[] {1, 2, 3});

  private AnalyticsCacheManager manager;
  private ExecutorService executor;

  @BeforeEach
  void setUp() {
    manager = new AnalyticsCacheManager(GcsCacheOptions.builder().build());
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  void getFooterFuture_notPresent_computesAndCachesValue() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    CompletableFuture<ByteBuffer> future =
        manager.getFooterFuture(
            ITEM_ID,
            itemId -> {
              callCount.incrementAndGet();
              return CompletableFuture.completedFuture(FOOTER.duplicate());
            });
    CompletableFuture<ByteBuffer> secondFuture =
        manager.getFooterFuture(
            ITEM_ID,
            itemId -> {
              callCount.incrementAndGet();
              return CompletableFuture.completedFuture(ByteBuffer.wrap(new byte[] {4, 5, 6}));
            });

    assertThat(AnalyticsCacheManager.join(future)).isEqualTo(FOOTER);
    assertThat(callCount.get()).isEqualTo(1);
    assertThat(AnalyticsCacheManager.join(secondFuture)).isEqualTo(FOOTER);
    assertThat(AnalyticsCacheManager.join(secondFuture).isReadOnly()).isTrue();
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  void prefetchFooter_initiatesAsyncLoad() throws IOException, InterruptedException {
    AtomicInteger callCount = new AtomicInteger(0);
    java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);

    manager.getFooterFuture(
        ITEM_ID,
        itemId ->
            CompletableFuture.supplyAsync(
                () -> {
                  callCount.incrementAndGet();
                  latch.countDown();
                  return FOOTER.duplicate();
                },
                executor));

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(callCount.get()).isEqualTo(1);

    // Subsequent getFooterFuture should hit cache
    CompletableFuture<ByteBuffer> future = manager.getFooterFuture(ITEM_ID, itemId -> null);
    assertThat(AnalyticsCacheManager.join(future)).isEqualTo(FOOTER);
    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  void getFooterFuture_joinsExistingPrefetch() throws IOException, InterruptedException {
    java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch finishLatch = new java.util.concurrent.CountDownLatch(1);

    manager.getFooterFuture(
        ITEM_ID,
        itemId ->
            CompletableFuture.supplyAsync(
                () -> {
                  startLatch.countDown();
                  try {
                    finishLatch.await();
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                  return FOOTER.duplicate();
                },
                executor));

    assertThat(startLatch.await(5, TimeUnit.SECONDS)).isTrue();

    // Call getFooterFuture while prefetch is running
    CompletableFuture<ByteBuffer> joinFuture =
        manager.getFooterFuture(ITEM_ID, itemId -> CompletableFuture.completedFuture(null));

    assertThat(joinFuture.isDone()).isFalse();

    finishLatch.countDown();
    ByteBuffer footer = AnalyticsCacheManager.join(joinFuture);

    assertThat(footer).isEqualTo(FOOTER);
  }

  @Test
  void getFooterFuture_loaderThrowsIOException_propagatesIOExceptionOnJoin() {
    CompletableFuture<ByteBuffer> future =
        manager.getFooterFuture(
            ITEM_ID,
            itemId -> {
              CompletableFuture<ByteBuffer> failed = new CompletableFuture<>();
              failed.completeExceptionally(new IOException("test-io-exception"));
              return failed;
            });

    assertThrows(IOException.class, () -> AnalyticsCacheManager.join(future));
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  void getFooterFuture_cacheDisabled_anyKey_callsLoaderEveryTime() throws IOException {
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(false).build());
    AtomicInteger callCount = new AtomicInteger(0);
    AnalyticsCacheManager.FooterLoader loader =
        itemId -> {
          callCount.incrementAndGet();
          return CompletableFuture.completedFuture(FOOTER.duplicate());
        };

    manager.getFooterFuture(ITEM_ID, loader);
    manager.getFooterFuture(ITEM_ID, loader);

    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  void invalidateFooter_cacheDisabled_anyKey_succeeds() {
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(false).build());

    manager.invalidateFooter(ITEM_ID);
  }

  @Test
  void invalidateAll_cacheDisabled_anyKey_succeeds() {
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(false).build());

    manager.invalidateAll();
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  void invalidateFooter_present_removesEntry() throws IOException {
    manager.getFooterFuture(
        ITEM_ID, itemId -> CompletableFuture.completedFuture(FOOTER.duplicate()));

    manager.invalidateFooter(ITEM_ID);

    AtomicInteger callCount = new AtomicInteger(0);
    manager.getFooterFuture(
        ITEM_ID,
        itemId -> {
          callCount.incrementAndGet();
          return CompletableFuture.completedFuture(FOOTER.duplicate());
        });
    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  void invalidateAll_withEntries_clearsCache() throws IOException {
    GcsItemId itemId2 = GcsItemId.builder().setBucketName("b").setObjectName("o2").build();
    manager.getFooterFuture(
        ITEM_ID, itemId -> CompletableFuture.completedFuture(FOOTER.duplicate()));
    manager.getFooterFuture(
        itemId2, itemId -> CompletableFuture.completedFuture(ByteBuffer.wrap(new byte[] {2})));

    manager.invalidateAll();

    AtomicInteger callCount = new AtomicInteger(0);
    manager.getFooterFuture(
        ITEM_ID,
        itemId -> {
          callCount.incrementAndGet();
          return CompletableFuture.completedFuture(FOOTER.duplicate());
        });
    manager.getFooterFuture(
        itemId2,
        itemId -> {
          callCount.incrementAndGet();
          return CompletableFuture.completedFuture(FOOTER.duplicate());
        });
    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  void getFooterFutureIfPresent_notPresent_returnsNull() {
    assertThat(manager.getFooterFutureIfPresent(ITEM_ID)).isNull();
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  void getFooterFutureIfPresent_inProgress_returnsFuture() throws InterruptedException {
    java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
    manager.getFooterFuture(
        ITEM_ID,
        itemId ->
            CompletableFuture.supplyAsync(
                () -> {
                  try {
                    latch.await();
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                  return FOOTER.duplicate();
                },
                executor));

    assertThat(manager.getFooterFutureIfPresent(ITEM_ID)).isNotNull();
    latch.countDown();
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  void getFooterFutureIfPresent_done_returnsFuture() throws IOException {
    manager.getFooterFuture(
        ITEM_ID, itemId -> CompletableFuture.completedFuture(FOOTER.duplicate()));

    assertThat(AnalyticsCacheManager.join(manager.getFooterFutureIfPresent(ITEM_ID)))
        .isEqualTo(FOOTER);
  }
}
