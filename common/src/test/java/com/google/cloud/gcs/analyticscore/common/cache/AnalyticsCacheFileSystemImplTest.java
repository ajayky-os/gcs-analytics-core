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

package com.google.cloud.gcs.analyticscore.common.cache;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnalyticsCacheFileSystemImplTest {

  private Path tempDir;
  private AnalyticsCacheFileSystemImpl<String> cache;

  @BeforeEach
  void setUp() throws IOException {
    tempDir = Files.createTempDirectory("fs-cache-test");
    cache = AnalyticsCacheFileSystemImpl.create(tempDir.toString(), 100 * 1024 * 1024L, key -> key);
  }

  @AfterEach
  void tearDown() {
    cache.close();
    cache.invalidateAll();
  }

  @Test
  void putAndGet_storesAndRetrievesDataSuccessfully() throws InterruptedException {
    ByteBuffer data = ByteBuffer.wrap(new byte[] {1, 2, 3});

    cache.put("key1", data);
    Thread.sleep(500); // wait for async write

    assertThat(cache.get("key1").isPresent()).isTrue();
    assertThat(cache.get("key1").get().array()).isEqualTo(new byte[] {1, 2, 3});
  }

  @Test
  void get_missingKey_returnsEmpty() {
    assertThat(cache.get("missing").isPresent()).isFalse();
  }

  @Test
  void invalidate_removesItem() throws InterruptedException {
    cache.put("key1", ByteBuffer.wrap(new byte[] {1, 2}));
    Thread.sleep(500); // wait for async write
    assertThat(cache.get("key1").isPresent()).isTrue();

    cache.invalidate("key1");

    assertThat(cache.get("key1").isPresent()).isFalse();
  }

  @Test
  void invalidateAll_removesAllItems() throws InterruptedException {
    cache.put("key1", ByteBuffer.wrap(new byte[] {1}));
    cache.put("key2", ByteBuffer.wrap(new byte[] {2}));
    Thread.sleep(500); // wait for async write

    cache.invalidateAll();

    assertThat(cache.get("key1").isPresent()).isFalse();
    assertThat(cache.get("key2").isPresent()).isFalse();
  }

  @Test
  void getWithLoader_loadsAndStores() throws Exception {
    ByteBuffer loaded = cache.get("key1", key -> ByteBuffer.wrap(new byte[] {5, 6}));
    Thread.sleep(500); // wait for async write

    assertThat(loaded.array()).isEqualTo(new byte[] {5, 6});
    assertThat(cache.get("key1").isPresent()).isTrue();
    assertThat(cache.get("key1").get().array()).isEqualTo(new byte[] {5, 6});
  }

  @Test
  void size_returnsApproximateDirectorySize() throws InterruptedException {
    cache.put("key1", ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}));
    cache.put("key2", ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}));
    Thread.sleep(500); // wait for async write

    assertThat(cache.size()).isEqualTo(10);
  }
}
