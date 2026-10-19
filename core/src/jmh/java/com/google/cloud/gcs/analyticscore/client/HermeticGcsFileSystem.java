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

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Builds a {@link GcsFileSystem} whose {@link GcsClientImpl} is backed by {@link InMemoryStorage},
 * so benchmarks can drive the real read path without credentials or network access.
 */
public final class HermeticGcsFileSystem {

  // GcsClientImpl calls createStorage() from its constructor, before subclass fields exist.
  private static final ThreadLocal<Storage> PENDING_STORAGE = new ThreadLocal<>();

  private HermeticGcsFileSystem() {}

  /** Creates a file system serving {@code objects}; telemetry is a no-op (no listeners). */
  public static GcsFileSystem create(
      Map<GcsItemId, byte[]> objects, GcsFileSystemOptions options) {
    checkArgument(!options.getGcsClientOptions().isBidiEnabled(), "bidi reads are not hermetic");
    ExecutorService executor =
        Executors.newFixedThreadPool(
            options.getReadThreadCount(),
            new ThreadFactoryBuilder().setNameFormat("hermetic-range-pool-%d").build());
    PENDING_STORAGE.set(InMemoryStorage.create(objects));
    try {
      return new GcsFileSystemImpl(
          new InMemoryGcsClient(ImmutableMap.copyOf(objects), options, executor), options);
    } finally {
      PENDING_STORAGE.remove();
    }
  }

  private static final class InMemoryGcsClient extends GcsClientImpl {
    private final ImmutableMap<GcsItemId, byte[]> objects;
    private final ExecutorService executor;

    private InMemoryGcsClient(
        ImmutableMap<GcsItemId, byte[]> objects,
        GcsFileSystemOptions options,
        ExecutorService executor) {
      super(options.getGcsClientOptions(), () -> executor, new Telemetry(ImmutableList.of()));
      this.objects = objects;
      this.executor = executor;
    }

    @Override
    protected Storage createStorage(Optional<Credentials> credentials) {
      return PENDING_STORAGE.get();
    }

    @Override
    BucketProperties getBucketProperties(String bucketName) {
      return BucketProperties.create(false);
    }

    @Override
    public GcsItemInfo getGcsItemInfo(GcsItemId itemId) {
      byte[] data = objects.get(itemId);
      return data == null
          ? GcsItemInfo.createNotFound(itemId)
          : GcsItemInfo.builder().setItemId(itemId).setSize(data.length).build();
    }

    @Override
    public void close() {
      executor.shutdownNow();
      super.close();
    }
  }
}
