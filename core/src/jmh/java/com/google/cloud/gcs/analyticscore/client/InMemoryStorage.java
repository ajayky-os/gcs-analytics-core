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

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Map;

/**
 * Minimal in-memory {@link Storage} for hermetic benchmarks. Only {@code reader(BlobId, ...)} and
 * {@code close()} are supported; any other call throws, which signals that the benchmark is no
 * longer exercising a network-free path.
 */
final class InMemoryStorage {

  private InMemoryStorage() {}

  /** Creates a {@link Storage} serving the given objects, keyed by {@code bucket/object}. */
  static Storage create(Map<GcsItemId, byte[]> objects) {
    ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
    objects.forEach(
        (id, data) -> builder.put(key(id.getBucketName(), id.getObjectName().get()), data));
    ImmutableMap<String, byte[]> contents = builder.buildOrThrow();
    return (Storage)
        Proxy.newProxyInstance(
            Storage.class.getClassLoader(),
            new Class<?>[] {Storage.class},
            (proxy, method, args) -> {
              if (method.getName().equals("reader") && args[0] instanceof BlobId) {
                BlobId blobId = (BlobId) args[0];
                byte[] data = contents.get(key(blobId.getBucket(), blobId.getName()));
                checkArgument(data != null, "Unknown object: %s", blobId);
                return new ByteArrayReadChannel(data);
              }
              if (method.getName().equals("close")) {
                return null;
              }
              throw new UnsupportedOperationException(
                  "InMemoryStorage does not support Storage." + method.getName());
            });
  }

  private static String key(String bucket, String object) {
    return bucket + "/" + object;
  }

  /** A {@link ReadChannel} over a byte array honoring seek, limit, isOpen and close. */
  static final class ByteArrayReadChannel implements ReadChannel {
    private final byte[] data;
    private long position;
    private long limit;
    private boolean open = true;

    ByteArrayReadChannel(byte[] data) {
      this.data = data;
      this.limit = data.length;
    }

    @Override
    public int read(ByteBuffer dst) throws ClosedChannelException {
      if (!open) {
        throw new ClosedChannelException();
      }
      long end = Math.min(limit, data.length);
      if (position >= end) {
        return -1;
      }
      int count = (int) Math.min(dst.remaining(), end - position);
      dst.put(data, (int) position, count);
      position += count;
      return count;
    }

    @Override
    public void seek(long position) {
      checkArgument(position >= 0, "position must be >= 0, got %s", position);
      this.position = position;
    }

    @Override
    public ReadChannel limit(long limit) {
      this.limit = limit;
      return this;
    }

    @Override
    public void setChunkSize(int chunkSize) {}

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() {
      open = false;
    }

    @Override
    public RestorableState<ReadChannel> capture() {
      throw new UnsupportedOperationException("capture() is not supported");
    }
  }
}
