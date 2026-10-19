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
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.UnaryOperator;

/**
 * In-memory {@link Storage} fake shared by hermetic unit tests and benchmarks.
 *
 * <p>Hermeticity guarantee: only {@code reader(BlobId, BlobSourceOption...)} and {@code close()}
 * are supported; every other {@link Storage} method throws {@link UnsupportedOperationException},
 * so a code path that would reach the network fails loudly. Objects are keyed by bucket and object
 * name (generations are ignored). Opening a reader for an unknown object throws a {@link
 * StorageException} with code 404, like the real SDK does on first contact with a missing object.
 *
 * <p>Test infrastructure only: no compatibility guarantee.
 */
public final class InMemoryStorage {
  private final ImmutableMap<GcsItemId, byte[]> objects;
  private final UnaryOperator<ReadChannel> readChannelDecorator;
  private final Storage storage;
  private final LongAdder readerOpens = new LongAdder();
  private final LongAdder bytesServed = new LongAdder();

  private InMemoryStorage(Map<GcsItemId, byte[]> objects, UnaryOperator<ReadChannel> decorator) {
    ImmutableMap.Builder<GcsItemId, byte[]> builder = ImmutableMap.builder();
    objects.forEach(
        (id, data) -> builder.put(key(id.getBucketName(), id.getObjectName().get()), data));
    this.objects = builder.buildOrThrow();
    this.readChannelDecorator = checkNotNull(decorator, "decorator must not be null");
    this.storage =
        (Storage)
            Proxy.newProxyInstance(
                Storage.class.getClassLoader(), new Class<?>[] {Storage.class}, this::invoke);
  }

  /** Creates a storage serving {@code objects}. */
  public static InMemoryStorage create(Map<GcsItemId, byte[]> objects) {
    return new InMemoryStorage(objects, UnaryOperator.identity());
  }

  /** Like {@link #create(Map)}, but wraps every channel returned by {@code reader}. */
  public static InMemoryStorage create(
      Map<GcsItemId, byte[]> objects, UnaryOperator<ReadChannel> readChannelDecorator) {
    return new InMemoryStorage(objects, readChannelDecorator);
  }

  /** The {@link Storage} view to hand to production code. */
  public Storage storage() {
    return storage;
  }

  /** Content of {@code itemId} (generation ignored), or empty if unknown. */
  public Optional<byte[]> find(GcsItemId itemId) {
    return Optional.ofNullable(
        objects.get(key(itemId.getBucketName(), itemId.getObjectName().get())));
  }

  /** Number of {@code reader(...)} calls since creation or the last {@link #resetCounters()}. */
  public long readerOpens() {
    return readerOpens.sum();
  }

  /** Bytes copied into callers' buffers since creation or the last {@link #resetCounters()}. */
  public long bytesServed() {
    return bytesServed.sum();
  }

  /** Zeroes {@link #readerOpens()} and {@link #bytesServed()}. */
  public void resetCounters() {
    readerOpens.reset();
    bytesServed.reset();
  }

  private Object invoke(Object proxy, Method method, Object[] args) {
    if (method.getDeclaringClass() == Object.class) {
      switch (method.getName()) {
        case "equals":
          return proxy == args[0];
        case "hashCode":
          return System.identityHashCode(proxy);
        default:
          return "InMemoryStorage{objects=" + objects.size() + "}";
      }
    }
    if (method.getName().equals("reader") && args[0] instanceof BlobId) {
      BlobId blobId = (BlobId) args[0];
      byte[] data = objects.get(key(blobId.getBucket(), blobId.getName()));
      if (data == null) {
        throw new StorageException(404, "Unknown object: " + blobId);
      }
      readerOpens.increment();
      return readChannelDecorator.apply(new ByteArrayReadChannel(data, bytesServed));
    }
    if (method.getName().equals("close")) {
      return null;
    }
    throw new UnsupportedOperationException(
        "InMemoryStorage does not support Storage." + method.getName());
  }

  private static GcsItemId key(String bucket, String object) {
    return GcsItemId.builder().setBucketName(bucket).setObjectName(object).build();
  }

  /** Zero-copy {@link ReadChannel} over a byte array; {@code limit} is an absolute end offset. */
  static final class ByteArrayReadChannel implements ReadChannel {
    private final byte[] data;
    private final LongAdder bytesServed;
    private long position;
    private long limit;
    private boolean open = true;

    ByteArrayReadChannel(byte[] data, LongAdder bytesServed) {
      this.data = data;
      this.bytesServed = bytesServed;
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
      bytesServed.add(count);
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
