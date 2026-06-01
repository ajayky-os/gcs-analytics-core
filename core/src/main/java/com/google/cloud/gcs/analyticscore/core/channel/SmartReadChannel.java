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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.core.optimizer.FormatOptimizer;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

/**
 * A {@link VectoredSeekableByteChannel} decorator that orchestrates {@link FormatOptimizer}s to
 * apply format-specific optimizations to read operations.
 */
public class SmartReadChannel implements VectoredSeekableByteChannel {

  private final VectoredSeekableByteChannel source;
  private final List<FormatOptimizer> optimizers;

  private SmartReadChannel(
      VectoredSeekableByteChannel source,
      GcsItemId itemId,
      AnalyticsCacheManager cacheManager,
      List<FormatOptimizer> optimizers) {
    this.source = checkNotNull(source, "source cannot be null");
    this.optimizers =
        optimizers.stream()
            .filter(optimizer -> optimizer.isApplicable(itemId))
            .collect(ImmutableList.toImmutableList());
    for (FormatOptimizer optimizer : this.optimizers) {
      optimizer.onOpen(itemId, cacheManager);
    }
  }

  private SmartReadChannel(
      VectoredSeekableByteChannel source,
      GcsFileInfo fileInfo,
      AnalyticsCacheManager cacheManager,
      List<FormatOptimizer> optimizers) {
    this.source = checkNotNull(source, "source cannot be null");
    this.optimizers =
        optimizers.stream()
            .filter(optimizer -> optimizer.isApplicable(fileInfo))
            .collect(ImmutableList.toImmutableList());
    for (FormatOptimizer optimizer : this.optimizers) {
      optimizer.onOpen(fileInfo, cacheManager);
    }
  }

  /** Returns a new builder for {@link SmartReadChannel}. */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    long position = source.position();
    for (FormatOptimizer optimizer : optimizers) {
      int bytesRead = optimizer.read(position, dst, source);
      if (bytesRead != 0) {
        if (bytesRead > 0) {
          source.position(position + bytesRead);
        }
        return bytesRead;
      }
    }
    return source.read(dst);
  }

  @Override
  public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    source.readVectored(ranges, allocate);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return source.write(src);
  }

  @Override
  public long position() throws IOException {
    return source.position();
  }

  @Override
  public VectoredSeekableByteChannel position(long newPosition) throws IOException {
    source.position(newPosition);
    return this;
  }

  @Override
  public long size() throws IOException {
    return source.size();
  }

  @Override
  public VectoredSeekableByteChannel truncate(long size) throws IOException {
    source.truncate(size);
    return this;
  }

  @Override
  public boolean isOpen() {
    return source.isOpen();
  }

  @Override
  public void close() throws IOException {
    try {
      for (FormatOptimizer optimizer : optimizers) {
        optimizer.onClose();
      }
    } finally {
      source.close();
    }
  }

  /** Builder for {@link SmartReadChannel}. */
  public static class Builder {
    private VectoredSeekableByteChannel source;
    private GcsItemId itemId;
    @Nullable private GcsFileInfo fileInfo;
    private AnalyticsCacheManager cacheManager;
    private final ImmutableList.Builder<FormatOptimizer> optimizers = ImmutableList.builder();

    public Builder setSource(VectoredSeekableByteChannel source) {
      this.source = source;
      return this;
    }

    public Builder setItemId(GcsItemId itemId) {
      this.itemId = itemId;
      return this;
    }

    public Builder setFileInfo(@Nullable GcsFileInfo fileInfo) {
      this.fileInfo = fileInfo;
      return this;
    }

    public Builder setCacheManager(AnalyticsCacheManager cacheManager) {
      this.cacheManager = cacheManager;
      return this;
    }

    public Builder addOptimizer(FormatOptimizer optimizer) {
      this.optimizers.add(optimizer);
      return this;
    }

    public SmartReadChannel build() {
      checkNotNull(source, "source must be set");
      checkNotNull(cacheManager, "cacheManager must be set");
      List<FormatOptimizer> optimizerList = optimizers.build();

      if (fileInfo != null) {
        return new SmartReadChannel(source, fileInfo, cacheManager, optimizerList);
      }
      checkNotNull(itemId, "itemId must be set if fileInfo is missing");
      return new SmartReadChannel(source, itemId, cacheManager, optimizerList);
    }
  }
}
