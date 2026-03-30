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
package com.google.cloud.gcs.analyticscore.core;

import static com.google.common.base.Preconditions.*;

import com.google.cloud.gcs.analyticscore.client.*;
import com.google.cloud.storage.BlobId;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is a seekable input stream for GCS objects. It is backed by a GcsFileSystem instance. */
public class GoogleCloudStorageInputStream extends SeekableInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorageInputStream.class);

  private ParquetMetadataCache parquetMetadataCache;
  // Used for single-byte reads to avoid repeated allocation.
  private final ByteBuffer singleByteBuffer = ByteBuffer.wrap(new byte[1]);

  private final GcsFileSystem gcsFileSystem;
  private final VectoredSeekableByteChannel channel;
  private long position;
  private final URI gcsPath;
  private GcsItemId gcsItemId;

  private volatile boolean closed;

  private long fileSize;

  private GcsFileInfo gcsFileInfo;

  public static GoogleCloudStorageInputStream create(
      GcsFileSystem gcsFileSystem, GcsFileInfo gcsFileInfo) throws IOException {
    checkState(gcsFileInfo != null, "GcsFileInfo shouldn't be null");
    VectoredSeekableByteChannel channel =
        gcsFileSystem.open(
            gcsFileInfo,
            gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions());
    return new GoogleCloudStorageInputStream(gcsFileSystem, channel, gcsFileInfo);
  }

  public static GoogleCloudStorageInputStream create(GcsFileSystem gcsFileSystem, URI path)
      throws IOException {
    checkState(gcsFileSystem != null, "GcsFileSystem shouldn't be null");
    GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(path);
    return create(gcsFileSystem, fileInfo);
  }

  public static GoogleCloudStorageInputStream create(GcsFileSystem gcsFileSystem, GcsItemId itemId)
      throws IOException {
    checkState(gcsFileSystem != null, "GcsFileSystem shouldn't be null");
    VectoredSeekableByteChannel channel =
        gcsFileSystem.open(
            itemId, gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions());
    return new GoogleCloudStorageInputStream(gcsFileSystem, channel, itemId);
  }

  private GoogleCloudStorageInputStream(
      GcsFileSystem gcsFileSystem, VectoredSeekableByteChannel channel, GcsFileInfo gcsFileInfo) {
    this(gcsFileSystem, channel, gcsFileInfo.getItemInfo().getItemId());
    initializeMetadata(gcsFileInfo);
  }

  private GoogleCloudStorageInputStream(
      GcsFileSystem gcsFileSystem, VectoredSeekableByteChannel channel, GcsItemId itemId) {
    this.gcsFileSystem = gcsFileSystem;
    this.channel = channel;
    this.gcsPath =
        URI.create(BlobId.of(itemId.getBucketName(), itemId.getObjectName().get()).toGsUtilUri());
    this.gcsItemId = itemId;
    this.position = 0;
    // TODO: Get baseGcsPath and localCacheDirPrefix from options
    String baseGcsPath = "gs://" + itemId.getBucketName();
    String localCacheDirPrefix = "gcsio";
    this.parquetMetadataCache = ParquetMetadataCache.getInstance(baseGcsPath, localCacheDirPrefix);
  }

  @Override
  public long getPos() {
    return position;
  }

  @Override
  public void seek(long newPos) throws IOException {
    checkArgument(newPos >= 0, "position can't be negative: %s", newPos);
    checkNotClosed("Cannot seek: already closed");
    position = newPos;
    channel.position(newPos);
  }

  @Override
  public int read() throws IOException {
    checkNotClosed("Cannot read: already closed");
    // Delegate to the byte array read method to reuse the cache logic.
    int bytesRead = read(singleByteBuffer.array(), 0, 1);
    if (bytesRead == -1) {
      return -1;
    }
    return singleByteBuffer.array()[0] & 0xFF;
  }

  @Override
  public int read(ByteBuffer byteBuffer) throws IOException {
    checkNotClosed("Cannot read: already closed");
    long channelPosition = channel.position();
    checkState(
        channelPosition == position,
        "Channel position (%s) and stream position (%s) should be the same",
        channelPosition,
        position);

    int bytesRead = channel.read(byteBuffer);
    if (bytesRead > 0) {
      position += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public int read(@Nonnull byte[] buffer, int offset, int length) throws IOException {
    checkNotClosed("Cannot read: already closed");
    checkNotNull(buffer, "buffer must not be null");

    if (offset < 0 || length < 0 || length > buffer.length - offset) {
      throw new IndexOutOfBoundsException();
    }
    if (length == 0) {
      return 0;
    }
    return read(ByteBuffer.wrap(buffer, offset, length));
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      if (channel != null) {
        channel.close();
      }
    }
  }

  private void checkNotClosed(String msg) throws IOException {
    if (closed) {
      throw new IOException(gcsPath + ": " + msg);
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    try (VectoredSeekableByteChannel byteChannel = openReadChannel()) {
      byteChannel.position(position);
      int numberOfBytesRead = byteChannel.read(ByteBuffer.wrap(buffer, offset, length));
      if (numberOfBytesRead < length) {
        throw new EOFException(
            "Reached the end of stream with "
                + (length - numberOfBytesRead)
                + " bytes left to read");
      }
    }
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    if (!isMetadataInitialized()) {
      initializeMetadata();
    }

    if (parquetMetadataCache != null) {
      Optional<ParquetMetadataCache.ParquetObjectMetadata> metadataOpt =
          parquetMetadataCache.getMetadata(gcsPath.toString());
      if (metadataOpt.isPresent()) {
        ParquetMetadataCache.ParquetObjectMetadata metadata = metadataOpt.get();
        byte[] rawMetadata = metadata.getRawMetadata();
        int footerLength = metadata.getFooterLength();
        long fileSize = metadata.getFileSize();

        if (rawMetadata != null && rawMetadata.length >= length) {
          // Ensure we don't read beyond the actual footer size
          int readLength = Math.min(length, footerLength);
          int srcPos = Math.max(0, rawMetadata.length - readLength);
          int destPos = offset + Math.max(0, length - readLength);
          int numBytesToCopy = Math.min(readLength, length);

          if (length > footerLength) {
            System.arraycopy(rawMetadata, 0, buffer, offset + length - footerLength, footerLength);
            // fill the rest of the buffer with 0s
            java.util.Arrays.fill(buffer, offset, offset + length - footerLength, (byte) 0);
            return length;
          } else {
            System.arraycopy(rawMetadata, rawMetadata.length - length, buffer, offset, length);
            return length;
          }
        } else {
          LOG.warn(
              "Metadata found in cache, but rawMetadata is smaller than requested length or null.");
        }
      } else {
        LOG.debug("Metadata not found in cache for {}. Falling back to GCS read.", gcsPath);
      }
    }

    // Fallback to reading directly from GCS
    try (VectoredSeekableByteChannel byteChannel = openReadChannel()) {
      long size = gcsFileInfo.getItemInfo().getSize();
      long startPosition = Math.max(0, size - length);
      byteChannel.position(startPosition);
      return byteChannel.read(ByteBuffer.wrap(buffer, offset, length));
    }
  }

  @Override
  public void readVectored(List<GcsObjectRange> fileRanges, IntFunction<ByteBuffer> alloc)
      throws IOException {
    channel.readVectored(fileRanges, alloc);
  }

  private VectoredSeekableByteChannel openReadChannel() throws IOException {
    if (gcsFileInfo != null) {
      return gcsFileSystem.open(
          gcsFileInfo,
          gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions());
    }
    return gcsFileSystem.open(
        gcsItemId, gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions());
  }

  private boolean isMetadataInitialized() {
    return gcsFileInfo != null;
  }

  private void initializeMetadata() throws IOException {
    initializeMetadata(gcsFileSystem.getFileInfo(gcsItemId));
  }

  private void initializeMetadata(GcsFileInfo fileInfo) {
    this.gcsFileInfo = fileInfo;
    this.gcsItemId = fileInfo.getItemInfo().getItemId();
    this.fileSize = fileInfo.getItemInfo().getSize();
  }
}
