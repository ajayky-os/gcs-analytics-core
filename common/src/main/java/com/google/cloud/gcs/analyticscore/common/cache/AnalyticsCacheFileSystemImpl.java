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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An {@link AnalyticsCache} implementation backed by the local file system. This implementation
 * provides cross-process safety, ensuring atomic writes and safe LRU eviction.
 *
 * @param <K> The type of keys maintained by this cache.
 */
public class AnalyticsCacheFileSystemImpl<K> implements AnalyticsCache<K, ByteBuffer> {

  private static final Logger logger =
      Logger.getLogger(AnalyticsCacheFileSystemImpl.class.getName());
  private static final String CACHE_SUFFIX = ".cache";
  private static final String TMP_SUFFIX = ".tmp";
  private static final String LOCK_FILE_NAME = ".cleanup_lock";

  private static final ConcurrentMap<Path, Long> registeredDirs = new ConcurrentHashMap<>();
  private static final AtomicBoolean isCleanerScheduled = new AtomicBoolean(false);
  private static final ExecutorService ASYNC_WRITE_EXECUTOR =
      Executors.newFixedThreadPool(
          Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
          new ThreadFactoryBuilder().setNameFormat("fs-cache-writer-%d").setDaemon(true).build());

  private static void ensureCleanerScheduled() {
    if (isCleanerScheduled.compareAndSet(false, true)) {
      Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder()
                  .setNameFormat("fs-cache-cleaner-%d")
                  .setDaemon(true)
                  .build())
          .scheduleWithFixedDelay(
              AnalyticsCacheFileSystemImpl::performGlobalCleanup, 10, 30, TimeUnit.SECONDS);
    }
  }

  private final Path baseDir;
  private final long maxSizeBytes;
  private final Function<K, String> keyEncoder;

  private AnalyticsCacheFileSystemImpl(
      String baseDir, long maxSizeBytes, Function<K, String> keyEncoder) {
    checkArgument(maxSizeBytes > 0, "maxSizeBytes must be positive");
    this.baseDir = Paths.get(checkNotNull(baseDir, "baseDir cannot be null"));
    this.maxSizeBytes = maxSizeBytes;
    this.keyEncoder = checkNotNull(keyEncoder, "keyEncoder cannot be null");

    try {
      Files.createDirectories(this.baseDir);
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to create cache directory: " + baseDir, e);
    }

    registeredDirs.put(this.baseDir, this.maxSizeBytes);
    ensureCleanerScheduled();
  }

  /**
   * Creates a new {@link AnalyticsCacheFileSystemImpl} with the specified base directory, max
   * bytes, and key encoder.
   */
  public static <K> AnalyticsCacheFileSystemImpl<K> create(
      String baseDir, long maxSizeBytes, Function<K, String> keyEncoder) {
    return new AnalyticsCacheFileSystemImpl<>(baseDir, maxSizeBytes, keyEncoder);
  }

  @Override
  public Optional<ByteBuffer> get(K key) {
    checkNotNull(key, "key cannot be null");
    Path cacheFile = getCachePath(key);

    if (Files.exists(cacheFile)) {
      try {
        byte[] data = Files.readAllBytes(cacheFile);
        return Optional.of(ByteBuffer.wrap(data));
      } catch (IOException e) {
        // If file is deleted concurrently between exists() and readAllBytes(), treat as cache miss
        logger.log(Level.FINE, "Cache miss due to concurrent read/delete for key: " + key, e);
      }
    }
    return Optional.empty();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <E extends Exception> ByteBuffer get(
      K key, ThrowingFunction<? super K, ? extends ByteBuffer, E> mappingFunction) throws E {
    checkNotNull(key, "key cannot be null");
    checkNotNull(mappingFunction, "mappingFunction cannot be null");

    Optional<ByteBuffer> existing = get(key);
    if (existing.isPresent()) {
      return existing.get();
    }

    ByteBuffer computed = mappingFunction.apply(key);
    if (computed == null) {
      throw new NullPointerException("mappingFunction returned null for key: " + key);
    }

    // Duplicate the buffer so we don't modify the position of the one being returned to the user
    put(key, computed.duplicate());
    return computed;
  }

  @Override
  public void put(K key, ByteBuffer value) {
    checkNotNull(key, "key cannot be null");
    checkNotNull(value, "value cannot be null");

    Path finalPath = getCachePath(key);
    Path tmpPath = getTmpPath(key);

    byte[] data = new byte[value.remaining()];
    value.get(data);

    ASYNC_WRITE_EXECUTOR.submit(
        () -> {
          try {
            Files.write(tmpPath, data, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            // Atomic move ensures readers never see a partially written file
            Files.move(
                tmpPath,
                finalPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
          } catch (IOException e) {
            logger.log(Level.WARNING, "Failed to write cache file: " + finalPath, e);
          }
        });
  }

  @Override
  public void invalidate(K key) {
    checkNotNull(key, "key cannot be null");
    try {
      Files.deleteIfExists(getCachePath(key));
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to invalidate key: " + key, e);
    }
  }

  @Override
  public void invalidateAll() {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(baseDir)) {
      for (Path entry : stream) {
        if (!entry.getFileName().toString().equals(LOCK_FILE_NAME)) {
          Files.deleteIfExists(entry);
        }
      }
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to invalidate all entries", e);
    }
  }

  @Override
  public long size() {
    // Return an estimate by summing up all valid cache files
    long totalSize = 0;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(baseDir, "*" + CACHE_SUFFIX)) {
      for (Path entry : stream) {
        totalSize += Files.size(entry);
      }
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to calculate cache size", e);
    }
    return totalSize;
  }

  @Override
  public void cleanUp() {
    // No-op manually, as the daemon thread handles this continuously
  }

  /** Shuts down the cleanup executor. Should be called when the cache is no longer needed. */
  public void close() {
    // Cleanup is managed globally
  }

  private Path getCachePath(K key) {
    return baseDir.resolve(keyEncoder.apply(key) + CACHE_SUFFIX);
  }

  private Path getTmpPath(K key) {
    return baseDir.resolve(keyEncoder.apply(key) + TMP_SUFFIX + "." + UUID.randomUUID());
  }

  /** The background task responsible for process-safe size eviction and garbage collection. */
  private static void performGlobalCleanup() {
    for (Path dir : registeredDirs.keySet()) {
      long maxBytes = registeredDirs.get(dir);
      performCleanupForDir(dir, maxBytes);
    }
  }

  private static void performCleanupForDir(Path dir, long maxBytes) {
    Path lockFile = dir.resolve(LOCK_FILE_NAME);

    // Ensure directory exists
    if (!Files.exists(dir)) {
      return;
    }

    try (FileChannel channel =
        FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

      // Attempt to acquire an exclusive, non-blocking lock. If another process holds it, we skip.
      FileLock lock = channel.tryLock();
      if (lock != null) {
        try {
          evictAndGarbageCollect(dir, maxBytes);
        } finally {
          lock.release();
        }
      }
    } catch (IOException e) {
      // Ignore: Could not open lock file or acquire lock.
    }
  }

  private static void evictAndGarbageCollect(Path dir, long maxBytes) {
    List<Path> cacheFiles = new ArrayList<>();
    long totalSize = 0;
    Instant now = Instant.now();

    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (Path entry : stream) {
        String fileName = entry.getFileName().toString();

        try {
          if (fileName.endsWith(CACHE_SUFFIX)) {
            long size = Files.size(entry);
            cacheFiles.add(entry);
            totalSize += size;
          } else if (fileName.contains(TMP_SUFFIX)) {
            // Garbage collect orphaned .tmp files older than 1 hour
            BasicFileAttributes attr = Files.readAttributes(entry, BasicFileAttributes.class);
            Instant lastModified = attr.lastModifiedTime().toInstant();
            if (Duration.between(lastModified, now).toHours() >= 1) {
              Files.deleteIfExists(entry);
            }
          }
        } catch (IOException e) {
          // File might have been deleted or modified concurrently by another process.
          // Ignore and continue scanning.
        }
      }

      // If we exceed max size, sort by last modified (LRU proxy) and delete oldest
      if (totalSize > maxBytes) {
        cacheFiles.sort(
            Comparator.comparing(
                p -> {
                  try {
                    return Files.readAttributes(p, BasicFileAttributes.class)
                        .lastModifiedTime()
                        .toInstant();
                  } catch (IOException e) {
                    return Instant.MIN; // Prioritize deleting files we can't read
                  }
                }));

        for (Path cacheFile : cacheFiles) {
          if (totalSize <= maxBytes) {
            break;
          }
          try {
            long fileSize = Files.size(cacheFile);
            if (Files.deleteIfExists(cacheFile)) {
              totalSize -= fileSize;
            }
          } catch (IOException e) {
            // File already deleted or inaccessible. Just continue.
          }
        }
      }
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed during background cleanup", e);
    }
  }
}
