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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCache;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCacheCaffeineImpl;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCacheNoOpImpl;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;

/**
 * Manages the caching layer for GCS objects. This class acts as a registry for format-specific
 * promises (e.g., Parquet footer prefetch).
 */
public class AnalyticsCacheManager {

  private final AnalyticsCache<GcsItemId, CompletableFuture<ByteBuffer>> footerCache;

  public AnalyticsCacheManager(GcsCacheOptions options) {
    checkNotNull(options, "options cannot be null");
    this.footerCache =
        options.isFooterCacheEnabled()
            ? AnalyticsCacheCaffeineImpl.create(options.getFooterCacheMaxEntries())
            : AnalyticsCacheNoOpImpl.create();
  }

  /**
   * Returns the future for the cached footer of the given {@code itemId}, obtaining it from the
   * {@code footerLoader} if necessary. This method is atomic.
   */
  public CompletableFuture<ByteBuffer> getFooterFuture(
      GcsItemId itemId, FooterLoader footerLoader) {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(footerLoader, "footerLoader cannot be null");
    return footerCache.get(itemId, footerLoader::load);
  }

  /** Returns the future for the cached footer if it exists, otherwise returns {@code null}. */
  @Nullable
  public CompletableFuture<ByteBuffer> getFooterFutureIfPresent(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    return footerCache.get(itemId).orElse(null);
  }

  /** Invalidates the cached footer for the given {@code itemId}. */
  public void invalidateFooter(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    footerCache.invalidate(itemId);
  }

  /** Invalidates all cached entries. */
  public void invalidateAll() {
    footerCache.invalidateAll();
  }

  /**
   * Joins the given future and returns its result as a read-only buffer, unwrapping any {@link
   * CompletionException} into its cause.
   *
   * @throws IOException if the future completed exceptionally with an {@link IOException}.
   */
  public static ByteBuffer join(CompletableFuture<ByteBuffer> future) throws IOException {
    try {
      return future.join().asReadOnlyBuffer();
    } catch (CompletionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IOException(cause);
    }
  }

  /** A loader for GCS object footers that returns a promise. */
  @FunctionalInterface
  public interface FooterLoader {
    /** Returns a future that will be completed with the footer data. */
    CompletableFuture<ByteBuffer> load(GcsItemId itemId);
  }
}
