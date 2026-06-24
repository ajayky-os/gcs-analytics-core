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

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.util.Map;

/** Configuration options for the GCS caching layer. */
@AutoValue
public abstract class GcsCacheOptions {
  public enum CacheType {
    IN_MEMORY,
    FILE_SYSTEM
  }

  private static final String FOOTER_CACHE_ENABLED_KEY = "analytics-core.footer.cache.enabled";
  private static final String FOOTER_CACHE_TYPE_KEY = "analytics-core.footer.cache.type";
  private static final String FOOTER_CACHE_MAX_SIZE_BYTES_KEY =
      "analytics-core.footer.cache.max-size-bytes";
  private static final String SMALL_OBJECT_CACHE_TYPE_KEY =
      "analytics-core.small-object.cache.type";
  private static final String SMALL_OBJECT_CACHE_MAX_SIZE_BYTES_KEY =
      "analytics-core.small-object.cache.max-size-bytes";
  private static final String CACHE_FILESYSTEM_BASE_DIR_KEY =
      "analytics-core.cache.filesystem.base-dir";

  private static final boolean DEFAULT_FOOTER_CACHE_ENABLED = true;
  private static final CacheType DEFAULT_FOOTER_CACHE_TYPE = CacheType.IN_MEMORY;
  private static final long DEFAULT_FOOTER_CACHE_MAX_SIZE_BYTES = 100 * 1024 * 1024L; // 100 MB
  private static final CacheType DEFAULT_SMALL_OBJECT_CACHE_TYPE = CacheType.IN_MEMORY;
  private static final long DEFAULT_SMALL_OBJECT_CACHE_MAX_SIZE_BYTES = 0; // Disabled by default
  private static final String DEFAULT_CACHE_FILESYSTEM_BASE_DIR = "/tmp/gcs-analytics-core-cache";

  /** Returns whether the Parquet footer cache is enabled. */
  public abstract boolean isFooterCacheEnabled();

  public abstract CacheType getFooterCacheType();

  public abstract long getFooterCacheMaxSizeBytes();

  public abstract CacheType getSmallObjectCacheType();

  public abstract long getSmallObjectCacheMaxSizeBytes();

  public abstract String getCacheFileSystemBaseDir();

  /**
   * Returns a builder for {@link GcsCacheOptions} with the same property values as this instance.
   */
  public abstract Builder toBuilder();

  /** Returns a new builder for {@link GcsCacheOptions} with default values. */
  public static Builder builder() {
    return new AutoValue_GcsCacheOptions.Builder()
        .setFooterCacheEnabled(DEFAULT_FOOTER_CACHE_ENABLED)
        .setFooterCacheType(DEFAULT_FOOTER_CACHE_TYPE)
        .setFooterCacheMaxSizeBytes(DEFAULT_FOOTER_CACHE_MAX_SIZE_BYTES)
        .setSmallObjectCacheType(DEFAULT_SMALL_OBJECT_CACHE_TYPE)
        .setSmallObjectCacheMaxSizeBytes(DEFAULT_SMALL_OBJECT_CACHE_MAX_SIZE_BYTES)
        .setCacheFileSystemBaseDir(DEFAULT_CACHE_FILESYSTEM_BASE_DIR);
  }

  /** Creates a {@link GcsCacheOptions} instance from a map of configuration options. */
  public static GcsCacheOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    GcsCacheOptions.Builder optionsBuilder = builder();
    if (analyticsCoreOptions.containsKey(prefix + FOOTER_CACHE_ENABLED_KEY)) {
      optionsBuilder.setFooterCacheEnabled(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + FOOTER_CACHE_ENABLED_KEY)));
    }

    if (analyticsCoreOptions.containsKey(prefix + FOOTER_CACHE_TYPE_KEY)) {
      optionsBuilder.setFooterCacheType(
          CacheType.valueOf(
              analyticsCoreOptions.get(prefix + FOOTER_CACHE_TYPE_KEY).toUpperCase()));
    }
    if (analyticsCoreOptions.containsKey(prefix + FOOTER_CACHE_MAX_SIZE_BYTES_KEY)) {
      optionsBuilder.setFooterCacheMaxSizeBytes(
          Long.parseLong(analyticsCoreOptions.get(prefix + FOOTER_CACHE_MAX_SIZE_BYTES_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + SMALL_OBJECT_CACHE_TYPE_KEY)) {
      optionsBuilder.setSmallObjectCacheType(
          CacheType.valueOf(
              analyticsCoreOptions.get(prefix + SMALL_OBJECT_CACHE_TYPE_KEY).toUpperCase()));
    }
    if (analyticsCoreOptions.containsKey(prefix + SMALL_OBJECT_CACHE_MAX_SIZE_BYTES_KEY)) {
      optionsBuilder.setSmallObjectCacheMaxSizeBytes(
          Long.parseLong(analyticsCoreOptions.get(prefix + SMALL_OBJECT_CACHE_MAX_SIZE_BYTES_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + CACHE_FILESYSTEM_BASE_DIR_KEY)) {
      optionsBuilder.setCacheFileSystemBaseDir(
          analyticsCoreOptions.get(prefix + CACHE_FILESYSTEM_BASE_DIR_KEY));
    }

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsCacheOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {
    /** Sets whether the Parquet footer cache is enabled. */
    public abstract Builder setFooterCacheEnabled(boolean footerCacheEnabled);

    public abstract Builder setFooterCacheType(CacheType type);

    public abstract Builder setFooterCacheMaxSizeBytes(long maxSizeBytes);

    public abstract Builder setSmallObjectCacheType(CacheType type);

    public abstract Builder setSmallObjectCacheMaxSizeBytes(long maxSizeBytes);

    public abstract Builder setCacheFileSystemBaseDir(String dir);

    abstract GcsCacheOptions autoBuild();

    /**
     * Builds the {@link GcsCacheOptions} instance.
     *
     * @throws IllegalArgumentException if {@code footerCacheMaxEntries} is non-positive when {@code
     *     footerCacheEnabled} is {@code true}.
     */
    public GcsCacheOptions build() {
      GcsCacheOptions options = autoBuild();
      if (options.isFooterCacheEnabled()) {
        checkArgument(
            options.getFooterCacheMaxSizeBytes() > 0,
            "footerCacheMaxSizeBytes must be positive when footerCacheEnabled is true");
      }
      return options;
    }
  }
}
