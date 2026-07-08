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

package com.google.cloud.gcs.analyticscore.core.optimizer;

import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A global, thread-safe registry that maps files to their historical read access patterns. This
 * effectively acts as a shared statistical Markov Chain for predictive prefetching.
 */
public class GlobalReadPatternRegistry {
  private static final GlobalReadPatternRegistry INSTANCE = new GlobalReadPatternRegistry();
  // Using /tmp/ to represent a local SSD mount for the POC
  private static final String CACHE_FILE = "/tmp/gcs_analytics_heuristic.dat";

  public static class PredictedRange implements Serializable {
    private static final long serialVersionUID = 1L;
    public final long offset;
    public final int length;

    public PredictedRange(long offset, int length) {
      this.offset = offset;
      this.length = length;
    }
  }

  private Map<String, Map<Long, PredictedRange>> transitions = new ConcurrentHashMap<>();
  private final java.util.concurrent.atomic.AtomicBoolean isPersisting =
      new java.util.concurrent.atomic.AtomicBoolean(false);

  private GlobalReadPatternRegistry() {
    load();
  }

  public static GlobalReadPatternRegistry getInstance() {
    return INSTANCE;
  }

  public void recordTransition(
      GcsItemId itemId, long previousOffset, long nextOffset, int nextLength) {
    String objectName = itemId.getObjectName().orElse("");
    if (objectName.isEmpty()) return;

    transitions
        .computeIfAbsent(objectName, k -> new ConcurrentHashMap<>())
        .put(previousOffset, new PredictedRange(nextOffset, nextLength));

    persist();
  }

  public PredictedRange predictNext(GcsItemId itemId, long currentOffset) {
    String objectName = itemId.getObjectName().orElse("");
    Map<Long, PredictedRange> fileTransitions = transitions.get(objectName);
    if (fileTransitions != null) {
      return fileTransitions.get(currentOffset);
    }
    return null;
  }

  private void persist() {
    // Debounce the persistence to avoid blocking the hot read path and thrashing the disk
    if (isPersisting.compareAndSet(false, true)) {
      @SuppressWarnings("FutureReturnValueIgnored")
      var unused =
          java.util.concurrent.CompletableFuture.runAsync(
              () -> {
                try {
                  synchronized (this) { // Keep the actual I/O operation atomic
                    try (ObjectOutputStream oos =
                        new ObjectOutputStream(new FileOutputStream(CACHE_FILE))) {
                      oos.writeObject(transitions);
                    }
                  }
                } catch (Exception e) {
                  // Ignore for POC
                } finally {
                  isPersisting.set(false);
                }
              });
    }
  }

  @SuppressWarnings("unchecked")
  private void load() {
    Path path = Paths.get(CACHE_FILE);
    if (Files.exists(path)) {
      try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(CACHE_FILE))) {
        transitions = (Map<String, Map<Long, PredictedRange>>) ois.readObject();
      } catch (Exception e) {
        // Ignore for POC
      }
    }
  }
}
