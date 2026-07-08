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
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "gcs-heuristic-persister");
            t.setDaemon(true);
            return t;
          });

  private GlobalReadPatternRegistry() {
    load();
    // Persist periodically with random jitter to avoid hot-path blocking and thundering herds
    scheduleNextPersist();
  }

  private void scheduleNextPersist() {
    long delaySeconds = 55L + java.util.concurrent.ThreadLocalRandom.current().nextInt(11);
    @SuppressWarnings("FutureReturnValueIgnored")
    var unused =
        scheduler.schedule(
            () -> {
              persist();
              scheduleNextPersist();
            },
            delaySeconds,
            TimeUnit.SECONDS);
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
    try {
      synchronized (this) {
        // 1. Load the absolute latest state from disk (what other processes learned)
        Map<String, Map<Long, PredictedRange>> diskState = loadFromDisk();

        // 2. Merge our in-memory learnings into the disk state.
        // Using putAll() ensures we overwrite with the latest observed transition for an offset,
        // rather than amplifying or duplicating entries.
        for (Map.Entry<String, Map<Long, PredictedRange>> entry : transitions.entrySet()) {
          diskState
              .computeIfAbsent(entry.getKey(), k -> new ConcurrentHashMap<>())
              .putAll(entry.getValue());
        }

        // 3. Update our own memory to include what other processes learned
        this.transitions = diskState;

        // 4. Write to a temporary file first, then atomic move to support multiple JVM processes
        // reading/writing concurrently on the same VM without file corruption.
        String tempFileName = CACHE_FILE + ".tmp." + UUID.randomUUID();
        Path tempPath = Paths.get(tempFileName);
        try (ObjectOutputStream oos =
            new ObjectOutputStream(new FileOutputStream(tempPath.toFile()))) {
          oos.writeObject(diskState);
        }
        Files.move(
            tempPath,
            Paths.get(CACHE_FILE),
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (Exception e) {
      // Ignore for POC
    }
  }

  private void load() {
    this.transitions = loadFromDisk();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Map<Long, PredictedRange>> loadFromDisk() {
    Path path = Paths.get(CACHE_FILE);
    if (Files.exists(path)) {
      try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(CACHE_FILE))) {
        return (Map<String, Map<Long, PredictedRange>>) ois.readObject();
      } catch (Exception e) {
        // Ignore for POC and return new map
      }
    }
    return new ConcurrentHashMap<>();
  }
}
