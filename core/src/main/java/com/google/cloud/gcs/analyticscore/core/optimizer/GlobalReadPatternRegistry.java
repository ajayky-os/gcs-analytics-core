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

  // Maps Object Name -> (Previous Offset -> Next Offset)
  private Map<String, Map<Long, Long>> transitions = new ConcurrentHashMap<>();

  private GlobalReadPatternRegistry() {
    load();
  }

  public static GlobalReadPatternRegistry getInstance() {
    return INSTANCE;
  }

  public void recordTransition(GcsItemId itemId, long previousOffset, long nextOffset) {
    String objectName = itemId.getObjectName().orElse("");
    if (objectName.isEmpty()) return;

    transitions
        .computeIfAbsent(objectName, k -> new ConcurrentHashMap<>())
        .put(previousOffset, nextOffset);

    persist();
  }

  public Long predictNext(GcsItemId itemId, long currentOffset) {
    String objectName = itemId.getObjectName().orElse("");
    Map<Long, Long> fileTransitions = transitions.get(objectName);
    if (fileTransitions != null) {
      return fileTransitions.get(currentOffset);
    }
    return null;
  }

  private synchronized void persist() {
    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(CACHE_FILE))) {
      oos.writeObject(transitions);
    } catch (Exception e) {
      // Ignore for POC
    }
  }

  @SuppressWarnings("unchecked")
  private void load() {
    Path path = Paths.get(CACHE_FILE);
    if (Files.exists(path)) {
      try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(CACHE_FILE))) {
        transitions = (Map<String, Map<Long, Long>>) ois.readObject();
      } catch (Exception e) {
        // Ignore for POC
      }
    }
  }
}
