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

package com.google.cloud.gcs.analyticscore.benchmarks;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class ReaderOpenInvariantsTest {
  @Test
  void footerPrefetch_parquetTailReads_matchesMeasuredReaderOpensAndBytes() throws IOException {
    try (var bench = new FooterPrefetchBenchmark()) {
      bench.setUp();
      bench.readFooterUnknownSize(new IoCounters());
      // GcsFooterOptimizer.readWithUnknownFileSize serves trailer+footer in 1 read (+1 eager open).
      assertThat(bench.storage.readerOpens()).isEqualTo(2);
      assertThat(bench.storage.bytesServed()).isEqualTo(131_080);
      bench.prefetchEnabled = false;
      bench.setUp();
      bench.readFooterUnknownSize(new IoCounters());
      assertThat(bench.storage.readerOpens()).isEqualTo(3);
    }
  }

  @Test
  void smallObjectCache_warmRead_servesZeroBytesFromStorage() throws IOException {
    try (var bench = new SmallObjectCacheBenchmark()) {
      bench.setUp();
      assertThat(bench.storage.bytesServed()).isEqualTo(256 * 1024);
      bench.storage.resetCounters();
      bench.warmRead(new IoCounters());
      // SmallObjectOptimizer.serveFromCache satisfies warm read from memory; openReadChannel still
      // eagerly opens a 0-byte SequentialReadStrategy before checking the cache.
      assertThat(bench.storage.bytesServed()).isEqualTo(0);
      assertThat(bench.storage.readerOpens()).isEqualTo(1);
      bench.coldRead(new IoCounters());
      assertThat(bench.storage.bytesServed()).isEqualTo(256 * 1024);
    }
  }

  @Test
  void readStrategy_forwardGap_skipsInPlaceWithinLimitAndReopensBeyond() throws IOException {
    try (var bench = new ReadStrategyBenchmark()) {
      bench.setUp();
      bench.seekAndRead(new IoCounters());
      // Gap 64 KiB <= 128 KiB (AbstractReadStrategy.skipInPlace): 1 initial open + 0 reopens.
      assertThat(bench.storage.readerOpens()).isEqualTo(1);
      assertThat(bench.storage.bytesServed()).isEqualTo(73_728);
      bench.forwardGapBytes = 1_048_576;
      bench.setUp();
      bench.seekAndRead(new IoCounters());
      // Gap 1 MiB > 128 KiB limit: AdaptiveReadStrategy.switchToRandom reopens (+1 = 2 opens).
      assertThat(bench.storage.readerOpens()).isEqualTo(2);
      assertThat(bench.storage.bytesServed()).isEqualTo(8_192);
    }
  }

  @Test
  void mergeGcsObjectRanges_withinVsBeyondMaxMergeGap_coalescesOnlyWithinLimit() {
    var bench = new VectoredRangeCoalescingBenchmark();
    bench.setUp();
    // VectoredIoUtil.canMerge requires gap < maxMergeGap (4 KiB default in GcsVectoredReadOptions).
    assertThat(bench.mergeRanges()).hasSize(1);
    bench.gapBytes = 1_048_576;
    bench.setUp();
    assertThat(bench.mergeRanges()).hasSize(16);
  }
}
