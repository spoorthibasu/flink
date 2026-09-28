/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.OperatorID;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OperatorCoordinatorCheckpoints}. */
class OperatorCoordinatorCheckpointsTest {

    @ParameterizedTest
    @CsvSource({"true,true,true", "true,false,false", "false,true,false", "false,false,false"})
    void testStateGoesToAFileOnlyIfEnabledAndSupported(
            boolean enabled, boolean supported, boolean expectedInFile) throws Exception {
        final OperatorCoordinatorCheckpoints.CoordinatorSnapshot snapshot =
                OperatorCoordinatorCheckpoints.triggerCoordinatorCheckpoint(
                                new TestingContext(supported), 1L, enabled)
                        .get();

        assertThat(snapshot.supportsStateInFile).isEqualTo(expectedInFile);
        assertThat(snapshot.state.getData()).containsExactly(1, 2, 3);
    }

    private static final class TestingContext implements OperatorCoordinatorCheckpointContext {
        private final OperatorID operatorId = new OperatorID();
        private final boolean supportsStateInFile;

        private TestingContext(boolean supportsStateInFile) {
            this.supportsStateInFile = supportsStateInFile;
        }

        @Override
        public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
            result.complete(new byte[] {1, 2, 3});
        }

        @Override
        public boolean supportsCoordinatorStateInFile() {
            return supportsStateInFile;
        }

        @Override
        public void abortCurrentTriggering() {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) {}

        @Override
        public void subtaskReset(int subtask, long checkpointId) {}

        @Override
        public OperatorID operatorId() {
            return operatorId;
        }

        @Override
        public int maxParallelism() {
            return 1;
        }

        @Override
        public int currentParallelism() {
            return 1;
        }
    }
}
