/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link RecreateOnResetOperatorCoordinator}. */
class RecreateOnResetOperatorCoordinatorTest {

    private static final OperatorID OPERATOR_ID = new OperatorID(1234L, 5678L);
    private static final int NUM_SUBTASKS = 1;
    private static final String STATE_READER_CLOSER_THREAD = "Coordinator state reader closer";

    @Test
    void testQuiesceableContextForwardsProperties() {
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator.QuiesceableContext quiesceableContext =
                new RecreateOnResetOperatorCoordinator.QuiesceableContext(context);

        assertThat(quiesceableContext.getOperatorId()).isEqualTo(OPERATOR_ID);
        assertThat(quiesceableContext.currentParallelism()).isEqualTo(NUM_SUBTASKS);
    }

    @Test
    void testQuiesceableContextNotQuiesced() {
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator.QuiesceableContext quiesceableContext =
                new RecreateOnResetOperatorCoordinator.QuiesceableContext(context);

        final Exception exception = new Exception();
        quiesceableContext.failJob(exception);

        assertThat(context.isJobFailed()).isTrue();
        assertThat(context.getJobFailureReason()).isEqualTo(exception);
    }

    @Test
    void testQuiescedContext() {
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator.QuiesceableContext quiesceableContext =
                new RecreateOnResetOperatorCoordinator.QuiesceableContext(context);

        quiesceableContext.quiesce();
        quiesceableContext.failJob(new Exception());

        assertThat(context.isJobFailed()).isFalse();
    }

    @Test
    void testResetToCheckpoint() throws Exception {
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider(null);
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator coordinator = createCoordinator(provider, context);

        RecreateOnResetOperatorCoordinator.QuiesceableContext contextBeforeReset =
                coordinator.getQuiesceableContext();
        TestingOperatorCoordinator internalCoordinatorBeforeReset =
                getInternalCoordinator(coordinator);

        byte[] stateToRestore = new byte[0];
        coordinator.resetToCheckpoint(1L, stateToRestore);

        // Use the checkpoint to ensure all the previous method invocation has succeeded.
        coordinator.waitForAllAsyncCallsFinish();

        assertThat(contextBeforeReset.isQuiesced()).isTrue();
        assertThat(internalCoordinatorBeforeReset.getLastRestoredCheckpointState()).isNull();

        TestingOperatorCoordinator internalCoordinatorAfterReset =
                getInternalCoordinator(coordinator);
        assertThat(internalCoordinatorAfterReset.getLastRestoredCheckpointState())
                .isEqualTo(stateToRestore);
        assertThat(internalCoordinatorBeforeReset).isNotEqualTo(internalCoordinatorAfterReset);
    }

    @Test
    @Timeout(60)
    void testResetToCheckpointReadsStateFromHandleInSeparateThread(@TempDir java.nio.file.Path tmp)
            throws Exception {
        final byte[] stateToRestore = new byte[4096];
        new Random().nextBytes(stateToRestore);
        final java.nio.file.Path file = tmp.resolve("coordinator-state");
        Files.write(file, stateToRestore);
        // the recorded size may be unknown
        final ThreadRecordingStateHandle handle =
                new ThreadRecordingStateHandle(new FileStateHandle(new Path(file.toUri()), -1));
        final RecreateOnResetOperatorCoordinator coordinator =
                createCoordinator(
                        new TestingCoordinatorProvider(null),
                        new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS));

        coordinator.resetToCheckpointFromHandle(1L, handle);

        assertThat(getInternalCoordinator(coordinator).getLastRestoredCheckpointState())
                .isEqualTo(stateToRestore);
        assertThat(handle.readingThread).isNotNull().isNotSameAs(Thread.currentThread());
    }

    @Test
    @Timeout(60)
    void testResetToCheckpointFailsJobIfStateCannotBeRead(@TempDir java.nio.file.Path tmp)
            throws Exception {
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final RecreateOnResetOperatorCoordinator coordinator =
                createCoordinator(new TestingCoordinatorProvider(null), context);

        coordinator.resetToCheckpointFromHandle(
                1L, new FileStateHandle(new Path(tmp.resolve("absent").toUri()), 4096));

        CommonTestUtils.waitUtil(
                context::isJobFailed,
                Duration.ofSeconds(10),
                "The job should fail because the coordinator state cannot be read.");
        assertThat(context.getJobFailureReason())
                .hasMessageContaining("Could not read the coordinator state of checkpoint 1");
    }

    @Test
    @Timeout(60)
    void testResetToCheckpointDoesNotWaitForTheStateToBeRead() throws Exception {
        final RecreateOnResetOperatorCoordinator coordinator =
                createCoordinator(
                        new TestingCoordinatorProvider(null),
                        new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS));
        final BlockingStateHandle slowState = new BlockingStateHandle(new byte[] {7}, false);

        coordinator.resetToCheckpointFromHandle(1L, slowState);
        CommonTestUtils.waitUtil(
                () -> slowState.readingThread != null,
                Duration.ofSeconds(10),
                "The state should be read.");
        assertThat(slowState.readingThread).isNotSameAs(Thread.currentThread());
        slowState.release();

        assertThat(getInternalCoordinator(coordinator).getLastRestoredCheckpointState())
                .containsExactly(7);
        assertThat(slowState.closed.getCount()).isZero();
    }

    @Timeout(60)
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLaterResetWinsOverEarlierResetStillReadingItsState(boolean earlierReadFails)
            throws Exception {
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final RecreateOnResetOperatorCoordinator coordinator =
                createCoordinator(new TestingCoordinatorProvider(null), context);
        final BlockingStateHandle slowState =
                new BlockingStateHandle(new byte[] {1}, earlierReadFails);

        coordinator.resetToCheckpointFromHandle(1L, slowState);
        slowState.reading.await();
        coordinator.resetToCheckpoint(2L, new byte[] {2});
        final TestingOperatorCoordinator afterLaterReset = getInternalCoordinator(coordinator);
        slowState.release();

        assertThat(afterLaterReset.getLastRestoredCheckpointState()).containsExactly(2);
        assertThat(getInternalCoordinator(coordinator)).isSameAs(afterLaterReset);
        assertThat(context.isJobFailed()).isFalse();
    }

    @Timeout(60)
    @ParameterizedTest(name = "replacedByLaterReset = {0}, stoppedWhileOpening = {1}")
    @CsvSource({"true, false", "false, false", "true, true", "false, true"})
    void testReadOfStateIsStoppedWhenTheResetIsReplacedOrClosed(
            boolean replacedByLaterReset, boolean stoppedWhileOpening) throws Exception {
        final MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        final RecreateOnResetOperatorCoordinator coordinator =
                createCoordinator(new TestingCoordinatorProvider(null), context);
        final StoppableStateHandle slowState = new StoppableStateHandle();

        coordinator.resetToCheckpointFromHandle(1L, slowState);
        CommonTestUtils.waitUtil(
                () -> slowState.readingThread != null,
                Duration.ofSeconds(10),
                "The state should be read.");
        if (!stoppedWhileOpening) {
            slowState.opened.countDown();
            slowState.stream.reading.await();
        }
        if (replacedByLaterReset) {
            coordinator.resetToCheckpoint(2L, new byte[] {2});
        } else {
            coordinator.close();
        }
        if (stoppedWhileOpening) {
            CommonTestUtils.waitUtil(
                    () -> !isStoppingAStateRead(),
                    Duration.ofSeconds(10),
                    "The read should be stopped.");
            slowState.opened.countDown();
        }

        slowState.stream.closed.await();
        slowState.readingThread.join();
        if (stoppedWhileOpening) {
            assertThat(slowState.stream.closingThread).isSameAs(slowState.readingThread);
        } else {
            assertThat(slowState.stream.closingThread.getName())
                    .startsWith(STATE_READER_CLOSER_THREAD);
        }
        if (replacedByLaterReset) {
            assertThat(getInternalCoordinator(coordinator).getLastRestoredCheckpointState())
                    .containsExactly(2);
        }
        assertThat(context.isJobFailed()).isFalse();
    }

    @Test
    void testResetToCheckpointTimeout() throws Exception {
        final long closingTimeoutMs = 1L;
        // Let the user coordinator block on close.
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider(new CountDownLatch(1));
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator) provider.create(context, closingTimeoutMs);

        coordinator.resetToCheckpoint(2L, new byte[0]);
        CommonTestUtils.waitUtil(
                context::isJobFailed,
                Duration.ofSeconds(5),
                "The job should fail due to resetToCheckpoint() timeout.");
    }

    @Test
    void testMethodCallsOnLongResetToCheckpoint() throws Exception {
        final long closingTimeoutMs = Long.MAX_VALUE;
        final CountDownLatch blockOnCloseLatch = new CountDownLatch(1);
        // Let the user coordinator block on close.
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider(blockOnCloseLatch);
        MockOperatorCoordinatorContext context = new MockOperatorCoordinatorContext(OPERATOR_ID, 2);
        RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator) provider.create(context, closingTimeoutMs);

        // Set up the testing variables.
        final byte[] restoredState = new byte[0];
        final TestingEvent testingEvent = new TestingEvent();
        final long completedCheckpointId = 1234L;

        // Reset the coordinator which closes the current internal coordinator
        // and then create a new one. The closing of the current internal
        // coordinator will block until the blockOnCloseLatch is pulled.
        coordinator.resetToCheckpoint(2L, restoredState);

        // The following method calls should be applied to the new internal
        // coordinator asynchronously because the current coordinator has not
        // been successfully closed yet.
        coordinator.handleEventFromOperator(1, 0, testingEvent);
        coordinator.executionAttemptFailed(1, 0, new Exception("Subtask Failure Exception."));
        coordinator.notifyCheckpointComplete(completedCheckpointId);

        // The new coordinator should not have been created because the resetToCheckpoint()
        // should block on closing the current coordinator.
        assertThat(provider.getCreatedCoordinators()).hasSize(1);

        // Now unblock the closing of the current coordinator.
        blockOnCloseLatch.countDown();

        // Take a checkpoint on the coordinator after reset.
        CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        coordinator.checkpointCoordinator(5678L, checkpointFuture);
        coordinator.waitForAllAsyncCallsFinish();

        // Verify that the methods calls have been made against the new coordinator.
        TestingOperatorCoordinator internalCoordinatorAfterReset =
                getInternalCoordinator(coordinator);
        // The internal coordinator after reset should have triggered a new checkpoint.
        assertThat(internalCoordinatorAfterReset.getLastTriggeredCheckpoint())
                .isEqualTo(checkpointFuture);
        // The internal coordinator after reset should be the second coordinator created by the
        // provider.
        assertThat(internalCoordinatorAfterReset)
                .isEqualTo(provider.getCreatedCoordinators().get(1));
        // The internal coordinator after reset should have been reset to the restored state.
        assertThat(internalCoordinatorAfterReset.getLastRestoredCheckpointState())
                .isEqualTo(restoredState);
        // The internal coordinator after reset should have received the testing event.
        assertThat(internalCoordinatorAfterReset.getNextReceivedOperatorEvent())
                .isEqualTo(testingEvent);
        // The internal coordinator after reset should have handled the failure of subtask 1.
        assertThat(internalCoordinatorAfterReset.getFailedTasks())
                .isEqualTo(Collections.singletonList(1));
        // The internal coordinator after reset should have the completedCheckpointId.
        assertThat(internalCoordinatorAfterReset.getLastCheckpointComplete())
                .isEqualTo(completedCheckpointId);
    }

    @Test
    @Timeout(30)
    void testConsecutiveResetToCheckpoint() throws Exception {
        final long closingTimeoutMs = Long.MAX_VALUE;
        final int numResets = 1000;
        // Let the user coordinator block on close.
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider();
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator coordinator =
                (RecreateOnResetOperatorCoordinator) provider.create(context, closingTimeoutMs);

        // Loop to get some interleaved method invocations on multiple instances
        // of active coordinators.
        for (int i = 0; i < numResets; i++) {
            coordinator.handleEventFromOperator(1, 0, new TestingEvent(i));
            coordinator.executionAttemptFailed(i, 0, new Exception());
            CompletableFuture<byte[]> future = CompletableFuture.completedFuture(new byte[i]);
            coordinator.checkpointCoordinator(i, future);
            final int loop = i;
            future.thenRun(() -> coordinator.notifyCheckpointComplete(loop));
            // The reset bytes has a length of i+1 here because this will be reset to the
            // next internal coordinator.
            coordinator.resetToCheckpoint(i, new byte[i + 1]);
        }

        coordinator.waitForAllAsyncCallsFinish();

        // Verify that the methods calls have been made against the coordinators.
        for (TestingOperatorCoordinator internalCoordinator : provider.getCreatedCoordinators()) {
            // The indexOfCoordinator is set to 0 by default because:
            // 1. For the initial internal coordinator, its index is 0.
            // 2. For all the subsequent internal coordinators, there are two cases:
            //    a. they have processed at least one method call. In that case the coordinator
            //       must have been restored to the given state. So the indexOfCoordinator will
            //       be updated correctly.
            //    b. no method call was processed. In this case the indexOfCoordinator does not
            //       matter because all the fields will either be empty or null.
            int indexOfCoordinator = 0;
            byte[] lastRestoredState = internalCoordinator.getLastRestoredCheckpointState();
            if (lastRestoredState != null) {
                indexOfCoordinator = lastRestoredState.length;
            }
            TestingEvent testingEvent =
                    (TestingEvent) internalCoordinator.getNextReceivedOperatorEvent();
            List<Integer> failedTasks = internalCoordinator.getFailedTasks();

            final int finalIndexOfCoordinator = indexOfCoordinator;
            assertThat(testingEvent)
                    .satisfiesAnyOf(
                            x -> assertThat(x).isNull(),
                            x -> assertThat(x.getId()).isEqualTo(finalIndexOfCoordinator));
            assertThat(failedTasks)
                    .satisfiesAnyOf(
                            x -> assertThat(x).isEmpty(),
                            x ->
                                    assertThat(x)
                                            .hasSize(1)
                                            .element(0)
                                            .isEqualTo(finalIndexOfCoordinator));
            assertThat(internalCoordinator)
                    .satisfiesAnyOf(
                            x -> assertThat(x.hasCompleteCheckpoint()).isFalse(),
                            x ->
                                    assertThat(x.getLastCheckpointComplete())
                                            .isEqualTo(finalIndexOfCoordinator));
            assertThat(internalCoordinator)
                    .satisfiesAnyOf(
                            x -> assertThat(x.hasTriggeredCheckpoint()).isFalse(),
                            x ->
                                    assertThat(x.getLastTriggeredCheckpoint().get())
                                            .hasSize(finalIndexOfCoordinator));
        }
        coordinator.close();
        TestingOperatorCoordinator internalCoordinator = getInternalCoordinator(coordinator);
        CommonTestUtils.waitUtil(
                internalCoordinator::isClosed,
                Duration.ofSeconds(5),
                "Timed out when waiting for the coordinator to close.");
    }

    @Test
    void testNotifyCheckpointAbortedSuccess() throws Exception {
        TestingCoordinatorProvider provider = new TestingCoordinatorProvider(null);
        MockOperatorCoordinatorContext context =
                new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS);
        RecreateOnResetOperatorCoordinator coordinator = createCoordinator(provider, context);
        TestingOperatorCoordinator internalCoordinatorAfterReset =
                getInternalCoordinator(coordinator);

        long checkpointId = 10L;
        coordinator.notifyCheckpointAborted(checkpointId);
        assertThat(internalCoordinatorAfterReset.getLastCheckpointAborted())
                .isEqualTo(checkpointId);
    }

    // ---------------

    private RecreateOnResetOperatorCoordinator createCoordinator(
            TestingCoordinatorProvider provider, OperatorCoordinator.Context context)
            throws Exception {
        return (RecreateOnResetOperatorCoordinator) provider.create(context);
    }

    private static boolean isStoppingAStateRead() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.getName().startsWith(STATE_READER_CLOSER_THREAD));
    }

    private TestingOperatorCoordinator getInternalCoordinator(
            RecreateOnResetOperatorCoordinator coordinator) throws Exception {
        return (TestingOperatorCoordinator) coordinator.getInternalCoordinator();
    }

    // ---------------

    @SuppressWarnings("serial")
    private static class TestingCoordinatorProvider
            extends RecreateOnResetOperatorCoordinator.Provider {

        private final CountDownLatch blockOnCloseLatch;
        private final List<TestingOperatorCoordinator> createdCoordinators;

        public TestingCoordinatorProvider() {
            this(null);
        }

        public TestingCoordinatorProvider(CountDownLatch blockOnCloseLatch) {
            super(OPERATOR_ID);
            this.blockOnCloseLatch = blockOnCloseLatch;
            this.createdCoordinators = new ArrayList<>();
        }

        @Override
        protected OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
            TestingOperatorCoordinator testingCoordinator =
                    new TestingOperatorCoordinator(context, blockOnCloseLatch);
            createdCoordinators.add(testingCoordinator);
            return testingCoordinator;
        }

        private List<TestingOperatorCoordinator> getCreatedCoordinators() {
            return createdCoordinators;
        }
    }

    /**
     * State whose reads block until released, even if the stream is closed, and then return the
     * given bytes or fail.
     */
    private static class BlockingStateHandle implements StreamStateHandle {
        private static final long serialVersionUID = 1L;
        private final byte[] bytes;
        private final boolean fail;
        private final CountDownLatch reading = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);
        private volatile Thread readingThread;

        private BlockingStateHandle(byte[] bytes, boolean fail) {
            this.bytes = bytes;
            this.fail = fail;
        }

        /** Lets the read finish and waits until the thread that read has completed the reset. */
        private void release() throws Exception {
            CommonTestUtils.waitUtil(
                    () -> readingThread != null,
                    Duration.ofSeconds(10),
                    "The state should be read.");
            released.countDown();
            readingThread.join();
        }

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            readingThread = Thread.currentThread();
            final FSDataInputStream data =
                    new ByteStreamStateHandle("state", bytes).openInputStream();
            return new FSDataInputStream() {
                @Override
                public int read() throws IOException {
                    reading.countDown();
                    try {
                        released.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    if (fail) {
                        throw new IOException("Expected test failure");
                    }
                    return data.read();
                }

                @Override
                public void close() {
                    closed.countDown();
                }

                @Override
                public void seek(long desired) throws IOException {
                    data.seek(desired);
                }

                @Override
                public long getPos() throws IOException {
                    return data.getPos();
                }
            };
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        @Override
        public PhysicalStateHandleID getStreamStateHandleID() {
            return new PhysicalStateHandleID("blocking");
        }

        @Override
        public void discardState() {}

        @Override
        public long getStateSize() {
            return bytes.length;
        }
    }

    /** State whose stream opens once allowed to, and whose reads block until it is closed. */
    private static class StoppableStateHandle implements StreamStateHandle {
        private static final long serialVersionUID = 1L;
        private final CountDownLatch opened = new CountDownLatch(1);
        private final BlockingInputStream stream = new BlockingInputStream();
        private volatile Thread readingThread;

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            readingThread = Thread.currentThread();
            try {
                opened.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            return stream;
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        @Override
        public PhysicalStateHandleID getStreamStateHandleID() {
            return new PhysicalStateHandleID("stoppable");
        }

        @Override
        public void discardState() {}

        @Override
        public long getStateSize() {
            return 0;
        }
    }

    /** A stream whose reads block until it is closed, and then fail. */
    private static class BlockingInputStream extends FSDataInputStream {
        private final CountDownLatch reading = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);
        private volatile Thread closingThread;

        @Override
        public int read() throws IOException {
            reading.countDown();
            try {
                closed.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            throw new IOException("The stream is closed.");
        }

        @Override
        public void close() {
            closingThread = Thread.currentThread();
            closed.countDown();
        }

        @Override
        public void seek(long desired) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPos() {
            return 0;
        }
    }

    /** Records the thread that reads the state. */
    private static class ThreadRecordingStateHandle implements StreamStateHandle {
        private static final long serialVersionUID = 1L;
        private final StreamStateHandle delegate;
        private volatile Thread readingThread;

        private ThreadRecordingStateHandle(StreamStateHandle delegate) {
            this.delegate = delegate;
        }

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            readingThread = Thread.currentThread();
            return delegate.openInputStream();
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        @Override
        public PhysicalStateHandleID getStreamStateHandleID() {
            return delegate.getStreamStateHandleID();
        }

        @Override
        public void discardState() throws Exception {
            delegate.discardState();
        }

        @Override
        public long getStateSize() {
            return delegate.getStateSize();
        }
    }

    private static class TestingEvent implements OperatorEvent {
        private static final long serialVersionUID = -3289352911927668275L;
        private final int id;

        private TestingEvent() {
            this(-1);
        }

        private TestingEvent(int id) {
            this.id = id;
        }

        private int getId() {
            return id;
        }
    }
}
