package net.snowflake.ingest.streaming.internal.concurrency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.junit.Assert;

class ConcurrencyTestUtils {
  /**
   * Executes an operation in multiple threads, wait for all of them to complete. If a thread throws
   * an exception, the exception is noted, printed at the and the whole method throws an exception.
   */
  static void doInManyThreads(int threadCount, Consumer<Integer> op) throws InterruptedException {
    final List<Thread> threads = new ArrayList<>();
    final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      final Thread t =
          new Thread(
              () -> {
                try {
                  op.accept(threadId);
                } catch (Exception e) {
                  exceptions.add(e);
                }
              });
      t.start();
      threads.add(t);
    }

    for (final Thread t : threads) {
      t.join();
    }

    if (!exceptions.isEmpty()) {
      for (final Exception e : exceptions) {
        e.printStackTrace();
      }
      Assert.fail(String.format("Exceptions thrown: %d", exceptions.size()));
    }
  }
}
