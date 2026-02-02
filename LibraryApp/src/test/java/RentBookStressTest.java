import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.junit.Test;
import pl.put.backend.BackendException;
import pl.put.backend.BackendSession;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RentBookStressTest {

    private final BackendSession backendSession = new BackendSession("127.0.0.1", "library");

    public RentBookStressTest() throws BackendException {
    }

    @Test
    public void stressTest_rentBookCassandra() throws InterruptedException, BackendException {
        int threads = 100;
        int requests = 1000;

        backendSession.upsertBookCassandra("test", "test", 5);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(requests);

        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failures = new AtomicInteger(0);

        long start = System.currentTimeMillis();

        for (int i = 0; i < requests; i++) {
            executor.submit(() -> {
                try {
                    String userId = UUID.randomUUID().toString();
                    String libraryId = "test";
                    String bookId = "test";

                    backendSession.rentBookCassandra(userId, libraryId, bookId);
                    success.incrementAndGet();
                } catch (WriteTimeoutException e) {
                    throw e;
                }
                catch (Exception e) {
                    failures.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        long end = System.currentTimeMillis();

        System.out.println("✅ Success: " + success.get());
        System.out.println("❌ Failures: " + failures.get());
        System.out.println("⏱ Time: " + (end - start) + " ms");
    }
}
