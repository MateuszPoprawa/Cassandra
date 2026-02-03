import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import pl.put.backend.BackendException;
import pl.put.backend.BackendSession;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(MockitoJUnitRunner.class)
public class RentBookStressTest {

    @Spy
    private final BackendSession backendSession = new BackendSession("127.0.0.1", "library");

    public RentBookStressTest() throws BackendException {
    }

    @Test
    public void stressTest_rentBookCassandra() throws InterruptedException, BackendException {
        int threads = 100;
        int requests = 200;

        Mockito.doReturn("test").when(backendSession).getLibraryFromTerminal();
        Mockito.doReturn("test").when(backendSession).getBookFromTerminal();

        Mockito.doAnswer(inv -> Thread.currentThread().getName())
                .when(backendSession)
                .getUserFromTerminal();

        backendSession.upsertBookCassandra("test", "test", 5);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(requests);

        AtomicInteger failures = new AtomicInteger(0);

        long start = System.currentTimeMillis();

        for (int i = 0; i < requests; i++) {
            executor.submit(() -> {
                try {
                    backendSession.rentBook();
                    if (backendSession.checkBookStatus() == 0) {
                        failures.incrementAndGet();
                    }
                    backendSession.returnBook();
                    if (backendSession.checkBookStatus() != 0) {
                        failures.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        long end = System.currentTimeMillis();

        System.out.println("❌ Failures: " + failures.get());
        System.out.println("⏱ Time: " + (end - start) + " ms");
        backendSession.getConflictCount();
    }
}
