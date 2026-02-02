package pl.put.backend;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

public class CustomRetryPolicy implements RetryPolicy {

    @Override
    public RetryDecision onWriteTimeout(Statement stmt, ConsistencyLevel cl,
                                        WriteType writeType, int requiredAcks,
                                        int receivedAcks, int nbRetry) {
        return RetryDecision.retry(cl);
    }

    @Override
    public RetryDecision onReadTimeout(Statement stmt, ConsistencyLevel cl,
                                       int requiredResponses, int receivedResponses,
                                       boolean dataRetrieved, int nbRetry) {
        return RetryDecision.retry(cl);
    }

    @Override
    public RetryDecision onUnavailable(Statement stmt, ConsistencyLevel cl,
                                       int requiredReplica, int aliveReplica, int nbRetry) {
        return RetryDecision.retry(cl);
    }

    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel consistencyLevel, DriverException e, int i) {
        return RetryDecision.rethrow();
    }

    @Override public void init(Cluster cluster) {}
    @Override public void close() {}
}
