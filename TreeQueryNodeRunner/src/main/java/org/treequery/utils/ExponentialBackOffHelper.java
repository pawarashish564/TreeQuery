package org.treequery.utils;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import lombok.Builder;

import java.util.function.Function;

@Builder
public class ExponentialBackOffHelper <T,U> {
    private final int MAX_RETRIES;
    private final int INITIAL_INTERVAL;
    private final double MULTIPLIER;
    private static final double RANDOMIZATION_FACTOR = 0.5;


    public U backOffCall(String retryName, Function<T,U> function, T arg1) {
        IntervalFunction intervalFn =
                IntervalFunction.ofExponentialRandomBackoff(INITIAL_INTERVAL, MULTIPLIER, RANDOMIZATION_FACTOR);

        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(MAX_RETRIES)
                .intervalFunction(intervalFn)
                .build();
        Retry retry = Retry.of(retryName, retryConfig);

        Function<T, U> connectFn = Retry
                .decorateFunction(retry, (arg)-> function.apply(arg));

        return connectFn.apply(arg1);
    }
}

