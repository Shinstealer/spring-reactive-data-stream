package com.spring.reactive.demo.streams;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.spring.reactive.demo.accessor.RedisAccessor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.StreamInfo;

import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class RedisStreamConsumer<T> {

    private int sleepMsec = 3000;

    private long blockingMsec = 3000L;

    private long acquiredMaxCount = 10L;

    private int consumerThreadPriority = Thread.NORM_PRIORITY;

    private String streamsKey;

    private String group;

    private String consumerName;

    private int threadPoolSize = 3;

    private int threadPoolPriority = Thread.NORM_PRIORITY;

    private Map.Entry<String, String> streamQuery;

    private ExecutorService threadPoolExecutorService;
    private ScheduledExecutorService scheduledExecutorService;

    private RedisAccessor redis;

    @Autowired
    private RedisStreamAsyncCommands cmd;

    public void init() {

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setPriority(consumerThreadPriority);
                return thread;
            }
        });

        this.threadPoolExecutorService = Executors.newFixedThreadPool(threadPoolSize, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setPriority(threadPoolPriority);
                return thread;
            }
        });

        redis.xCreateGroup(this.streamsKey, this.consumerName);// create consumer group
    }

    public void start(Consumer<T> entityConsumer, Class<T> cls) {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {

            List<T> streamEntries = redis.xRead(this.streamsKey, this.group, this.consumerName,
                    Duration.ofMillis(this.blockingMsec), this.acquiredMaxCount, cls);
            if (streamEntries != null) {
                streamEntries.stream().map(e -> (T) e)
                        .forEach(entity -> threadPoolExecutorService.submit(() -> entityConsumer.accept(entity)));
            }

        }, 0, 1, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        shutdownQuietly(threadPoolExecutorService);
        shutdownQuietly(scheduledExecutorService);
    }

    private void shutdownQuietly(ExecutorService es) {
        try {
            es.shutdown();
        } catch (Throwable t) {
            // TODO: handle exception
        }
    }

}
