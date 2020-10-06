package com.spring.reactive.demo.streams;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
public class RedisStreamConsumer<T> {

  /**
   * sleep sec when error
   */
  private int sleepMsec = 3000;

  /**
   * blocking sec of redis XGroupRead
   */
  private long blockingMsec = 3000L;

  private long acquiredMaxCount = 10L;

  private int consumerThreadPriority = Thread.NORM_PRIORITY;

  /**
   * Redis key
   */
  private String streamsKey;

  /**
   * consumer group
   */
  private String group;

  /**
   * consumer name
   */
  private String consumerName;

  private int threadPoolSize = 3;

  private int threadPoolPriority = Thread.NORM_PRIORITY;

  private ExecutorService threadPoolExecutorService;

  private ScheduledExecutorService scheduledExecutorService;

  private StringRedisTemplate stringRedisTemplate;

  private final ObjectMapper serializer = new ObjectMapper();

  public static <T> RedisStreamConsumerBuilder<T> builder(Class<T> cls) {
    return new RedisStreamConsumerBuilder<>(cls);
  };

  public void init() {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setPriority(consumerThreadPriority);
        return thread;
      }
    });

    this.threadPoolExecutorService =
        Executors.newFixedThreadPool(threadPoolSize, new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setPriority(threadPoolPriority);
            return thread;
          }
        });

    try {
      stringRedisTemplate.opsForStream().createGroup(streamsKey, group);
    } catch (RedisSystemException e) {
      // TODO: handle exception
    }
  }

  /**
   * fetching streams data
   * 
   * @param entityConsumer
   * @param cls
   */
  public void start(Consumer<T> entityConsumer, Class<T> cls) {
    scheduledExecutorService.scheduleWithFixedDelay(() -> {

      List<T> streamEntries = xreadGroup(cls);
      if (streamEntries != null) {
        streamEntries.stream().map(e -> (T) e).forEach(
            entity -> threadPoolExecutorService.submit(() -> entityConsumer.accept(entity)));
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

  private List<T> xreadGroup(Class<T> cls) {
    List<ObjectRecord<String, String>> message =
        stringRedisTemplate.opsForStream().read(String.class,
            org.springframework.data.redis.connection.stream.Consumer.from(group, consumerName),
            StreamReadOptions.empty().block(Duration.ofMillis(blockingMsec))
                .count(acquiredMaxCount),
            toArray(StreamOffset.create(streamsKey, ReadOffset.lastConsumed())));
    if (message == null) {
      return Collections.emptyList();
    } else {
      return message.stream().map(r -> deserialize(r.getValue(), cls))
          .filter(java.util.Objects::nonNull).collect(Collectors.toList());
    }
  }

  @SafeVarargs
  private StreamOffset<String>[] toArray(StreamOffset<String>... array) {
    return array;
  }


  private T deserialize(String value, Class<T> cls) {
    try {
      return serializer.readValue(value, cls);
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  /**
   * To check global variable
   */
  @ToString
  public static class RedisStreamConsumerBuilder<K> {
    private final RedisStreamConsumer<K> consumer;

    public RedisStreamConsumerBuilder(Class<K> cls) {
      this.consumer = new RedisStreamConsumer<K>();
    }

    public RedisStreamConsumerBuilder<K> sleepMsec(int sleepMsec) {
      consumer.sleepMsec = sleepMsec;
      return this;
    }

    public RedisStreamConsumerBuilder<K> blockingMsec(int blockingMsec) {
      consumer.blockingMsec = blockingMsec;
      return this;
    }

    public RedisStreamConsumerBuilder<K> acquiredMaxCount(int acquiredMaxCount) {
      consumer.acquiredMaxCount = acquiredMaxCount;
      return this;
    }

    public RedisStreamConsumerBuilder<K> streamsKey(String streamsKey) {
      consumer.streamsKey = streamsKey;
      return this;
    }

    public RedisStreamConsumerBuilder<K> group(String group) {
      consumer.group = group;
      return this;
    }

    public RedisStreamConsumerBuilder<K> consumerName(String consumerName) {
      consumer.consumerName = consumerName;
      return this;
    }

    public RedisStreamConsumerBuilder<K> stringRedisTemplate(
        StringRedisTemplate stringRedisTemplate) {
      consumer.stringRedisTemplate = stringRedisTemplate;
      return this;
    }

    public RedisStreamConsumerBuilder<K> threadPoolSize(int threadPoolSize) {
      consumer.threadPoolSize = threadPoolSize;
      return this;
    }

    public RedisStreamConsumerBuilder<K> threadPoolPriority(int threadPoolPriority) {
      consumer.threadPoolPriority = threadPoolPriority;
      return this;
    }

    public RedisStreamConsumerBuilder<K> consumerThreadPriority(int consumerThreadPriority) {
      consumer.consumerThreadPriority = consumerThreadPriority;
      return this;
    }

    public RedisStreamConsumer<K> build() {
      if (consumer.streamsKey == null || consumer.group == null || consumer.consumerName == null
          || consumer.stringRedisTemplate == null) {
        throw new IllegalStateException("reqiured settings" + consumer);
      }
      consumer.init();
      return consumer;
    }


  }

}
