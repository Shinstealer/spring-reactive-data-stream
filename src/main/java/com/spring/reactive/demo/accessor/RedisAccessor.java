package com.spring.reactive.demo.accessor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoConsumers;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;

public abstract class RedisAccessor {

  private StringRedisTemplate stringRedisTemplate;

  private RedisTemplate<String, Object> redisTemplate;

  GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer();

  public RedisAccessor(RedisTemplate<String, Object> redisTemplate, StringRedisTemplate stringRedisTemplate) {
    this.stringRedisTemplate = stringRedisTemplate;
    this.redisTemplate = redisTemplate;
  }

  public RedisAccessor(StringRedisTemplate stringRedisTemplate) {
    this.stringRedisTemplate = stringRedisTemplate;
    this.redisTemplate = null;
  }

  /**
   * stream name , group name
   */
  public void xCreateGroup(String streamName, String groupName) {
    StreamOperations<String, Object, Object> ops = redisTemplate.opsForStream();
    ops.createGroup(streamName, groupName);
  }

  /**
   * stream name , value
   */
  public RecordId xAdd(String streamName, Object value) {
    byte[] serializedValue = serializer.serialize(value);
    ObjectRecord<String, Object> record = StreamRecords.newRecord().in(streamName).ofObject(serializedValue);
    return redisTemplate.opsForStream().add(record);
  }

  public <T> List<T> xRead(String streamName, String groupName, String consumerName, Duration toMillSec,
      long maxOfMessages, Class<T> cls) {
    StreamOperations<String, Object, Object> ops = redisTemplate.opsForStream();
    List<ObjectRecord<String, Object>> message = ops.read(Object.class, Consumer.from(groupName, consumerName),
        StreamReadOptions.empty().block(toMillSec).count(maxOfMessages),
        StreamOffset.create(streamName, ReadOffset.lastConsumed()));

    return message.stream().map(r -> serializer.deserialize((byte[]) r.getValue(), cls)).collect(Collectors.toList());
  }

  public XInfoConsumers xConsumers(String groupName, String consumer) {
    return redisTemplate.opsForStream().consumers(groupName, consumer);

  }

  public void set(String key, String value) {
    stringRedisTemplate.opsForValue().set(key, value);
  }

  public String get(String key) {
    return stringRedisTemplate.opsForValue().get(key);
  }

  public String bLPop(String key, long timeout) {
    return stringRedisTemplate.opsForList().leftPop(key, timeout, TimeUnit.SECONDS);
  }

  public void putAll(String key, Map<Object, Object> m) {
    stringRedisTemplate.opsForHash().putAll(key, m);
  }

  public void hset(String key, String field, String value) {
    stringRedisTemplate.opsForHash().put(key, field, value);
  }

  public void hset(String key, String field, Integer value) {
    stringRedisTemplate.opsForHash().put(key, field, String.valueOf(value));
  }

  public Boolean del(String key) {
    return stringRedisTemplate.delete(key);
  }

  public Object hget(String key, Object hashKey) {
    return stringRedisTemplate.opsForHash().get(key, hashKey);
  }

  public Map<Object, Object> hgetall(String key) {
    return stringRedisTemplate.opsForHash().entries(key);
  }

  public Long hincrby(String key, String field, long delta) {
    return stringRedisTemplate.opsForHash().increment(key, field, delta);
  }

  public void multi() {
    stringRedisTemplate.multi();
  }

  public List<Object> exec() {
    return stringRedisTemplate.exec();
  }
}
