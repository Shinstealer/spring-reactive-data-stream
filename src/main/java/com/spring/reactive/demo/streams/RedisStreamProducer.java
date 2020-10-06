package com.spring.reactive.demo.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@NoArgsConstructor
public class RedisStreamProducer {

  private StringRedisTemplate stringRedisTemplate;

  private long maxLength;

  private final ObjectMapper serializer = new ObjectMapper();

  public RecordId xadd(String streamkey, Object value) throws JsonProcessingException {
    String serializedVal = serialize(value);
    ObjectRecord<String, String> r =
        StreamRecords.newRecord().in(streamkey).ofObject(serializedVal);
    RecordId recordId = stringRedisTemplate.opsForStream().add(r);
    long trimmedCount = stringRedisTemplate.opsForStream().trim(streamkey, maxLength);
    log.info("trimmed records" + trimmedCount);
    return recordId;
  }

  private String serialize(Object value) throws JsonProcessingException {
    return serializer.writeValueAsString(value);
  }


  public static class RedisStreamProducerBuilder {

    private final RedisStreamProducer producer;

    public RedisStreamProducerBuilder() {
      this.producer = new RedisStreamProducer();
    }

    public RedisStreamProducerBuilder stringRedisTemplate(StringRedisTemplate stringRedisTemplate) {
      producer.stringRedisTemplate = stringRedisTemplate;
      return this;
    }

    public RedisStreamProducerBuilder maxLenth(long maxLength) {
      producer.maxLength = maxLength;
      return this;
    }

    public RedisStreamProducer build() {
      return producer;
    }

  }

}
