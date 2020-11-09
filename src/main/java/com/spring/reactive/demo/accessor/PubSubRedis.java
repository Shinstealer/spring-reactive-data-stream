package com.spring.reactive.demo.accessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;

public class PubSubRedis {

  private StringRedisTemplate stringRedisTemplate;

  private ObjectMapper objectMapper = new ObjectMapper();

  public PubSubRedis(
      @Qualifier("pubsubStringRedisTemplate") StringRedisTemplate stringRedisTemplate) {
  }

  public void convertAndSend(String channel, Object message) throws JsonProcessingException {
    stringRedisTemplate.convertAndSend(channel, objectMapper.writeValueAsString(message));
  }

}
