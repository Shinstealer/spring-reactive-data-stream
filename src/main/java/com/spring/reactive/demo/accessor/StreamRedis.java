package com.spring.reactive.demo.accessor;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class StreamRedis {

  private StringRedisTemplate stringRedisTemplate;
  
  public StreamRedis(
      @Qualifier("streamStringRedisTemplate") StringRedisTemplate stringRedisTemplate) {
    this.stringRedisTemplate = stringRedisTemplate;
  }

  public StringRedisTemplate getStringRedisStream(){
    return stringRedisTemplate;
  }
}
