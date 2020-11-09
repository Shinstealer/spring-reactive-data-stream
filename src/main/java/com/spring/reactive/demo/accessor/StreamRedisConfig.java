package com.spring.reactive.demo.accessor;

import java.time.Duration;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.StringRedisTemplate;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.Delay;

public class StreamRedisConfig {

  @Bean(destroyMethod = "shutdown") // shutdown callback
  @Primary
  ClientResources clientResources() {
    return DefaultClientResources.builder().reconnectDelay(Delay.constant(Duration.ofSeconds(3)))
        .build();
  }

  @Bean
  public RedisStandaloneConfiguration redisStandaloneConfiguration() {
    String redisHost = "localhost";
    int port = 6379;
    return new RedisStandaloneConfiguration(redisHost, port);
  }

  @Bean
  public TimeoutOptions timeoutOptions() {
    return TimeoutOptions.builder().connectionTimeout().fixedTimeout(Duration.ofSeconds(1000))
        .build();
  }

  @Bean
  public ClientOptions clientOptions(TimeoutOptions timeoutOptions) {
    return ClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .timeoutOptions(timeoutOptions).build();
  }

  @Bean
  public GenericObjectPoolConfig<?> genericObjectPoolConfig() {
    GenericObjectPoolConfig<?> config = new GenericObjectPoolConfig<>();
    config.setMaxTotal(5);
    return config;
  }

  @Bean
  LettucePoolingClientConfiguration lettucePoolConfig(ClientOptions options,
      ClientResources clientResources, GenericObjectPoolConfig<?> genericObjectPoolConfig) {
    return LettucePoolingClientConfiguration.builder().poolConfig(genericObjectPoolConfig)
        .clientOptions(options).clientResources(clientResources).build();
  }

  @Bean(name = "streamRedisConnectionFactory")
  public RedisConnectionFactory connectionFactory(
      RedisStandaloneConfiguration redisStandaloneConfiguration,
      LettucePoolingClientConfiguration lettucePoolConfig) {
    return new LettuceConnectionFactory(redisStandaloneConfiguration, lettucePoolConfig);
  }

  @Bean(name = "streamStringRedisTemplate")
  public StringRedisTemplate stringRedisTemplate(
      @Qualifier("streamRedisConnectionFactory") RedisConnectionFactory redisConnectionFactory) {
    return new StringRedisTemplate(redisConnectionFactory);
  }
  
}
