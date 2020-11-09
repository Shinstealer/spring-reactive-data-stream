package com.spring.reactive.demo.accessor;

import java.time.Duration;
import com.spring.reactive.demo.config.DemoDesignConf;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class PubSubRedisConfig {

  @Bean(destroyMethod = "shutdown") // shutdown callback
  @Primary
  ClientResources clientResources() {
    return DefaultClientResources.builder().reconnectDelay(Delay.constant(Duration.ofSeconds(3)))
        .build();
  }

  @Bean(name = "pubsubRedisStandaloneConfig")
  @Primary
  public RedisStandaloneConfiguration redisStandaloneConfiguration(DemoDesignConf conf) {
    String redisHost = conf.getRedisHost();
    int redisPort = conf.getRedisPort();
    log.info("redis host : " + redisHost , "redis port :" + redisPort + " connected");
    return new RedisStandaloneConfiguration(redisHost, redisPort);
  }

  @Bean(name = "pubsubRedisTimeoutOptions")
  @Primary
  public TimeoutOptions timeoutOptions() {
    return TimeoutOptions.builder().connectionTimeout().fixedTimeout(Duration.ofSeconds(1000))
        .build();
  }

  @Bean(name = "pubsubRedisClientOptions")
  @Primary
  public ClientOptions clientOptions(TimeoutOptions timeoutOptions) {
    return ClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .timeoutOptions(timeoutOptions).build();
  }

  @Bean(name = "pubsubRedisGenericObjectPoolConfig")
  @Primary
  public GenericObjectPoolConfig<?> genericObjectPoolConfig() {
    GenericObjectPoolConfig<?> config = new GenericObjectPoolConfig<>();
    config.setMaxTotal(5);
    return config;
  }

  @Bean(name = "pubsubRedisLettucePoolConfig")
  @Primary
  LettucePoolingClientConfiguration lettucePoolConfig(ClientOptions options,
      ClientResources clientResources, GenericObjectPoolConfig<?> genericObjectPoolConfig) {
    return LettucePoolingClientConfiguration.builder().poolConfig(genericObjectPoolConfig)
        .clientOptions(options).clientResources(clientResources).build();
  }

  @Bean(name = "pubsubRedisConnectionFactory")
  @Primary
  public RedisConnectionFactory connectionFactory(
      RedisStandaloneConfiguration redisStandaloneConfiguration,
      LettucePoolingClientConfiguration lettucePoolConfig) {
    return new LettuceConnectionFactory(redisStandaloneConfiguration, lettucePoolConfig);
  }

  @Bean(name = "pubsubStringRedisTemplate")
  @Primary
  @ConditionalOnBean(name = "stringRedisTemplate")
  public StringRedisTemplate stringRedisTemplate(
      @Qualifier("pubsubRedisConnectionFactory") RedisConnectionFactory redisConnectionFactory) {
    return new StringRedisTemplate(redisConnectionFactory);
  }

}
