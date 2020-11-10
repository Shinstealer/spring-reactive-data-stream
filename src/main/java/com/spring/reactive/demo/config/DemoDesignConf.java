package com.spring.reactive.demo.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;


@Configuration
@PropertySource(value = "classpath:demo-design.yml", factory = YamlPropertySourceFactory.class,
    ignoreResourceNotFound = true)
@Data
@Slf4j
public class DemoDesignConf {

  @Value("demo.service.netty.url")
  private String nettyUrl;
  
  @Value("demo.service.netty.port")
  private int nettyPort;

  @Value("demo.service.redis.stream.host")
  private String redisHost;

  @Value("demo.service.redis.stream.port")
  private int redisPort;

  public void print() {
    log.info(toString());
  }

}
