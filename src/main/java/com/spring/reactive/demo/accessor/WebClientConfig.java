package com.spring.reactive.demo.accessor;


import com.spring.reactive.demo.config.DemoDesignConf;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

@Component
@Slf4j
public class WebClientConfig {

  private final WebClient webClient;

  private DemoDesignConf demoDesignConf;

  public WebClientConfig(DemoDesignConf demoDesignConf) {
    this.demoDesignConf = demoDesignConf;
    this.webClient = builder();
  }

  private WebClient builder() {

    try {
      SslContext sslContext =
          SslContextBuilder.forClient()
          .trustManager(InsecureTrustManagerFactory.INSTANCE)
              .sslProvider(SslProvider.OPENSSL)
              .sessionCacheSize(0)
              .sessionTimeout(0)
              .build();

      HttpClient httpClient = HttpClient.create().
      secure(sslSpec -> sslSpec.sslContext(sslContext))
          .baseUrl(demoDesignConf.getNettyUrl())
          .port(demoDesignConf.getNettyPort())
          .tcpConfiguration(
              tcpClient -> tcpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5 * 1000)
                  .doOnConnected(conn -> conn.addHandlerLast(new ReadTimeoutHandler(10))
                      .addHandlerLast(new WriteTimeoutHandler(10))));
        return WebClient.builder()
        .clientConnector(new ReactorClientHttpConnector(httpClient))
        .build();
    } catch (Exception e) {
      log.warn("failed to build " , e);
      return null;
    }
  }

  public Mono<String> post(String url , String message){
    return webClient.post()
    .uri(url)
    .bodyValue(message)
    .retrieve()
    .bodyToMono(String.class)
    .switchIfEmpty(Mono.error(new Exception()));
  }

}
