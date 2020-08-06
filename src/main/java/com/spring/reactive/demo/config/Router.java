package com.spring.reactive.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import lombok.AllArgsConstructor;

import static org.springframework.web.reactive.function.server.RouterFunctions.route;

import com.spring.reactive.demo.service.BasicService;

@Configuration
@AllArgsConstructor
public class Router {

    private final BasicService service;

    @Bean
    RouterFunction<ServerResponse> empRouterList() {
        return route()
                .GET("/reactive-list",
                        request -> ServerResponse.ok().contentType(MediaType.TEXT_EVENT_STREAM)
                                .body(service.findReactorList(), String.class))
                .GET("/normal-list", request -> ServerResponse.ok().contentType(MediaType.TEXT_EVENT_STREAM)
                        .body(service.findNormalList(), String.class))
                .GET("/load", request -> {
                    service.loadData();
                    return ServerResponse.ok().body(BodyInserters.fromObject("Load Data Completed"));
                }).build();

    }

}