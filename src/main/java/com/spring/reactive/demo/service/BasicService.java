package com.spring.reactive.demo.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.spring.reactive.demo.accessor.StreamRedis;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import reactor.core.publisher.Flux;

@Service
public class BasicService {

    private final ReactiveRedisConnectionFactory factory;

    private final ReactiveRedisOperations<String, String> reactiveRedisOperations;

    private final StringRedisTemplate stringRedisTemplate;

    private static final AtomicInteger count = new AtomicInteger(0);

    public BasicService(ReactiveRedisConnectionFactory factory,
            ReactiveRedisOperations<String, String> reactiveRedisOperations, StringRedisTemplate stringRedisTemplate , StreamRedis streamRedis) {
        this.factory = factory;
        this.reactiveRedisOperations = reactiveRedisOperations;
        this.stringRedisTemplate = streamRedis.getStringRedisStream();

    }

    public void loadData() {

        List<String> data = new ArrayList<>();
        IntStream.range(0, 100000).forEach(i -> data.add(UUID.randomUUID().toString()));

        Flux<String> stringFlux = Flux.fromIterable(data);
        factory.getReactiveConnection().serverCommands().flushAll()
                .thenMany(stringFlux.flatMap(
                        uid -> reactiveRedisOperations.opsForValue().set(String.valueOf(count.getAndAdd(1)), uid)))
                .subscribe();
    }

    /**
     * 
     * Reactive 호출 10만건의 데이터를 실시간으로 Streaming 하게 응답 한다. 브라우저에서 pending 없이 실시간으로 처리한다.
     */
    public Flux<String> findReactorList() {
        return reactiveRedisOperations.keys("*").flatMap(key -> reactiveRedisOperations.opsForValue().get(key));
    }

    /**
     * Blocking 호출 10만건의 데이터를 조회가 끝날 때까지 브라우저가 Pending 한다.
     */
    public Flux<String> findNormalList() {
        return Flux.fromIterable(Objects.requireNonNull(stringRedisTemplate.keys("*")).stream()
                .map(key -> stringRedisTemplate.opsForValue().get(key)).collect(Collectors.toList()));
    }

}