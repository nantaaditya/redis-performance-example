package com.nantaaditya.redisdemo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Arrays;
import java.util.List;

/**
 * created by pramuditya.anantanur
 **/
@RestController
@RequestMapping(value = "/api/redis")
public class RedisController {
  
  @Autowired
  private ReactiveStringRedisTemplate reactiveStringRedisTemplate;
  
  @PostMapping(
      produces = MediaType.APPLICATION_JSON_VALUE
  )
  public Mono<Boolean> save() {
    return Flux.range(1, 10)
        .map(item -> "Item-"+item)
        .collectMap(item -> item)
        .flatMap(items -> reactiveStringRedisTemplate.opsForValue().multiSet(items));
  }
  
  @GetMapping(
      produces = MediaType.APPLICATION_JSON_VALUE
  )
  public Mono<List<String>> get() {
    return Mono.zip(
        reactiveStringRedisTemplate.opsForValue().get("Item-1"),
        reactiveStringRedisTemplate.opsForValue().get("Item-2"),
        reactiveStringRedisTemplate.opsForValue().get("Item-3"),
        reactiveStringRedisTemplate.opsForValue().get("Item-4"),
        reactiveStringRedisTemplate.opsForValue().get("Item-5"),
        reactiveStringRedisTemplate.opsForValue().get("Item-6"),
        reactiveStringRedisTemplate.opsForValue().get("Item-7"),
        reactiveStringRedisTemplate.opsForValue().get("Item-8")
    )
        .map(items -> Arrays.asList(
            items.getT1(),
            items.getT2(),
            items.getT3(),
            items.getT4(),
            items.getT5(),
            items.getT6(),
            items.getT7(),
            items.getT8()
            )
        );     
  }

  @GetMapping(value = "/parallel",
      produces = MediaType.APPLICATION_JSON_VALUE
  )
  public Mono<List<String>> getParallel() {
    return Mono.zip(
        reactiveStringRedisTemplate.opsForValue().get("Item-1").subscribeOn(Schedulers.parallel()),
        reactiveStringRedisTemplate.opsForValue().get("Item-2").subscribeOn(Schedulers.parallel()),
        reactiveStringRedisTemplate.opsForValue().get("Item-3").subscribeOn(Schedulers.parallel()),
        reactiveStringRedisTemplate.opsForValue().get("Item-4").subscribeOn(Schedulers.parallel()),
        reactiveStringRedisTemplate.opsForValue().get("Item-5").subscribeOn(Schedulers.parallel()),
        reactiveStringRedisTemplate.opsForValue().get("Item-6").subscribeOn(Schedulers.parallel()),
        reactiveStringRedisTemplate.opsForValue().get("Item-7").subscribeOn(Schedulers.parallel()),
        reactiveStringRedisTemplate.opsForValue().get("Item-8").subscribeOn(Schedulers.parallel())
    )
        .map(items -> Arrays.asList(
            items.getT1(),
            items.getT2(),
            items.getT3(),
            items.getT4(),
            items.getT5(),
            items.getT6(),
            items.getT7(),
            items.getT8()
            )
        );
  }

  @GetMapping(value = "/bulk",
      produces = MediaType.APPLICATION_JSON_VALUE
  )
  public Mono<List<String>> getBulk() {
    return Flux.range(1, 8)
        .map(item -> "Item-"+item)
        .collectList()
        .flatMap(items -> reactiveStringRedisTemplate.opsForValue().multiGet(items));
  }
}
