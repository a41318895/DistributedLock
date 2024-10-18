package com.akichou.distributedlock.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

    @Value("${spring.data.redis.host}")
    private String redisHost ;

    @Value("${spring.data.redis.port}")
    private int redisPort ;

    // When application shutdown, RedissonClient shutdown too.
    @Bean(destroyMethod = "shutdown")
    public RedissonClient redisson() {

        Config config = new Config() ;

        config.useSingleServer()
                .setAddress("redis://" + redisHost + ":" + redisPort) ;

        return Redisson.create(config) ;
    }
}