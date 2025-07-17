package com.microservices.config;

import com.microservices.utils.RedisUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.server.WebFilter;

@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass({WebFilter.class})
public class RedisUtilsAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public RedisUtils redisUtils(
            org.springframework.data.redis.core.RedisTemplate<String, Object> redisTemplate,
            ObjectProvider<org.springframework.data.redis.listener.RedisMessageListenerContainer>
                    lcProvider) {
        return new RedisUtils(redisTemplate, lcProvider.getIfAvailable());
    }
}
