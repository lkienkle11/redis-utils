package com.microservices.config;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.util.ErrorHandler;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(RedisMessageListenerContainer.class)              // chỉ kích hoạt khi có spring-data-redis
@ConditionalOnMissingBean(RedisMessageListenerContainer.class)
@ConditionalOnBean(RedisConnectionFactory.class) // tránh “đè” bean do app chính tạo
public class RedisListenerAutoConfiguration {

    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(
            RedisConnectionFactory connectionFactory,
            ObjectProvider<TaskExecutor> taskExecutor,        // tùy chọn
            ObjectProvider<ErrorHandler> errorHandler) {      // tùy chọn

        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        taskExecutor.ifAvailable(container::setTaskExecutor);
        errorHandler.ifAvailable(container::setErrorHandler);
        return container;
    }
}
