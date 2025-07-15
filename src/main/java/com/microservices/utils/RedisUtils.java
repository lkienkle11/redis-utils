package com.microservices.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Component
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class RedisUtils {
    @Getter
    RedisTemplate<String, Object> redisTemplate;

    RedisMessageListenerContainer listenerContainer;

    ObjectMapper objectMapper;

    public RedisUtils(RedisTemplate<String, Object> redisTemplate,
                      RedisMessageListenerContainer listenerContainer) {
        this.redisTemplate = redisTemplate;
        this.listenerContainer = listenerContainer;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.registerModule(new JavaTimeModule()); // Hỗ trợ LocalDateTime
    }

    // Phương thức generic để deserialize dữ liệu từ Redis thành bất kỳ kiểu nào
    public <T> T getFromRedis(String key, Class<T> clazz) {
        Object data = redisTemplate.opsForValue().get(key);
        if (data == null) {
            return null;
        }
        return objectMapper.convertValue(data, clazz);
    }

    public Object getFromRedis(String key) {
        return redisTemplate.opsForValue().get(key);
    }

    public void saveToRedis(String key, Object value) {
        redisTemplate.opsForValue().set(key, value);
    }

    // Phương thức lưu dữ liệu vào Redis (không cần generic vì RedisTemplate đã xử lý)
    public void saveToRedis(String key, Object value, long timeout, TimeUnit unit) {
        redisTemplate.opsForValue().set(key, value, timeout, unit);
    }

    public void saveToSet(String key, Object value) {
        redisTemplate.opsForSet().add(key, value);
    }

    public void saveToSet(String key, Object value, long timeout, TimeUnit unit) {
        redisTemplate.opsForSet().add(key, value);
        if (timeout > 0) {
            redisTemplate.expire(key, timeout, unit);
        }
    }

    public <T> T popFromSet(String key, Class<T> clazz) {
        Object data = redisTemplate.opsForSet().pop(key);
        if (data == null) {
            return null;
        }
        return objectMapper.convertValue(data, clazz);
    }

    public <T> Set<T> getFromSet(String key, Class<T> clazz) {
        // Lấy toàn bộ members (O(M) với M = số phần tử trong set)
        Set<Object> rawMembers = redisTemplate.opsForSet().members(key);
        if (rawMembers == null || rawMembers.isEmpty()) {
            return Collections.emptySet();
        }
        // Chuyển kiểu từng phần tử về T
        return rawMembers.stream()
                .map(item -> objectMapper.convertValue(item, clazz))
                .collect(Collectors.toSet());
    }

    public boolean isMember(String key, Object value) {
        return Boolean.TRUE.equals(redisTemplate.opsForSet().isMember(key, value));
    }

    // Phương thức kiểm tra xem key có tồn tại trong Redis không
    public boolean hasKey(String key) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    public void deleteKey(String key) {
        redisTemplate.delete(key);
    }

    public void deleteKeys(String... keys) {
        redisTemplate.delete(List.of(keys));
    }

    public void deleteKeysWithPattern(String pattern) {
        redisTemplate.delete(Objects.requireNonNull(redisTemplate.keys(pattern)));
    }

    public List<Object> multiGet(List<String> keys) {
        return redisTemplate.opsForValue().multiGet(keys);
    }

    public void publish(String channel, Object message) {
        redisTemplate.convertAndSend(channel, message);
    }

    @SuppressWarnings("unchecked")
    public void publishes(List<Pair<String, Object>> dataMessages) {
        if (dataMessages == null || dataMessages.isEmpty()) return;

        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            var keySer = redisTemplate.getStringSerializer();
            var valueSer = (RedisSerializer<Object>) redisTemplate.getValueSerializer();

            for (Pair<String, Object> pair : dataMessages) {
                String channel = pair.getFirst();
                Object message = pair.getSecond();

                byte[] rawChannel = keySer.serialize(channel);
                byte[] rawMsg = valueSer.serialize(message);
                if (rawChannel == null || rawMsg == null) {
                    continue;
                }
                connection.publish(rawChannel, rawMsg);   // xếp hàng trong pipeline
            }
            return null;   // bắt buộc trả về
        });
    }

    @SuppressWarnings("unchecked")
    public void publishes(String channel, List<Object> messages) {
        if (messages == null || messages.isEmpty() || channel == null) return;

        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            var keySer = redisTemplate.getStringSerializer();
            var valueSer = (RedisSerializer<Object>) redisTemplate.getValueSerializer();

            byte[] rawChannel = keySer.serialize(channel);          // serialise 1 lần duy nhất
            if (rawChannel == null) {
                return null;                                    // nếu channel không hợp lệ thì dừng
            }
            for (Object msg : messages) {
                if (msg == null) continue;                          // bỏ qua null
                byte[] rawMsg = valueSer.serialize(msg);
                if (rawMsg == null) continue;                       // bỏ qua nếu không thể serialize
                connection.publish(rawChannel, rawMsg);             // dồn vào pipeline
            }
            return null;                                            // bắt buộc trả về
        });
    }

    public void subscribe(String channel, Consumer<String> handler) {
        MessageListener listener = (message, pattern) -> {
            Object raw = redisTemplate.getValueSerializer().deserialize(message.getBody());
            String body;
            try {
                if (raw instanceof String) {
                    body = (String) raw;
                } else {
                    body = objectMapper.writeValueAsString(raw);
                }
            } catch (JsonProcessingException e) {
                body = "ERROR_PARSING:" + e.getMessage();
            }
            handler.accept(body);
        };
        listenerContainer.addMessageListener(
                listener,
                new PatternTopic(channel)
        );
    }
}
