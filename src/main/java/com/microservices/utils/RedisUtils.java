package com.microservices.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.microservices.model.MessageData;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
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

    private static final int SCAN_COUNT = 1_000;  // mỗi vòng SCAN lấy tối đa ~1000 keys
    private static final int DEL_BATCH  = 1_000;  // xoá theo lô 1000 keys/lần

    public static final String REMOVE_KEY_MESSAGE_PREFIX = "cacheRemove::";

    public RedisUtils(RedisTemplate<String, Object> redisTemplate,
                      @Nullable RedisMessageListenerContainer listenerContainer) {
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

    public void deleteKeysWithPatternScan(String pattern) {
        redisTemplate.execute((RedisCallback<Void>) connection -> {
            ScanOptions options = ScanOptions.scanOptions()
                    .match(pattern)
                    .count(SCAN_COUNT)
                    .build();

            try (Cursor<byte[]> cursor = connection.scan(options)) {
                List<byte[]> batch = new ArrayList<>(DEL_BATCH);
                while (cursor.hasNext()) {
                    batch.add(cursor.next());
                    if (batch.size() >= DEL_BATCH) {
                        // Spring Data Redis 3.x: dùng keyCommands().del(...)
                        connection.keyCommands().del(batch.toArray(new byte[0][]));
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    connection.keyCommands().del(batch.toArray(new byte[0][]));
                }
            }
            return null;
        });
    }

    /** (Tuỳ chọn) Xoá non-blocking ở Redis side: UNLINK thay cho DEL */
    public void unlinkKeysWithPatternScan(String pattern) {
        redisTemplate.execute((RedisCallback<Void>) connection -> {
            ScanOptions options = ScanOptions.scanOptions()
                    .match(pattern)
                    .count(SCAN_COUNT)
                    .build();

            try (Cursor<byte[]> cursor = connection.scan(options)) {
                List<byte[]> batch = new ArrayList<>(DEL_BATCH);
                while (cursor.hasNext()) {
                    batch.add(cursor.next());
                    if (batch.size() >= DEL_BATCH) {
                        connection.keyCommands().unlink(batch.toArray(new byte[0][]));
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    connection.keyCommands().unlink(batch.toArray(new byte[0][]));
                }
            }
            return null;
        });
    }


    public void deleteAndPublishInvalidateCache(String localCacheName, String key) {
        redisTemplate.delete(key);
        publish(REMOVE_KEY_MESSAGE_PREFIX + localCacheName, key);
    }

    public void deleteAndPublishInvalidateCacheWithPattern(String localCacheName, String pattern) {
        Set<String> keys = redisTemplate.keys(pattern);
        if (!keys.isEmpty()) {
            redisTemplate.delete(keys);
            publishes(keys.stream()
                    .map(k -> MessageData.builder()
                            .channel(REMOVE_KEY_MESSAGE_PREFIX + localCacheName)
                            .message(k)
                            .build())
                    .toList());
        }
    }

    public void deleteAndPublishInvalidateCache(String localCacheName, String key, MessageData customMessage) {
        redisTemplate.delete(key);
        publish(REMOVE_KEY_MESSAGE_PREFIX + localCacheName, customMessage);
    }

    public void deleteAndPublishInvalidateCacheWithPattern(String pattern, List<MessageData> customMessages) {
        deleteKeysWithPattern(pattern);
        if (customMessages == null || customMessages.isEmpty()) return;
        publishes(customMessages);
    }

    public void deleteAndPublishInvalidateCacheWithPattern(String localCacheName, String pattern, List<Object> customMessages) {
        deleteKeysWithPattern(pattern);
        if (customMessages == null || customMessages.isEmpty()) return;
        publishes(REMOVE_KEY_MESSAGE_PREFIX + localCacheName, customMessages);
    }

    public List<Object> multiGet(List<String> keys) {
        return redisTemplate.opsForValue().multiGet(keys);
    }

    public void publish(String channel, Object message) {
        redisTemplate.convertAndSend(channel, message);
    }

    @SuppressWarnings("unchecked")
    public void publishes(List<MessageData> dataMessages) {
        if (dataMessages == null || dataMessages.isEmpty()) return;

        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            var keySer = redisTemplate.getStringSerializer();
            var valueSer = (RedisSerializer<Object>) redisTemplate.getValueSerializer();

            for (MessageData pair : dataMessages) {
                byte[] rawChannel = keySer.serialize(pair.getChannel());
                byte[] rawMsg = valueSer.serialize(pair.getMessage());
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
        if (listenerContainer == null) {
            throw new IllegalStateException("RedisMessageListenerContainer is not configured");
        }
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
