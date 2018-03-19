package ca.momoperes.gsredis.io;

import ca.momoperes.gsredis.config.RedisChunkServiceConfiguration;
import net.glowstone.GlowWorld;
import net.glowstone.io.WorldMetadataService;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class RedisMetadataService implements WorldMetadataService {
    private final String namespace;
    private final GlowWorld world;
    private final JedisPool redisPool;
    private final RedisChunkServiceConfiguration config;
    private Jedis redis;

    private final String worldKey;

    public RedisMetadataService(String namespace, GlowWorld world, JedisPool redisPool, RedisChunkServiceConfiguration config) {
        this.namespace = namespace;
        this.world = world;
        this.redisPool = redisPool;
        this.config = config;
        this.worldKey = namespace + ":worlds:" + world.getName() + ":meta";
    }

    private void initRedis() {
        if (this.redis == null) {
            redis = redisPool.getResource();
        }
        redis.select(config.getDatabaseIndex());
    }

    @Override
    public WorldFinalValues readWorldData() {
        initRedis();
        String uidString = null;
        UUID uid;
        if (redis.hexists(worldKey, "uid")) {
            uidString = redis.hget(worldKey, "uid");
        }
        if (uidString == null) {
            uid = UUID.randomUUID();
        } else {
            uid = UUID.fromString(uidString);
        }

        long seed = 0;
        String seedString = null;
        if (redis.hexists(worldKey, "seed")) {
            seedString = redis.hget(worldKey, "seed");
        }
        if (seedString != null) {
            seed = Long.valueOf(seedString);
        }

        return new WorldFinalValues(seed, uid);
    }

    @Override
    public void writeWorldData() throws IOException {
        if (config.isReadOnly()) {
            return;
        }
        initRedis();
        Map<String, String> fields = new HashMap<>();
        fields.put("uid", world.getUID().toString());
        fields.put("seed", String.valueOf(world.getSeed()));
        redis.hmset(worldKey, fields);
    }
}
