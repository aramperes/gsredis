package ca.momoperes.gsredis.io;

import ca.momoperes.gsredis.config.RedisPluginConfiguration;
import net.glowstone.GlowWorld;
import net.glowstone.io.*;
import net.glowstone.io.data.WorldFunctionIoService;
import net.glowstone.io.json.JsonPlayerStatisticIoService;
import net.glowstone.io.nbt.NbtScoreboardIoService;
import net.glowstone.io.nbt.NbtStructureDataService;
import redis.clients.jedis.JedisPool;

import java.io.File;

public class RedisWorldStorageProvider implements WorldStorageProvider {

    private final String worldName;
    private final JedisPool redisPool;
    private final RedisPluginConfiguration config;
    private GlowWorld world;

    private ChunkIoService chunkIoService;
    private WorldMetadataService metadataService;
    private PlayerDataService playerDataService;
    private StructureDataService structureDataService;
    private ScoreboardIoService scoreboardIoService;
    private PlayerStatisticIoService playerStatisticIoService;
    private FunctionIoService functionIoService;

    public RedisWorldStorageProvider(String worldName, JedisPool redisPool, RedisPluginConfiguration config) {
        this.worldName = worldName;
        this.redisPool = redisPool;
        this.config = config;
    }

    @Override
    public void setWorld(GlowWorld world) {
        if (this.world != null) {
            throw new IllegalArgumentException("World is already set.");
        }
        this.world = world;
        chunkIoService = new RedisChunkIoService(config.getNamespace(), worldName, redisPool, config.getChunkService());
        metadataService = new RedisMetadataService(config.getNamespace(), world, redisPool);
        playerDataService = new RedisPlayerDataService(config.getNamespace(), world, redisPool, config.getPlayerData());

        // todo: redis-ify
        structureDataService = new NbtStructureDataService(world, new File(worldName + "_structures"));
        scoreboardIoService = new NbtScoreboardIoService(world.getServer(), new File(worldName + "_score"));
        playerStatisticIoService = new JsonPlayerStatisticIoService(world.getServer(), new File(worldName + "_stats"));
        functionIoService = new WorldFunctionIoService(world, new File(worldName + "_funcs"));
    }

    @Override
    public File getFolder() {
        // filesystem is not used
        return null;
    }

    @Override
    public ChunkIoService getChunkIoService() {
        return chunkIoService;
    }

    @Override
    public WorldMetadataService getMetadataService() {
        return metadataService;
    }

    @Override
    public PlayerDataService getPlayerDataService() {
        return playerDataService;
    }

    @Override
    public StructureDataService getStructureDataService() {
        return structureDataService;
    }

    @Override
    public ScoreboardIoService getScoreboardIoService() {
        return scoreboardIoService;
    }

    @Override
    public PlayerStatisticIoService getPlayerStatisticIoService() {
        return playerStatisticIoService;
    }

    @Override
    public FunctionIoService getFunctionIoService() {
        return functionIoService;
    }
}
