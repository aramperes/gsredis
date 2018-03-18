package ca.momoperes.gsredis;

import ca.momoperes.gsredis.config.RedisChunkServiceConfiguration;
import ca.momoperes.gsredis.config.RedisPluginConfiguration;
import ca.momoperes.gsredis.io.RedisWorldStorageProvider;
import net.glowstone.GlowServer;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisWorldPlugin extends JavaPlugin {

    private static JedisPool pool;
    private RedisPluginConfiguration config;

    @Override
    public void onLoad() {
        saveDefaultConfig();
        config = readConfiguration(getConfig());

        pool = new JedisPool(new JedisPoolConfig(),
                config.getHost(), config.getPort(), 2000, config.getPassword(), config.getDefaultDatabaseIndex());

        GlowServer server = (GlowServer) getServer();
        server.setStorageProvider(worldName -> new RedisWorldStorageProvider(worldName, pool, config));
        getLogger().info("Redis world provider has been enabled.");
    }

    private RedisPluginConfiguration readConfiguration(FileConfiguration config) {
        String host = config.getString("host", "localhost");
        int port = config.getInt("port", 6379);
        int defaultDatabaseIndex = config.getInt("defaultDatabaseIndex", 0);
        boolean usePassword = config.getBoolean("usePassword", false);
        String password = (config.contains("password") && usePassword)
                ? config.getString("password", "password")
                : null;

        // chunk service
        int chunkServiceDatabaseIndex = config.getInt("chunkService.databaseIndex", defaultDatabaseIndex);
        boolean chunkServiceReadOnly = config.getBoolean("chunkService.readOnly", false);
        RedisChunkServiceConfiguration chunkServiceConfig = new RedisChunkServiceConfiguration(chunkServiceDatabaseIndex, chunkServiceReadOnly);

        return new RedisPluginConfiguration(
                host,
                port,
                defaultDatabaseIndex,
                password,
                chunkServiceConfig
        );
    }

    @Override
    public void onDisable() {
        if (config.getChunkService().isReadOnly()) {
            getLogger().warning("Chunk Service is in read-only, changes will not be saved to Redis.");
        }
    }
}
