package ca.momoperes.gsredis;

import ca.momoperes.gsredis.config.RedisChunkServiceConfiguration;
import ca.momoperes.gsredis.config.RedisPlayerDataServiceConfiguration;
import ca.momoperes.gsredis.config.RedisPluginConfiguration;
import ca.momoperes.gsredis.io.RedisWorldStorageProvider;
import net.glowstone.GlowServer;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.File;

public class RedisWorldPlugin extends JavaPlugin {

    private static JedisPool pool;
    private RedisPluginConfiguration config;

    @Override
    public void onLoad() {
        File configYaml = new File(getDataFolder(), "config.yml");
        boolean firstTime = !configYaml.exists();
        saveDefaultConfig();
        config = readConfiguration(getConfig());

        if (firstTime) {
            getLogger().warning("It appears this is the first time you use gsredis.");
            getLogger().warning("Redis world storage is not enabled on the first run.");
            getLogger().warning("Please configure gsredis in plugins/gsredis/config.yml");
            return;
        }

        pool = new JedisPool(new JedisPoolConfig(),
                config.getHost(), config.getPort(), 2000, config.getPassword(), config.getDefaultDatabaseIndex());

        GlowServer server = (GlowServer) getServer();
        server.setStorageProvider(worldName -> new RedisWorldStorageProvider(worldName, pool, config));
        getLogger().info("Redis world provider has been enabled.");
    }

    private RedisPluginConfiguration readConfiguration(FileConfiguration config) {
        // basic config
        String namespace = config.getString("namespace", "gsredis_server_X");
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

        // player data service
        int playerDataServiceDatabaseIndex = config.getInt("playerDataService.databaseIndex", defaultDatabaseIndex);
        boolean playerDataServiceReadOnly = config.getBoolean("playerDataService.readOnly", false);
        RedisPlayerDataServiceConfiguration playerDataConfiguration = new RedisPlayerDataServiceConfiguration(playerDataServiceDatabaseIndex, playerDataServiceReadOnly);

        return new RedisPluginConfiguration(
                namespace,
                host,
                port,
                defaultDatabaseIndex,
                password,
                chunkServiceConfig,
                playerDataConfiguration
        );
    }

    @Override
    public void onDisable() {
        if (config.getChunkService().isReadOnly()) {
            getLogger().warning("Chunk Service is in read-only, changes will not be saved to Redis.");
        }
    }
}
