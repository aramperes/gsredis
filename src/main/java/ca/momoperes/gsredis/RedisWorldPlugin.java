package ca.momoperes.gsredis;

import ca.momoperes.gsredis.io.RedisWorldStorageProvider;
import net.glowstone.GlowServer;
import org.bukkit.plugin.java.JavaPlugin;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisWorldPlugin extends JavaPlugin {

    private static JedisPool pool;

    @Override
    public void onLoad() {
        // todo: configurable IP
        pool = new JedisPool(new JedisPoolConfig(), "localhost");

        GlowServer server = (GlowServer) getServer();
        server.setStorageProvider(worldName -> new RedisWorldStorageProvider(worldName, pool));
    }

    @Override
    public void onDisable() {

    }
}
