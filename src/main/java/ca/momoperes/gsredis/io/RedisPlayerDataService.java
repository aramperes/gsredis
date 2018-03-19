package ca.momoperes.gsredis.io;

import ca.momoperes.gsredis.config.RedisPlayerDataServiceConfiguration;
import net.glowstone.GlowOfflinePlayer;
import net.glowstone.GlowWorld;
import net.glowstone.entity.GlowPlayer;
import net.glowstone.io.PlayerDataService;
import net.glowstone.io.entity.EntityStorage;
import net.glowstone.util.nbt.CompoundTag;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.OfflinePlayer;
import org.bukkit.World;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RedisPlayerDataService implements PlayerDataService {
    private final String namespace;
    private final GlowWorld world;
    private final JedisPool redisPool;
    private final RedisPlayerDataServiceConfiguration config;
    private Jedis redis;

    private final String worldKey;

    public RedisPlayerDataService(String namespace, GlowWorld world, JedisPool redisPool, RedisPlayerDataServiceConfiguration config) {
        this.namespace = namespace;
        this.world = world;
        this.redisPool = redisPool;
        this.config = config;
        this.worldKey = namespace + ":worlds:" + world.getName() + ":players";
    }

    private void initRedis() {
        if (this.redis == null) {
            redis = redisPool.getResource();
        }
        redis.select(config.getDatabaseIndex());
    }

    private String playerKey(UUID uuid) {
        return worldKey + ":" + uuid.toString();
    }

    @Override
    public PlayerReader beginReadingData(UUID uuid) {
        return new RedisPlayerReader(uuid);
    }

    @Override
    public void readData(GlowPlayer player) {
        initRedis();
        CompoundTag tag = new CompoundTag();
        EntityStorage.load(player, tag);
        // note: certain information (position, last known name, etc.) are read in the PlayerReader impl.

        // todo: inventory storage?
    }

    @Override
    public void writeData(GlowPlayer player) {
        if (config.isReadOnly()) {
            return;
        }
        String key = playerKey(player.getUniqueId());
        Map<String, String> playerData = new HashMap<>();
        Location location = player.getLocation();
        Location bedLocation = player.getLocation();

        playerData.put("Pos", location.getX() + ";" + location.getY() + ";" + location.getZ() + ";" + location.getYaw() + ";" + location.getPitch());
        if (bedLocation != null) {
            playerData.put("BedSpawnPos", bedLocation.getWorld().getName() + ";" + bedLocation.getX() + ";" + bedLocation.getY() + ";" + bedLocation.getZ());
        }
        playerData.put("FirstPlayed", String.valueOf(player.getFirstPlayed() == 0 ? player.getJoinTime() : player.getFirstPlayed()));
        playerData.put("LastPlayed", String.valueOf(player.getJoinTime()));
        playerData.put("LastKnownName", player.getName());
        redis.hmset(key, playerData);
        redis.sadd(worldKey, player.getUniqueId().toString());

        // todo: inventory storage?
    }

    @Override
    public CompletableFuture<Collection<OfflinePlayer>> getOfflinePlayers() {
        initRedis();
        if (redis.exists(worldKey)) {
            Set<String> playerIds = redis.smembers(worldKey);
            List<CompletableFuture<GlowOfflinePlayer>> futures = new ArrayList<>(playerIds.size());
            for (String playerId : playerIds) {
                futures.add(GlowOfflinePlayer.getOfflinePlayer(world.getServer(), UUID.fromString(playerId)));
            }
            CompletableFuture<Void> gotAll = CompletableFuture.allOf((CompletableFuture[]) futures.toArray(new CompletableFuture[0]));
            return gotAll.thenApplyAsync((v) -> futures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList())
            );
        }
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    private class RedisPlayerReader implements PlayerReader {

        private final boolean hasPlayedBefore;
        private final Location location;
        private final Location bedSpawnLocation;
        private final long firstPlayed;
        private final long lastPlayed;
        private final String lastKnownName;

        RedisPlayerReader(UUID uuid) {
            initRedis();
            String key = playerKey(uuid);
            if (!redis.exists(key)) {
                hasPlayedBefore = false;
                location = null;
                bedSpawnLocation = null;
                firstPlayed = 0;
                lastPlayed = 0;
                lastKnownName = null;
                return;
            }
            Map<String, String> playerData = redis.hgetAll(playerKey(uuid));
            if (playerData.containsKey("Pos")) {
                String pos = playerData.get("Pos");
                String[] split = pos.split(";");
                double posX = Double.parseDouble(split[0]);
                double posY = Double.parseDouble(split[1]);
                double posZ = Double.parseDouble(split[2]);
                float yaw = 0;
                float pitch = 0;
                if (split.length >= 5) {
                    yaw = Float.parseFloat(split[3]);
                    pitch = Float.parseFloat(split[4]);
                }
                location = new Location(world, posX, posY, posZ, yaw, pitch);
            } else {
                location = null;
            }
            if (playerData.containsKey("BedSpawnPos")) {
                String bedSpawnPos = playerData.get("BedSpawnPos");
                String[] split = bedSpawnPos.split(";");
                String worldName = split[0];
                double posX = Double.parseDouble(split[1]);
                double posY = Double.parseDouble(split[2]);
                double posZ = Double.parseDouble(split[3]);
                World bedWorld = Bukkit.getWorld(worldName);
                if (bedWorld == null) bedWorld = world;
                bedSpawnLocation = new Location(bedWorld, posX, posY, posZ);
            } else {
                bedSpawnLocation = null;
            }
            if (playerData.containsKey("FirstPlayed")) {
                firstPlayed = Long.parseLong(playerData.get("FirstPlayed"));
            } else {
                firstPlayed = 0;
            }
            if (playerData.containsKey("LastPlayed")) {
                lastPlayed = Long.parseLong(playerData.get("LastPlayed"));
            } else {
                lastPlayed = 0;
            }
            lastKnownName = playerData.getOrDefault("LastKnownName", null);
            hasPlayedBefore = true;
        }

        @Override
        public boolean hasPlayedBefore() {
            return hasPlayedBefore;
        }

        @Override
        public Location getLocation() {
            return location;
        }

        @Override
        public Location getBedSpawnLocation() {
            return bedSpawnLocation;
        }

        @Override
        public long getFirstPlayed() {
            return firstPlayed;
        }

        @Override
        public long getLastPlayed() {
            return lastPlayed;
        }

        @Override
        public String getLastKnownName() {
            return lastKnownName;
        }

        @Override
        public void readData(GlowPlayer player) {
            RedisPlayerDataService.this.readData(player);
        }

        @Override
        public void close() {
        }
    }
}
