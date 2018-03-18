package ca.momoperes.gsredis.io;

import net.glowstone.chunk.ChunkSection;
import net.glowstone.chunk.GlowChunk;
import net.glowstone.chunk.GlowChunkSnapshot;
import net.glowstone.io.ChunkIoService;
import net.glowstone.util.NibbleArray;
import net.glowstone.util.nbt.CompoundTag;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RedisChunkIoService implements ChunkIoService {

    private final String worldName;
    private final JedisPool redisPool;
    private Jedis redis;

    public RedisChunkIoService(String worldName, JedisPool redisPool) {
        this.worldName = worldName;
        this.redisPool = redisPool;
        // the chunk service is initialized in the main thread,
        // the initRedis controller is initialized when it needs to be used first.
    }

    private void initRedis() {
        if (this.redis == null) {
            redis = redisPool.getResource();
        }
    }

    private String chunkKey(int x, int z) {
        return "worlds:" + worldName + ":c:" + x + "_" + z;
    }

    private String sectionListKey(String chunkKey) {
        return chunkKey + ":sections";
    }

    private static boolean readSectionExists(DataInputStream stream) throws IOException {
        boolean exists = stream.readBoolean();
        return exists;
    }

    private static boolean writeSectionExists(ChunkSection section, DataOutputStream stream) throws IOException {
        stream.writeBoolean(section != null);
        return section != null;
    }

    private static byte[] readByteArray(DataInputStream stream) throws IOException {
        int arrayLength = stream.readInt();
        byte[] arr = new byte[arrayLength];
        stream.read(arr);
        return arr;
    }

    private static void writeByteArray(byte[] arr, DataOutputStream stream) throws IOException {
        int length = arr.length;
        stream.writeInt(length);
        stream.write(arr);
    }

    private static Optional<byte[]> readOptionalByteArray(DataInputStream stream) throws IOException {
        boolean exists = stream.readBoolean();
        if (!exists) {
            return Optional.empty();
        }
        return Optional.of(readByteArray(stream));
    }

    private static void writeOptionalByteArray(byte[] arr, DataOutputStream stream) throws IOException {
        if (arr == null) {
            stream.writeBoolean(false);
            return;
        }
        writeByteArray(arr, stream);
    }

    @Override
    public boolean read(GlowChunk chunk) throws IOException {
        initRedis();
        int x = chunk.getX();
        int z = chunk.getZ();
        String chunkKey = chunkKey(x, z);
        String sectionSetKey = sectionListKey(chunkKey);
        if (!redis.exists(sectionSetKey)) {
            return false;
        }

        ChunkSection[] chunkSections = new ChunkSection[GlowChunk.SEC_COUNT];
        List<byte[]> sections = redis.lrange(sectionSetKey, 0, 15)
                .stream()
                .map(String::getBytes).collect(Collectors.toList());
        for (int i = 0; i < chunkSections.length; i++) {
            byte[] bytes = sections.get(i);
            DataInputStream stream = new DataInputStream(new ByteArrayInputStream(bytes));

            boolean exists = readSectionExists(stream);
            if (!exists) {
                chunkSections[i] = null;
                continue;
            }

            byte[] rawTypes = readByteArray(stream);
            Optional<byte[]> extTypes = readOptionalByteArray(stream);
            byte[] data = readByteArray(stream);

            byte[] blockLight = readByteArray(stream);
            byte[] skyLight = readByteArray(stream);

            CompoundTag tag = new CompoundTag();
            tag.putByteArray("Blocks", rawTypes);
            if (extTypes.isPresent()) {
                tag.putByteArray("Add", extTypes.get());
            }
            tag.putByteArray("Data", data);
            tag.putByteArray("BlockLight", blockLight);
            tag.putByteArray("SkyLight", skyLight);

            chunkSections[i] = ChunkSection.fromNbt(tag);
        }

        chunk.initializeSections(chunkSections);
        boolean terrainPopulated = redis.hexists(chunkKey, "TerrainPopulated")
                && redis.hget(chunkKey, "TerrainPopulated").equalsIgnoreCase("true");
        chunk.setPopulated(terrainPopulated);

        if (redis.hexists(chunkKey, "Biomes")) {
            byte[] biomes = redis.hget(chunkKey, "Biomes").getBytes();
            chunk.setBiomes(biomes);
        }

        if (redis.hexists(chunkKey, "HeightMap")) {
            byte[] heightMapRaw = redis.hget(chunkKey, "HeightMap").getBytes();
            int[] heightMap = new int[heightMapRaw.length / 4];
            ByteBuffer buffer = ByteBuffer.wrap(heightMapRaw);
            int index = 0;
            while (buffer.hasRemaining()) {
                heightMap[index++] = buffer.getInt();
            }
            chunk.setHeightMap(heightMap);
        } else {
            chunk.automaticHeightMap();
        }

        // todo: entities
        // todo: block entities
        // todo: tile ticks

        return true;
    }

    @Override
    public void write(GlowChunk chunk) throws IOException {
        initRedis();
        int x = chunk.getX();
        int z = chunk.getZ();
        String chunkKey = chunkKey(x, z);
        String sectionSetKey = sectionListKey(chunkKey);
        byte[] sectionSetKeyBytes = sectionSetKey.getBytes();

        redis.hset(chunkKey, "TerrainPopulated", "true");
        GlowChunkSnapshot snapshot = chunk.getChunkSnapshot(true, true, false);
        ChunkSection[] sections = snapshot.getRawSections();

        for (int i = sections.length - 1; i >= 0; i--) {
            ChunkSection sec = sections[i];
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            DataOutputStream stream = new DataOutputStream(byteStream);
            boolean exists = writeSectionExists(sec, stream);
            if (!exists) {
                // we're done here
                redis.lpush(sectionSetKeyBytes, byteStream.toByteArray());
                continue;
            }
            sec.optimize();
            CompoundTag tag = new CompoundTag();
            sec.writeToNbt(tag);

            byte[] rawTypes = tag.getByteArray("Blocks");
            byte[] extTypes = tag.containsKey("Add") ? tag.getByteArray("Add") : null;
            if (extTypes != null && extTypes.length == 0) {
                extTypes = null;
            }
            byte[] data = tag.getByteArray("Data");
            byte[] blockLight = tag.getByteArray("BlockLight");
            byte[] skyLight = tag.getByteArray("SkyLight");

            writeByteArray(rawTypes, stream);
            writeOptionalByteArray(extTypes, stream);
            writeByteArray(data, stream);
            writeByteArray(blockLight, stream);
            writeByteArray(skyLight, stream);

            redis.lpush(sectionSetKeyBytes, byteStream.toByteArray());
            stream.close();
        }
        redis.ltrim(sectionSetKey, 0, 15);

        redis.hset(chunkKey.getBytes(), "Biomes".getBytes(), snapshot.getRawBiomes());
        int[] rawHeightmap = snapshot.getRawHeightmap();
        ByteBuffer byteBuffer = ByteBuffer.allocate(rawHeightmap.length * 4);
        IntBuffer intBuffer = byteBuffer.asIntBuffer();
        intBuffer.put(rawHeightmap);
        redis.hset(chunkKey.getBytes(), "HeightMap".getBytes(), byteBuffer.array());

        // todo: entities
        // todo: block entities
        // todo: tile ticks
    }

    @Override
    public void unload() throws IOException {
        if (redis != null) {
            redis.close();
        }
    }
}
