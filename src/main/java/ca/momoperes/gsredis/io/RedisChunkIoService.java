package ca.momoperes.gsredis.io;

import net.glowstone.chunk.ChunkSection;
import net.glowstone.chunk.GlowChunk;
import net.glowstone.chunk.GlowChunkSnapshot;
import net.glowstone.io.ChunkIoService;
import net.glowstone.util.NibbleArray;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Collections;
import java.util.List;
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
        Collections.reverse(sections);
        for (int i = 0; i < chunkSections.length; i++) {
            byte[] bytes = sections.get(i);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            boolean exists = buffer.get() != 0;
            if (!exists) {
                chunkSections[i] = null;
                continue;
            }

            int blockLength = buffer.getInt();
            byte[] rawTypes = new byte[blockLength];
            buffer.get(rawTypes);

            NibbleArray extTypes = null;
            boolean hasExtTypes = buffer.get() != 0;
            if (hasExtTypes) {
                int extTypesLength = buffer.getInt();
                byte[] extTypesRaw = new byte[extTypesLength];
                buffer.get(extTypesRaw);
                extTypes = new NibbleArray(extTypesRaw);
            }

            NibbleArray data;
            int dataLength = buffer.getInt();
            try {
                byte[] rawData = new byte[dataLength];
                buffer.get(rawData);
                data = new NibbleArray(rawData);
            } catch (BufferUnderflowException ex) {
                System.out.println("Buffer underflow: param=" + dataLength + ", actual=" + buffer.remaining());
                return false;
            }
            int blockLightLength = buffer.getInt();
            byte[] blockLightRaw = new byte[blockLightLength];
            buffer.get(blockLightRaw);
            NibbleArray blockLight = new NibbleArray(blockLightRaw);

            int skyLightLength = buffer.getInt();
            byte[] skyLightRaw = new byte[skyLightLength];
            buffer.get(skyLightRaw);
            NibbleArray skyLight = new NibbleArray(skyLightRaw);

            char[] types = new char[rawTypes.length];
            for (int j = 0; j < rawTypes.length; j++) {
                types[j] = (char) (((extTypes == null || extTypes.size() == 0) ? 0 : extTypes
                        .get(j)) << 12 | (rawTypes[j] & 0xff) << 4 | data.get(j));
            }

            chunkSections[i] = new ChunkSection(types, skyLight, blockLight);
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
            if (sec == null) {
                redis.lpush(sectionSetKeyBytes, new byte[1]); // byte will be 0; does not exist
                continue;
            }
            sec.optimize();
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            DataOutputStream stream = new DataOutputStream(byteStream);
            stream.writeByte(1); // exists
            char[] types = sec.getTypes();
            byte[] rawTypes = new byte[ChunkSection.ARRAY_SIZE];
            NibbleArray extTypes = null;
            NibbleArray data = new NibbleArray(ChunkSection.ARRAY_SIZE);
            for (int j = 0; j < ChunkSection.ARRAY_SIZE; j++) {
                char type = types[j];
                rawTypes[j] = (byte) (type >> 4 & 0xFF);
                byte extType = (byte) (type >> 12);
                if (extType > 0) {
                    if (extTypes == null) {
                        extTypes = new NibbleArray(ChunkSection.ARRAY_SIZE);
                    }
                    extTypes.set(j, extType);
                }
                data.set(j, (byte) (type & 0xF));
            }
            stream.writeInt(rawTypes.length);
            stream.write(rawTypes);

            if (extTypes != null) {
                stream.write(1);
                stream.writeInt(extTypes.byteSize());
                stream.write(extTypes.getRawData());
            } else {
                stream.write(0);
            }
            stream.writeInt(data.byteSize());
            stream.write(data.getRawData());
            NibbleArray blockLight = sec.getBlockLight();
            NibbleArray skyLight = sec.getSkyLight();
            stream.writeInt(blockLight.byteSize());
            stream.write(blockLight.getRawData());
            stream.writeInt(skyLight.byteSize());
            stream.write(skyLight.getRawData());

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
