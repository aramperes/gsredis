package ca.momoperes.gsredis.io;

import net.glowstone.chunk.ChunkSection;
import net.glowstone.chunk.GlowChunk;
import net.glowstone.chunk.GlowChunkSnapshot;
import net.glowstone.io.ChunkIoService;
import net.glowstone.util.NibbleArray;
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

    private static byte[] readRawTypes(DataInputStream stream) throws IOException {
        int arrayLength = stream.readInt();
        byte[] rawTypes = new byte[arrayLength];
        stream.read(rawTypes);
        return rawTypes;
    }

    private static void writeRawTypes(byte[] rawTypes, DataOutputStream stream) throws IOException {
        int length = rawTypes.length;
        stream.writeInt(length);
        stream.write(rawTypes);
    }

    private static Optional<NibbleArray> readExtTypes(DataInputStream stream) throws IOException {
        boolean exists = stream.readBoolean();
        if (!exists) {
            return Optional.empty();
        }
        int length = stream.readInt();
        byte[] extTypesRaw = new byte[length];
        stream.read(extTypesRaw);
        return Optional.of(new NibbleArray(extTypesRaw));
    }

    private static void writeExtTypes(NibbleArray extTypes, DataOutputStream stream) throws IOException {
        boolean exists = extTypes != null;
        stream.writeBoolean(exists);
        if (!exists) {
            return;
        }
        byte[] rawData = extTypes.getRawData();
        stream.writeInt(rawData.length);
        stream.write(rawData);
    }

    private static NibbleArray readData(DataInputStream stream) throws IOException {
        int length = stream.readInt();
        byte[] raw = new byte[length];
        stream.read(raw);
        return new NibbleArray(raw);
    }

    private static void writeData(NibbleArray data, DataOutputStream stream) throws IOException {
        byte[] raw = data.getRawData();
        stream.writeInt(raw.length);
        stream.write(raw);
    }

    private static NibbleArray readNibble(DataInputStream stream) throws IOException {
        int length = stream.readInt();
        byte[] raw = new byte[length];
        stream.read(raw);
        return new NibbleArray(raw);
    }

    private static void writeNibble(NibbleArray nibble, DataOutputStream stream) throws IOException {
        byte[] raw = nibble.getRawData();
        stream.writeInt(raw.length);
        stream.write(raw);
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

            byte[] rawTypes = readRawTypes(stream);
            Optional<NibbleArray> extTypes = readExtTypes(stream);
            NibbleArray data = readData(stream);

            NibbleArray blockLight = readNibble(stream);
            NibbleArray skyLight = readNibble(stream);

            char[] types = new char[rawTypes.length];
            for (int j = 0; j < rawTypes.length; j++) {
                types[j] = (char) (((!extTypes.isPresent() || extTypes.get().size() == 0) ? 0 : extTypes
                        .get().get(j)) << 12 | (rawTypes[j] & 0xff) << 4 | data.get(j));
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
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            DataOutputStream stream = new DataOutputStream(byteStream);
            boolean exists = writeSectionExists(sec, stream);
            if (!exists) {
                // we're done here
                redis.lpush(sectionSetKeyBytes, byteStream.toByteArray());
                continue;
            }
            sec.optimize();
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

            writeRawTypes(rawTypes, stream);
            writeExtTypes(extTypes, stream);
            writeData(data, stream);

            NibbleArray blockLight = sec.getBlockLight();
            NibbleArray skyLight = sec.getSkyLight();
            writeNibble(blockLight, stream);
            writeNibble(skyLight, stream);

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
