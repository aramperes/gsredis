package ca.momoperes.gsredis.config;

public class RedisPluginConfiguration {
    private String namespace;
    private String host;
    private int port;
    private int defaultDatabaseIndex;
    private String password;
    private RedisChunkServiceConfiguration chunkService;

    public RedisPluginConfiguration(String namespace, String host, int port, int defaultDatabaseIndex, String password, RedisChunkServiceConfiguration chunkService) {
        this.namespace = namespace;
        this.host = host;
        this.port = port;
        this.defaultDatabaseIndex = defaultDatabaseIndex;
        this.password = password;
        this.chunkService = chunkService;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getDefaultDatabaseIndex() {
        return defaultDatabaseIndex;
    }

    public void setDefaultDatabaseIndex(int defaultDatabaseIndex) {
        this.defaultDatabaseIndex = defaultDatabaseIndex;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public RedisChunkServiceConfiguration getChunkService() {
        return chunkService;
    }

    public void setChunkService(RedisChunkServiceConfiguration chunkService) {
        this.chunkService = chunkService;
    }
}
