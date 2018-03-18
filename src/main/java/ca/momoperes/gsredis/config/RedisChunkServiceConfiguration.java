package ca.momoperes.gsredis.config;

public class RedisChunkServiceConfiguration {
    private int databaseIndex;
    private boolean readOnly;

    public RedisChunkServiceConfiguration(int databaseIndex, boolean readOnly) {
        this.databaseIndex = databaseIndex;
        this.readOnly = readOnly;
    }

    public int getDatabaseIndex() {
        return databaseIndex;
    }

    public void setDatabaseIndex(int databaseIndex) {
        this.databaseIndex = databaseIndex;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }
}
