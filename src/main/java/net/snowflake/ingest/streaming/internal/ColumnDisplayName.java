package net.snowflake.ingest.streaming.internal;

import java.util.Objects;

public final class ColumnDisplayName {
    private final String displayName;


    public ColumnDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public ColumnInternalName toInternalName() {
        return ColumnInternalName.fromDisplayName(this.displayName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnDisplayName that = (ColumnDisplayName) o;

        return Objects.equals(displayName, that.displayName);
    }

    @Override
    public int hashCode() {
        return displayName != null ? displayName.hashCode() : 0;
    }

    @Override
    public String toString() {
        return displayName;
    }
}
