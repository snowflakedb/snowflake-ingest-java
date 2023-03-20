package net.snowflake.ingest.streaming.internal;

import java.util.Objects;

public final class ColumnInternalName {

    private final String internalName;

    public ColumnInternalName(String internalName) {
        this.internalName = internalName;
    }

    public static ColumnInternalName fromDisplayName(String displayName) {
        return new ColumnInternalName(LiteralQuoteUtils.unquoteColumnName(displayName));
    }

    public String getInternalName() {
        return internalName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnInternalName that = (ColumnInternalName) o;

        return Objects.equals(internalName, that.internalName);
    }

    @Override
    public int hashCode() {
        return internalName != null ? internalName.hashCode() : 0;
    }


    @Override
    public String toString() {
        return internalName;
    }
}
