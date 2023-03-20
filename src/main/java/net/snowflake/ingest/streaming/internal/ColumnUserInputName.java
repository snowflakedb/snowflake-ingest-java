package net.snowflake.ingest.streaming.internal;

import java.util.Objects;

public final class ColumnUserInputName {

    private final String userInputName;

    public ColumnUserInputName(String userInputName) {
        this.userInputName = userInputName;
    }

    public String getUserInputName() {
        return userInputName;
    }

    public ColumnInternalName toInternalName() {
        return new ColumnInternalName(LiteralQuoteUtils.unquoteColumnName(userInputName));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnUserInputName that = (ColumnUserInputName) o;

        return Objects.equals(userInputName, that.userInputName);
    }

    @Override
    public int hashCode() {
        return userInputName != null ? userInputName.hashCode() : 0;
    }

    @Override
    public String toString() {
        return userInputName;
    }
}
