package org.apache.parquet.column.impl;

public enum EmptyParquetBdecColumnWriter implements ParquetBdecColumnWriter {
    INSTANCE;

    EmptyParquetBdecColumnWriter() {}

    @Override
    public void write(byte[] value) {

    }

    @Override
    public void write(String value) {

    }

    @Override
    public void write(int value) {

    }

    @Override
    public void write(long value) {

    }

    @Override
    public void write(boolean value) {

    }

    @Override
    public void write(float value) {

    }

    @Override
    public void write(double value) {

    }

    @Override
    public void writeNull() {

    }

    @Override
    public void close() {

    }

    @Override
    public long getBufferedSizeInMemory() {
        return 0;
    }
}
