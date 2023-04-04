package org.apache.parquet.column.impl;

public class ArrayParquetBdecColumnWriter implements ParquetBdecColumnWriter {
    private final int index;
    private final Object[] row;

    public ArrayParquetBdecColumnWriter(int index, Object[] row) {
        this.index = index;
        this.row = row;
    }


    @Override
    public void write(byte[] value) {
        row[index] = value;
    }

    @Override
    public void write(String value) {
        row[index] = value;
    }

    @Override
    public void write(int value) {
        row[index] = value;
    }

    @Override
    public void write(long value) {
        row[index] = value;
    }

    @Override
    public void write(boolean value) {
        row[index] = value;
    }

    @Override
    public void write(float value) {
        row[index] = value;
    }

    @Override
    public void write(double value) {
        row[index] = value;
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
