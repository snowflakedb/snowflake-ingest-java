package org.apache.parquet.column.impl;

import org.apache.parquet.io.api.Binary;

public interface ParquetBdecColumnWriter {
    /**
     * writes the current value
     * @param value an int value
     */
    void write(int value);

    /**
     * writes the current value
     * @param value a long value
     */
    void write(long value);

    /**
     * writes the current value
     * @param value a boolean value
     */
    void write(boolean value);

    /**
     * writes the current value
     * @param value a float value
     */
    void write(float value);

    /**
     * writes the current value
     * @param value a double value
     */
    void write(double value);

    /**
     * writes the current null value
     */
    void writeNull();

    void write(byte[] value);

    void write(String value);

    /**
     * Close the underlying store. This should be called when there are no
     * more data to be written.
     */
    void close();

    public long getBufferedSizeInMemory();
}
