// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.qe.protocol;

import com.google.common.base.Suppliers;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * NoResizeByteOutputStream: no resize when serialize huge object
 */
public class NoResizeByteOutputStream extends OutputStream {
    private final int initialCapacity;
    private List<byte[]> buffers;
    private byte[] currentBuffer;
    private int currentBufferWriteIndex;
    private int totalWriteSize;

    private static Supplier<Constructor<ByteString>> ropeByteStringBuilder = Suppliers.memoize(() -> {
        // try to invoke
        try {
            Class<?> ropeByteStringClazz = Class.forName("com.google.protobuf.RopeByteString");
            Constructor constructor = ropeByteStringClazz.getDeclaredConstructor(ByteString.class, ByteString.class);
            constructor.setAccessible(true);

            constructor.newInstance(ByteString.EMPTY, ByteString.EMPTY);
            return constructor;
        } catch (Throwable t) {
            return null;
        }
    });

    public NoResizeByteOutputStream(int initialCapacity) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initial capacity must be greater than 0, but was " + initialCapacity);
        }
        this.initialCapacity = initialCapacity;
        this.currentBuffer = new byte[initialCapacity];
        this.buffers = new ArrayList<>();
        buffers.add(this.currentBuffer);
    }

    @Override
    public void write(int i) {
        writeSingleByte((byte) i);
    }

    @Override
    public void write(@NotNull byte[] array) {
        write(array, 0, array.length);
    }

    @Override
    public void write(@NotNull byte[] array, int offset, int len) {
        if (len == 1) {
            writeSingleByte(array[offset]);
        } else {
            writeBytes(array, offset, len);
        }
    }

    private void writeSingleByte(byte b) {
        if (currentBufferWriteIndex >= currentBuffer.length) {
            addBuffer(1);
        }
        currentBuffer[currentBufferWriteIndex++] = b;
        totalWriteSize++;
    }

    private void writeBytes(byte[] array, int offset, int len) {
        int writableSize = Math.min(currentBuffer.length - currentBufferWriteIndex, len);
        System.arraycopy(array, offset, currentBuffer, currentBufferWriteIndex, writableSize);
        currentBufferWriteIndex += writableSize;

        int remainSize = len - writableSize;
        if (remainSize > 0) {
            addBuffer(remainSize);
            System.arraycopy(array, writableSize, currentBuffer, 0, remainSize);
            currentBufferWriteIndex = remainSize;
        }
        totalWriteSize += len;
    }

    public byte[] toByteArray() {
        byte[] result = new byte[totalWriteSize];
        int currentIndex = 0;
        for (int i = 0; i < buffers.size(); i++) {
            boolean isLastBuffer = i == buffers.size() - 1;
            byte[] buffer = buffers.get(i);
            int writeLength = isLastBuffer ? currentBufferWriteIndex : buffer.length;
            System.arraycopy(buffer, 0, result, currentIndex, writeLength);
            currentIndex += buffer.length;
        }
        return result;
    }

    public ByteString toByteString() {
        if (totalWriteSize == 0) {
            return ByteString.EMPTY;
        }
        try {
            Constructor<ByteString> constructor = ropeByteStringBuilder.get();
            if (constructor == null) {
                byte[] byteArray = toByteArray();
                return UnsafeByteOperations.unsafeWrap(byteArray);
            } else {
                return toByteString(0, buffers.size());
            }
        } catch (Throwable t) {
            // should never reach
            byte[] byteArray = toByteArray();
            return UnsafeByteOperations.unsafeWrap(byteArray);
        }
    }

    private ByteString toByteString(int startInclusive, int endExclusive)
            throws InvocationTargetException, InstantiationException, IllegalAccessException {
        if (startInclusive + 1 == endExclusive) {
            return UnsafeByteOperations.unsafeWrap(buffers.get(startInclusive));
        }
        int midExclusive = endExclusive / 2;
        ByteString left = toByteString(0, midExclusive);
        ByteString right = toByteString(midExclusive, endExclusive);
        Constructor<ByteString> constructor = ropeByteStringBuilder.get();
        return constructor.newInstance(left, right);
    }

    public void reset() {
        if (totalWriteSize > 0) {
            buffers = new ArrayList<>();
            currentBuffer = new byte[Math.max(initialCapacity, totalWriteSize)];
            buffers.add(currentBuffer);
            currentBufferWriteIndex = 0;
            totalWriteSize = 0;
        }
    }

    private void addBuffer(int preferGrowSize) {
        // at least grow two times
        int newBufferSize = Math.max(totalWriteSize, preferGrowSize);
        this.currentBuffer = new byte[newBufferSize];
        buffers.add(currentBuffer);
        currentBufferWriteIndex = 0;
    }
}
