/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.common.codec;

public class ByteBitMap {
    private int size;
    private int length;
    private byte[] buf;

    public ByteBitMap(int sizeInBits) {
        size = sizeInBits;
        length = (sizeInBits >> 3) + ((sizeInBits & 0x07) == 0 ? 0 : 1);
        buf = new byte[length];
    }

    public int size() {
        return this.size;
    }

    public boolean at(int offset) {
        int index = indexFor(offset);
        return (buf[index] & (1 << (offset & 0x07))) > 0;
    }

    public void atPut(int offset, boolean value) {
        int index = indexFor(offset);
        buf[index] |=  (1 << (offset & 0x07));
    }

    public void clear() {
        for (int i = 0; i < length; i++) {
            buf[i] = 0;
        }
    }

    public byte[] getBuffer() {
        return buf;
    }

    public boolean allSetted() {
        if ((size & 0x07) == 0) {
            for (int i = 0; i < length; i++) {
                if (buf[i] != (byte)0xFF) {
                    return false;
                }
            }
        } else {
            for (int i = 0; i < length - 1; i++) {
                if (buf[i] != (byte)0xFF) {
                    return false;
                }
            }
            int val = (1 << (size & 0x07)) - 1;
            if (buf[length - 1] != val) {
                return false;
            }
        }
        return true;
    }

    private int indexFor(int offset) {
        return offset >> 3;
    }
}
