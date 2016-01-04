/**
 * Copyright 2015-2016 Emmanuel Keller / QWAZR
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.qwazr.database.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.qwazr.utils.CharsetUtils;
import com.qwazr.utils.SerializationUtils;
import com.qwazr.utils.json.JsonMapper;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

public interface ByteConverter<T> {

    byte[] toBytes(T value) throws IOException;

    T toValue(byte[] bytes) throws IOException;

    void forEach(T value, ValueConsumer consumer);

    void forFirst(T value, ValueConsumer consumer);

    class IntegerByteConverter implements ByteConverter<Integer> {

	public final static IntegerByteConverter INSTANCE = new IntegerByteConverter();

	@Override
	final public byte[] toBytes(Integer value) {
	    return ByteBuffer.allocate(4).putInt(value).array();
	}

	@Override
	final public Integer toValue(byte[] bytes) {
	    return ByteBuffer.wrap(bytes).getInt();
	}

	@Override
	final public void forEach(Integer value, final ValueConsumer consumer) {
	    if (value != null)
		consumer.consume(value);
	}

	@Override
	final public void forFirst(Integer value, ValueConsumer consumer) {
	    forEach(value, consumer);
	}

    }

    class LongByteConverter implements ByteConverter<Long> {

	public final static LongByteConverter INSTANCE = new LongByteConverter();

	@Override
	final public byte[] toBytes(Long value) {
	    return ByteBuffer.allocate(8).putLong(value).array();
	}

	@Override
	final public Long toValue(byte[] bytes) {
	    return ByteBuffer.wrap(bytes).getLong();
	}

	@Override
	final public void forEach(Long value, final ValueConsumer consumer) {
	    if (value != null)
		consumer.consume(value);
	}

	@Override
	final public void forFirst(Long value, ValueConsumer consumer) {
	    forEach(value, consumer);
	}
    }

    class DoubleByteConverter implements ByteConverter<Double> {

	public final static DoubleByteConverter INSTANCE = new DoubleByteConverter();

	@Override
	final public byte[] toBytes(Double value) {
	    return ByteBuffer.allocate(8).putDouble(value).array();
	}

	@Override
	final public Double toValue(byte[] bytes) {
	    return ByteBuffer.wrap(bytes).getDouble();
	}

	@Override
	final public void forEach(Double value, final ValueConsumer consumer) {
	    if (value != null)
		consumer.consume(value);
	}

	@Override
	final public void forFirst(Double value, ValueConsumer consumer) {
	    forEach(value, consumer);
	}
    }

    class StringByteConverter implements ByteConverter<String> {

	public final static StringByteConverter INSTANCE = new StringByteConverter();

	@Override
	final public byte[] toBytes(String value) {
	    return CharsetUtils.encodeUtf8(value);
	}

	@Override
	final public String toValue(byte[] bytes) {
	    return CharsetUtils.decodeUtf8(bytes);
	}

	@Override
	final public void forEach(String value, final ValueConsumer consumer) {
	    if (value != null)
		consumer.consume(value);
	}

	@Override
	final public void forFirst(String value, ValueConsumer consumer) {
	    forEach(value, consumer);
	}
    }

    class JsonByteConverter<T> implements ByteConverter<T> {

	private final Class<T> objectClass;

	public JsonByteConverter(Class<T> objectClass) {
	    this.objectClass = objectClass;
	}

	@Override
	final public byte[] toBytes(T value) throws JsonProcessingException {
	    return JsonMapper.MAPPER.writeValueAsBytes(value);
	}

	@Override
	final public T toValue(byte[] bytes) throws IOException {
	    return JsonMapper.MAPPER.readValue(bytes, objectClass);
	}

	@Override
	final public void forEach(T value, final ValueConsumer consumer) {
	    throw new RuntimeException("Function not implemented");
	}

	@Override
	final public void forFirst(T value, ValueConsumer consumer) {
	    throw new RuntimeException("Function not implemented");
	}
    }

    class JsonTypeByteConverter<T> implements ByteConverter<T> {

	private final TypeReference<T> typeReference;

	public JsonTypeByteConverter(TypeReference<T> typeReference) {
	    this.typeReference = typeReference;
	}

	@Override
	final public byte[] toBytes(T value) throws JsonProcessingException {
	    return JsonMapper.MAPPER.writeValueAsBytes(value);
	}

	@Override
	final public T toValue(byte[] bytes) throws IOException {
	    return JsonMapper.MAPPER.readValue(bytes, typeReference);
	}

	@Override
	final public void forEach(T value, final ValueConsumer consumer) {
	    throw new RuntimeException("Function not implemented");
	}

	@Override
	final public void forFirst(T value, ValueConsumer consumer) {
	    throw new RuntimeException("Function not implemented");
	}
    }

    class SerializableByteConverter<T extends Serializable> implements ByteConverter<T> {

	@Override
	final public byte[] toBytes(T value) {
	    return SerializationUtils.serialize(value);
	}

	@Override
	final public T toValue(byte[] bytes) {
	    return SerializationUtils.deserialize(bytes);
	}

	@Override
	final public void forEach(T value, final ValueConsumer consumer) {
	    throw new RuntimeException("Function not implemented");
	}

	@Override
	final public void forFirst(T value, ValueConsumer consumer) {
	    throw new RuntimeException("Function not implemented");
	}
    }

    class IntArrayByteConverter implements ByteConverter<int[]> {

	public final static IntArrayByteConverter INSTANCE = new IntArrayByteConverter();

	@Override
	final public byte[] toBytes(int[] intArray) throws IOException {
	    return Snappy.compress(intArray);
	}

	@Override
	final public int[] toValue(byte[] compressedByteArray) throws IOException {
	    if (compressedByteArray == null)
		return null;
	    return Snappy.uncompressIntArray(compressedByteArray);
	}

	@Override
	final public void forEach(int[] values, final ValueConsumer consumer) {
	    if (values == null)
		return;
	    for (int value : values)
		consumer.consume(value);
	}

	@Override
	final public void forFirst(int[] values, ValueConsumer consumer) {
	    if (values == null || values.length == 0)
		return;
	    consumer.consume(values[0]);
	}
    }

    class LongArrayByteConverter implements ByteConverter<long[]> {

	public final static LongArrayByteConverter INSTANCE = new LongArrayByteConverter();

	@Override
	final public byte[] toBytes(long[] intArray) throws IOException {
	    return Snappy.compress(intArray);
	}

	@Override
	final public long[] toValue(byte[] compressedByteArray) throws IOException {
	    if (compressedByteArray == null)
		return null;
	    return Snappy.uncompressLongArray(compressedByteArray);
	}

	@Override
	final public void forEach(long[] values, final ValueConsumer consumer) {
	    if (values == null)
		return;
	    for (long value : values)
		consumer.consume(value);
	}

	@Override
	final public void forFirst(long[] values, ValueConsumer consumer) {
	    if (values == null || values.length == 0)
		return;
	    consumer.consume(values[0]);
	}
    }

    class DoubleArrayByteConverter implements ByteConverter<double[]> {

	public final static DoubleArrayByteConverter INSTANCE = new DoubleArrayByteConverter();

	@Override
	final public byte[] toBytes(double[] doubleArray) throws IOException {
	    return Snappy.compress(doubleArray);
	}

	@Override
	final public double[] toValue(byte[] compressedByteArray) throws IOException {
	    if (compressedByteArray == null)
		return null;
	    return Snappy.uncompressDoubleArray(compressedByteArray);
	}

	@Override
	final public void forEach(double[] values, final ValueConsumer consumer) {
	    if (values == null)
		return;
	    for (double value : values)
		consumer.consume(value);
	}

	@Override
	final public void forFirst(double[] values, ValueConsumer consumer) {
	    if (values == null || values.length == 0)
		return;
	    consumer.consume(values[0]);
	}
    }

    class StringArrayByteConverter implements ByteConverter<String[]> {

	public final static StringArrayByteConverter INSTANCE = new StringArrayByteConverter();

	@Override
	final public byte[] toBytes(String[] stringArray) throws IOException {
	    int l = 0;
	    for (String s : stringArray)
		l += (s.length() + 1);
	    char[] chars = new char[l];
	    CharBuffer buffer = CharBuffer.wrap(chars);
	    for (String s : stringArray) {
		buffer.append(s);
		buffer.append((char) 0);
	    }
	    return Snappy.compress(chars);
	}

	private final static String[] emptyStringArray = new String[0];

	@Override
	final public String[] toValue(byte[] compressedByteArray) throws IOException {
	    if (compressedByteArray == null)
		return null;
	    char[] chars = Snappy.uncompressCharArray(compressedByteArray);
	    if (chars.length == 0)
		return emptyStringArray;
	    List<String> array = new ArrayList<>();
	    int last = 0;
	    int pos = 0;
	    for (char c : chars) {
		if (c == 0) {
		    array.add(new String(chars, last, pos - last));
		    last = pos + 1;
		}
		pos++;
	    }
	    return array.toArray(new String[array.size()]);
	}

	@Override
	final public void forEach(String[] values, final ValueConsumer consumer) {
	    if (values == null)
		return;
	    for (String value : values)
		consumer.consume(value);
	}

	@Override
	final public void forFirst(String[] values, ValueConsumer consumer) {
	    if (values == null || values.length == 0)
		return;
	    consumer.consume(values[0]);
	}
    }

}
