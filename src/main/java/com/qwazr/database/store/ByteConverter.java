/*
 * Copyright 2015-2017 Emmanuel Keller / QWAZR
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
import com.qwazr.server.ServerException;
import com.qwazr.utils.CharsetUtils;
import com.qwazr.utils.ObjectMappers;
import com.qwazr.utils.SerializationUtils;
import org.xerial.snappy.Snappy;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

public interface ByteConverter<T> {

	T convert(Object value) throws IOException;

	byte[] toBytes(T value) throws IOException;

	T toValue(byte[] bytes) throws IOException;

	default T toValue(ByteBuffer byteBuffer) throws IOException {
		final byte[] bytes = new byte[byteBuffer.remaining()];
		System.arraycopy(byteBuffer.array(), byteBuffer.arrayOffset(), bytes, 0, bytes.length);
		return toValue(bytes);
	}

	default void forEach(T value, ValueConsumer consumer) {
		throw new ServerException(Response.Status.NOT_ACCEPTABLE, "Function not implemented");
	}

	default void forFirst(T value, ValueConsumer consumer) {
		throw new ServerException(Response.Status.NOT_ACCEPTABLE, "Function not implemented");
	}

	abstract class AbstractNumberConverter<T extends Number> implements ByteConverter<T> {

		final public T convert(Object value) throws IOException {
			if (value == null)
				return null;
			if (value instanceof Number)
				return fromNumber((Number) value);
			else if (value instanceof String) {
				try {
					return fromString((String) value);
				} catch (NumberFormatException e) {
					// Handled next
				}
			}
			throw new ServerException(Response.Status.NOT_ACCEPTABLE,
					"Cannot convert " + value.getClass() + " to a number");
		}

		abstract T fromNumber(Number number) throws IOException;

		abstract T fromString(String string) throws IOException;

	}

	class IntegerByteConverter extends AbstractNumberConverter<Integer> {

		public final static IntegerByteConverter INSTANCE = new IntegerByteConverter();

		@Override
		final public byte[] toBytes(final Integer value) {
			return ByteBuffer.allocate(4).putInt(value).array();
		}

		@Override
		final Integer fromString(final String string) {
			return Integer.parseInt(string);
		}

		@Override
		final Integer fromNumber(final Number number) {
			return number.intValue();
		}

		@Override
		final public Integer toValue(final byte[] bytes) {
			return ByteBuffer.wrap(bytes).getInt();
		}

		@Override
		final public Integer toValue(final ByteBuffer byteBuffer) {
			return byteBuffer.getInt();
		}

		@Override
		final public void forEach(final Integer value, final ValueConsumer consumer) {
			if (value != null)
				consumer.consume(value);
		}

		@Override
		final public void forFirst(final Integer value, final ValueConsumer consumer) {
			forEach(value, consumer);
		}

	}

	class LongByteConverter extends AbstractNumberConverter<Long> {

		public final static LongByteConverter INSTANCE = new LongByteConverter();

		@Override
		final public byte[] toBytes(final Long value) {
			return ByteBuffer.allocate(8).putLong(value).array();
		}

		@Override
		final Long fromString(final String string) {
			return Long.parseLong(string);
		}

		@Override
		final Long fromNumber(final Number number) {
			return number.longValue();
		}

		@Override
		final public Long toValue(final byte[] bytes) {
			return ByteBuffer.wrap(bytes).getLong();
		}

		@Override
		final public Long toValue(final ByteBuffer byteBuffer) {
			return byteBuffer.getLong();
		}

		@Override
		final public void forEach(final Long value, final ValueConsumer consumer) {
			if (value != null)
				consumer.consume(value);
		}

		@Override
		final public void forFirst(Long value, ValueConsumer consumer) {
			forEach(value, consumer);
		}
	}

	class DoubleByteConverter extends AbstractNumberConverter<Double> {

		public final static DoubleByteConverter INSTANCE = new DoubleByteConverter();

		@Override
		final public byte[] toBytes(Double value) {
			return ByteBuffer.allocate(8).putDouble(value).array();
		}

		@Override
		final Double fromString(final String string) {
			return Double.parseDouble(string);
		}

		@Override
		final Double fromNumber(final Number number) {
			return number.doubleValue();
		}

		@Override
		final public Double toValue(byte[] bytes) {
			return ByteBuffer.wrap(bytes).getDouble();
		}

		@Override
		final public Double toValue(final ByteBuffer byteBuffer) {
			return byteBuffer.getDouble();
		}

		@Override
		final public void forEach(final Double value, final ValueConsumer consumer) {
			if (value != null)
				consumer.consume(value);
		}

		@Override
		final public void forFirst(final Double value, final ValueConsumer consumer) {
			forEach(value, consumer);
		}
	}

	class StringByteConverter implements ByteConverter<String> {

		public final static StringByteConverter INSTANCE = new StringByteConverter();

		@Override
		final public String convert(final Object value) {
			return value instanceof String ? (String) value : value.toString();
		}

		@Override
		final public byte[] toBytes(final String value) {
			return CharsetUtils.encodeUtf8(value.toString());
		}

		@Override
		final public String toValue(final byte[] bytes) {
			return CharsetUtils.decodeUtf8(bytes);
		}

		@Override
		final public String toValue(final ByteBuffer byteBuffer) throws IOException {
			return CharsetUtils.decodeUtf8(byteBuffer);
		}

		@Override
		final public void forEach(final String value, final ValueConsumer consumer) {
			if (value != null)
				consumer.consume(value);
		}

		@Override
		final public void forFirst(final String value, final ValueConsumer consumer) {
			forEach(value, consumer);
		}
	}

	abstract class AbstractCastConvert<T> implements ByteConverter<T> {

		@Override
		final public T convert(final Object value) {
			return (T) value;
		}
	}

	class JsonByteConverter<T> extends AbstractCastConvert<T> {

		private final Class<T> objectClass;

		public JsonByteConverter(Class<T> objectClass) {
			this.objectClass = objectClass;
		}

		@Override
		final public byte[] toBytes(T value) throws JsonProcessingException {
			return ObjectMappers.JSON.writeValueAsBytes(value);
		}

		@Override
		final public T toValue(byte[] bytes) throws IOException {
			return ObjectMappers.JSON.readValue(bytes, objectClass);
		}

	}

	final class JsonTypeByteConverter<T> extends AbstractCastConvert<T> {

		private final TypeReference<T> typeReference;

		public JsonTypeByteConverter(TypeReference<T> typeReference) {
			this.typeReference = typeReference;
		}

		@Override
		final public byte[] toBytes(Object value) throws JsonProcessingException {
			return ObjectMappers.JSON.writeValueAsBytes(value);
		}

		@Override
		final public T toValue(byte[] bytes) throws IOException {
			return ObjectMappers.JSON.readValue(bytes, typeReference);
		}

	}

	final class SerializableByteConverter<T extends Serializable> extends AbstractCastConvert<T> {

		@Override
		final public byte[] toBytes(T value) throws IOException {
			return SerializationUtils.toDefaultBytes(value);
		}

		@Override
		final public T toValue(byte[] bytes) throws IOException {
			try {
				return (T) SerializationUtils.fromDefaultBytes(bytes);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

	}

	class IntArrayByteConverter extends AbstractCastConvert<int[]> {

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

	class LongArrayByteConverter extends AbstractCastConvert<long[]> {

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

	class DoubleArrayByteConverter extends AbstractCastConvert<double[]> {

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

	class StringArrayByteConverter extends AbstractCastConvert<String[]> {

		public final static StringArrayByteConverter INSTANCE = new StringArrayByteConverter();

		@Override
		final public byte[] toBytes(final String[] stringArray) throws IOException {
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
