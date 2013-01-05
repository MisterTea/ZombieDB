package com.github.mistertea.zombiedb;

import java.util.Iterator;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;

public class ThriftWrapperIterator<F extends TFieldIdEnum, T extends TBase<?,F>> implements Iterator<T> {
	private Class<T> type;
	private Iterator<byte[]> byteIterator;
	private TDeserializer deserializer;

	public ThriftWrapperIterator(Class<T> type, TDeserializer deserializer, Iterator<byte[]> byteIterator) {
		this.type = type;
		this.deserializer = deserializer;
		this.byteIterator = byteIterator;
	}

	@Override
	public boolean hasNext() {
		return byteIterator.hasNext();
	}

	@Override
	public T next() {
		try {
			T emptyThrift = type.newInstance();
			deserializer.deserialize(emptyThrift, byteIterator.next());
			return emptyThrift;
		} catch (TException e) {
			e.printStackTrace();
			return null;
		} catch (InstantiationException e) {
			e.printStackTrace();
			return null;
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void remove() {
		byteIterator.remove();
	}
	
}
