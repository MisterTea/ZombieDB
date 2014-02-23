package com.github.mistertea.zombiedb;

import java.io.IOException;
import java.util.Iterator;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;

public class ThriftWrapperIterator<F extends TFieldIdEnum, T extends TBase<?, F>>
    implements CloseableIterator<T> {
  private Class<T> type;
  private CloseableIterator<byte[]> byteIterator;
  private TDeserializer deserializer;

  public ThriftWrapperIterator(Class<T> type, TDeserializer deserializer,
      CloseableIterator<byte[]> byteIterator) {
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

  @Override
  public void close() throws IOException {
    byteIterator.close();
  }

}
