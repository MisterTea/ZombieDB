package com.github.mistertea.zombiedb;

import java.io.IOException;
import java.util.Iterator;


public class EmptyCloseableIteratorWrapper<T> implements CloseableIterator<T> {
  private Iterator<T> baseIterator;

  public EmptyCloseableIteratorWrapper(Iterator<T> baseIterator) {
    this.baseIterator = baseIterator;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public boolean hasNext() {
    return baseIterator.hasNext();
  }

  @Override
  public T next() {
    return baseIterator.next();
  }

  @Override
  public void remove() {
    baseIterator.remove();
  }
}
