package de.tuda.dmdb.operator.exercise;

import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.operator.SortBase;
import de.tuda.dmdb.storage.AbstractRecord;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Sort extends SortBase {
  boolean isOpen = false;

  public Sort(Operator child, Comparator<AbstractRecord> recordComparator) {
    super(child, recordComparator);
  }

  @Override
  public void open() {
    if (isOpen)
      return;
    isOpen = true;
    this.child.open();
    this.sortedRecords = new PriorityQueue<>();

    AbstractRecord rec;
    while ((rec = child.next()) != null)
      sortedRecords.add(rec);

  }

  @Override
  public AbstractRecord next() {
    if (!isOpen)
      throw new NullPointerException();
    return sortedRecords.poll();
  }

  @Override
  public void close() {
    if (!isOpen)
      return;
    isOpen = false;
    this.child.close();
    this.sortedRecords.clear();
  }
}
