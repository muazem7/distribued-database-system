package de.tuda.dmdb.mapReduce.operator.exercise;

import de.tuda.dmdb.mapReduce.operator.ReducerBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * similar to
 * https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/Reducer.java
 *
 * <p>
 * Reduces a set of intermediate values which share a key to a smaller set of values.
 *
 * <p>
 * A Reducer performs three primary tasks: 1) Read sorted input from child 2) Group input from child
 * (ie., prepare input to reduce function) 3) Invoke the reduce() method on the prepared input to
 * generate new output pairs
 *
 * <p>
 * The output of the <code>Reducer</code> is <b>not re-sorted</b>.
 *
 * @param <KEYIN> SQLValue type of the input key
 * @param <VALUEIN> SQLValue type of the input value
 * @param <KEYOUT> SQLValue type of the output key
 * @param <VALUEOUT> SQLValue type of the output value
 * 
 */
public class Reducer<KEYIN extends AbstractSQLValue, VALUEIN extends AbstractSQLValue, KEYOUT extends AbstractSQLValue, VALUEOUT extends AbstractSQLValue>
    extends ReducerBase<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  boolean isOpen = false;

  @Override
  public void open() {
    if (isOpen)
      return;
    isOpen = true;
    this.child.open();
    this.nextList = new LinkedBlockingQueue<>();
  }

  @Override
  @SuppressWarnings("unchecked")
  public AbstractRecord next() {
    // this method has to prepare the input to the reduce function
    // it also returns the result of the reduce function as next
    // Implement the grouping of mapper-outputs here
    // You can assume that the input to the reducer is sorted. This makes the grouping operation
    // easier. Keep this in mind if you write your own tests (make sure that input to reducer is
    // sorted)

    if (!isOpen)
      throw new NullPointerException();
    AbstractRecord rec = this.lastRecord;
    if (rec == null)
      rec = this.child.next();
    if (rec == null)
      return this.nextList.poll();

    Queue TempQueue = new LinkedBlockingQueue();
    KEYIN key = (KEYIN) rec.getValue(KEY_COLUMN);
    while (rec != null) {
      if (rec.getValue(KEY_COLUMN).equals(key))
        TempQueue.add(rec.getValue(VALUE_COLUMN));
      else
        break;
      rec = this.child.next();
      this.lastRecord = rec;
    }
    this.reduce(key, TempQueue, this.nextList);
    TempQueue.clear();

    return this.nextList.poll();
  }

  @Override
  public void close() {
    if (!isOpen)
      return;
    isOpen = false;
    this.child.close();
    this.nextList.clear();
  }
}
