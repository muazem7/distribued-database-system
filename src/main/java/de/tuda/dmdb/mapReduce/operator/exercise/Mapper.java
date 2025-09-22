package de.tuda.dmdb.mapReduce.operator.exercise;

import de.tuda.dmdb.mapReduce.operator.MapperBase;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * similar to:
 * https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/Mapper.java
 *
 * <p>
 * Maps input key/value pairs to a set of intermediate key/value pairs.
 *
 * <p>
 * Maps are the individual tasks which transform input records into a intermediate records. A given
 * input pair may map to zero or many output pairs. Output pairs are normally written to the
 * nextList member
 *
 * <p>
 * The class implements the iterator model and works on records. Records are read to retrieve the
 * KEY and VALUE, which are passed to the map() function The map function performs the defined
 * mapping and writes the new output to the passed in record List Use the member nextList as input
 * to the map() function to cache produced output across multiple next() calls.
 *
 * @param <KEYIN> SQLValue type of the input key
 * @param <VALUEIN> SQLValue type of the input value
 * @param <KEYOUT> SQLValue type of the output key
 * @param <VALUEOUT> SQLValue type of the output value
 * 
 */
public class Mapper<KEYIN extends AbstractSQLValue, VALUEIN extends AbstractSQLValue, KEYOUT extends AbstractSQLValue, VALUEOUT extends AbstractSQLValue>
    extends MapperBase<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

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
    // this method has to retrieve and prepare the input to the map function
    // it also returns the result of the map function as next
    // keep in mind that a mapper potentially maps one AbstractRecord to multiple other
    // AbstractRecords

    if (!isOpen)
      throw new NullPointerException();
    AbstractRecord rec = this.child.next();
    if (rec == null)
      return this.nextList.poll();
    map((KEYIN) rec.getValue(KEY_COLUMN), (VALUEIN) rec.getValue(VALUE_COLUMN), this.nextList);
    return this.nextList.poll(); // return first element or null
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
