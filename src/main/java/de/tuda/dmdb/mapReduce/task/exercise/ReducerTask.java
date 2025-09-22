package de.tuda.dmdb.mapReduce.task.exercise;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.operator.ReducerBase;
import de.tuda.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.dmdb.mapReduce.operator.exercise.Reducer;
import de.tuda.dmdb.mapReduce.task.ReducerTaskBase;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

import java.lang.reflect.InvocationTargetException;

/**
 * Defines what happens during the reduce-phase of a map-reduce job Ie. implements the operator
 * chains for a reduce-phase The last operator in the chain writes to the output, ie. is used to
 * populate the output
 *
 * 
 */
public class ReducerTask extends ReducerTaskBase {

  public ReducerTask(HeapTable input, HeapTable output,
      Class<? extends ReducerBase<? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue>> reducerClass) {
    super(input, output, reducerClass);
  }

  @Override
  public void run() {
    // read data from input (Remember: There is a special operator to read data from a Table)
    TableScan scanner = new TableScan(input);
    // instantiate the reduce-operator
    Reducer reducer;
    try {
      reducer = (Reducer) reducerClass.getConstructor().newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }

    reducer.setChild(scanner);
    reducer.open();
    AbstractRecord rec;
    while ((rec = reducer.next()) != null)
      this.output.insert(rec);
    reducer.close();
  }
}
