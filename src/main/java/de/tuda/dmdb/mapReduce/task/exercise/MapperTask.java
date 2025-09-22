package de.tuda.dmdb.mapReduce.task.exercise;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.operator.MapperBase;
import de.tuda.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.dmdb.mapReduce.task.MapperTaskBase;
import de.tuda.dmdb.operator.Operator;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

import java.lang.reflect.InvocationTargetException;

/**
 * Defines what happens during the map-phase of a map-reduce job Ie. implements the operator chains
 * for a reduce-phase The last operator in the chain writes to the output, ie. is used to populate
 * the output
 *
 * 
 */
public class MapperTask extends MapperTaskBase {

  public MapperTask(HeapTable input, HeapTable output,
      Class<? extends MapperBase<? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue>> mapperClass) {
    super(input, output, mapperClass);
  }

  @Override
  public void run() {
    // read data from input (Remember: There is a special operator to read data from a Table)
    TableScan scanner = new TableScan(input);
    // instantiate the mapper-operator
    Mapper mapper;
    try {
      mapper = (Mapper) mapperClass.getConstructor().newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }

    mapper.setChild(scanner);
    mapper.open();
    AbstractRecord rec;
    while ((rec = mapper.next()) != null)
      this.output.insert(rec);
    mapper.close();
  }
}
