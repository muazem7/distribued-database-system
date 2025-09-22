package de.tuda.dmdb.mapReduce.task.exercise;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.operator.MapperBase;
import de.tuda.dmdb.mapReduce.operator.ReducerBase;
import de.tuda.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.dmdb.mapReduce.operator.exercise.Reducer;
import de.tuda.dmdb.mapReduce.task.SinglePhaseTaskBase;
import de.tuda.dmdb.operator.exercise.HashRepartitionExchange;
import de.tuda.dmdb.operator.exercise.Sort;
import de.tuda.dmdb.operator.exercise.TableScan;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.types.AbstractSQLValue;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Defines what happens during the execution a map-reduce task Ie. implements the operator chains
 * for a complete map-reduce task We assume the same number of mappers and reducers (no need to
 * change hashFunction of shuffle) The Operator chain that this task implements is: Scan-{@literal
 * >}Mapper-{@literal >}Shuffle-{@literal >}Sort-{@literal >}Reducer The last operator in the chain
 * writes to the output, ie. is used to populate the output
 *
 * 
 */
public class SinglePhaseTask extends SinglePhaseTaskBase {

  public SinglePhaseTask(HeapTable input, HeapTable output, int nodeId,
      Map<Integer, String> nodeMap, int partitionColumn,
      Class<? extends MapperBase<? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue>> mapperClass,
      Class<? extends ReducerBase<? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue>> reducerClass) {
    super(input, output, nodeId, nodeMap, partitionColumn, mapperClass, reducerClass);
  }

  @Override
  public void run() {
    // Scan(input partition)
    TableScan scanner = new TableScan(this.input);
    // Mapper-Operator
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

    // Exchange-Operator
    HashRepartitionExchange hashExchange = new HashRepartitionExchange(mapper, this.nodeId,
        this.nodeMap, this.port, this.partitionColumn);
    // Sort-Operator
    Sort sorter = new Sort(hashExchange, this.recordComparator);

    // Reducer-Operator
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

    reducer.setChild(sorter);
    reducer.open();
    // Write to output
    AbstractRecord rec;
    while ((rec = reducer.next()) != null)
      this.output.insert(rec);
    reducer.close();
    mapper.close();

  }
}
