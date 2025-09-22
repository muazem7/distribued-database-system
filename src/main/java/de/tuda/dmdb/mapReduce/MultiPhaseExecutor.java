package de.tuda.dmdb.mapReduce;

import de.tuda.dmdb.access.exercise.HeapTable;
import de.tuda.dmdb.mapReduce.task.exercise.MapperTask;
import de.tuda.dmdb.mapReduce.task.exercise.ReducerTask;
import de.tuda.dmdb.mapReduce.task.exercise.ShuffleSortTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Executes map-reduce job as multiple phases and writes out intermediate results after each phase.
 * The three phases map, shuffle+sort, reduce are executed sequentially.
 */
public class MultiPhaseExecutor extends MapReduceExecutor {

  @Override
  public void submit(Job job) {
    init(job); // prepare execution

    // -----------------------
    // MAP PHASE
    // -----------------------
    List<HeapTable> mapperResults = new ArrayList<>();
    for (HeapTable partition : job.getInputs()) {
      HeapTable mapperResult = new HeapTable(job.getOutputPrototype());
      mapperResults.add(mapperResult);
      Future<?> future =
          cluster.submit(new MapperTask(partition, mapperResult, job.getMapperClass()));
      tasks.add(future);
    }

    // wait for map phase to finish
    this.waitForCompletion();

    // -----------------------
    // SHUFFLE & SORT PHASE
    // -----------------------
    List<HeapTable> shuffleResults = new ArrayList<>();
    for (int i = 0; i < this.numReducers; i++) {
      shuffleResults.add(new HeapTable(job.getOutputPrototype()));
    }

    // assign mapper outputs to shuffle tables
    for (int i = 0; i < mapperResults.size(); i++) {
      Future<?> future = cluster.submit(
          new ShuffleSortTask(mapperResults.get(i), shuffleResults.get(i % this.numReducers), // mapper
                                                                                              // ->
                                                                                              // reducer
              i, this.nodeMap, job.getConfiguration().getPartitionColumn(), this.numReducers));
      tasks.add(future);
    }

    // wait for shuffle phase to finish
    this.waitForCompletion();

    // -----------------------
    // REDUCE PHASE
    // -----------------------

    // ensure job outputs list has exactly numReducers tables
    while (job.getOutputs().size() < this.numReducers) {
      job.getOutputs().add(new HeapTable(job.getOutputPrototype()));
    }
    while (job.getOutputs().size() > this.numReducers) {
      job.getOutputs().remove(job.getOutputs().size() - 1);
    }

    // submit reducers
    for (int i = 0; i < this.numReducers; i++) {
      Future<?> future = cluster.submit(
          new ReducerTask(shuffleResults.get(i), job.getOutputs().get(i), job.getReducerClass()));
      tasks.add(future);
    }

    // wait for reduce phase to finish
    this.waitForCompletion();

    // -----------------------
    // PRINT FINAL RESULTS
    // -----------------------
    System.out.println("===== Final Job Output =====");
    int partitionNum = 0;
    for (HeapTable output : job.getOutputs()) {
      System.out.println("Output Partition " + partitionNum + ": " + output);
      partitionNum++;
    }
    System.out.println("===== End of Output =====");

    this.completed = true;
  }
}
