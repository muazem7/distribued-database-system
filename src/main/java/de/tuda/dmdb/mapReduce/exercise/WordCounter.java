package de.tuda.dmdb.mapReduce.exercise;

import de.tuda.dmdb.mapReduce.Configuration;
import de.tuda.dmdb.mapReduce.Job;
import de.tuda.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.dmdb.mapReduce.operator.exercise.Reducer;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;

import java.util.Queue;
import java.util.StringTokenizer;

/**
 * Counts how often words each word was used. Input is textId,text. Output is word, numOccurrences.
 */
public class WordCounter implements BaseMapReduceExercise {
  public static class WMapper extends Mapper<SQLInteger, SQLVarchar, SQLVarchar, SQLInteger> {
    private static final int one = 1;
    private SQLVarchar word = new SQLVarchar(255);

    @Override
    public void map(SQLInteger key, SQLVarchar value, Queue<AbstractRecord> outList) {

      StringTokenizer itr = new StringTokenizer(value.getValue());
      while (itr.hasMoreTokens()) {
        word.setValue(itr.nextToken());

        AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
        newRecord.setValue(MapReduceOperator.KEY_COLUMN, word.clone());
        newRecord.setValue(MapReduceOperator.VALUE_COLUMN, new SQLInteger(one));
        outList.add(newRecord);
      }
    }
  }

  public static class WReducer extends Reducer<SQLVarchar, SQLInteger, SQLVarchar, SQLInteger> {

    private SQLInteger result = new SQLInteger();

    @Override
    public void reduce(SQLVarchar key, Iterable<SQLInteger> values, Queue<AbstractRecord> outList) {
      int sum = 0;
      for (SQLInteger val : values) {
        sum += val.getValue();
      }
      result.setValue(sum);
      AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
      newRecord.setValue(MapReduceOperator.KEY_COLUMN, (SQLVarchar) key);
      newRecord.setValue(MapReduceOperator.VALUE_COLUMN, result.clone());
      outList.add(newRecord);
    }
  }

  @Override
  public Job createJob(Configuration config) {
    Job job = Job.getInstance(config, "word count");
    job.setMapperClass(WordCounter.WMapper.class);
    job.setReducerClass(WordCounter.WReducer.class);

    AbstractRecord outputPrototype = new Record(2);
    outputPrototype.setValue(0, new SQLVarchar("key", 255));
    outputPrototype.setValue(1, new SQLInteger(0));
    job.setOutputPrototype(outputPrototype);

    return job;
  }
}
