package de.tuda.dmdb.mapReduce.exercise;

import de.tuda.dmdb.mapReduce.Configuration;
import de.tuda.dmdb.mapReduce.Job;
import de.tuda.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.dmdb.mapReduce.operator.exercise.Reducer;
import de.tuda.dmdb.storage.AbstractRecord;
import de.tuda.dmdb.storage.Record;
import de.tuda.dmdb.storage.types.exercise.SQLDouble;
import de.tuda.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.dmdb.storage.types.exercise.SQLVarchar;

import java.util.Queue;

/**
 * Performs the query "SELECT userId,COUNT(sessionLength) FROM user GROUP BY userId". The input
 * table is user with key=userId, value=sessionLength. You should output userId,COUNT(sessionLength)
 */
public class UserSessionCount implements BaseMapReduceExercise {
  public static class WMapper extends Mapper<SQLInteger, SQLInteger, SQLInteger, SQLInteger> {

    @Override
    public void map(SQLInteger key, SQLInteger value, Queue<AbstractRecord> outList) {
      AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
      newRecord.setValue(MapReduceOperator.KEY_COLUMN, key);
      newRecord.setValue(MapReduceOperator.VALUE_COLUMN, value);
      outList.add(newRecord);
    }
  }

  public static class WReducer extends Reducer<SQLInteger, SQLInteger, SQLInteger, SQLInteger> {

    private SQLInteger result = new SQLInteger();

    @Override
    public void reduce(SQLInteger key, Iterable<SQLInteger> values, Queue<AbstractRecord> outList) {
      int c = 0;
      for (SQLInteger ignored : values) {
        c++;
      }
      result.setValue(c);
      AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
      newRecord.setValue(MapReduceOperator.KEY_COLUMN, key);
      newRecord.setValue(MapReduceOperator.VALUE_COLUMN, result.clone());
      outList.add(newRecord);
    }
  }

  @Override
  public Job createJob(Configuration config) {
    Job job = Job.getInstance(config, "count user session");
    job.setMapperClass(UserSessionCount.WMapper.class);
    job.setReducerClass(UserSessionCount.WReducer.class);

    AbstractRecord outputPrototype = new Record(2);
    outputPrototype.setValue(0, new SQLInteger(1));
    outputPrototype.setValue(1, new SQLInteger(0));
    job.setOutputPrototype(outputPrototype);
    return job;
  }
}
