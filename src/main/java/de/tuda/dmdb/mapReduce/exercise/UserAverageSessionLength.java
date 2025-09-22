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
import java.util.StringTokenizer;

/**
 * Performs the query "SELECT userId,AVG(sessionLength) FROM user GROUP BY userId". The input table
 * is user with key=userId, value=sessionLength. You should output userId,AVG(sessionLength).
 */
public class UserAverageSessionLength implements BaseMapReduceExercise {
  public static class WMapper extends Mapper<SQLInteger, SQLDouble, SQLInteger, SQLDouble> {

    @Override
    public void map(SQLInteger key, SQLDouble value, Queue<AbstractRecord> outList) {
      AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
      newRecord.setValue(MapReduceOperator.KEY_COLUMN, key);
      newRecord.setValue(MapReduceOperator.VALUE_COLUMN, value);
      outList.add(newRecord);
    }
  }

  public static class WReducer extends Reducer<SQLInteger, SQLDouble, SQLInteger, SQLDouble> {

    private SQLDouble result = new SQLDouble();

    @Override
    public void reduce(SQLInteger key, Iterable<SQLDouble> values, Queue<AbstractRecord> outList) {
      double sum = 0;
      int c = 0;
      for (SQLDouble val : values) {
        sum += val.getValue();
        c++;
      }
      result.setValue(sum / c);
      AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
      newRecord.setValue(MapReduceOperator.KEY_COLUMN, key);
      newRecord.setValue(MapReduceOperator.VALUE_COLUMN, result.clone());
      outList.add(newRecord);
    }
  }

  @Override
  public Job createJob(Configuration config) {
    Job job = Job.getInstance(config, "average user session");
    job.setMapperClass(UserAverageSessionLength.WMapper.class);
    job.setReducerClass(UserAverageSessionLength.WReducer.class);

    AbstractRecord outputPrototype = new Record(2);
    outputPrototype.setValue(0, new SQLInteger(1));
    outputPrototype.setValue(1, new SQLDouble(0.0));
    job.setOutputPrototype(outputPrototype);
    return job;
  }
}
