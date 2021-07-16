import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class ReducerImpl extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
    @Override
    public void reduce(IntWritable integer, Iterator<IntWritable> iterator,
                       OutputCollector<IntWritable, NullWritable> outputCollector, Reporter reporter) throws IOException {
        int result = 0;
        while (iterator.hasNext()) {
            result += iterator.next().get();
        }
        outputCollector.collect(new IntWritable(result), NullWritable.get());
    }
}
