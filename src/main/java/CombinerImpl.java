import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class CombinerImpl extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    IntWritable key = new IntWritable(1);
    @Override
    public void reduce(IntWritable integer, Iterator<IntWritable> iterator,
                       OutputCollector<IntWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int result = 0;
        while (iterator.hasNext()) {
            result += iterator.next().get();
        }
        outputCollector.collect(key, new IntWritable(result));
    }
}
