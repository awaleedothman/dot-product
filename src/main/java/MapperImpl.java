import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Scanner;

public class MapperImpl extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
    IntWritable key = new IntWritable(1);
    @Override
    public void map(LongWritable longWritable, Text text,
                    OutputCollector<IntWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
        String line = text.toString();
        Scanner sc = new Scanner(line);
        Integer i1 = sc.nextInt(), i2 = sc.nextInt();
        outputCollector.collect(key, new IntWritable(i1 * i2));
    }
}
