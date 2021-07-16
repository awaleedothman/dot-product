import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class Runner {
    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(Runner.class);
        conf.setJobName("DotProduct");
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(MapperImpl.class);
        conf.setCombinerClass(CombinerImpl.class);
        conf.setReducerClass(ReducerImpl.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
