
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.lang.model.SourceVersion;
import javax.tools.Tool;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;


public class Hw3WordCount extends Configured implements Tool{
    @Override
    public int run(InputStream in, OutputStream out, OutputStream err, String... arguments) {
        return 0;
    }

    @Override
    public Set<SourceVersion> getSourceVersions() {
        return null;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // For each line in <input file>, read the queryword, then start a new job with that parameter
        conf.set("queryword", "neighbourhood");

        Job job = new Job(conf, "HW3");
        job.setJarByClass(Hw3WordCount.class);
        job.setJobName("HW3 Word Counter");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
