import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration mapConfig = context.getConfiguration();

        String queryWord = mapConfig.get("queryword").toLowerCase();

        // make the text lowercase and split it on anything that's not a-z.

        String[] words = value.toString().toLowerCase().split("([^a-z])+");
        if (Arrays.asList(words).contains(queryWord)){
            for (int i=0; i<words.length; i++){
                // Count - make sure to skip the first instance of the queryword
                word.set(words[i]);
                context.write(word, one);
            }
        }
    }
}
