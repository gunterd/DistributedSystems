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
        boolean foundFirstInstance = false;
        Configuration mapConfig = context.getConfiguration();

        String[] contextWords = mapConfig.get("contextWord").toLowerCase().split(" ");
        String[] queryWords = mapConfig.get("queryWord").toLowerCase().split(" ");

        // make the text lowercase and split it on anything that's not a-z.
        for (int i=0; i < contextWords.length; i++){
            String contextWord = contextWords[i];
            String queryWord = queryWords[i];
            String[] words = value.toString().toLowerCase().split("([^a-z])+");
            if (Arrays.asList(words).contains(contextWord)){
                for (int k=0; k<words.length; k++){
                    // Count - make sure to skip the first instance of the context word
                    if (words[k].equals(contextWord) && !foundFirstInstance){
                        foundFirstInstance = true;
                        continue;
                    }else if (words[k].equals(queryWord)){
                        word.set(words[k]);
                        context.write(new Text(contextWord + "\t" + word), one);
                    }

                }
            }
        }
    }
}
