import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanReducer
	extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			for (Text value:values){
				String line = key.toString() + "," + value.toString();
				context.write(new Text(line), new Text(""));
			}
	}
}