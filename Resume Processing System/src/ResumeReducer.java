import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ResumeReducer extends Reducer<Text, Text, Text, Text>
{
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text out = new Text();
		Text key_val = new Text();
		String ret="";
		System.out.println("PRInTING: "+values);
		boolean flag = true;
		for(Text val: values)
		{
			if(flag)
			{
				ret=val.toString().trim();
				flag=false;
			}
			else
				ret=ret+","+val.toString().trim();
		}
		out.set(ret);
		key_val.set("");
		context.write(key_val, out);
	}

	
}