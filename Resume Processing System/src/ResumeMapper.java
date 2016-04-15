import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ResumeMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private Text word = new Text();
	private Text samp = new Text();
	

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
			
			String out = "";
			
			String cur = value.toString();
			if(cur.contains(":"))
			{
				StringTokenizer st = new StringTokenizer(cur, ":");
				String desc = st.nextToken();
				String val = st.nextToken();
				out=val.trim();
			
			
				System.out.println("***Wrote***"+PdfInputDriver.count+out);
				word.set(""+PdfInputDriver.count);
				context.progress();
				samp.set(out);
				context.write(word, samp);
			}
	}
}