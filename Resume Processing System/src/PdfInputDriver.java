import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PdfInputDriver {

	static public int count=0;
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		Job job = new Job(conf, "Pdfwordcount");
		job.setJarByClass(PdfInputDriver.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(PdfInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		String inp_path = "/home/training/Desktop/in_resume";
		String out_path = "/home/training/Desktop/out_resume";
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.setMapperClass(ResumeMapper.class);
		job.setReducerClass(ResumeReducer.class);
		
		System.out.println(job.waitForCompletion(true));
	}
}
