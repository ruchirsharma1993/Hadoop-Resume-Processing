import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.parser.PdfReaderContentParser;
import com.itextpdf.text.pdf.parser.SimpleTextExtractionStrategy;
import com.itextpdf.text.pdf.parser.TextExtractionStrategy;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



public class PdfRecordReader extends RecordReader {

	private String[] lines = null;
	private LongWritable key = null;
	private Text value = null;

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException 
	{
		PdfInputDriver.count++;
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		final Path file = split.getPath();

		/*
		 * The below code contains the logic for opening the file and seek to
		 * the start of the split. Here we are applying the Pdf Parsing logic
		 */

		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		
		PdfReader reader = new PdfReader(fileIn);
	    PdfReaderContentParser parser = new PdfReaderContentParser(reader);
	    TextExtractionStrategy strategy;
	    String parsedText="";
	 
	    StringBuffer sb = new StringBuffer("");
	    boolean desc=false;
	    for (int i = 1; i <= reader.getNumberOfPages(); i++)
	    {
	         strategy = parser.processContent(i, new SimpleTextExtractionStrategy());
	         String cur = strategy.getResultantText().toString();
	         parsedText+=cur;
	    }
	    reader.close();
	   
	    String arr[] = parsedText.split("\n");
	    parsedText="";
		for(int i=0;i<arr.length;i++)
		{
			 String cur = arr[i];
			 //System.out.println(cur+cur.contains("Description:"));
	         if(cur.contains("Description:"))
	         {
	        	 System.out.println("***DESC START");
	        	 desc=true;

	        	 cur=cur.trim();
	        	 System.out.println("Appending: "+cur);
	        	 sb.append(cur);
	         }
	         else if(desc&&cur.contains(":"))
	         {
	        	 desc=false;
	        	 parsedText+=sb.toString()+"\n";

	        	 System.out.println("Final:"+sb.toString());
	        	 sb.delete(0, sb.length());
	        
	        	 parsedText+=cur+"\n";;
	         }
	         else if(desc)
	         {
	        	 cur=cur.trim();
	        	 System.out.println("Appending: "+cur);
	        	 sb.append(cur);
	         }
	         else
	        	 parsedText+=cur+"\n";
		}
	    this.lines = parsedText.split("\n");
	    System.out.println(parsedText);
}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (key == null) {
			key = new LongWritable();
			key.set(1);
			value = new Text();
			value.set(lines[0]);
		} else {
			int temp = (int) key.get();
			if (temp < (lines.length - 1)) {
				int count = (int) key.get();
				value = new Text();
				value.set(lines[count]);
				count = count + 1;
				key = new LongWritable(count);
			} else {
				return false;
			}

		}
		if (key == null || value == null) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {

		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {

		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {

		return 0;
	}

	@Override
	public void close() throws IOException {

	}

}