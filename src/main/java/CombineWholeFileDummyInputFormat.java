// cc WholeFileInputFormat An InputFormat for reading a whole file as a record
import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;


//vv WholeFileInputFormat
public class CombineWholeFileDummyInputFormat
    extends CombineFileInputFormat<Text, NullWritable> {

	public CombineWholeFileDummyInputFormat(){
	    super();
	    setMaxSplitSize(67108864); // 64 MB, default block size on hadoop
	    //~ setMaxSplitSize(10000000);
	}


	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public RecordReader<Text, NullWritable> createRecordReader(
	InputSplit split, TaskAttemptContext context) throws IOException
	{
		CombineWholeFileDummyRecordReader reader = new CombineWholeFileDummyRecordReader();

		try {
			reader.initialize(split, context);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		return reader;
	}
}



