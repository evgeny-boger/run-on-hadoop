import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

class CombineWholeFileDummyRecordReader extends RecordReader<Text, NullWritable> {

  private CombineFileSplit fileSplit;
  private Configuration conf;
  private BytesWritable value = new BytesWritable();
  private boolean processed = false;
  private int curFileIdx = -1;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    this.fileSplit = (CombineFileSplit) split;
    this.conf = context.getConfiguration();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
	  if (curFileIdx + 1< fileSplit.getNumPaths()) {
		  curFileIdx += 1;
		  return true;
	  } else {
		  return false;
	  }
  }

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		//~ String paths = "";
		//~ for (int i = 0; i < fileSplit.getNumPaths(); ++i) {
			//~ paths += fileSplit.getPath(0).toString();
			//~ paths += ",";
		//~ }
//~
		//~ return new Text(paths);

		return new Text(fileSplit.getPath(curFileIdx).toString());
	}

  @Override
  public NullWritable getCurrentValue() throws IOException,
      InterruptedException {
    return NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException {
    return processed ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }
}
