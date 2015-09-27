// cc SmallFilesToSequenceFileConverter A MapReduce program for packaging a collection of small files as a single SequenceFile
import java.io.IOException;
import java.util.*;
import java.io.*;

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.fs.FileSystem;
import java.util.Random;
import java.lang.ProcessBuilder;

import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.fs.LocalFileSystem;

//vv SmallFilesToSequenceFileConverter
public class BypassRunner extends Configured
    implements Tool {

	static class BypassMapper
		extends Mapper<Text, NullWritable, Text, Text> {

        static final String envPrefix = "RUNONHADOOP_";
		private Text filenameKey;
		static final Random rand = new Random();

	    @Override
	    protected void setup(Context context) throws IOException,
			InterruptedException
		{
			Configuration configuration = context.getConfiguration();

			CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
			filenameKey = new Text(inputSplit.toString());

			int taskId = context.getTaskAttemptID().getTaskID().getId();

			Path outPath = FileOutputFormat.getWorkOutputPath((TaskInputOutputContext) context);
			Path intermediateOutPath = new Path(outPath,  "_intermediate");



			FileSystem targetFS = outPath.getFileSystem(configuration);
			targetFS.mkdirs(intermediateOutPath);

/*
 *
 * side effect files
 *
			 pass to child process:
			   list of input files in HDFS
			   unique ID (either "task ID" or "<task ID>_<file map no>"

			   HDFS output directory
			   HDFS intermediate output directory


			   temporary dir (will be cleared after the end of the Task) - replaced with cwd

			   local output directory (step 2)
			   local intermediate output directory (step 2)

			  Child process can create files in local output and local intermediate output directories.
			  The files created in these directories will be automatically proliferated
			  * to the corresponding HDFS directories. Files MUST contain unique ID (see above) in their
			  * relative paths. (step 2)


*/


			Runtime rt = Runtime.getRuntime();

			ProcessBuilder pb = new ProcessBuilder("./wrapper.py");
			Map<String, String> env = pb.environment();

			env.put(envPrefix + "UID", String.format("%d", taskId));
			env.put(envPrefix + "HDFS_OUTPUT_DIR", outPath.toString());
			env.put(envPrefix + "HDFS_INTERMEDIATE_DIR", intermediateOutPath.toString());
			//~ env.put("RUNONHADOOP_TEMP_DIR", Path.getPathWithoutSchemeAndAuthority(tempDirPath).toString());

			env.put(envPrefix + "HDFS_INPUT_COUNT", String.valueOf(inputSplit.getNumPaths()));
			for (int i = 0; i < inputSplit.getNumPaths(); ++i) {
				env.put( String.format(envPrefix + "HDFS_INPUT_%d", i),
						 inputSplit.getPath(i).toString());
			}


			pb.redirectErrorStream(true);
			Process proc = pb.start();

            InputStream stdout = proc.getInputStream(); // yes, it's process stdout
            InputStreamReader isr = new InputStreamReader(stdout);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            System.out.println("<OUT>");
            while ( (line = br.readLine()) != null)
                System.out.println(line);
            System.out.println("</OUT>");
            int exitVal = proc.waitFor();
            System.out.println("Process exitValue: " + exitVal);




	    }

	    @Override
	    protected void map(Text key, NullWritable value, Context context)
	        throws IOException, InterruptedException
	    {
			context.write(filenameKey, key);
			String outDir = FileOutputFormat.getWorkOutputPath((TaskInputOutputContext) context).toString();
			context.write(filenameKey, new Text(outDir));
	    }

	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = SelectorMapper.parseInputAndOutput(this, getConf(), args);
		if (job == null) {
			return -1;
		}

		job.setInputFormatClass(CombineWholeFileDummyInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(BypassMapper.class);
		job.setNumReduceTasks(0);


		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new BypassRunner(), args);
		System.exit(exitCode);
	}
}
