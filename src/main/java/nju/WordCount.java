package nju;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.WritableComparable;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    static enum CountersEnum { INPUT_WORDS }

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();
    private Configuration conf;
    private BufferedReader fis;

    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      conf = context.getConfiguration();
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
      if (conf.getBoolean("wordcount.skip.patterns", false)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        for (URI patternsURI : patternsURIs) {
          Path patternsPath = new Path(patternsURI.getPath());
          String patternsFileName = patternsPath.getName().toString();
          parseSkipFile(patternsFileName);
        }
      }
    }

    private void parseSkipFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + StringUtils.stringifyException(ioe));
      }
    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = (caseSensitive) ?
          value.toString() : value.toString().toLowerCase();
      line = line.replaceAll("\\d+", "");//数字
      line = line.replaceAll("[\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×]","");//一切标点
      for (String pattern : patternsToSkip) {
        line = line.replaceAll(pattern, "");
      }
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        String nextToken = itr.nextToken(); //提取每个单词  而不是一行Line  用来匹配停词
        if(nextToken.length()<3) continue;
        for(String pattern : patternsToSkip){
          if(nextToken.equals(pattern)) continue;
        }
        word.set(nextToken);
        context.write(word, one);
        Counter counter = context.getCounter(CountersEnum.class.getName(),
            CountersEnum.INPUT_WORDS.toString());
        counter.increment(1);
        
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
/*
  private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
    public int compare(WritableComparable a,WritableComparable b){
        return -super.compare(a, b);
      }
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return -super.compare(b1, s1, l1, b2, s2, l2);
      }     
  }
*/

	private static class Decrease extends IntWritable.Comparator {
		public int compare(WritableComparable value1, WritableComparable value2) {
			int t1 = -super.compare(value1, value2);
			return t1;
		}
		public int compare(byte[] key1, int value1, int value12, byte[] key2, int value2, int value22) {
			int t2 = -super.compare(key1, value1, value12, key2, value2, value22);
			return t2;
		}
	}

	public static class MyReducer extends Reducer <IntWritable,Text,Text,IntWritable>{
		private int OutputSum=0;
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val:values)
			{
				OutputSum = OutputSum+1;
				if(OutputSum > 100)             //输出前（）个
					return;
				else{
					String t = OutputSum + ":" + val.toString() + ",";
					Text WORD = new Text(t);
					context.write(WORD, key);
				}
			}
		}
	}
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
      System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(SequenceFileOutputFormat.class);



    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      if ("-skip".equals(remainingArgs[i])) {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }

    Path intermediatePath = new Path("IntermediateOutput");
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, intermediatePath);
    job.waitForCompletion(true);

    System.out.println("开始第二个任务");

    Job sortjob = Job.getInstance(conf, "word count sort");
    FileInputFormat.addInputPath(sortjob, intermediatePath);
    FileOutputFormat.setOutputPath(sortjob, new Path(otherArgs.get(1)));
    sortjob.setInputFormatClass(SequenceFileInputFormat.class);
    sortjob.setMapperClass(InverseMapper.class);
    sortjob.setReducerClass(MyReducer.class);
    sortjob.setSortComparatorClass(Decrease.class);
    sortjob.setOutputKeyClass(IntWritable.class);
    sortjob.setOutputValueClass(Text.class);

    sortjob.waitForCompletion(true);

    FileSystem.get(conf).delete(intermediatePath);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}