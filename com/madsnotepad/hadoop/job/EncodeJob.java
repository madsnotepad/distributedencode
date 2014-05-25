package com.madsnotepad.hadoop.job.encode;

/************************************************************************
 *  The code had been tested but has been changed several times or will *
 *  be changed to rearrange the code. So there is no warranty that it 	*
 *  will work as it is but the concept works.							*
 ************************************************************************/

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.*;
import org.apache.hadoop.util.*;

import com.madsnotepad.hadoop.util.HadoopUtil;
import com.madsnotepad.hadoop.util.FFMpegUtil;

public class EncodeJob {

	public static class EncodeMapper extends MapReduceBase implements Mapper<Text, BytesWritable, Text, Text> {
		private String hdfsSourceVideoDirectory;
		private String localSourceVideoDirectory;
		private String hdfsEncodedVideoDirectory;
		private String localEncodedVideoDirectory;

		public void configure(JobConf jobConf) {
			hdfsSourceVideoDirectory = jobConf.get("hdfsSourceVideoDirectory");
			localSourceVideoDirectory = jobConf.get("localSourceVideoDirectory");
			hdfsEncodedVideoDirectory = jobConf.get("hdfsEncodedVideoDirectory");
			localEncodedVideoDirectory = jobConf.get("localEncodedVideoDirectory");
		}

		public void map(Text key, BytesWritable value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String fileName = key.toString();
			//the value is copied locally at localSourceVideoDirectory passed in config
			HadoopUtil.getInstance().copyToLocalFileSystem(value, localSourceVideoDirectory + fileName);
			String outputFileName = FFMpegUtil.getInstance().getFileNameWithoutExtn(fileName) + ".mp4";
			//delete output file if it exists. Old files if the job is rerun would be there that needs to be removed
			//before starting the new encoding
			FFMpegUtil.getInstance().deleteFile(localSourceVideoDirectory + outputFileName);
			//encode here
			FFMpegUtil.getInstance().encodeFile(localSourceVideoDirectory + fileName, localSourceVideoDirectory + outputFileName);
			//copy the output to HDFS
			HadoopUtil.getInstance().copyToHdfsFileSystem(localEncodedVideoDirectory + outputFileName, hdfsEncodedVideoDirectory + outputFileName, true);
			output.collect(new Text(outputFileName), new Text(""));
			System.out.println("===========================Completed file copying ==================" + outputFileName);
			//cleanup the downloaded source
			FFMpegUtil.getInstance().deleteFile(localSourceVideoDirectory + fileName);
			reporter.setStatus("===========================Completed file copying ==================" + outputFileName);
		}
	}


	public static class MergeMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		private String hdfsEncodedVideoDirectory;
		private String localMergeVideoDirectory;
		private String hdfsMergeVideoDirectory;
		private String outPrefix;


		public void configure(JobConf jobConf) {
			hdfsEncodedVideoDirectory = jobConf.get("hdfsEncodedVideoDirectory");
			localMergeVideoDirectory = jobConf.get("localMergeVideoDirectory");
			hdfsMergeVideoDirectory = jobConf.get("hdfsMergeVideoDirectory");
			outPrefix = jobConf.get("outPrefix");
		}

		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//Iterate the part file to get the encoded files to be merged
			Scanner files = new Scanner(value.toString());
			String concatContent = "";
			List<String> encodedFileList = new ArrayList<String>();
			while (files.hasNext()) {
				String file = files.next();
				HadoopUtil.getInstance().copyToLocalFileSystem(hdfsEncodedVideoDirectory + file, localMergeVideoDirectory + file, false);
				File f = new File(localMergeVideoDirectory + file);
				//create the concat text
				concatContent += "file '" + file + "'" + String.format("%n");
				encodedFileList.add(localMergeVideoDirectory + file);
			}
			if (concatContent.equals("")) {
				reporter.setStatus("===========================Will not proceed to merge since concat is empty ==================" + key.toString());
				return;
			}
			FFMpegUtil.getInstance().writeToFile(concatContent, localMergeVideoDirectory + "concat.txt");
			String outputFile = outPrefix + key.toString() + ".mp4";
			//delete output file if it exists
			FFMpegUtil.getInstance().deleteFile(localMergeVideoDirectory + outputFile);
			FFMpegUtil.getInstance().performMerge(localMergeVideoDirectory + "concat.txt", localMergeVideoDirectory + outputFile);
			HadoopUtil.getInstance().copyToHdfsFileSystem(localMergeVideoDirectory + outputFile, hdfsMergeVideoDirectory + outputFile, true);
			output.collect(new Text(outputFile), new Text(""));
			//cleanup concat text
			FFMpegUtil.getInstance().deleteFile(localMergeVideoDirectory + "concat.txt");
			//cleanup other encoded files that are merged
			for (String encodedFile : encodedFileList) {
				FFMpegUtil.getInstance().deleteFile(encodedFile);
			}
			reporter.setStatus("===========================Completed file copying ==================" + key.toString());
		}
	}


	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private String allFiles = "";
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			output.collect(key, values.next());
			reporter.setStatus("==============================Completed reduce for ==================== " + key.toString());
		}
	}

	private JobConf getEncodeJobConf(String jobInputPath, String jobOutputPath, int numOfReduces) throws IOException {
		HadoopUtil.getInstance().deleteHdfsFile(jobOutputPath);
		JobConf conf = new JobConf(EncodeJob.class);
		conf.setJobName("encodejob");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(EncodeMapper.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(BinaryFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setPartitionerClass(EncodePartitioner.class);
		conf.setNumReduceTasks(numOfReduces);
		FileInputFormat.setInputPaths(conf, new Path(jobInputPath));
		FileOutputFormat.setOutputPath(conf, new Path(jobOutputPath));
		conf.set("hdfsSourceVideoDirectory", "/user/hduser/video/");
		conf.set("localSourceVideoDirectory", "/home/hduser/filesplits/");
		conf.set("hdfsEncodedVideoDirectory", "/user/hduser/encodedvideo/");
		conf.set("localEncodedVideoDirectory", "/home/hduser/filesplits/");
		return conf;
	}

	private JobConf getMergeJobConf(String jobName, String jobInputPath, String jobOutputPath, String outPrefix) throws IOException {
		HadoopUtil.getInstance().deleteHdfsFile(jobOutputPath);
		JobConf conf = new JobConf(EncodeJob.class);
		conf.setJobName(jobName);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(MergeMapper.class);
		//confM.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(MergeFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(jobInputPath));
		FileOutputFormat.setOutputPath(conf, new Path(jobOutputPath));
		conf.set("hdfsEncodedVideoDirectory", "/user/hduser/encodedvideo/");
		conf.set("localMergeVideoDirectory", "/home/hduser/mergedvideo/");
		conf.set("hdfsMergeVideoDirectory", "/user/hduser/mergedvideo/");
		conf.set("outPrefix", outPrefix);

		return conf;
	}

	public static void main(String[] args) throws Exception {
		EncodeJob encode = new EncodeJob();
		System.out.println("About the start job");
		JobConf encodeConf = encode.getEncodeJobConf("/user/hduser/video", "/user/hduser/voutput", 14);
		Job encodeJob = new Job(encodeConf);
		encodeJob.waitForCompletion(true);

		JobConf mergeConf = encode.getMergeJobConf("mergejob", "/user/hduser/voutput/part-*", "/user/hduser/mergedout1", "m1");
		Job mergeJob = new Job(mergeConf);
		mergeJob.waitForCompletion(true);

		mergeConf = encode.getMergeJobConf("finalmergejob", "/user/hduser/mergedout1/part-*", "/user/hduser/mergedout2", "m2");
		mergeConf.set("hdfsEncodedVideoDirectory", "/user/hduser/mergedvideo/");
		mergeConf.set("localMergeVideoDirectory", "/home/hduser/fmergedvideo/");
		mergeConf.set("hdfsMergeVideoDirectory", "/user/hduser/fmergedvideo/");
		mergeJob = new Job(mergeConf);
		mergeJob.waitForCompletion(true);


	}
}