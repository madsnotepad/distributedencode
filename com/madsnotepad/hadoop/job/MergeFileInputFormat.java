package com.madsnotepad.hadoop.job.encode;

import java.io.IOException;

import oyo.EncodeFileRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MergeFileInputFormat extends FileInputFormat<Text, Text> {

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		//dont split the file so that it could read fully
		return false;
	}

	@Override
	public RecordReader<Text, Text> getRecordReader(
		InputSplit split, JobConf job, Reporter reporter) throws IOException {
		return new TextFileRecordReader((FileSplit) split, job);
	}
}
