package com.madsnotepad.hadoop.job.encode;

/************************************************************************
 *  The code had been tested but has been changed several time			*
 *  to rearrange the code. So there is no warranty that it will work	*
 *  as it is but the concept works.										*
 ************************************************************************/

import java.io.IOException;

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

import com.madsnotepad.hadoop.job.encode.EncodeFileRecordReader;


public class BinaryFileInputFormat extends FileInputFormat<Text, BytesWritable> {

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		//dont split the file so that it could read fully
		return false;
	}

	@Override
	public RecordReader<Text, BytesWritable> getRecordReader(
		InputSplit split, JobConf job, Reporter reporter) throws IOException {
		return new VideoFileRecordReader((FileSplit) split, job);
	}
}
