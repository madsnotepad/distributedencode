package com.madsnotepad.hadoop.job.encode;

import java.io.IOException;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.RecordReader;

public class VideoFileRecordReader implements RecordReader<Text, BytesWritable> {

	private FileSplit fileSplit;
	private Configuration conf;
	private boolean processed = false;
	private Text key = createKey();
	private BytesWritable value = createValue();

	public VideoFileRecordReader(FileSplit fileSplit, Configuration conf) throws IOException, InterruptedException {
		this.fileSplit = fileSplit;
		this.conf = conf;
	}

	public boolean next(Text key, BytesWritable value) throws IOException {
		if (!processed) {
			byte[] contents = new byte[(int) fileSplit.getLength()];
			System.out.println("length of file split is ============> " + contents.length);
			Path file = fileSplit.getPath();
			key.set(file.getName());
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			try {
				in = fs.open(file);
				IOUtils.readFully(in, contents, 0, (int)fileSplit.getLength());
				value.set(contents, 0, (int)fileSplit.getLength());
			} finally {
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public BytesWritable createValue() {
		return new BytesWritable();
	}

	@Override
	public long getPos() throws IOException {
		return processed ? fileSplit.getLength() : 0;
	}

	@Override
	public float getProgress() throws IOException{
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}

}