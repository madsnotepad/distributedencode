package com.madsnotepad.hadoop.util;

/************************************************************************
 *  The code had been tested but has been changed several times or will *
 *  be changed to rearrange the code. So there is no warranty that it 	*
 *  will work as it is but the concept works.							*
 ************************************************************************/

import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.*;
import org.apache.hadoop.util.*;


public class HadoopUtil {

	private static HadoopUtil instance = new HadoopUtil();

	private HadoopUtil() {

	}

	public static HadoopUtil getInstance() {
		return instance;
	}

	public Configuration getDefaultConfiguration() {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		return conf;
	}

	public void copyToLocalFileSystem(String hdfsFile, String localFile, boolean deleteSource) throws IOException {
		Configuration conf = getDefaultConfiguration();
		FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
		fs.copyToLocalFile(deleteSource, new Path(hdfsFile), new Path(localFile));
		System.out.println("===========================copyied to local===========================" + hdfsFile);
	}

	public void copyToLocalFileSystem(BytesWritable data, String localFile) throws IOException {
		byte[] content = new byte[data.getLength()];
		System.arraycopy(data.getBytes(), 0, content, 0, data.getLength());
		FileOperationsUtil.getInstance().writeToFile(content, localFile);
	}


	public void copyToHdfsFileSystem(String localFile, String hdfsFile, boolean deleteSource) throws IOException {
		Configuration conf = getDefaultConfiguration();
		FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
		fs.copyFromLocalFile(deleteSource, new Path(localFile), new Path(hdfsFile));
		System.out.println("===========================copyied to hdfs===========================" + localFile);
	}

	public void deleteHdfsFile(String filePath) throws IOException {
		Configuration conf = getDefaultConfiguration();
		FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
		fs.delete(new Path(filePath), true);
		System.out.println("===========================deleted file ===========================" + filePath);
	}

}