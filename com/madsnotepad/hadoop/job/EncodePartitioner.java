package com.madsnotepad.hadoop.job.encode;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class EncodePartitioner implements Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		String fileName = key.toString();
		if (fileName.endsWith(".mp4")) {
			fileName = fileName.substring(fileName.lastIndexOf("/") + 1, fileName.length());
			int fileIndex = Integer.parseInt(fileName.substring(0, fileName.indexOf(".mp4")));
			int partition = 0;
			int min = 0;
			int max = 10;
			while(true) {
				if(isBetween(fileIndex, min, max)) {
					break;
				}
				partition++;
				min += 10;
				max += 10;
			}
			return partition;
		}
		return 0;
	}

	@Override
	public void configure(JobConf arg0) {

	}

   private boolean isBetween(int x, int min, int max) {
	   if (x == 0 && min == 0) {
		   return true;
	   }
       return ((x > min) && (x <= max));
   }
}
