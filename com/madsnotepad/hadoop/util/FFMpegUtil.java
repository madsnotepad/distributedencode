package com.madsnotepad.hadoop.util;

/************************************************************************
 *  The code had been tested but has been changed several times or will *
 *  be changed to rearrange the code. So there is no warranty that it 	*
 *  will work as it is but the concept works.							*
 ************************************************************************/




import java.io.*;

public class FFMpegUtil {

	public static final String FFMPEG_MERGE_CMD = "ffmpeg -f concat -i " + concatFile + " -vcodec copy -acodec copy " + outputFile;
	public static final String FFMPEG_ENCODE_CMD = "ffmpeg -i " + inputFile + " -q:v 3 " + outputFile;

	private static FFMpegUtil instance = new FFMpegUtil();

	private FFMpegUtil() {

	}

	public static FFMpegUtil getInstance() {
		return instance;
	}

	public String getFileNameWithoutExtn(String fileName) {
		if (fileName == null || fileName.trim().equals("")) {
				throw new RuntimeException("File name cannot be null");
		}
		return fileName.substring(0, fileName.lastIndexOf("."));
	}

	public void performMerge(String concatFile, String outputFile) throws IOException {
		Process pr = Runtime.getRuntime().exec(FFMPEG_MERGE_CMD);
		InputStream err = pr.getErrorStream();
		byte[] b = new byte[1024];
		int size = 0;
		while((size = err.read(b)) != -1){
			System.out.println("===========================Merge error ==================" + new String(b, 0, size));
		}
		InputStream is = pr.getInputStream();
		size = 0;
		while((size = is.read(b)) != -1){
			System.out.println("===========================Merge output ==================" + new String(b, 0, size));
		}
	}


	public String encodeFile(String inputFile, String outputFile) throws IOException {
		Process pr = Runtime.getRuntime().exec(FFMPEG_ENCODE_CMD);
		InputStream err = pr.getErrorStream();
		byte[] b = new byte[1024];
		int size = 0;
		while((size = err.read(b)) != -1){
			System.out.println("===========================Merge error ==================" + new String(b, 0, size));
		}
		InputStream is = pr.getInputStream();
		size = 0;
		while((size = is.read(b)) != -1){
			System.out.println("===========================Merge output ==================" + new String(b, 0, size));
		}
		return outputFile;
	}

	public void writeToFile(String content, String fileName) throws IOException {
		FileOutputStream out = null;
		File file;
		try {
			file = new File(fileName);
			out = new FileOutputStream(file);
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
			out.write(content.getBytes());
			out.flush();
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	public void deleteFile(String fileName) throws IOException {
		File file = new File(fileName);
		if (file.exists()) {
			file.delete();
		}
	}


}