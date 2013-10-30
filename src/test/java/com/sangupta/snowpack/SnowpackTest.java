package com.sangupta.snowpack;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;

import com.sangupta.snowpack.domain.Flake;

/**
 * A test pack to make sure that entire {@link Snowpack} works just fine
 * by creating a new pack, adding files, retrieving files, the closing it out
 * and then re-opening and working over it.
 * 
 * This is not a <code>JUnit</code> based test case.
 * 
 * @author sangupta
 *
 */
public class SnowpackTest {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		File root = new File("snowpack-test-1");
		root.mkdirs();
		
		// create a new snowpackit
		Snowpack snowpack = new Snowpack(root);
		System.out.println("snowpack open now!");
		
		final int MAX_FILES = 1000 * 1000;
		
		// start adding files to it
		writeFiles(snowpack, "a", MAX_FILES);
//		writeIndividualFiles(root, "b", MAX_FILES);
		
		// now that we are done
		// try read any number of random files from the snowpack
		readFlakes(snowpack, MAX_FILES);
		
		// close the snowpack
		try {
			snowpack.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("snowpack closed");
	}

	private static void readFlakes(Snowpack snowpack, int maxFiles) {
		int failed = 0, success = 0;
		
		long start = System.currentTimeMillis();
		try {
			for(int index = 0; index < maxFiles; index++) {
				String fileName = "File-a-" + index;
				Flake flake = snowpack.getFlake(fileName);
				if(flake == null) {
					failed++;
				} else {
					String val = String.valueOf(index);
					byte[] bytes = getFlakeData(val);
					
					// compare data
					if(ArrayUtils.isEquals(flake.bytes, bytes)) {
						success++;
					} else {
						failed++;
					}
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}		
		
		long end = System.currentTimeMillis();
		System.out.println("Pass=" + success + "; failed=" + failed + " in " + (end - start) + "ms.");
		
	}

	private static void writeFiles(Snowpack snowpack, String prefix, final int maxFiles) {
		long start = System.currentTimeMillis();

		for(int index = 0; index < maxFiles; index++) {
			// generate a unique name for the file
			String fileName = "File-" + prefix + "-" + index;
			String val = String.valueOf(index);
			
			snowpack.saveFlake(fileName, getFlakeData(val));
		}
		
		long end = System.currentTimeMillis();
		System.out.println(maxFiles + " files added in " + (end - start) + "ms.");
	}

	private static void writeIndividualFiles(File root, String prefix, final int maxFiles) throws IOException {
		long start = System.currentTimeMillis();

		for(int index = 0; index < maxFiles; index++) {
			// generate a unique name for the file
			String fileName = "File-" + prefix + "-" + index;
			String val = String.valueOf(index);
			
			FileUtils.writeByteArrayToFile(new File(root, fileName), getFlakeData(val));
		}
		
		long end = System.currentTimeMillis();
		System.out.println(maxFiles + " files added in " + (end - start) + "ms.");
	}

	private static byte[] getFlakeData(String val) {
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < 10; i++) {
			builder.append(val);
		}
		return builder.toString().getBytes();
	}
}
