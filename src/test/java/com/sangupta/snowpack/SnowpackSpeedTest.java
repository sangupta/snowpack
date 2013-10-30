package com.sangupta.snowpack;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

/**
 * Write a few million small flakes of 100 bytes each and test
 * the performance issues.
 * 
 * @author sangupta
 *
 */
public class SnowpackSpeedTest {
	
	private static final long maxFiles = 1l * 1000l * 1000l; // a million flakes
	
	private static final int FLAKE_SIZE = 100; // flake size of 100 bytes each
	
	private static long written = 0;
	
	private static long read = 0;
	
	public static void main(String[] args) throws IOException {
		File dir = new File("snowpack-speed");
		
		// start and run test
		Snowpack snowpack = new Snowpack(dir);
		try {
//			testWrite(snowpack, dir);
			
			testRead(snowpack);
		} catch(Throwable t) {
			t.printStackTrace();
		} finally {
			System.out.println("Written: " + written);
			System.out.println("Read: " + read);
			
			// close snowpack
			snowpack.close();
		}
	}
	
	private static void testWrite(Snowpack snowpack, File dir) throws IOException {
		// delete any previous run data
		if(dir.exists() && dir.isDirectory()) {
			FileUtils.deleteQuietly(dir);
		}
		
		// start writing
		final byte[] bytes = new byte[FLAKE_SIZE];
		Arrays.fill(bytes, (byte) 'a');
		
		// write speed test
		long start = System.currentTimeMillis();
		for(long i = 0; i < maxFiles; i++) {
			snowpack.saveFlake(String.valueOf(i), bytes);
//			written++;
		}
		long end = System.currentTimeMillis();
		
		System.out.println(maxFiles + " written in " + (end - start) + "ms.");
	}
	
	private static void testRead(Snowpack snowpack) throws IOException {
		// read speed test
		long start = System.currentTimeMillis();
		for(long i = 0; i < maxFiles; i++) {
			snowpack.getFlake(String.valueOf(i));
//			read++;
		}
		long end = System.currentTimeMillis();
		
		System.out.println(maxFiles + " read in " + (end - start) + "ms.");
	}

}
