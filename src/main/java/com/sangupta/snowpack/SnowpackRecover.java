/**
 *
 * snowpack - Pack flakes in chunks
 * Copyright (c) 2013, Sandeep Gupta
 * 
 * http://www.sangupta/projects/snowpack
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.sangupta.snowpack;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;

import com.sangupta.snowpack.domain.ChunkInfo;
import com.sangupta.snowpack.domain.FlakeMetadata;
import com.sangupta.snowpack.domain.SnowpackMeta;

/**
 * A tool that can recover a crashed Snowpack directory
 * by reading all the snowpack data files and rebuilding
 * the metadata DB as well as the snowpack meta information
 * needed to run Snowpack.
 * 
 * Currently, recovery is possible only from the entire chunk
 * file. If the file is partially corrupt, recovery is currently
 * not possible. This will be added in a future release.
 * 
 * @author sangupta
 *
 */
public class SnowpackRecover {
	
	public static void main(String[] args) {
		if(args.length != 1) {
			System.out.println("Usage: java -classpath snowpack.jar com.sangupta.snowpack.SnowpackRecover <base-folder>");
			return;
		}
		
		File base = new File(args[0]);
		if(!base.exists()) {
			System.out.println("Base folder does not exists.");
			return;
		}
		
		if(!base.isDirectory()) {
			System.out.println("Base folder does not represent a valid directory on disk.");
			return;
		}
		
		long start = System.currentTimeMillis();
		recover(base);
		long end = System.currentTimeMillis();
		
		System.out.println("Recovery process complete in " + (end - start) + "ms.");
	}
	
	/**
	 * Start the recovery process.
	 * 
	 * @param baseDirectory
	 */
	public static void recover(final File baseDirectory) {
		// basic checks
		if(baseDirectory == null) {
			throw new IllegalArgumentException("Base directory cannot be empty");
		}
		
		if(!baseDirectory.isDirectory()) {
			throw new IllegalArgumentException("Base directory path does not represent a valid directory");
		}
		
		// now read all files from disk
		File[] files = baseDirectory.listFiles();
		if(files == null || files.length == 0) {
			System.out.println("No file found in the base directory... nothing to recover.");
			return;
		}
		
		// get all valid chunks from disk
		Map<Integer, File> validChunks = getValidChunks(files);
		
		// now check each chunk to find out the number of files in it
		if(validChunks.isEmpty()) {
			System.out.println("No valid snowpack chunks were found... nothing to recover.");
			return;
		}
		
		// rename the current metadata directory if any
		try {
			renameOldMetadataDirectory(baseDirectory);
		} catch (IOException e) {
			System.out.println("Unable to rename existing metadata directory... rename and run recovery again!");
			return;
		}
		
		// create a new metadata directory
		System.out.println("Creating new METADATA database for recovery...");
		final SnowpackMetadataDB metadataDB = new SnowpackMetadataDB(baseDirectory, false, 1000); // do not cache metadata 
		
		// iterate over all chunks
		List<ChunkInfo> chunkInfos = new ArrayList<ChunkInfo>();
		
		int totalFiles = 0;
		
		for(Entry<Integer, File> entry : validChunks.entrySet()) {
			File chunkFile = entry.getValue();
			int chunkID = entry.getKey();
			
			// now recover the file
			System.out.print("Recovering from chunk file: " + chunkFile.getAbsolutePath() + "...");
			ChunkInfo chunkInfo = null;
			try {
				chunkInfo = recoverChunkInfo(chunkID, chunkFile, metadataDB);
			} catch (FileNotFoundException e) {
				// this shall never happen as we just read the file
				// eat up
			} catch(IOException e) {
				// this happens when we are unable to read through file
				// or descriptors are not correct
				// eat up
			}
			
			if(chunkInfo == null) {
				System.out.println("failed.");
				continue;
			}
			
			// add to list of chunk infos
			chunkInfos.add(chunkInfo);
			totalFiles += chunkInfo.numFiles;
			System.out.println("recovered!");
			
			System.out.println("Recovered chunk-info: " + chunkInfo);
		}
		
		// close the metadata DB
		System.out.println("Closing recovered METADATA database...");
		metadataDB.close();
		
		// now check if something was recovered
		if(chunkInfos.isEmpty()) {
			System.out.println("Unable to recover anything from the chunk files... Sorry!");
			return;
		}
		
		// save this chunk info file
		Collections.sort(chunkInfos);
		writeSnowpackMeta(baseDirectory, chunkInfos);
		
		// output total files
		System.out.println("Total number of files in pack: " + totalFiles);
	}

	/**
	 * Rename any previous metadata directory to show that it is pre-recovery.
	 * 
	 * @param baseDirectory
	 * @throws IOException 
	 */
	private static void renameOldMetadataDirectory(File baseDirectory) throws IOException {
		File dir = new File(baseDirectory, SnowpackConstants.SNOWPACK_METADATA_DIRECTORY);
		File preRecover = new File(baseDirectory, SnowpackConstants.SNOWPACK_METADATA_DIRECTORY + ".prerecover." + System.currentTimeMillis());
		if(dir.exists() && dir.isDirectory()) {
			FileUtils.moveDirectoryToDirectory(dir, preRecover, true);
		}
	}

	/**
	 * Write the meta information for all the chunks that we were able to recover.
	 * 
	 * @param baseDirectory
	 * @param chunkInfos
	 * @throws IOException
	 */
	private static void writeSnowpackMeta(final File baseDirectory, final List<ChunkInfo> chunkInfos) {
		System.out.println("Writing snowpack meta information...");
		// update meta
		SnowpackMeta newMeta = new SnowpackMeta();
		newMeta.updateChunks(chunkInfos);
		
		// save up
		String metaInfo = Snowpack.GSON.toJson(newMeta);
		
		// write the information on current write chunk to disk
		// we currently use a JSON format to write data
		File infoFile = new File(baseDirectory, SnowpackConstants.SNOWPACK_INFO_FILENAME);
		
		try {
			FileUtils.writeStringToFile(infoFile, metaInfo, false);
		} catch (IOException e) {
			System.out.println("Unable to write the snowpack meta information to the base folder... recovery is not complete!");
			System.out.println("Write the following information to the file called '" + SnowpackConstants.SNOWPACK_INFO_FILENAME + "':");
			System.out.println("");
			System.out.println(metaInfo);
		}
	}

	/**
	 * Try and recover from a chunk.
	 * 
	 * @param chunkID
	 * @param chunkFile
	 * @param metadataDB 
	 * @return
	 * @throws IOException 
	 */
	private static ChunkInfo recoverChunkInfo(final int chunkID, final File chunkFile, SnowpackMetadataDB metadataDB) throws IOException {
		// open the file for reading
		RandomAccessFile raf = new RandomAccessFile(chunkFile, "r");
		
		// read the length first
		int nameLength, length, terminator, headerLength, numFiles = 0;
		long offset;
		
		List<FlakeMetadata> metas = new ArrayList<FlakeMetadata>();
		
		try {
			while(raf.getFilePointer() < raf.length()) {
				offset = raf.getFilePointer();
				
				nameLength = raf.readInt();
				byte[] name = new byte[nameLength];
				raf.readFully(name);
				
				length = raf.readInt();
				raf.readLong();
				raf.skipBytes((int) length);
		
				terminator = raf.readByte();
				if(terminator != 0) {
					System.out.print(" invalid descriptor found...");
					return null;
				}
				
				headerLength = 4 + name.length + 4 + 8;
				
				numFiles++;
				
				metas.add(new FlakeMetadata(new String(name), nameLength, chunkID, offset, headerLength));
			}
		} finally {
			raf.close();
		}
		
		// all clear for recovery
		
		// save all metadata in new DB
		for(FlakeMetadata meta : metas) {
			metadataDB.save(meta);
		}
		
		// return chunk info
		ChunkInfo info = new ChunkInfo();
		info.chunkID = chunkID;
		info.numFiles = numFiles;
		info.writePointer = -1;
		
		return info;
	}

	/**
	 * Read all valid chunks (valid by name) from disk. A valid chunk filename
	 * comprises of the {@link SnowpackConstants#SNOWFLAKE_FILENAME_PREFIX} and then
	 * a number that identifies the chunk itself.
	 * 
	 * @param files
	 * @return
	 */
	private static Map<Integer, File> getValidChunks(final File[] files) {
		Map<Integer, File> validChunks = new TreeMap<Integer, File>();
		
		for(File file : files) {
			// check if we have the files with name "snowpack-*"
			if(file.getName().startsWith(SnowpackConstants.SNOWFLAKE_FILENAME_PREFIX) && file.getName().endsWith(SnowpackConstants.SNOWFLAKE_FILENAME_SUFFIX)) {
				// check if the remaining part if a number
				String remaining = file.getName().substring(SnowpackConstants.SNOWFLAKE_FILENAME_PREFIX.length());
				remaining = remaining.substring(0, remaining.length() - SnowpackConstants.SNOWFLAKE_FILENAME_SUFFIX.length());
				
				if(remaining != null && remaining.length() > 0) {
					try {
						int number = Integer.parseInt(remaining);
						
						// a valid chunk file is found - valid only by name
						validChunks.put(number, file);
					} catch(NumberFormatException e) {
						// eat up
					}
				}
			}
		}
		
		return validChunks;
	}

}
