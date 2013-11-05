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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sangupta.snowpack.domain.ChunkInfo;
import com.sangupta.snowpack.domain.Flake;
import com.sangupta.snowpack.domain.FlakeMetadata;
import com.sangupta.snowpack.domain.SnowpackConfig;
import com.sangupta.snowpack.domain.SnowpackMeta;
import com.sangupta.snowpack.io.ChunkReader;
import com.sangupta.snowpack.io.ChunkWriter;

/**
 * Handles functions related to one snowpack. A snowpack stores millions
 * of smaller files, called flakes into few large files, called as chunks.
 * 
 * @author sangupta
 *
 */
public class Snowpack {
	
	/**
	 * The GSON instance to use for storing information
	 */
	static final Gson GSON = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.IDENTITY).create();
	
	/**
	 * The associated snowpack configuration
	 */
	private final SnowpackConfig configuration;
	
	/**
	 * The caching system to be used
	 */
	private final SnowpackCache cache;

	/**
	 * The metadata DB associated with it
	 */
	private final SnowpackMetadataDB metadataDB;
	
	/**
	 * The metadata for the snowpack itself
	 */
	private final SnowpackMeta meta;
	
	/**
	 * Stores the root to the base directory.
	 * 
	 */
	private final File baseDirectory;
	
	/**
	 * Number of chunk readers
	 */
	private final List<ChunkReader> chunkReaders = new ArrayList<ChunkReader>(10); // 10 chunks to start with
	
	/**
	 * The current chunk writer which is going to write data to disk
	 */
	private final AtomicReference<ChunkWriter> chunkWriter = new AtomicReference<ChunkWriter>();
	
	/**
	 * Keeps track of whether this snowpack has closed or not.
	 * 
	 */
	private volatile boolean closed = false; 
	
	/**
	 * Create a {@link Snowpack} with default configuration.
	 * 
	 * @param root
	 */
	public Snowpack(File root) {
		this(root, new SnowpackConfig());
	}
	
	/**
	 * Create a {@link Snowpack} with the given configuration.
	 * 
	 * 
	 * @param root
	 */
	public Snowpack(File root, SnowpackConfig config) {
		if(root == null) {
			throw new IllegalArgumentException("Root directory for Snowpack cannot be null");
		}
		
		this.baseDirectory = root;
		this.configuration = config;
		
		if(!this.baseDirectory.exists()) {
			this.baseDirectory.mkdirs();
		}

		// read all the configuration from the file-system
		this.meta = readSnowpackMeta();

		// initialize the metadata DB
		this.metadataDB = new SnowpackMetadataDB(this.baseDirectory, this.configuration.readCachingEnabled, this.configuration.maxEntriesInMetadataCache);

		// the cache system
		this.cache = new SnowpackCache(this.configuration.readCachingEnabled, this.configuration.maxEntriesInReadCache);
		
		// load all readers/writers
		try {
			initialize();
		} catch(Exception e) {
			throw new RuntimeException("Unable to start snowpack", e);
		}
	}

	/**
	 * Initialize snowpack into this directory.
	 * @throws IOException 
	 * 
	 */
	private void initialize() throws IOException {
		// check if last chunk is full
		long writePointer = 0;
		int lastChunkIndex = this.meta.numChunks - 1;
		if(lastChunkIndex < 0) {
			lastChunkIndex = 0;
		}
		
		if(isChunkFull(this.meta.getLastChunk(), true)) {
			lastChunkIndex++;
		} else {
			if(this.meta.getLastChunk() != null) {
				writePointer = this.meta.getLastChunk().writePointer;
			}
		}
		
		// start reading all chunks
		for(int index = 0; index < this.meta.numChunks - 1; index++) {
			ChunkReader reader = new ChunkReader(getChunkFile(index), index);
			this.chunkReaders.add(reader);
		}
		
		// now load the current chunk writer or create a new one
		ChunkWriter writer = new ChunkWriter(getChunkFile(lastChunkIndex), writePointer, lastChunkIndex, this.metadataDB, this.configuration); // subtracting one to reload the chunk
		boolean updated = this.chunkWriter.compareAndSet(null, writer);
		if(!updated) {
			throw new IllegalStateException("Unable to create a chunk writer");
		}
		
		// update the chunk count
		this.meta.numChunks = lastChunkIndex + 1;
	}
	
	/**
	 * Release any cache that we may be holding up. This will reduce the memory
	 * requirements of Snowpack in trade-of off performance.
	 */
	public void emptyCache() {
		// empty the global flake cache
		this.cache.emptyCache();
		
		// empty the metadata DB cache
		this.metadataDB.emptyCache();
	}
	
	/**
	 * Method that checks whether the given chunk is full or not.
	 * 
	 * @param chunkInfo
	 * @return
	 */
	private boolean isChunkFull(ChunkInfo chunkInfo, boolean checkDelta) {
		if(chunkInfo == null) {
			return false;
		}
		
		long delta = this.configuration.maxFileSize - chunkInfo.writePointer;
		if(delta < 0) {
			return true;
		}
		
		if(checkDelta) {
			if(delta < this.configuration.averageExpectedSize) {
				return true;
			}
		}
		
		return false;
	}

	/**
	 * Retrieve a given flake from the snowpack.
	 * 
	 * @param flakeName
	 * @return
	 * @throws IOException 
	 */
	public Flake getFlake(String flakeName) throws IOException {
		if(this.closed) {
			throw new IllegalStateException("This snowpack has already been closed.");
		}
		
		// flake in memory cache
		if(this.cache.containsKey(flakeName)) {
			return this.cache.get(flakeName);
		}
		
		// flake is with us
		FlakeMetadata metadata = this.metadataDB.get(flakeName);
		if(metadata == null) {
			// no such flake
			return null;
		}
		
		// now check which chunk reader should process it
		ChunkWriter myWriter = this.chunkWriter.get();
		if(metadata.chunk == myWriter.getChunkIndex()) {
			// this is the chunk being written to
			// return it from the memory
			return myWriter.readFlake(metadata);
		}
		
		// no flake in global cache
		// read from disk
		return this.chunkReaders.get(metadata.chunk).readFlake(metadata);
	}
	
	/**
	 * Checks if there exists a flake in the database with the given flake name 
	 * or not.
	 * 
	 * @param flakeName
	 * @return
	 */
	public boolean hasFlake(String flakeName) {
		if(flakeName == null || flakeName.isEmpty()) {
			throw new IllegalArgumentException("Flake name cannot be null/empty");
		}
		
		if(this.closed) {
			throw new IllegalStateException("This snowpack has already been closed.");
		}
		
		// flake in memory cache
		if(this.cache.containsKey(flakeName)) {
			return true;
		}
		
		// flake is with us
		return this.metadataDB.has(flakeName);
	}
	
	/**
	 * Store a new flake in the snowpack.
	 * 
	 * @param flakeName
	 * @param bytes
	 */
	public boolean saveFlake(String flakeName, byte[] bytes) {
		if(this.closed) {
			throw new IllegalStateException("This snowpack has already been closed.");
		}
		
		if(flakeName == null || flakeName.isEmpty()) {
			throw new IllegalArgumentException("Flake name cannot be null/empty");
		}
		
		if(bytes == null || bytes.length == 0) {
			throw new IllegalArgumentException("Flake data cannot be null/empty");
		}
		
		if(bytes.length > this.configuration.maxFileSize) {
			throw new IllegalArgumentException("Flake size is greater than maximum allowed size");
		}
		
		Flake flake = new Flake();
		flake.flakeName = flakeName;
		flake.length = bytes.length;
		flake.bytes = bytes;
		flake.created = System.currentTimeMillis();
		
		// obtain a copy locally for we may need to atomically switch
		ChunkWriter myWriter = this.chunkWriter.get();
		
		try {
			boolean overflow = myWriter.save(flake);

			// check if the chunk is full or not
			if(overflow) {
				// we need to roll over
				int index = this.meta.numChunks++;
				ChunkWriter newWriter = new ChunkWriter(getChunkFile(index), 0l, index, this.metadataDB, this.configuration);
				
				// switch if no other thread has switched till now
				boolean updated = this.chunkWriter.compareAndSet(myWriter, newWriter);
				
				if(updated) {
					// make the current chunk writer a chunk reader
					ChunkReader reader = myWriter.getReader();
					
					// add this to global readers
					this.chunkReaders.add(reader);
					
					// release current writer
					myWriter = null;

					// output the metadata
					this.writeCurrentMetadata();
				}
			}
			
			// all set and done
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	/**
	 * Close this {@link Snowpack} by writing all data to disk.
	 * @throws IOException 
	 * 
	 */
	public void close() throws IOException {
		if(this.closed) {
			// already closed
			return;
		}
		
		// close this one
		this.closed = true;
		
		// start closing everything else
		this.cache.close();
		this.metadataDB.close();
		
		// close chunk writer - for it will clean up the cache
		final ChunkWriter chunkWriter = this.chunkWriter.get();
		chunkWriter.close();
		
		// close all current readers
		for(ChunkReader reader : this.chunkReaders) {
			// start closing as well
			reader.close();
		}
		
		// write the metadata
		this.writeCurrentMetadata();
	}
	
 	/**
	 * Register a shutdown hook with the Java runtime. Useful for web applications
	 * when closing of the pack might not be initiated from code due to direct shutdown
	 * of the container.
	 * 
	 */
	public void registerShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			
			/**
			 * @see java.lang.Thread#run()
			 */
			@Override
			public void run() {
				super.run();
				
				// close snowpack
				try {
					Snowpack.this.close();
				} catch (IOException e) {
					System.out.println("Unable to shutdown snowpack cleanly.");
					e.printStackTrace();
				}
			}
			
		});
	}
	
	/**
	 * Write the current metadata to disk - so that we know where we are.
	 * 
	 * @throws IOException
	 */
	private void writeCurrentMetadata() throws IOException {
		// read the info chunks
		List<ChunkInfo> infos = new ArrayList<ChunkInfo>();
		
		// update the meta object
		// read all data from all chunks
		for(ChunkReader reader : this.chunkReaders) {
			infos.add(reader.getChunkInfo());
		}
		
		// get chunk writer's chunk info
		infos.add(this.chunkWriter.get().getChunkInfo());
		
		// update meta
		SnowpackMeta newMeta = new SnowpackMeta();
		newMeta.updateChunks(infos);
		
		// save up
		String metaInfo = GSON.toJson(newMeta);
		
		// write the information on current write chunk to disk
		// we currently use a JSON format to write data
		File infoFile = new File(this.baseDirectory, SnowpackConstants.SNOWPACK_INFO_FILENAME);
		
		FileUtils.writeStringToFile(infoFile, metaInfo, false);
	}
	
	/**
	 * Create a chunk file.
	 * 
	 * @param index
	 * @return
	 */
	private File getChunkFile(int index) {
		return new File(this.baseDirectory, SnowpackConstants.SNOWFLAKE_FILENAME_PREFIX + index + SnowpackConstants.SNOWFLAKE_FILENAME_SUFFIX);
	}

	/**
	 * Load the metadata information from the info file. The file
	 * formnat is plain JSON format of the {@link SnowpackMeta} object
	 * converted using Google GSON library.
	 * 
	 * @return 
	 * 
	 */
	private SnowpackMeta readSnowpackMeta() {
		File infoFile = new File(this.baseDirectory, SnowpackConstants.SNOWPACK_INFO_FILENAME);
		
		// file exists?
		if(!infoFile.exists()) {
			return new SnowpackMeta();
		}
		
		// if definitely a file?
		if(!infoFile.isFile()) {
			return new SnowpackMeta();
		}
		
		// read and parse
		try {
			String text = FileUtils.readFileToString(infoFile);
			if(text == null || text.length() == 0) {
				return new SnowpackMeta();
			}
			
			// load the GSON
			SnowpackMeta meta = GSON.fromJson(text, SnowpackMeta.class);
			
			// sort the chunks collections
			if(meta.chunks != null && !meta.chunks.isEmpty()) {
				Collections.sort(meta.chunks);
			}
			
			// return the meta file
			return meta;
		} catch(IOException e) {
			throw new RuntimeException("Snowpack information file is corrupt.");
		}
	}

}
