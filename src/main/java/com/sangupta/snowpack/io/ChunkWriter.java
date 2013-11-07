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

package com.sangupta.snowpack.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.sangupta.snowpack.SnowpackMetadataDB;
import com.sangupta.snowpack.domain.ChunkInfo;
import com.sangupta.snowpack.domain.Flake;
import com.sangupta.snowpack.domain.FlakeMetadata;
import com.sangupta.snowpack.domain.SnowpackConfig;

/**
 * Writes a flake to the chunk associated with this writer.
 * 
 * @author sangupta
 *
 */
public class ChunkWriter extends ChunkIO {
	
	/**
	 * The location of the current write pointer in the file
	 */
	private volatile long currentWritePointer;
	
	/**
	 * Signifies that the writer has been closed
	 */
	private volatile boolean closed = false;
	
	/**
	 * Holds all flakes written inside this 
	 */
	private final ConcurrentMap<String, Flake> CACHE;
	
	/**
	 * The metadata DB instance.
	 */
	private final SnowpackMetadataDB metadataDB;
	
	/**
	 * Write caching settings
	 */
	private final SnowpackConfig snowpackConfig;
	
	/**
	 * Create a new chunk writer on the given file.
	 * 
	 * @param chunkFile
	 * @throws IOException 
	 */
	public ChunkWriter(File chunkFile, final long writePointer, final int chunkIndex, SnowpackMetadataDB metadataDB, SnowpackConfig snowpackConfig) throws IOException {
		// super constructor
		super(chunkFile, chunkIndex, "rw");
		
		// save configuration
		this.snowpackConfig = snowpackConfig;
		
		// caching settings
		if(this.snowpackConfig.writeCachingEnabled) {
			this.CACHE = new ConcurrentHashMap<String, Flake>();
		} else {
			this.CACHE = null;
		}

		// init meta-data db
		this.metadataDB = metadataDB;
		
		// pre-allocate disk space
		if(!this.chunkFile.exists()) {
			// pre-allocate size for this chunk
			synchronized (this.handler) {
				if(this.handler.length() == 0) {
					this.handler.setLength(this.snowpackConfig.preAllocationChunkSize);
				}
				
				this.currentWritePointer = 0;
			}
			
			this.numFiles = 0;
			return;
		}
		
		// set write pointer
		if(writePointer > 0) {
			this.currentWritePointer = writePointer;
			
			// read all flakes from this file and load
			// them into the cache
			loadFileIntoCache();
		}
	}
	
	/**
	 * Read the flake from the memory and return. In no case we will
	 * read this flake from the disk. If it is not in memory, it is not
	 * on disk.
	 * 
	 * @param flakeMetadata
	 * @return
	 */
	public Flake readFlake(FlakeMetadata flakeMetadata) {
		if(!this.snowpackConfig.writeCachingEnabled) {
			throw new IllegalStateException("Write caching is disabled... cannot read simultaneously while writing.");
		}
		
		if(!CACHE.containsKey(flakeMetadata.flakeName)) {
			return null;
		}
		
		return CACHE.get(flakeMetadata.flakeName);
	}

	/**
	 * Save the location on disk.
	 * 
	 * @param flake
	 * @throws IOException 
	 */
	public boolean save(Flake flake) throws IOException {
		if(this.closed) {
			throw new IllegalStateException("ChunkWriter has been closed");
		}
		
		// make it available to all other reading threads
		if(this.snowpackConfig.writeCachingEnabled) {
			CACHE.put(flake.flakeName, flake);
		}

		// obtain a write lock
		readWriteLock.writeLock().lock();
		
		long pointer = this.currentWritePointer;
		byte[] name = flake.flakeName.getBytes();
		
		final int headerLength = 4 + name.length + 4 + 8; // size of name, name data size of flake, creation time
		final int recordSize = headerLength + flake.bytes.length + 1; // the header, bytes, terminating character
		this.currentWritePointer += recordSize;
		
		this.handler.seek(pointer);
		if(this.closed) {
			throw new IllegalStateException("ChunkWriter has been closed");
		}
		
		this.handler.writeInt(name.length);
		this.handler.write(name);
		this.handler.writeInt(flake.length);
		this.handler.writeLong(flake.created);
		this.handler.write(flake.bytes);
		this.handler.write(0); // write a null terminator to the file
		this.numFiles++;
		
		// release the lock
		readWriteLock.writeLock().unlock();
		
		// create meta object
		FlakeMetadata flakeMeta = new FlakeMetadata(flake, this.chunkIndex, pointer, headerLength);
		
		// write the info to disk
		this.metadataDB.save(flakeMeta);
		
		return isOverflow();
	}
	
	/**
	 * Check if we are overflowing with data or not?
	 * 
	 * @return
	 */
	public boolean isOverflow() {
		// check if we are overboard or not
		if(this.currentWritePointer > this.snowpackConfig.maxFileSize) {
			return true;
		}
		
		if(this.snowpackConfig.maxFlakesInChunk > 0 && this.numFiles > this.snowpackConfig.maxFlakesInChunk) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * Return the {@link ChunkReader} object for this {@link ChunkWriter} object.
	 * 
	 * @return
	 * @throws IOException 
	 */
	public ChunkReader getReader() throws IOException {
		// close current file
		this.close();
		
		// open the reader
		return new ChunkReader(this.chunkFile, this.chunkIndex, this.numFiles);
	}
	
	/**
	 * Return the current size of the data that has been written.
	 * 
	 * @return
	 */
	public final long getCurrentDataSize() {
		return this.currentWritePointer;
	}
	
	/**
	 * Close this writer.
	 * @throws IOException 
	 * 
	 */
	@Override
	public void close() throws IOException {
		// mark we are closing
		this.closed = true;
		
		// close file handler
		this.handler.close();
		
		// clear up the cache
		if(this.CACHE != null) {
			this.CACHE.clear();
		}
	}

	/**
	 * Return the information on this chunk.
	 * 
	 * @return
	 */
	@Override
	public ChunkInfo getChunkInfo() {
		ChunkInfo info = new ChunkInfo();
		
		info.chunkID = this.chunkIndex;
		info.numFiles = this.numFiles;
		info.writePointer = this.currentWritePointer;
		
		return info;
	}
	
	/**
	 * Load the contents of the entire file into the cache
	 * @throws IOException 
	 * 
	 */
	private void loadFileIntoCache() throws IOException {
		if(!this.snowpackConfig.writeCachingEnabled) {
			return;
		}
		
		RandomAccessFile handler = null;
		try {
			handler = new RandomAccessFile(this.chunkFile, "r");
			
			// start reading byte by byte
			final long length = handler.length();
			while(handler.getFilePointer() < length) {
				int nameLength = handler.readInt();
//				if(nameLength <= 0) {
//					break;
//				}
				
				byte[] name = new byte[nameLength];
				handler.readFully(name);
				
				int flakeLength = handler.readInt();
				long created = handler.readLong();
				
				byte[] bytes = new byte[flakeLength];
				handler.readFully(bytes);
				
				byte terminator = handler.readByte(); // write a null terminator to the file
				if(terminator != 0) {
					throw new IllegalStateException("Existing chunk seems corrupted");
				}
				
				String flakeName = new String(name);
				Flake flake = new Flake(flakeName, flakeLength, created, bytes);
				CACHE.put(flakeName, flake);
			}
		} finally {
			// close the handler
			if(handler != null) {
				handler.close();
			}
		}
	}
}
