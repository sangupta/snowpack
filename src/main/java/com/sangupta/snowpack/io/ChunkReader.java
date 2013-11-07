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

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.sangupta.snowpack.domain.Flake;
import com.sangupta.snowpack.domain.FlakeMetadata;

/**
 * Reader associated with a snowpack chunk.
 * 
 * @author sangupta
 *
 */
public class ChunkReader extends ChunkIO implements Closeable {
	
	/**
	 * 
	 * @param chunkIndex
	 * @param baseDirectory
	 * @throws FileNotFoundException 
	 */
	public ChunkReader(File chunkFile, int chunkIndex, int numFiles) throws FileNotFoundException {
		super(chunkFile, chunkIndex, "r");
		this.numFiles = numFiles;
	}

	/**
	 * Read flake from the file.
	 * 
	 * @param flakeMetadata
	 * @return
	 * @throws IOException
	 */
	public byte[] read(FlakeMetadata flakeMetadata) throws IOException {
		if(flakeMetadata == null) {
			throw new IllegalArgumentException("Flake metadata cannot be null");
		}
		
		if(this.chunkIndex != flakeMetadata.chunk) {
			throw new IllegalArgumentException("Flake not from this chunk");
		}
		
		// obtain the read lock
		this.readWriteLock.readLock().lock();
		
		// read data
		this.handler.seek(flakeMetadata.offset + flakeMetadata.headerLength);
		byte[] bytes = new byte[flakeMetadata.length];
		this.handler.readFully(bytes);
		
		// release lock
		this.readWriteLock.readLock().unlock();
		
		return bytes;
	}
	
	public Flake readFlake(FlakeMetadata flakeMetadata) throws IOException {
		if(flakeMetadata == null) {
			throw new IllegalArgumentException("Flake metadata cannot be null");
		}
		
		if(this.chunkIndex != flakeMetadata.chunk) {
			throw new IllegalArgumentException("Flake not from this chunk");
		}
		
		// obtain the read lock
		this.readWriteLock.readLock().lock();
		
		// read data
		this.handler.seek(flakeMetadata.offset);
		
		int nameLength = this.handler.readInt();
		byte[] name = new byte[nameLength];
		this.handler.readFully(name);
		
		int length = this.handler.readInt();
		long creationTime = this.handler.readLong();
		
		byte[] bytes = new byte[flakeMetadata.length];

		this.handler.readFully(bytes);
		
		// release lock
		this.readWriteLock.readLock().unlock();
		
		return new Flake(flakeMetadata.flakeName, length, creationTime, bytes);
	}
	
	/**
	 * Close this chunk reader.
	 * 
	 */
	@Override
	public void close() throws IOException {
		this.handler.close();
	}
}
