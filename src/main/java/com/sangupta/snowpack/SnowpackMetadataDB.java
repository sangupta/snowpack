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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.sangupta.snowpack.domain.FlakeMetadata;

/**
 * Handles the metadata DB.
 * 
 * @author sangupta
 *
 */
public class SnowpackMetadataDB implements Closeable {
	
	/**
	 * Map that keeps all meta data in memory to speed up access
	 */
	private final Cache<String, FlakeMetadata> availableFlakes;
	
	/**
	 * The reference to the database
	 */
	private final DB db;
	
	/**
	 * Defines if caching of values is enabled
	 */
	private final boolean readCachingEnabled;
	
	/**
	 * Default constructor - intialize the database as well.
	 * 
	 * @param baseLocation
	 */
	public SnowpackMetadataDB(File baseLocation, boolean readCachingEnabled, final int maxEntriesInMetadataCache) {
		Options options = new Options();
		options.compressionType(CompressionType.SNAPPY);
		options.createIfMissing(true);
		
		int bufferSize = 10 * 1024 * 1024;
		options.writeBufferSize(bufferSize); // 10 mb
		options.cacheSize(10l * 1024l * 1024l); // 10 mb cache only
		
		File file = new File(baseLocation, SnowpackConstants.SNOWPACK_METADATA_DIRECTORY);
		
		try {
			this.db = new Iq80DBFactory().open(file, options);
		} catch (IOException e) {
			throw new RuntimeException("Unable to open/create database");
		}
		
		this.readCachingEnabled = readCachingEnabled;
		
		if(readCachingEnabled) {
			this.availableFlakes = CacheBuilder.newBuilder()
												.maximumSize(maxEntriesInMetadataCache) // read max 5000 entries
												.build();
		} else {
			this.availableFlakes = null;
		}
	}
	
	/**
	 * Empty all cache right away - this may be needed to reduce the memory pressure
	 * on the Snowpack in trade-off of performance.
	 * 
	 */
	public void emptyCache() {
		if(this.availableFlakes != null) {
			this.availableFlakes.invalidateAll();
		}
	}
	
	/**
	 * Check if there exists metadata for a flake with the given
	 * flake name.
	 * 
	 * @param flakeName
	 * @return
	 */
	public boolean has(String flakeName) {
		if(flakeName == null) {
			return false;
		}
		
		// check in cache
		if(this.readCachingEnabled) {
			if(this.availableFlakes.getIfPresent(flakeName) != null) {
				return true;
			}
		}
		
		// check in db
		if(this.db.get(flakeName.getBytes()) != null) {
			return true;
		}
		
		// no we don't have the flake
		return false;
	}
	
	/**
	 * Read a flake. First we check the memory cache, and then the DB. If the flake
	 * is read from the cache, it is put back into the memory.
	 * 
	 * @param flakeName
	 * @return
	 */
	public FlakeMetadata get(String flakeName) {
		if(flakeName == null) {
			return null;
		}
		
		FlakeMetadata meta;

		// check in cache first
		if(this.readCachingEnabled) {
			meta = this.availableFlakes.getIfPresent(flakeName);
			if(meta != null) {
				return meta;
			}
		}
		
		// check in the db
		byte[] bytes = this.db.get(flakeName.getBytes());
		if(bytes == null) {
			return null;
		}
		
		// deserialize object
		meta = new FlakeMetadata(flakeName, bytes);
		
		// put in cache
		if(this.readCachingEnabled) {
			if(meta != null) {
				this.availableFlakes.put(flakeName, meta);
			}
		}
		
		return meta;
	}

	/**
	 * Save a flake into the database. The entry is also added to the
	 * in-memory cache.
	 * 
	 * @param flakeMetadata
	 */
	public void save(FlakeMetadata flakeMetadata) {
		if(flakeMetadata == null) {
			return;
		}
		
		// serialize the object
		byte[] bytes = flakeMetadata.asBytes();
		this.db.put(flakeMetadata.flakeName.getBytes(), bytes);
		
		// put this in cache
		if(this.readCachingEnabled) {
			this.availableFlakes.put(flakeMetadata.flakeName, flakeMetadata);
		}
	}
	
	/**
	 * Remove the entry from the DB and the cache.
	 * 
	 * @param flakeName
	 */
	public void remove(String flakeName) {
		if(flakeName == null) {
			return;
		}
		
		// delete from DB
		this.db.delete(flakeName.getBytes());
		
		// remove from cache
		if(this.readCachingEnabled) {
			this.availableFlakes.invalidate(flakeName);
		}
	}

	/**
	 * Close the database.
	 * 
	 */
	@Override
	public void close() {
		try {
			this.db.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
