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
import java.io.IOException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.sangupta.snowpack.domain.Flake;

/**
 * Caching class for snowpack using Google Guava library.
 * 
 * @author sangupta
 *
 */
public class SnowpackCache implements Closeable {

	/**
	 * The global cache that stores all recently accessed flakes amongst all available
	 * flake readers. Only the flakes in the current reader are kept in a different cache.
	 */
	private final Cache<String, Flake> GLOBAL_FLAKE_CACHE;

	/**
	 * Defines if caching is enabled and should be used or not
	 */
	private final boolean cachingEnabled;
	
	/**
	 * Constructor that creates a new caching instance for all chunk
	 * readers.
	 * 
	 * @param cachingEnabled
	 */
	public SnowpackCache(boolean cachingEnabled, final int maxEntriesInReadCache) {
		this.cachingEnabled = cachingEnabled;
		
		if(cachingEnabled) {
			GLOBAL_FLAKE_CACHE = CacheBuilder.newBuilder()
											.maximumSize(maxEntriesInReadCache)  // keep only last 10000 entries in cache
											.build();
		} else {
			GLOBAL_FLAKE_CACHE = null;
		}
	}
	
	/**
	 * Clear the entire global cache to reduce the memory pressure
	 * on Snowpack.
	 */
	public void emptyCache() {
		if(GLOBAL_FLAKE_CACHE != null) {
			GLOBAL_FLAKE_CACHE.invalidateAll();
		}
	}
	
	/**
	 * Check if a flake is contained in the cache or not.
	 * 
	 * @param flakeName
	 * @return
	 */
	public boolean containsKey(String flakeName) {
		if(!this.cachingEnabled) {
			return false;
		}
		
		Flake flake = GLOBAL_FLAKE_CACHE.getIfPresent(flakeName);
		return flake != null;
	}

	/**
	 * Get the flake with the name.
	 * 
	 * @param flakeName
	 * @return
	 */
	public Flake get(String flakeName) {
		return GLOBAL_FLAKE_CACHE.getIfPresent(flakeName);
	}

	/**
	 * Put the flake in the cache.
	 * 
	 * @param flake
	 */
	public void put(Flake flake) {
		if(!this.cachingEnabled) {
			return;
		}
		
		if(flake == null) {
			return;
		}
		
		GLOBAL_FLAKE_CACHE.put(flake.flakeName, flake);
	}

	/**
	 * Remove the flake from the cache.
	 * 
	 * @param flakeName
	 */
	public void remove(String flakeName) {
		if(!this.cachingEnabled) {
			return;
		}
		
		Flake flake = this.get(flakeName);
		if(flake == null) {
			return;
		}
		
		GLOBAL_FLAKE_CACHE.invalidate(flake);
	}

	/**
	 * Close the cache and invalidate everything.
	 * 
	 */
	@Override
	public void close() throws IOException {
		if(GLOBAL_FLAKE_CACHE != null) {
			GLOBAL_FLAKE_CACHE.invalidateAll();
		}
	}
}	
