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

package com.sangupta.snowpack.domain;

import com.sangupta.snowpack.Snowpack;

/**
 * The configuration object when creating a {@link Snowpack} instance. Defines
 * various parameters to tune the performance of the system.
 * 
 * @author sangupta
 *
 */
public class SnowpackConfig {

	public long preAllocationChunkSize = 10 * 1024l * 1024l; // max chunk size if 10 MB
	
	public long maxFileSize = 10 * 1024l * 1024l; // max file size of 10 MB
	
	public long averageExpectedSize = 20 * 1024l; // 20 kb
	
	public boolean readCachingEnabled = true;
	
	public boolean writeCachingEnabled = true;
	
	public int maxFlakesInChunk = 0; // set this value to zero - to store according to the size of the chunk
	
	public int maxEntriesInReadCache = 1000; // the more number of items you cache - the more memory you will need
	
	public int maxEntriesInMetadataCache = 1000; // maximum number of flake metadata entries to cache
	
}
