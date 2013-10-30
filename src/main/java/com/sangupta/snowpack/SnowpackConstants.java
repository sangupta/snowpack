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

/**
 * 
 * @author sangupta
 *
 */
public interface SnowpackConstants {

	/**
	 * The prefix for all data files
	 */
	public static final String SNOWFLAKE_FILENAME_PREFIX = "snowpack-";
	
	/**
	 * The suffix and extension for all data files
	 */
	public static final String SNOWFLAKE_FILENAME_SUFFIX = ".dat";
	
	/**
	 * The suffix and extension for all flake data
	 */
	public static final String SNOWFLAKE_FLAKE_FILENAME_SUFFIX = ".flake.dat";
	
	/**
	 * Holds the information on all chunks
	 */
	public static final String SNOWPACK_META_FILENAME = "snowpack.chunks.dat";
	
	/**
	 * The filename to store the information about the snowpack
	 */
	public static final String SNOWPACK_INFO_FILENAME = "snowpack.info";

	public static final String SNOWPACK_METADATA_DIRECTORY = "metadata";

}
