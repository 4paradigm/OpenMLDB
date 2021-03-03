/*
 * java/fesql-flink/src/main/java/com/_4paradigm/fesql/flink/common/ParquetHelper.java
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.fesql.flink.common;

import org.apache.flink.formats.parquet.ParquetRowInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class ParquetHelper {

	public static DataStream<Row> ParquetHelper(final StreamExecutionEnvironment env, String source) {
		MessageType schema = readParquetSchema(source);
		ParquetRowInputFormat parquetRowInputFormat = new ParquetRowInputFormat(new org.apache.flink.core.fs.Path(source), schema);
		return env.readFile(parquetRowInputFormat, source, FileProcessingMode.PROCESS_ONCE, 10);
	}

	public static MessageType readParquetSchema(String path) {
		Configuration _default = new Configuration();
		List<LocatedFileStatus> files = listFiles(_default, path, null);
		if (files.isEmpty()) {
			throw new RuntimeException(String.format("Empty path [%s] ", path));
		}
		Path headPath = files.get(0).getPath();
		try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(headPath, _default))) {
			return r.getFooter().getFileMetaData().getSchema();
		} catch (IOException e) {
			throw new RuntimeException(String.format("Error read Parquet file schema, path [%s].", headPath), e);
		}
	}

	public static List<LocatedFileStatus> listFiles(Configuration conf, String path, String ends) {
		List<LocatedFileStatus> files = new ArrayList<>();
		try {
			FileSystem fs = FileSystem.get(new URI(path), conf);
			RemoteIterator<LocatedFileStatus> fileStatus = fs.listFiles(new Path(path), true);
			if (fileStatus != null) {
				while (fileStatus.hasNext()) {
					LocatedFileStatus cur = fileStatus.next();
					if (cur.isFile()) {
						String fileName = cur.getPath().getName();
						if (fileName.startsWith(".") || fileName.startsWith("_")) {
							continue;
						}
						if (ends == null || fileName.endsWith(ends)) {
							files.add(cur);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return files;
	}
}
