/*
 * Copyright 2022 paxos.cn.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.paxos.mysql;

import cn.paxos.mysql.engine.QueryResultColumn;

import java.util.List;

/**
 * Response writer.
 *
 * A normal result must call a writeColumns() before all invoking of writeRow() and call finish() finally
 */
public interface ResultSetWriter {

    /**
     * Write columns.
     * It must be called before any invoking of writeRow()
     *
     * @param columns columns
     */
    void writeColumns(List<QueryResultColumn> columns);

    /**
     * Write a row.
     * finish() must be called after writing all rows
     *
     * @param row row of cells ordered by columns
     */
    void writeRow(List<String> row);

    /**
     * Finish the response
     */
    void finish();
}
