/*
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

package com._4paradigm.openmldb.sdk.impl;

import com._4paradigm.openmldb.common.codec.CodecMetaData;
import com._4paradigm.openmldb.sdk.Common;
import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.ProcedureInfo;

public class Deployment {

    private CodecMetaData inputMetaData;
    private CodecMetaData outputMetaData;
    private ProcedureInfo proInfo;

    public Deployment(com._4paradigm.openmldb.proto.SQLProcedure.ProcedureInfo info) throws Exception {
        proInfo = Common.convertProcedureInfo(info);
        inputMetaData = new CodecMetaData(info.getInputSchemaList());
        outputMetaData = new CodecMetaData(info.getOutputSchemaList());
    }

    public CodecMetaData getInputMetaData() {
        return inputMetaData;
    }

    public CodecMetaData getOutputMetaData() {
        return outputMetaData;
    }

    public int getRouterCol() {
        return proInfo.getRouterCol();
    }

    public String getDatabase() {
        return proInfo.getDbName();
    }

    public String getName() {
        return proInfo.getProName();
    }

    public String getSQL() {
        return proInfo.getSql();
    }

    public Schema getInputSchema() {
        return proInfo.getInputSchema();
    }

    public Schema getOutputSchema() {
        return proInfo.getOutputSchema();
    }
}
