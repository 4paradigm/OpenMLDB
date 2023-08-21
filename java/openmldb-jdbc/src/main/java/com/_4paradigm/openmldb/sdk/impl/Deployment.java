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
import com._4paradigm.openmldb.sdk.ProcedureInfo;

import java.sql.SQLException;

public class Deployment {

    private CodecMetaData inputMetaData;
    private CodecMetaData outputMetaData;
    private ProcedureInfo proInfo;
    private int routerCol = -1;

    public Deployment(com._4paradigm.openmldb.ProcedureInfo info) throws Exception {
        proInfo = Common.convertProcedureInfo(info);
        inputMetaData = new CodecMetaData(Common.convert2ProtoSchema(proInfo.getInputSchema()));
        outputMetaData = new CodecMetaData(Common.convert2ProtoSchema(proInfo.getOutputSchema()));
    }

    public Deployment(ProcedureInfo info) throws Exception {
        proInfo = info;
        inputMetaData = new CodecMetaData(Common.convert2ProtoSchema(proInfo.getInputSchema()));
        outputMetaData = new CodecMetaData(Common.convert2ProtoSchema(proInfo.getOutputSchema()));
    }

    public CodecMetaData getInputMetaData() {
        return inputMetaData;
    }

    public CodecMetaData getOutputMetaData() {
        return outputMetaData;
    }

    public int getRouterCol() {
        return routerCol;
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
}
