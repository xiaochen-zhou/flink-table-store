/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.utils.TimeUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.time.Duration;

/** A base procedure to create or replace a tag. */
public abstract class CreateOrReplaceTagBaseProcedure extends ProcedureBase {

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "tag", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "snapshot_id",
                        type = @DataTypeHint("BIGINT"),
                        isOptional = true),
                @ArgumentHint(
                        name = "time_retained",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(name = "branch", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String tagName,
            @Nullable Long snapshotId,
            @Nullable String timeRetained,
            String branchName)
            throws Catalog.TableNotExistException {
        FileStoreTable fileStoreTable = (FileStoreTable) table(tableId);
        if (StringUtils.isNotEmpty(branchName)) {
            fileStoreTable = fileStoreTable.switchToBranch(branchName);
        }
        createOrReplaceTag(fileStoreTable, tagName, snapshotId, toDuration(timeRetained));
        return new String[] {"Success"};
    }

    abstract void createOrReplaceTag(
            Table table, String tagName, Long snapshotId, Duration timeRetained);

    @Nullable
    private static Duration toDuration(@Nullable String s) {
        if (s == null) {
            return null;
        }

        return TimeUtils.parseDuration(s);
    }
}
