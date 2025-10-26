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
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Delete tag procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.delete_tag('tableId', 'tagName')
 *  CALL sys.delete_tag('tableId', 'tagName', 'branchName')
 * </code></pre>
 */
public class DeleteTagProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "delete_tag";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "tag", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "branch", type = @DataTypeHint("STRING"), isOptional = true),
            })
    public String[] call(
            ProcedureContext procedureContext, String branchName, String tableId, String tagNameStr)
            throws Catalog.TableNotExistException {
        FileStoreTable table = (FileStoreTable) table(tableId);
        if (StringUtils.isNotEmpty(branchName)) {
            table = table.switchToBranch(branchName);
        }
        table.deleteTags(tagNameStr);
        return new String[] {"Success"};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
