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

package org.apache.paimon.catalog;

import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileSystemCatalog}. */
public class FileSystemCatalogTest extends CatalogTestBase {

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        catalog =
                new FileSystemCatalog(
                        fileIO, new Path(warehouse), CatalogContext.create(new Options()));
    }

    @Test
    public void testCreateTableCaseSensitive() throws Exception {
        catalog.createDatabase("test_db", false);
        Identifier identifier = Identifier.create("test_db", "new_TABLE");
        Schema schema =
                Schema.newBuilder()
                        .column("Pk1", DataTypes.INT())
                        .column("pk2", DataTypes.STRING())
                        .column("pk3", DataTypes.STRING())
                        .column(
                                "Col1",
                                DataTypes.ROW(
                                        DataTypes.STRING(),
                                        DataTypes.BIGINT(),
                                        DataTypes.TIMESTAMP(),
                                        DataTypes.ARRAY(DataTypes.STRING())))
                        .column("col2", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
                        .column("col3", DataTypes.ARRAY(DataTypes.ROW(DataTypes.STRING())))
                        .partitionKeys("Pk1", "pk2")
                        .primaryKey("Pk1", "pk2", "pk3")
                        .build();
        catalog.createTable(identifier, schema, false);
    }

    @Test
    public void testAlterDatabase() throws Exception {
        String databaseName = "test_alter_db";
        catalog.createDatabase(databaseName, false);
        assertThatThrownBy(
                        () ->
                                catalog.alterDatabase(
                                        databaseName,
                                        Lists.newArrayList(PropertyChange.removeProperty("a")),
                                        false))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testListDatabasesWithFilter() throws Exception {

        String[] testDatabases = {
            "test_db_1", "test_db_2", "prod_db_1", "prod_db_2", "dev_database"
        };
        for (String dbName : testDatabases) {
            catalog.dropDatabase(dbName, true, true);
        }
        for (String dbName : testDatabases) {
            catalog.createDatabase(dbName, false);
        }

        try {
            List<String> testDbs = catalog.listDatabases(name -> name.startsWith("test_"));
            assertThat(testDbs).containsExactlyInAnyOrder("test_db_1", "test_db_2");

            List<String> prodDbs = catalog.listDatabases(name -> name.startsWith("prod_"));
            assertThat(prodDbs).containsExactlyInAnyOrder("prod_db_1", "prod_db_2");

            List<String> dbsWithDatabase = catalog.listDatabases(name -> name.contains("database"));
            assertThat(dbsWithDatabase).containsExactly("dev_database");

            List<String> noDbs = catalog.listDatabases(name -> name.startsWith("nonexistent_"));
            assertThat(noDbs).isEmpty();

            List<String> allDbs = catalog.listDatabases(name -> true);
            assertThat(allDbs)
                    .contains("test_db_1", "test_db_2", "prod_db_1", "prod_db_2", "dev_database");

        } finally {
            for (String dbName : testDatabases) {
                catalog.dropDatabase(dbName, true, true);
            }
        }
    }

    @Test
    public void testListTablesWithFilter() throws Exception {
        String databaseName = "testListTablesWithFilter";
        catalog.dropDatabase(databaseName, true, true);
        catalog.createDatabase(databaseName, false);

        try {
            String[] tableNames = {
                "user_table",
                "user_profile",
                "user_settings",
                "order_table",
                "order_items",
                "order_history",
                "product_catalog",
                "product_reviews",
                "temp_table_1",
                "temp_table_2"
            };

            for (String tableName : tableNames) {
                catalog.createTable(
                        Identifier.create(databaseName, tableName),
                        Schema.newBuilder().column("id", DataTypes.INT()).build(),
                        false);
            }
            List<String> userTables =
                    catalog.listTables(databaseName, name -> name.startsWith("user_"));
            assertThat(userTables)
                    .containsExactlyInAnyOrder("user_table", "user_profile", "user_settings");

            List<String> orderTables =
                    catalog.listTables(databaseName, name -> name.startsWith("order_"));
            assertThat(orderTables)
                    .containsExactlyInAnyOrder("order_table", "order_items", "order_history");

            List<String> productTables =
                    catalog.listTables(databaseName, name -> name.contains("product"));
            assertThat(productTables)
                    .containsExactlyInAnyOrder("product_catalog", "product_reviews");

            List<String> tempTables =
                    catalog.listTables(databaseName, name -> name.startsWith("temp_"));
            assertThat(tempTables).containsExactlyInAnyOrder("temp_table_1", "temp_table_2");

            List<String> noTables =
                    catalog.listTables(databaseName, name -> name.startsWith("nonexistent_"));
            assertThat(noTables).isEmpty();

            List<String> allTables = catalog.listTables(databaseName, name -> true);
            assertThat(allTables).containsExactlyInAnyOrder(tableNames);

        } finally {
            catalog.dropDatabase(databaseName, true, true);
        }
    }
}
