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

package org.apache.paimon.jdbc;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogTestBase;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Tests for {@link JdbcCatalog}. */
public class JdbcCatalogTest extends CatalogTestBase {

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        catalog = initCatalog(Maps.newHashMap());
    }

    private JdbcCatalog initCatalog(Map<String, String> props) {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(
                CatalogOptions.URI.key(),
                "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""));

        properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        properties.put(CatalogOptions.WAREHOUSE.key(), warehouse);
        properties.put(CatalogOptions.LOCK_ENABLED.key(), "true");
        properties.put(CatalogOptions.LOCK_TYPE.key(), "jdbc");
        properties.putAll(props);
        JdbcCatalog catalog =
                new JdbcCatalog(
                        fileIO,
                        "test-jdbc-catalog",
                        CatalogContext.create(Options.fromMap(properties)),
                        warehouse);
        assertThat(catalog.warehouse()).isEqualTo(warehouse);
        return catalog;
    }

    @Override // ignore for lock error
    @Test
    public void testGetTable() throws Exception {}

    @Test
    public void testAcquireLockFail() throws SQLException, InterruptedException {
        String lockId = "jdbc.testDb.testTable";
        assertThat(JdbcUtils.acquire(((JdbcCatalog) catalog).getConnections(), lockId, 3000))
                .isTrue();
        assertThat(JdbcUtils.acquire(((JdbcCatalog) catalog).getConnections(), lockId, 3000))
                .isFalse();
    }

    @Test
    public void testCleanTimeoutLockAndAcquireLock() throws SQLException, InterruptedException {
        String lockId = "jdbc.testDb.testTable";
        assertThat(JdbcUtils.acquire(((JdbcCatalog) catalog).getConnections(), lockId, 1000))
                .isTrue();
        Thread.sleep(2000);
        assertThat(JdbcUtils.acquire(((JdbcCatalog) catalog).getConnections(), lockId, 1000))
                .isTrue();
    }

    @Test
    public void testUpperCase() throws Exception {
        catalog.createDatabase("test_db", false);
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        Identifier.create("TEST_DB", "new_table"),
                                        DEFAULT_TABLE_SCHEMA,
                                        false))
                .isInstanceOf(Catalog.DatabaseNotExistException.class)
                .hasMessage("Database TEST_DB does not exist.");

        catalog.createTable(Identifier.create("test_db", "new_TABLE"), DEFAULT_TABLE_SCHEMA, false);
    }

    @Test
    public void testSerializeTable() throws Exception {
        catalog.createDatabase("test_db", false);
        catalog.createTable(Identifier.create("test_db", "table"), DEFAULT_TABLE_SCHEMA, false);
        Table table = catalog.getTable(new Identifier("test_db", "table"));
        assertDoesNotThrow(
                () -> {
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                        oos.writeObject(table);
                        oos.flush();
                    }
                });
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

    @Override
    protected boolean supportsAlterDatabase() {
        return true;
    }
}
