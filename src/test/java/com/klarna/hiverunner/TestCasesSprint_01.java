/*
 * Copyright 2013 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.klarna.hiverunner;

import com.klarna.hiverunner.annotations.HiveProperties;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import org.apache.commons.collections.MapUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hive Runner Reference implementation.
 * <p/>
 * All HiveRunner tests should run with the StandaloneHiveRunner
 */
@RunWith(StandaloneHiveRunner.class)
public class TestCasesSprint_01 {


    /**
     * Cater for all the parameters in the script that we want to test.
     * Note that the "hadoop.tmp.dir" is one of the dirs defined by the test harness
     */
    @HiveProperties
    public Map<String, String> hiveProperties = MapUtils.putAll(new HashMap(), new Object[]{
            "MY.HDFS.DIR", "${hadoop.tmp.dir}",
            "my.schema", "bar",
    });

    /**
     * In this example, the scripts under test expects a schema to be already present in hive so
     * we do that with a setup script.
     * <p/>
     * There may be multiple setup scripts but the order of execution is undefined.
     */
    @HiveSetupScript
    private String createSchemaScript = "create schema ${hiveconf:my.schema}";

    /**
     * Create some data in the target directory. Note that the 'targetFile' references the
     * same dir as the create table statement in the script under test.
     * <p/>
     * This example is for defining the data in line as a string.
     */
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/foo/data_from_string.csv")
    private String dataFromString = "2,World\n3,!";

    /**
     * Create some data in the target directory. Note that the 'targetFile' references the
     * same dir as the create table statement in the script under test.
     * <p/>
     * This example is for defining the data in in a resource file.
     */
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/clients/clients_table.csv")
    private File dataFromFile =
            new File(ClassLoader.getSystemResource("testCasesSprint_01/tables/clients_table.txt").getPath());

    /**
     * Define the script files under test. The files will be loaded in the given order.
     * <p/>
     * The HiveRunner instantiate and inject the HiveShell
     */
    @HiveSQL(files = {
            //"helloHiveRunner/create_table.sql",
            //"helloHiveRunner/create_ctas.sql",
            "testCasesSprint_01/creates/create_clients.hql",
            "testCasesSprint_01/creates/create_transactions.hql",
            "testCasesSprint_01/creates/create_categories.hql",
            "testCasesSprint_01/creates/create_ccards.hql",
            "testCasesSprint_01/creates/create_locations.hql",
            "testCasesSprint_01/creates/create_products.hql",
            "testCasesSprint_01/creates/create_sales.hql"},encoding = "UTF-8")
    private HiveShell hiveShell;


    @Test
    public void testTablesCreated() {
        List<String> expected = Arrays.asList("clients","transactions","categories","ccards","locations","products","sales");//,"foo", "foo_prim");
        List<String> actual = hiveShell.executeQuery("show tables");

        Collections.sort(expected);
        Collections.sort(actual);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testQuery_01() {
        List<String> expected = Arrays.asList("1");
        List<String> actual = hiveShell.executeQuery("SELECT trans_cli_id, COUNT(*) as cnt FROM transactions GROUP BY cli_id ORDER BY cnt DESC;");
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_02() {
        List<String> expected = Arrays.asList("1");
        List<String> actual = hiveShell.executeQuery("SELECT trans_cli_id,trans_value FROM transactions ORDER BY trans_value DESC; ");
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_03() {
        List<String> expected = Arrays.asList("1");
        List<String> actual = hiveShell.executeQuery("SELECT clients.cli_firstname,clients.cli_lastname,transactions.* FROM transactions JOIN clients ON (trans_cli_id = cli_id);");
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_04() {
        List<String> expected = Arrays.asList("Dominique Atkinson","2010­09­11 19:20:20 29.998 80 "); 
/*Berk Gill
2012­07­22 00:33:33 29.992 68 
Signe Townsend
2013­07­31 00:16:16 29.99 45 
Galena
Levy 2013­06­04 16:08:08 29.984 33 
Warren
Alston 2012­11­13 05:30:30 29.983 67 ");*/
        List<String> actual = hiveShell.executeQuery("SELECT trans_cli_id,trans_value FROM transactions ORDER BY trans_value DESC limit 1; ");
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_05() {
        List<String> expected = Arrays.asList("1");
        List<String> actual = hiveShell.executeQuery("SELECT clients.cli_firstname,clients.cli_lastname,transactions.trans_date,transactions.trans_value FROM transactions JOIN clients ON (trans_cli_id = cli_id) ORDER BY clients.cli_firstname;");
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_06() {
        List<String> expected = Arrays.asList("1");
        List<String> actual = hiveShell.executeQuery("SELECT clients.cli_firstname,clients.cli_lastname,transactions.trans_date,transactions.trans_value,transactions.trans_loc_id FROM transactions JOIN clients ON (trans_cli_id = cli_id) ORDER BY trans_date DESC LIMIT 20;");
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_07() {
        List<String> expected = Arrays.asList("Graiden","Armstrong","2014­07­12 02:00:00","7.112","26","Ahmedabad","GJ","Cocos","(Keeling) Islands");
        System.out.println(expected);
        List<String> actual = hiveShell.executeQuery("SELECT clients.cli_firstname, clients.cli_lastname, transactions.trans_date, transactions.trans_value,transactions.trans_loc_id, locations.loc_city, locations.loc_region, locations.loc_country FROM transactions JOIN clients ON (trans_cli_id = cli_id) JOIN locations ON (trans_loc_id = loc_id) ORDER BY locations.loc_city LIMIT 1;");
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_08() {
        List<String> expected = Arrays.asList("1");
        List<String> actual = hiveShell.executeQuery("SELECT trans_cli_id, SUM(trans_value) as trans_total FROM transactions GROUP BY trans_cli_id ORDER BY trans_total DESC LIMIT 5; ");
        Assert.assertEquals(expected, actual);
    }

}
