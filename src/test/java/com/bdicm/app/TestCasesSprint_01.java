package com.bdicm.app;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveProperties;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import com.klarna.hiverunner.StandaloneHiveRunner;

import org.apache.commons.collections.MapUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(StandaloneHiveRunner.class)
public class TestCasesSprint_01 {


    private static final Charset ENCODING = StandardCharsets.UTF_8;
    public List<File> queryFiles = getFiles("/home/usuario/Desktop/queries");
    public List<File> resultFiles = getFiles("/home/usuario/Desktop/results");

	@HiveProperties
    public Map<String, String> hiveProperties = MapUtils.putAll(new HashMap(), new Object[]{
            "MY.HDFS.DIR", "${hadoop.tmp.dir}",
            "my.schema", "bar",
    });

    @HiveSetupScript
    private String createSchemaScript = "create schema ${hiveconf:my.schema}";

    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/foo/data_from_string.csv")
    private String dataFromString = "2,World\n3,!";

    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/clients/clients_table.csv")
    private File dataFromFile =
            new File(ClassLoader.getSystemResource("testCasesResources/sales_table.txt").getPath());

    @HiveSQL(files = {
            //"helloHiveRunner/create_table.sql",
            //"helloHiveRunner/create_ctas.sql",*/
            "testCasesResources/create_clients.hql",
            "testCasesResources/create_transactions.hql",
            "testCasesResources/create_categories.hql",
            "testCasesResources/create_ccards.hql",
            "testCasesResources/create_locations.hql",
            "testCasesResources/create_products.hql",
            "testCasesResources/create_sales.hql"}, encoding = "UTF-8")
    private HiveShell hiveShell;

    //Metodos auxiliares para leitura de arquivos
    List<File> getFiles(String folderName)
    {
    	final File folder = new File(folderName);
    	List<File> files = new ArrayList();
    	
    	for (final File fileEntry : folder.listFiles())
            if (!fileEntry.isDirectory())
                files.add(fileEntry);
    	return files;
    }
    
    List<String> readTextFile(File aFileName) throws IOException {
        Path path = Paths.get(aFileName.getPath());
        return Files.readAllLines(path, ENCODING);
    }
    //FIm do trecho para metodos auxiliares
    
    @Test
    public void testTablesCreated() {
        List<String> expected = Arrays.asList("transactions","categories","ccards","clients","locations","products","sales");//,"transactions","categories","ccards","locations","products","sales");//,"foo", "foo_prim");
        List<String> actual = hiveShell.executeQuery("show tables");

        Collections.sort(expected);
        Collections.sort(actual);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testQuery_01() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(0));
    	List<String> result = readTextFile(resultFiles.get(0));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_02() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(1));
    	List<String> result = readTextFile(resultFiles.get(1));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    //@Test
    public void testQuery_03() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(2));
    	List<String> result = readTextFile(resultFiles.get(2));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    //@Test
    public void testQuery_04() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(3));
    	List<String> result = readTextFile(resultFiles.get(3));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    //@Test
    public void testQuery_05() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(4));
    	List<String> result = readTextFile(resultFiles.get(4));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    //@Test
    public void testQuery_06() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(5));
    	List<String> result = readTextFile(resultFiles.get(5));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    //@Test
    public void testQuery_07() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(6));
    	List<String> result = readTextFile(resultFiles.get(6));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    //@Test
    public void testQuery_08() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(7));
    	List<String> result = readTextFile(resultFiles.get(7));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }

}
