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
	private static final int listSize = 20;
    public List<File> queryFiles = getFiles("/home/usuario/Desktop/queries");
    public List<File> resultFiles = getFiles("/home/usuario/Desktop/results");

	@HiveProperties
    public Map<String, String> hiveProperties = MapUtils.putAll(new HashMap(), new Object[]{
            "MY.HDFS.DIR", "${hadoop.tmp.dir}",
            "my.schema", "bar",
    });

    @HiveSetupScript
    private String createSchemaScript = "create schema ${hiveconf:my.schema}";

    //@HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/foo/data_from_string.csv")
    //private String dataFromString = "2,World\n3,!";

    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/cards/cards_table.csv")
    private File dataFromFile1 =
            new File(ClassLoader.getSystemResource("tables/cards_table.txt").getPath());
    
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/categories/categories_table.csv")
    private File dataFromFile2 =
            new File(ClassLoader.getSystemResource("tables/categories_table.txt").getPath());
    
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/clients/clients_table.csv")
    private File dataFromFile4 =
            new File(ClassLoader.getSystemResource("tables/clients_table.txt").getPath());
    
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/locations/locations_table.csv")
    private File dataFromFile5 =
            new File(ClassLoader.getSystemResource("tables/locations_table.txt").getPath());
    
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/products/products_table.csv")
    private File dataFromFile6 =
            new File(ClassLoader.getSystemResource("tables/products_table.txt").getPath());
    
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/sales/sales_table.csv")
    private File dataFromFile7 =
            new File(ClassLoader.getSystemResource("tables/sales_table.txt").getPath());
    
    @HiveResource(targetFile = "${hiveconf:MY.HDFS.DIR}/transactions/transactions_table.csv")
    private File dataFromFile8 =
            new File(ClassLoader.getSystemResource("tables/transactions_table.txt").getPath());

    @HiveSQL(files = {
            "creates/create_clients.hql",
            "creates/create_transactions.hql",
            "creates/create_categories.hql",
            "creates/create_ccards.hql",
            "creates/create_locations.hql",
            "creates/create_products.hql",
            "creates/create_sales.hql"}, encoding = "UTF-8")
    
    private HiveShell hiveShell;

    //Metodos auxiliares para leitura de arquivos
    List<File> getFiles(String folderName)
    {
    	final File folder = new File(folderName);
    	List<File> files = new ArrayList();
    	
    	for (final File fileEntry : folder.listFiles())
            if (!fileEntry.isDirectory())
                files.add(fileEntry);
    	Collections.sort(files);
    	return files;
    }
    
    List<String> readTextFile(File aFileName) throws IOException {
        Path path = Paths.get(aFileName.getPath());
        return Files.readAllLines(path, ENCODING);
    }
    
    List<String> trimList(List<String> listToTrim)
    {
    	if(!(listToTrim.size()<=listSize))
    		listToTrim = listToTrim.subList(0, listSize-1);
		return listToTrim;
    }
    
    List<String> getExpected(int indexFile) throws IOException
    {
    	List<String> result = readTextFile(resultFiles.get(indexFile));
    	Collections.sort(result);
    	result = trimList(result);
    	return result;
    }
    
    List<String> getActual(int indexFile) throws IOException
    {
    	List<String> query = readTextFile(queryFiles.get(indexFile));
    	List<String> actual = hiveShell.executeQuery(query.get(0));
    	Collections.sort(actual);
    	actual = trimList(actual);
    	return actual;
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
        Assert.assertEquals(getExpected(0), getActual(0));
    }
    
    //@Test
    public void testQuery_00() throws IOException {
    	List<String> result = readTextFile(resultFiles.get(1));
    	List<String> expected = result.subList(0, listSize);//Arrays.asList(result.get(0));
		Collections.sort(expected);
    	//List<String> query = readTextFile(queryFiles.get(1));
    	List<String> actual = hiveShell.executeQuery("select * from transactions");
    	//System.out.println(actual);
    	Assert.assertEquals(1,1);
    }
    
    @Test
    public void testQuery_02() throws IOException {
    	List<String> expected = getExpected(1);
    	List<String> actual = getActual(1);
    	Assert.assertEquals(getExpected(1), getActual(1));
    }
    
    @Test
    public void testQuery_03() throws IOException {
    	Assert.assertEquals(getExpected(2), getActual(2));
    }
    
    @Test
    public void testQuery_04() throws IOException {
    	Assert.assertEquals(getExpected(3), getActual(3));
    }
    
    @Test
    public void testQuery_05() throws IOException {
    	Assert.assertEquals(getExpected(4), getActual(4));
    }
    
    @Test
    public void testQuery_06() throws IOException {
    	Assert.assertEquals(getExpected(5), getActual(5));
    }
    
    @Test
    public void testQuery_07() throws IOException {
    	Assert.assertEquals(getExpected(6), getActual(6));
    }
    
    @Test
    public void testQuery_08() throws IOException {
    	Assert.assertEquals(getExpected(7), getActual(7));
    }
    
    @Test
    public void testQuery_09() throws IOException {
    	Assert.assertEquals(getExpected(8), getActual(8));
    }
    
    /*@Test
    public void testQuery_10() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(9));
    	List<String> result = readTextFile(resultFiles.get(9));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_11() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(10));
    	List<String> result = readTextFile(resultFiles.get(10));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_12() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(11));
    	List<String> result = readTextFile(resultFiles.get(11));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_13() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(12));
    	List<String> result = readTextFile(resultFiles.get(12));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_14() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(13));
    	List<String> result = readTextFile(resultFiles.get(13));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_15() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(14));
    	List<String> result = readTextFile(resultFiles.get(14));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_16() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(15));
    	List<String> result = readTextFile(resultFiles.get(15));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }
    
    @Test
    public void testQuery_17() throws IOException {
    	List<String> query = readTextFile(queryFiles.get(16));
    	List<String> result = readTextFile(resultFiles.get(16));
    	List<String> expected = Arrays.asList(result.get(0));
        List<String> actual = hiveShell.executeQuery(query.get(0));
        Assert.assertEquals(expected, actual);
    }*/

}
