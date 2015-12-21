package com.mobivans.hive-jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hive.jdbc.HiveConnection;

public class HiveUtils {
	// hive uri
	static String hiveServer = "jdbc:hive2://mobi3:10000/hivans";
	/**
	 * connect hiveserver
	 * @return
	 */
	private HiveConnection connectHiveServer2(){
		// hive-jdbc 
		HiveConnection conn = null;
		//the properties of hive-jdbc connection：user/pswd..etc
		Properties hiveProp = new Properties();
		hiveProp.setProperty("user", "root");
		try {
			conn = new HiveConnection(hiveServer, hiveProp);
		} catch (SQLException e) {
			System.out.println("Failed to connect to hive server! Info: "+e);
		}
		return conn;
	}
	
	/**
	 * close resources
	 * @param conn
	 */
	private void disconnect(ResultSet rs, Statement stmt, HiveConnection conn){
		if(rs != null){
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if(stmt != null){
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if(conn != null){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * create a partitioned table
	 * @param tableName
	 * @param columns
	 * @param partitions
	 */
	public void createPartitionedTable(String tableName, Map<String, Object> columns, List<Object> partitions){
		// Connection
		HiveConnection conn = connectHiveServer2();
		// Statement
		Statement stmt = null;
		try {
			//
			stmt = conn.createStatement();
			// hql
			String _prefix = "CREATE TABLE "+tableName+"(";
			String _suffix = ") PARTITIONED BY(";
			String parts = "";
			for (Object part : partitions) {
				parts += part+" string, ";
			}
			String cols = "";
			cols = spliceKeyValue(columns, " ");
			String _end = ")";
			String _format = " ROW FORMAT DELIMITED FIELDS TERMINATED BY" + " '|'";
			String _collec = " COLLECTION ITEMS TERMINATED BY" + " '/'";
			String _mapKey = " MAP KEYS TERMINATED BY" + " '='";
			// splice hql
			String sql = _prefix + cols.substring(0, cols.lastIndexOf(", ")) + _suffix + parts.substring(0, parts.lastIndexOf(", ")) + _end
					+ _format + _collec + _mapKey;
			System.out.println(sql);
			
			stmt.execute(sql);
			System.out.println("Create table " + tableName + " successfully!");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * add a partition to a specified table
	 * @param tableName
	 * @param partition
	 */
	public void addPartition(String tableName, Map<String, Object> partition){
		// Connection
		HiveConnection conn = connectHiveServer2();
		// Statement
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String sql = "ALTER TABLE " + tableName + " ADD PARTITION(" + spliceKeyValue(partition, "=") + ")";
			stmt.execute(sql );
			System.out.println("ADD PARTITIONS SUCCESSFULLY! ");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			disconnect(null, stmt, conn);
		}
	}
	
	/**
	 * load data into a partitioned table specified
	 * @param tableName
	 * @param isLocal
	 * @param filePath
	 * @param partitions
	 */
	public void loadDataIntoTable(String tableName, boolean isLocal, String filePath, Map<String, Object> partitions){
		// Connection
		HiveConnection conn = connectHiveServer2();
		// Statement
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String local = isLocal ? " LOCAL" : "";
			String sql = "LOAD DATA" + local + " INPATH '" + filePath + "'" + " INTO TABLE " + tableName +" PARTITION(" + spliceKeyValue(partitions, "=") + ")";
			System.out.println(sql);
			stmt.execute(sql);
			System.out.println("Load file " + filePath + "successfully!");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			disconnect(null, stmt, conn);
		}
	}
	
	/**
	 * Execute specified sql with a list of variables
	 * @param sql 
	 * @param params 
	 */
	public List<Map<String, Object>> executeQuery(String sql, List<Object> params){
		// Connection
		HiveConnection conn = connectHiveServer2();
		// Statement
		PreparedStatement prepStmt = null;
		// Resultset
		ResultSet resultSet = null;
		// list of rows metched
		List<Map<String, Object>> rows = new ArrayList<>();
		try {
			// preload sql
			prepStmt = conn.prepareStatement(sql);
			if(null != params && !params.isEmpty()){
				int i = 1;
				for (Object param : params) {
					prepStmt.setObject(i++, param);
				}
			}
			// execute sql
			resultSet = prepStmt.executeQuery();
			// ResultSetMetaData
			ResultSetMetaData metaData = resultSet.getMetaData();
			// iterate the ResultSet for each row
			while(resultSet.next()){
				// row 
				Map<String, Object> row = new HashMap<>();
				// iterate the row's whole columns, then put them in a map
				for(int i=1; i<=metaData.getColumnCount(); i++){
					row.put(metaData.getColumnName(i), resultSet.getObject(metaData.getColumnLabel(i)));
				}
				// add row to list
				rows.add(row);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			//close the connection
			disconnect(resultSet, prepStmt, conn);
		}
		return rows;
	}
	
	/**
	 * splice the condition of hql
	 * @param partitions
	 * @param split 
	 * @return
	 */
	private static String spliceKeyValue(Map<String, Object> partitions, String split){
		Iterator<String> it = partitions.keySet().iterator();
		String cons = "";
		while(it.hasNext()){
			String key = it.next();
			String val = "'"+String.valueOf(partitions.get(key))+"'";
			cons += key + split + val + ", ";
		}
		return cons.substring(0, cons.lastIndexOf(", "));
	}
	
	public static void main(String[] args) {
		HiveUtils hu = new HiveUtils();
		Map<String, Object> columns = new LinkedHashMap<>();
		columns.put("opt", "array<string>");
		columns.put("params", "map<string, string>");
		columns.put("datetime", "string");
		columns.put("ip", "string");
		columns.put("d_ver", "string");
		columns.put("col5", "string");
		columns.put("ad", "string");
		columns.put("httpcode", "string");
		columns.put("url", "string");
		columns.put("brsr", "string");
		columns.put("col11", "string");
		columns.put("col12", "string");
		columns.put("", "");
		List<Object> partits = new ArrayList<>();
		partits.add("dt");
		//create partition table
//		hu.createTable("jt", columns, partits);
		
		Map<String, Object> partition = new HashMap<>();
		partition.put("dt", "20150605");
		//add partition
//		hu.addPartition("jt", partition);
		
		//load data
//		hu.loadDataIntoTable("jt", false, "/hivetmp/20150605/201506052230_123.56.155.33.log", partition);
		
		//query data from table
		String sql = "select * from jt where dt = 20150605";
		List<Map<String, Object>> list = hu.executeQuery(sql, null);
		System.out.println(list.get(0));
	}
	
}
