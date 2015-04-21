// package team14;

import java.util.Hashtable;
import java.util.Iterator;


import java.util.Properties;
import jdbm.RecordManager;
import jdbm.btree.BTree;
import jdbm.RecordManagerFactory;
import java.io.*;

public class DBApp {
	
	private String name;
	private String column;
	private BTree table;
	private RecordManager recordManager;
	private Properties properties;

	public DBApp(String name){
		this.name = name;
		try {
		this.properties = new Properties();
		properties.load(new FileInputStream("../../config/DBApp.properties"));
		this.recordManager = RecordManagerFactory.createRecordManager(name,properties);
		}catch(IOException e){

		}
	}

	public void init( ){
		;
	}
	
	public void createTable(String strTableName,
	Hashtable<String,String> htblColNameType,
	Hashtable<String,String>htblColNameRefs,
	String strKeyColName)
	throws DBAppException{
		CreateTableCmd cmd = new CreateTableCmd(strTableName,htblColNameType,htblColNameRefs,strKeyColName,this);
	    cmd.execute();
	}
	
	public void createIndex(String strTableName,
	String strColName)
	throws DBAppException{
		CreateIndexCmd cmd = new CreateIndexCmd(strTableName,strColName,this);
		cmd.execute();
	}

	public void insertIntoTable(String strTableName,
	Hashtable<String,String> htblColNameValue)
	throws DBAppException{
		InsertIntoTableCmd cmd = new InsertIntoTableCmd(strTableName,htblColNameValue,this);
		cmd.execute(); 
	}

	public void deleteFromTable(String strTableName,
	Hashtable<String,String> htblColNameValue,
	String strOperator)
	throws DBEngineException{
		
	}

	@SuppressWarnings("rawtypes")
	public Iterator selectFromTable(String strTable,
	Hashtable<String,String> htblColNameValue,
	String strOperator)
	throws DBEngineException{
		SelectFromTableCmd cmd = new SelectFromTableCmd(strTable,htblColNameValue,strOperator,this);
		cmd.execute();
		return cmd.getResult();
	}
	
	public void saveAll( ) throws DBEngineException{
		
	}


	//Getters and Setters
	public String getColumn(){
		return this.column;
	}

	public void setColumn(String column){
		this.column = column;
	}
	
	public String getName(){
		return this.name;
	}

	public void setName(String name){
		this.name = name;
	}
	
	public BTree getTable(){
		return this.table;
	}

	public void setTable(BTree table){
		this.table = table;
	}
	
	public RecordManager getRecordManager(){
		return this.recordManager;
	}
	
	public void setRecordManager(RecordManager recordManager){
		this.recordManager = recordManager;
	}

	public void showProperties(){
		for(String key : properties.stringPropertyNames()) {
		  	String value = properties.getProperty(key);
		  	System.out.println(key + " => " + value);
		}
	}

	public Properties getProperties(){
		return properties;
	}

	public void setProperties(Properties properties){
		this.properties = properties;
	}
}
