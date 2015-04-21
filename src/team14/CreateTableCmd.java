// package team14;

import java.io.*;
import java.util.*;


import jdbm.btree.BTree;
import jdbm.helper.StringComparator;


public class CreateTableCmd implements Command {
	private String strTableName;
	private Hashtable<String, String> htblColNameType;
	private Hashtable<String, String> htblColNameRefs;
	private String strKeyColName;
	private String column;
	private DBApp currentDB;

	public CreateTableCmd(String strTableName,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameRefs, String strKeyColName,
			DBApp currentDB) {
		this.strTableName = strTableName;
		this.htblColNameType = htblColNameType;
		this.htblColNameRefs = htblColNameRefs;
		this.strKeyColName = strKeyColName;
		this.column = currentDB.getColumn();
		this.currentDB = currentDB;
	}

	@Override
	public void execute() {
		try {
			File newFile = new File("../../Data/" + strTableName.toLowerCase()
					+ "_0.csv");

			if (newFile.exists() && !newFile.isDirectory()) {
				System.out.println("Table already exists ...");
			} else {
				//creating csv file
				System.out.println("creating Table " + strTableName + "...");
				BufferedWriter writer = new BufferedWriter(new FileWriter(
						newFile));
				writer.write(column);
				writer.close();
				// update Metadata File
				newFile = new File("../../Data/metadata.csv");
				String txt = "";
				writer = new BufferedWriter(new FileWriter(newFile, true));
				writer.write(this.prepare_meta(txt));
				writer.close();
				
				//create index table BTree
		        // Properties props = new Properties();
		        // this.getCurrentDB().setRecordManager(RecordManagerFactory.createRecordManager(getCurrentDB().getName(),props));
		       	

		       	//create new BTree for the created Table
		        this.getCurrentDB().setTable(BTree.createInstance( this.getCurrentDB().getRecordManager(), new StringComparator()));
		        this.getCurrentDB().getRecordManager().setNamedObject(strTableName, this.getCurrentDB().getTable().getRecid());
		        		       
		        this.getCurrentDB().getRecordManager().commit();
		        //this.getCurrentDB().getRecordManager().close();

	            }
				
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String prepare_meta(String txt) {
		Enumeration<String> colNames = htblColNameType.keys();
		Enumeration<String> types = htblColNameType.elements();
		Enumeration<String> refrences = htblColNameRefs.elements();
		String tem;
		String[] keys = strKeyColName.split(":");
		boolean key;
		boolean index;
		for (int i = 0; i < htblColNameType.size(); i++) {
			key = false;
			index = false;
			tem = (String) colNames.nextElement();
			for (int j = 0; j < keys.length; j++) {
				if (tem.equals(keys[j])) {
					key = true;
					index = true;
					break;
				}
			}
			txt += this.strTableName + "," + tem + "," + types.nextElement()
					+ "," + key + "," + index + "," + refrences.nextElement()
					+ "\n";
		}
		return txt;
	}

	public DBApp getCurrentDB() {
		return currentDB;
	}

	public void setCurrentDB(DBApp currentDB) {
		this.currentDB = currentDB;
	}
}
