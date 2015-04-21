// package team14;

import java.io.*;

import jdbm.helper.StringComparator;
import jdbm.btree.BTree;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

public class CreateIndexCmd implements Command {
	private String tableName;
	private String colName;
	private DBApp currentDB;

	public CreateIndexCmd(String tableName,String colName,DBApp currentDB){
		this.tableName = tableName;
		this.colName = colName;
		this.currentDB = currentDB;
	}

	@Override
	public void execute() {
		try{
			long recid = this.getCurrentDB().getRecordManager().getNamedObject( tableName +"_"+colName);
			if (recid != 0){
				System.out.println("This index already exists !!");
				return;
			}
			this.getCurrentDB().setTable(BTree.createInstance( this.getCurrentDB().getRecordManager(), new StringComparator()));
			this.getCurrentDB().getRecordManager().setNamedObject(tableName+"_"+colName,this.getCurrentDB().getTable().getRecid());
		    this.getCurrentDB().getRecordManager().commit();

			for(int i = 0;true;i++){
				File csv = new File("../../Data/"+tableName +"_"+ i +".csv");
				if (!(csv.exists() && !csv.isDirectory())) {
					return;
				}
				BufferedReader reader = new BufferedReader(new FileReader(csv));
				this.fetchData(reader,this.colName);
				reader.close();

				Tuple tuple = new Tuple();
	            TupleBrowser  browser;
	            browser = this.getCurrentDB().getTable().browse();

	            while ( browser.getNext( tuple ) ) {
	               System.out.println(tuple.getKey());
	               System.out.println(tuple.getValue());
	            }
			}


		}catch(Exception e){

		}
	}

	//method to read data from csv file insert it into the loaded B+tree

	public void fetchData(BufferedReader reader,String colName){
		try{
			String line = reader.readLine();
			String [] columns  = line.replace("\"","").split(",");
			int offset = getOffset(columns,colName);
			int primaryKeyOffset = columns.length -1 ;
			
			line = reader.readLine();
			
			while(line != null && offset != -1){
				columns = line.replace("\"","").split(",");

				// check if key is already exists
				if (this.getCurrentDB().getTable().find((String)columns[offset]) == null){
					this.getCurrentDB().getTable().insert(columns[offset],columns[primaryKeyOffset],false);
				}else{
					String replaced_value = (String)this.getCurrentDB().getTable().find(columns[offset]);
					replaced_value += "," + columns[primaryKeyOffset];
					this.getCurrentDB().getTable().insert(columns[offset],replaced_value,true);
				}
				line = reader.readLine();
			}
		}catch(Exception e){

		}
	}

	public static int getOffset(String [] line,String colName){
		for(int i=0;i< line.length;i++){
			if(line[i].equals(colName)){
				return i;
			}
		}
		return -1;
	}

	public String getTableName(){
		return this.tableName;
	}

	public void setTableName(String tableName){
		this.tableName = tableName;
	}

	public String getColName(){
		return this.colName;
	}

	public void setColName(String colName){
		this.colName = colName;
	}

	public DBApp getCurrentDB(){
		return this.currentDB;
	}

	public void setCurrentDB(DBApp currentDB){
		this.currentDB = currentDB;
	}
}
