// package team14;

import java.io.*;
import java.util.Hashtable;
import java.util.Date;

import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.btree.BTree;


public class InsertIntoTableCmd implements Command{
    private DBApp currentDB;
    private String strTableName;
    private String first_line;
    private Hashtable<String,String> htblColNameValue;


    public InsertIntoTableCmd(String strTableName,Hashtable<String,String> htblColNameValue,DBApp currentDB){
        this.strTableName = strTableName;
        this.htblColNameValue = htblColNameValue;
        this.currentDB = currentDB;
    }

	@Override
	public void execute(){
		try{
            // Insert Into CSV file
            File table = new File("../../Data/" + strTableName.toLowerCase()
                    + "_0.csv");
            String record = "";
            this.fetchFirstLine(table);
            record = this.prepare_record(record,table);
            long recid = this.getCurrentDB().getRecordManager().getNamedObject( strTableName );
            System.out.println(recid);
            this.getCurrentDB().setTable(BTree.load(this.getCurrentDB().getRecordManager(),recid));
            String [] a = record.replace("\"","").split(",");
            
            if(this.getCurrentDB().getTable().find(a[a.length-1]) == null ){
                
                //write on the right csv file
                int totalRecordNo = this.getCurrentDB().getTable().size();
                int maxRows = Integer.parseInt(this.getCurrentDB().getProperties().getProperty("MaximumRowsCountinPage"));
                int fileNo = (int) (totalRecordNo/maxRows);
                String row;

                table = new File("../../Data/" + strTableName.toLowerCase()
                    + "_"+ fileNo +".csv");
                if(table.exists() && !table.isDirectory()){
                    row = "\n" + record;
                }else{
                    row = record;
                }

                BufferedWriter writer = new BufferedWriter(new FileWriter(
                    table,true));
                writer.write(row); 
                writer.close();
  
                // Insert Into primary BTree
                // primary key and line number from begining deal with all csv files
                // of same table as one file
                this.getCurrentDB().getTable().insert(a[a.length-1], count("../../Data/"+strTableName.toLowerCase()+"_"+ fileNo +".csv") + (fileNo * maxRows) ,false);

                this.getCurrentDB().getRecordManager().commit();

                // print tree test
                Tuple tuple = new Tuple();
                TupleBrowser  browser;
                browser = this.getCurrentDB().getTable().browse();

                while ( browser.getNext( tuple ) ) {
                   System.out.println(tuple.getKey());
                   System.out.println(tuple.getValue());
                }

                //Insert Into Secondary BTree(s)
                String [] colNames = first_line.replace("\"","").split(",");
                for(int i=2;i<a.length -1;i++){
                    recid = this.getCurrentDB().getRecordManager().getNamedObject( strTableName + "_" + colNames[i]);
                    if(recid != 0){
                        this.getCurrentDB().setTable(BTree.load(this.getCurrentDB().getRecordManager(),recid));
                        String val = "";
                        if(this.getCurrentDB().getTable().find(a[i].replace("\"","")) != null){
                            val = this.getCurrentDB().getTable().find(a[i]) + ",";
                        }
                        val += a[a.length-1];
                        this.getCurrentDB().getTable().insert(a[i],val,true);
                        this.getCurrentDB().getRecordManager().commit();
                    }
                }

            }
            else{
                System.out.println("Cannot insert !! Record with same primary key exists");
            }
            
            // print tree test
            Tuple tuple = new Tuple();
            TupleBrowser  browser;
            browser = this.getCurrentDB().getTable().browse();

            while ( browser.getNext( tuple ) ) {
               System.out.println(tuple.getKey());
               System.out.println(tuple.getValue());
            }

        }catch(Exception e){
            e.printStackTrace();
        }
	}

    //Getters and Setters
    public DBApp getCurrentDB(){
        return this.currentDB;
    }

    public void setCurrentDB(DBApp currentDB){
        this.currentDB = currentDB;
    }

    // helper methods

    public String prepare_record(String record, File table){
        Date nw = new Date();
        try{
            String [] cols = first_line.split(",");
            for (int i = cols.length-1; i > 1 ;i--){
                cols[i] = cols[i].replace("\"", "");
                record = "," + this.htblColNameValue.get(cols[i]) + record ;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        record = "\"" + nw.toString()+ "\"" + "," + "\"" + nw.toString() + "\""+ record;
        System.out.println(record);
        return record;
    }

    //Line number 

	public static int count(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 1;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            if(filename.contains("0")){
                return (count == 0 && !empty) ? 1 : count - 1;
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }

    public void fetchFirstLine(File table){
        try{
            BufferedReader reader = new BufferedReader(new FileReader(table));
            first_line = reader.readLine();
            reader.close();
        }catch(Exception e){

        }
    }

}
