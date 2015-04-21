// package team14;

import java.util.*;
import java.io.*;

import javax.swing.RowFilter.Entry;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.btree.BTree;

public class SelectFromTableCmd implements Command {
	private Iterator result;
	private ArrayList<String> values;
	private String strTable;
	private Hashtable<String, String> htblColNameValue;
	private String strOperator;
	private DBApp currentDB;
	private String first_line;
	private String[] columns;

	public SelectFromTableCmd(String strTable,
			Hashtable<String, String> htblColNameValue, String strOperator,
			DBApp currentDB) {
		this.strTable = strTable;
		this.htblColNameValue = htblColNameValue;
		this.strOperator = strOperator;
		this.currentDB = currentDB;
		this.values = new ArrayList<String>();
	}

	@Override
	public void execute() {
		try {
			File table = new File("../../Data/" + strTable.toLowerCase()
					+ "_0.csv");
			this.fetchFirstLine(table);
			switch (this.strOperator) {
			case "AND":
				// if primary key is provided
				processAnd();
				break;
			case "OR":
				Hashtable<String, String> hashCopy = new Hashtable<String, String>();
				hashCopy.putAll(htblColNameValue);
				htblColNameValue.clear();
				for (Map.Entry<String, String> entry : hashCopy.entrySet()) {
					htblColNameValue.put(entry.getKey(), entry.getValue());
					processAnd();
					htblColNameValue.clear();
				}
				break;
			}
			if (!(values.isEmpty())) {
				this.iterate_result();
			} else {
				System.out.println("No Data Matches !!");
			}
		} catch (Exception e) {

		}
	}

	public void processAnd() {
		if (htblColNameValue.containsKey(columns[columns.length - 1])) {
			this.getWithPrimaryKey();
		} else {
			// if any column is a indexed one
			this.getWithSecondaryIndex();
		}
	}

	public void getWithSecondaryIndex() {
		try {
			for (int i = 0; i < columns.length; i++) {
				long recid = this.getCurrentDB().getRecordManager()
						.getNamedObject(strTable + "_" + columns[i]);
				if (recid != 0) {
					this.getCurrentDB().setTable(
							BTree.load(this.getCurrentDB().getRecordManager(),
									recid));
					String priValues = (String) this
							.getCurrentDB()
							.getTable()
							.find(this.htblColNameValue.get(columns[i])
									.replace("\"", ""));
					String[] temp = priValues.split(",");
					this.getWithPrimaryKey(temp);
					return;
				}
			}
			long recid = this.getCurrentDB().getRecordManager()
					.getNamedObject(strTable);
			this.getCurrentDB().setTable(
					BTree.load(this.getCurrentDB().getRecordManager(), recid));

			int maxRows = Integer.parseInt(this.getCurrentDB().getProperties()
					.getProperty("MaximumRowsCountinPage"));

			for (int i = 0; true; i++) {
				File csv = new File("../../Data/" + strTable + "_" + i + ".csv");
				if (!(csv.exists() && !csv.isDirectory())) {
					return;
				}

				BufferedReader reader = new BufferedReader(new FileReader(csv));
				String line = reader.readLine();
				if (i == 0) {
					line = reader.readLine();
				}

				while (line != null) {
					String[] tem = line.split(",");
					boolean valid = true;
					for (int j = 0; j < tem.length; j++) {
						if (this.htblColNameValue.containsKey(columns[j])) {
							if (!this.htblColNameValue.get(columns[j]).equals(
									tem[j])) {
								valid = false;
							}
						}
					}
					if (valid) {
						values.add(line);
					}
					line = reader.readLine();
				}

			}
		} catch (Exception e) {

		}
	}

	public void getWithPrimaryKey(String[] primaries) {
		try {
			long recid = this.getCurrentDB().getRecordManager()
					.getNamedObject(strTable);
			this.getCurrentDB().setTable(
					BTree.load(this.getCurrentDB().getRecordManager(), recid));

			int[] fileOffsets = new int[primaries.length];
			int[] rowNumbers = new int[primaries.length];

			for (int i = 0; i < primaries.length; i++) {
				fileOffsets[i] = this.getFileOffsetS(primaries[i]);
				rowNumbers[i] = this
						.getRowNumberS(fileOffsets[i], primaries[i]);
			}

			for (int j = 0; j < fileOffsets.length; j++) {

				File table = new File("../../Data/" + strTable + "_"
						+ fileOffsets[j] + ".csv");
				BufferedReader reader = new BufferedReader(
						new FileReader(table));

				if (fileOffsets[j] == 0) {
					reader.readLine();
				}

				String record = "";
				for (int i = 1; i <= rowNumbers[j]; i++) {
					record = reader.readLine();
				}

				String[] tem = record.split(",");
				boolean valid = true;
				for (int i = 0; i < columns.length; i++) {
					if (this.htblColNameValue.containsKey(columns[i])) {
						if (!this.htblColNameValue.get(columns[i]).equals(
								tem[i])) {
							valid = false;
						}
					}
				}

				if (valid) {
					values.add(record);
				}

			}
		} catch (Exception e) {

		}

	}

	public int getFileOffsetS(String key) {
		int offset = -1;
		try {
			int recordLine = (int) (this.getCurrentDB().getTable().find(key));
			int maxRows = Integer.parseInt(this.getCurrentDB().getProperties()
					.getProperty("MaximumRowsCountinPage"));
			while (recordLine > 0) {
				recordLine -= maxRows;
				offset++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return offset;
	}

	public int getRowNumberS(int fileOffset, String key) {
		try {
			int recordLine = (int) (this.getCurrentDB().getTable().find(key));
			int maxRows = Integer.parseInt(this.getCurrentDB().getProperties()
					.getProperty("MaximumRowsCountinPage"));
			return recordLine - (fileOffset * maxRows);
		} catch (Exception e) {

		}
		return 0;

	}

	public void getWithPrimaryKey() {
		try {
			long recid = this.getCurrentDB().getRecordManager()
					.getNamedObject(strTable);
			this.getCurrentDB().setTable(
					BTree.load(this.getCurrentDB().getRecordManager(), recid));
			int fileOffset = this.getFileOffset();
			int rowNumber = this.getRowNumber(fileOffset);
			if (fileOffset == -1 || rowNumber == 0) {
				return;
			}
			this.getRecords(fileOffset, rowNumber);
		} catch (Exception e) {

		}
	}

	public void getRecords(int fileOffset, int rowNumber) {
		try {
			File table = new File("../../Data/" + strTable + "_" + fileOffset
					+ ".csv");
			BufferedReader reader = new BufferedReader(new FileReader(table));

			if (fileOffset == 0) {
				reader.readLine();
			}
			String record = "";
			for (int i = 1; i <= rowNumber; i++) {
				record = reader.readLine();
			}

			String[] tem = record.split(",");
			boolean valid = true;
			for (int i = 0; i < columns.length; i++) {
				if (this.htblColNameValue.containsKey(columns[i])) {
					if (!this.htblColNameValue.get(columns[i]).equals(tem[i])) {
						valid = false;
					}
				}
			}

			if (valid) {
				values.add(record);
			}
		} catch (Exception e) {

		}
	}

	public int getFileOffset() {
		int offset = -1;
		try {
			int recordLine = (int) (this.getCurrentDB().getTable()
					.find(this.htblColNameValue
							.get(columns[columns.length - 1])));
			int maxRows = Integer.parseInt(this.getCurrentDB().getProperties()
					.getProperty("MaximumRowsCountinPage"));
			while (recordLine > 0) {
				recordLine -= maxRows;
				offset++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return offset;
	}

	public int getRowNumber(int fileOffset) {
		try {
			int recordLine = (int) (this.getCurrentDB().getTable()
					.find(this.htblColNameValue
							.get(columns[columns.length - 1])));
			int maxRows = Integer.parseInt(this.getCurrentDB().getProperties()
					.getProperty("MaximumRowsCountinPage"));
			return recordLine - (fileOffset * maxRows);
		} catch (Exception e) {

		}
		return 0;
	}

	public void fetchFirstLine(File table) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(table));
			first_line = reader.readLine();
			reader.close();
			columns = first_line.replace("\"", "").split(",");
			// this.fetchColIndex();
		} catch (Exception e) {

		}
	}

	public void iterate_result() {
		removeDublicateRecords();
		for (int i = 0; i < values.size(); i++) {
			System.out.println(values.get(i));
		}
	}

	public void removeDublicateRecords() {
		for (int i = 1; i <= values.size(); i++) {
			for (int j = i; j <= values.size() - i; j++) {
				if (values.get(i-1).equals(values.get(j))){
					values.remove(j);
				}
			}
		}
	}

	public Iterator getResult() {
		return this.result;
	}

	public String getStrOperator() {
		return this.strOperator;
	}

	public void setStrOperator(String strOperator) {
		this.strOperator = strOperator;
	}

	public DBApp getCurrentDB() {
		return this.currentDB;
	}

	public void setCurrentDB(DBApp currentDB) {
		this.currentDB = currentDB;
	}

}
