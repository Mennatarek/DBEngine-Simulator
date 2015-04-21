// package team14;

import java.util.Hashtable;
import java.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class DBAppTest {

	public static void main(String[] args) {
		DBApp db = new DBApp("db");
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				System.in));
		Session session = new Session(db);
		List<String> session_input = new ArrayList<String>();
		boolean run = true;
		try {
			while (run) {
				// initialize shell
				System.out.print("Query shell> ");
				String commandLine = reader.readLine();
				session = new Session(db);

				String strTableName;
				// cmd
				session_input = Arrays.asList(commandLine.split(" "));

				switch (session_input.get(0)) {
				case "createTable":
					// initialize user data Objects
					String strKeyColName;
					Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
					Hashtable<String, String> htblColNameRefs = new Hashtable<String, String>();
					String column_names = "";
					if (session_input.size() <= 2) {
						System.out
								.println("Where is the rest of the Cmd colname:type:reference ... ");
						break;
					}
					column_names = get_createTable_args(session_input,
							htblColNameType, htblColNameRefs, column_names);
					if (column_names.equals("")) {
						break;
					}
					try {
						strTableName = session_input.get(1);
						strKeyColName = session_input
								.get(session_input.size() - 1);
						if (htblColNameType.get(strKeyColName) != null) {
							column_names = "\"created_at\",\"updated_at\""
									+ column_names;
							session.getCurrentDb().setColumn(column_names);
							session.getCurrentDb().createTable(strTableName,
									htblColNameType, htblColNameRefs,
									strKeyColName);
						} else {
							System.out.println("No column " + strKeyColName
									+ " exists to be the primary key!!");
						}
					} catch (DBAppException e) {
					} finally {
						break;
					}
				case "insertIntoTable":
					// initialize user data Objects
					Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
					try {
						strTableName = session_input.get(1);
						if (get_insertIntoTable_args(session_input,
								htblColNameValue)) {
							session.getCurrentDb().insertIntoTable(
									strTableName, htblColNameValue);
						}
					} catch (Exception e) {
						System.out
								.println("Where is the rest of the Cmd Tablename colname:value ... ");
					} finally {
						break;
					}
				case "selectFromTable":
					htblColNameValue = new Hashtable<String, String>();
					strTableName = session_input.get(1);
					String strOperator = session_input
							.get(session_input.size() - 1);
					try {
						if (selectIterator(strTableName, session_input,
								htblColNameValue, strOperator)) {
							session.getCurrentDb()
									.selectFromTable(strTableName,
											htblColNameValue, strOperator);
						}
					} catch (DBEngineException e) {

					}
					break;
				case "deleteFromTable":
					break;
				case "showProperties":
					session.getCurrentDb().showProperties();
					break;
				case "createIndex":
					if (session_input.size() == 3) {
						strTableName = session_input.get(1);
						String colName = session_input.get(2);
						if (indexValidator(strTableName, colName)) {
							session.getCurrentDb().createIndex(strTableName,
									colName);
						}
					} else {
						System.out.println("Your comand is not well formed !!");
					}
					break;
				case "quit":
					run = false;
					break;
				default:
					System.out.println("Cannot recognize your command");
					break;
				}
			}
			db.getRecordManager().close();
		} catch (Exception e) {

		} finally {

		}
	}

	public static String prepare_colums(String[] temp, String column, String key) {
		if (temp[1].toLowerCase().equals("string")
				|| temp[1].toLowerCase().equals("text")) {
			if (temp[0].equals(key)) {
				column = column + "," + "\"" + temp[0] + "\"";
			} else {
				column = "," + "\"" + temp[0] + "\"" + column;
			}
		} else {
			if (temp[0].equals(key)) {
				column = column + "," + temp[0];
			} else {
				column = "," + temp[0] + column;
			}
		}
		return column;
	}

	public static String get_createTable_args(List<String> session_input,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameRefs, String column) {
		try {
			String key = session_input.get(session_input.size() - 1);
			for (int i = 2; i < session_input.size() - 1; i++) {
				boolean col_found = false;
				String[] temp = session_input.get(i).split(":");
				if (!creatValidator(temp[1])) {
					System.out.println("No Data Type of " + temp[1]);
					return "";
				}
				if (!(temp[2].equals("null") || temp[2].equals("Null") || temp[2]
						.equals("NULL"))) {
					System.out.println(temp[2]);
					String[] ref = temp[2].split("\\.");
					System.out.println(ref[0]);
					System.out.println(ref[0] + " " + ref[1]);

					File check_ref = new File("../../Data/"
							+ ref[0].toLowerCase() + "_0.csv");
					if (check_ref.exists() && !check_ref.isDirectory()) {
						BufferedReader reader = new BufferedReader(
								new FileReader(check_ref));
						String first_line = reader.readLine();
						reader.close();
						String[] cols = first_line.split(",");
						for (int j = 2; j < cols.length; j++) {
							cols[j] = cols[j].replace("\"", "");
							if (cols[j].equals(ref[1])) {
								col_found = true;
							}
						}
						if (!col_found) {
							System.out.println("Column [" + ref[1]
									+ "] in table" + ref[0]
									+ " does not exist !!");
							return "";
						}
					} else {
						System.out.println("Table " + ref[0]
								+ " does not exist");
						return "";
					}
				}
				htblColNameType.put(temp[0], temp[1]);
				htblColNameRefs.put(temp[0], temp[2]);
				column = prepare_colums(temp, column, key);
			}
			return column;
		} catch (Exception e) {
			System.out.println("Unknown error !!");
		}
		return "";
	}

	// method for parsing the cmd and checks whether the Table exists and the
	// passed columns
	public static boolean get_insertIntoTable_args(List<String> session_input,
			Hashtable<String, String> htblColNameValue) {
		Hashtable<String, String> colNameType = dataRetriever(session_input
				.get(1));
		boolean flag = false;
		for (int i = 2; i < session_input.size(); i++) {
			flag = true;
			String[] temp = session_input.get(i).split(":");
			if (colNameType.get(temp[0]) != null
					&& insertionValidator(colNameType.get(temp[0]), temp[1])) {
				htblColNameValue.put(temp[0], temp[1]);
			} else {
				if (colNameType.isEmpty()) {
					System.out.println("Table " + session_input.get(1)
							+ " does not Exist");
				} else {
					System.out.println("Column " + temp[0]
							+ " does not exist or invalid data type!!");
				}
				return false;
			}
		}
		if (!flag) {
			System.out.println("Cmd is not well formed !! CHECK SYNTAX");
		}
		return flag;
	}

	// Validators

	public static boolean insertionValidator(String type, String value) {
		switch (type) {
		case "Integer":
			return isInteger(value);
		case "Double":
			return isDouble(value);
		case "String":
			return isString(value);
		case "Boolean":
			return isBoolean(value);
		default:
			return false;
		}
	}

	public static boolean isBoolean(String value) {
		if (value.equals("0") || value.equals("false") || value.equals("1")
				|| value.equals("true")) {
			return true;
		}
		return false;
	}

	public static boolean isInteger(String value) {
		try {
			Integer.parseInt(value);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

	public static boolean isString(String value) {
		return true;
	}

	public static boolean isDouble(String value) {
		try {
			Double.parseDouble(value);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

	public static boolean creatValidator(String input) {
		if (input.equals("Integer") || input.equals("Double")
				|| input.equals("String") || input.equals("Boolean"))
			return true;
		return false;
	}

	public static Hashtable<String, String> dataRetriever(String tableName) {
		Hashtable<String, String> ht = new Hashtable<String, String>();
		String temp;
		String[] currentLine;
		try {
			File metadata = new File("../../Data/metadata.csv");
			BufferedReader br = new BufferedReader(new FileReader(metadata));
			br.readLine();
			temp = br.readLine();
			while (!(temp == null)) {
				currentLine = temp.split(",");
				if (currentLine[0].equals(tableName)) {
					ht.put(currentLine[1], currentLine[2]);
				}
				temp = br.readLine();
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ht;
	}

	public static boolean selectIterator(String strTableName,
			List<String> session_input,
			Hashtable<String, String> htblColNameValue, String strOperator) {
		Hashtable<String, String> dataRetriver = dataRetriever(session_input
				.get(1));
		if (!(dataRetriver.isEmpty()) && operatorValidator(strOperator)) {
			for (int i = 2; i < session_input.size() - 1; i++) {
				String[] temp = session_input.get(i).split(":");
				if ((dataRetriver.containsKey(temp[0]))) {
					htblColNameValue.put(temp[0], temp[1]);
				} else {
					System.out.println("column " + temp[0] + "does not Exist");
					return false;
				}
			}
			return true;
		} else {
			if (dataRetriver.isEmpty()) {
				System.out.println("Table " + session_input.get(1)
						+ " does not exists");
			} else {
				System.out.println("Cannot find " + strOperator
						+ " as an operator !!");
			}
			return false;
		}
	}

	public static Boolean operatorValidator(String operator) {
		if (operator.equals("AND") || operator.equals("OR")
				|| operator.equals(""))
			return true;
		return false;
	}

	public static boolean indexValidator(String tableName, String colname) {
		ArrayList<String> copy = new ArrayList<String>();
		String temp;
		String[] currentLine;

		try {
			File metadata = new File("../../Data/metadata.csv");
			BufferedReader br = new BufferedReader(new FileReader(metadata));
			boolean flag = false;
			boolean tableExists = false;
			copy.add(br.readLine());
			temp = br.readLine();
			while (!(temp == null)) {
				currentLine = temp.split(",");

				if (currentLine[0].equals(tableName)
						&& currentLine[1].equals(colname)
						&& currentLine[4].equals("true")) {
					System.out.println("This index already exists !!");
					br.close();
					return false;
				}
				if (currentLine[0].equals(tableName)
						&& currentLine[1].equals(colname)
						&& currentLine[4].equals("false")) {
					currentLine[4] = "true";
					temp = "";
					for (int i = 0; i < currentLine.length - 1; i++) {
						temp += currentLine[i] + ",";
					}
					temp += currentLine[currentLine.length - 1];

					copy.add(temp);
					tableExists = true;
					flag = true;
				}
				if (!flag) {
					copy.add(temp);
				} else {
					flag = false;
				}

				temp = br.readLine();
			}

			br.close();

			if (tableExists) {
				BufferedWriter writer = new BufferedWriter(new FileWriter(
						metadata));
				updateMetaIndex(writer, copy);
				writer.close();
				return true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Table name or column does not exists !!");
		return false;
	}

	public static void updateMetaIndex(BufferedWriter writer,
			ArrayList<String> copy) {
		try {
			writer.write(copy.get(0) + "\n");
			for (int i = 1; i < copy.size(); i++) {
				writer.write(copy.get(i) + "\n");
			}
		} catch (Exception e) {

		}
	}
}
