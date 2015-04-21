// package team14;

public class Session{
	
	private DBApp currentDB;

	public Session(DBApp db){
		this.currentDB = db;	
	}

	public DBApp getCurrentDb(){
		return this.currentDB;
	}

}