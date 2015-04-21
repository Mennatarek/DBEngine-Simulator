JCC = javac
JFLAGS = -g
.SUFFIXES: .java .class
.java.class:
	javac $<
DBAppException.class: DBAppException.java
	$(JCC) $(JFLAGS) DBAppException.java
DBEngineException.class: DBEngineException.java     
	$(JCC) $(JFLAGS) DBEngineException.java
Command.class: Command.java
	$(JCC) $(JFLAGS) Command.java
CreateIndexCmd.class: CreateIndexCmd.java        
	$(JCC) $(JFLAGS) CreateIndexCmd.java
CreateTableCmd.class: CreateTableCmd.java        
	$(JCC) $(JFLAGS) CreateTableCmd.java
DBApp.class: DBApp.java
	$(JCC) $(JFLAGS) DBApp.java
DBAppTest.class: DBAppTest.java
	$(JCC) $(JFLAGS) DBAppTest.java
DeleteFromTableCmd.class: DeleteFromTableCmd.java
	$(JCC) $(JFLAGS) DeleteFromTableCmd.java
SelectFromTableCmd.class: SelectFromTableCmd.java
	$(JCC) $(JFLAGS) SelectFromTableCmd.java
TakingCommand.class: TakingCommand.java
	$(JCC) $(JFLAGS) TakingCommand.java
clean:
	$(RM) *.class