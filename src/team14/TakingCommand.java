// package team14;

import java.util.ArrayList;
import java.util.List;

public class TakingCommand {
	public TakingCommand(){
		
	}
	   private List<Command> history = new ArrayList<Command>();
	   public void storeAndExecute(Command cmd) {
	      this.history.add(cmd); // To keep Track of the commands done
	      cmd.execute();
	}
}