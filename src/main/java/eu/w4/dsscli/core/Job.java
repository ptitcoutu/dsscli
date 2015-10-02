package eu.w4.dsscli.core;

import java.util.List;

public interface Job {
	static abstract public class Option {
		public boolean mainOption = false;
		public String name;
		public String description;
		abstract public void setValue(String optionValue);
	}	
	public List<Option> getAvailableOptions();
	public String getDescription();	
	public boolean openSession();
	public void closeSession();
	public void launch() throws Exception;
	public void setFileName(String fileName);
}
