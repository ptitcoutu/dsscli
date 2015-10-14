package eu.w4.dsscli.cmd;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import eu.w4.dsscli.core.DeleteJob;
import eu.w4.dsscli.core.ExportAllJob;
import eu.w4.dsscli.core.ExportJob;
import eu.w4.dsscli.core.ImportAllJob;
import eu.w4.dsscli.core.ImportJob;
import eu.w4.dsscli.core.Job;
import eu.w4.dsscli.core.Job.Option;

public class CLI {
	public static Logger logger = Logger.getLogger("dsscli");
	static public Map<String, Job> jobMap = new HashMap<String, Job>();

	static {
		jobMap.put("import", new ImportJob());
		jobMap.put("export", new ExportJob());
		jobMap.put("importAll", new ImportAllJob());
		jobMap.put("exportAll", new ExportAllJob());
		jobMap.put("delete", new DeleteJob());
	}

	public static void main(String[] args) {

		if (args.length == 0) {
			showUsage("please precise command to launch");
		}
		String cmd = args[0];
		if ("version".equals(cmd)) {
			System.out.println("DSS CLI 2.0 - Apache Licence");
			return;
		}
		if ("help".equals(cmd)) {
			displayHelp(System.out);
			return;
		}
		if (!jobMap.containsKey(cmd)) {
			showUsage("invalide command " + cmd);
		}
		Job job = jobMap.get(cmd);
		List<Option> options = job.getAvailableOptions();
		Map<String, Option> optionMap = new HashMap<String, Job.Option>();
		for (Option option : options) {
			optionMap.put(option.name, option);
		}
		int argsLength = args.length;
		for (int i = 1; i < argsLength; i += 2) {
			String optName = args[i];
			if (i == (argsLength - 1)) {
				if (optName.startsWith("-")) {
					showUsage("invalid option " + optName);
				} else {
					job.setFileName(optName);
				}
				continue;
			}
			String optionValue = args[i + 1];
			if (optionMap.containsKey(optName)) {
				optionMap.get(optName).setValue(optionValue);
			} else {
				showUsage("invalid option " + optName);
			}
		}
		try {
			if (job.openSession()) {
				job.launch();
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			showUsage(e.getMessage());
		} finally {
			job.closeSession();
		}

	}

	static private void showUsage(String error) {
		System.err.println("error : " + error);
		displayHelp(System.err);
		System.exit(-1);
	}

	static public void displayHelp(PrintStream out) {
		out.println(
				"usage:dsscli <command> [-host hostname] [-port portNumber] [-usr user] [-pwd password] [-trace trace level] [specific command options] fileName");
		out.println(
				"<command> : command to launch or help to display this message or version to have version number of the DSS CLI");
		out.println(" -host : hostname without port number");
		out.println(" -port : tcp port number");
		out.println(" -usr  : w4 user login");
		out.println(" -pwd  : w4 user password");
		out.println(" -trace : trace level - (default) OFF, ALL, CONFIG, FINE, FINER, FINEST, INFO, SEVERE, WARNING");
		out.println("available commands are : ");
		String[] cmdNames = jobMap.keySet().toArray(new String[0]);
		Arrays.sort(cmdNames);
		for (String cmdName : cmdNames) {
			Job job = jobMap.get(cmdName);
			out.println(cmdName + " : " + job.getDescription());
			for (Option option : job.getAvailableOptions()) {
				if (option.mainOption) {
					continue;
				}
				out.println("  " + option.name + " : " + option.description);
			}
		}

	}
}
