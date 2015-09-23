package eu.w4.dsscli.cmd;

import java.util.logging.Level;

import eu.w4.dsscli.core.TransferJob;

public class Transfer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TransferJob transferJob = new TransferJob();
		//RepositoryAdmin.logger.setLevel(Level.OFF);
		String cmd = "export";
		int argsLength = args.length;
		for (int i = 0; i < argsLength; i += 2) {
			String optName = args[i];
			if (i == (argsLength - 1)) {
				if (optName.startsWith("-")) {
					showUsage("invalid option");
				} else {
					transferJob.setFilename(optName);
				}
				continue;
			}
			String optionValue = args[i + 1];
			if (optName.equalsIgnoreCase("-host")) {
				transferJob.setHostname(optionValue);
			} else if (optName.equalsIgnoreCase("-port")) {
				transferJob.setPortNumber(optionValue);
			} else if (optName.equalsIgnoreCase("-usr")) {
				transferJob.setLogin(optionValue);
			} else if (optName.equalsIgnoreCase("-pwd")) {
				transferJob.setPassword(optionValue);
			} else if (optName.equalsIgnoreCase("-trace")) {
				TransferJob.logger.setLevel(Level.parse(optionValue));
			} else if (optName.equalsIgnoreCase("-cmd")) {
				cmd = optionValue;
			} else if (optName.equalsIgnoreCase("-limit")) {
				transferJob.setSizeLimit(optionValue);
			} else {
				showUsage("invalid option");
			}
		}
		if (transferJob.openSession()) {
			if (cmd.equalsIgnoreCase("import")) {
				transferJob.importAll(false);
			} else if (cmd.equalsIgnoreCase("export")) {
				transferJob.exportAll(false);
			} else if (cmd.equalsIgnoreCase("importAll")) {
				transferJob.importAll(true);
			} else if (cmd.equalsIgnoreCase("exportAll")) {
				transferJob.exportAll(true);
			} else {
				showUsage("invalid command (use import or export)");
			}
		} else {
			showUsage("can't connect to w4 on " + transferJob.getHostname() + ":"
					+ transferJob.getPortNumber());
		}
		transferJob.closeSession();
	}

	/**
	 * 
	 */
	private static void showUsage(String error) {
		System.err.println("error : " + error);
		System.err
				.println("usage:dsscli [-cmd admin command] [-host hostname] [-port portNumber] [-usr user] [-pwd password] [-trace trace level] fileName");
		System.err.println(" -cmd : admin command - (default)export, import, exportAll (export types, folders, documents and attached files");
		System.err.println(" -host : hostname without port number");
		System.err.println(" -port : tcp port number");
		System.err.println(" -usr  : w4 user login");
		System.err.println(" -pwd  : w4 user password");
		System.err
				.println(" -trace : trace level - (default) OFF, ALL, CONFIG, FINE, FINER, FINEST, INFO, SEVERE, WARNING");
		System.exit(-1);
	}

}
