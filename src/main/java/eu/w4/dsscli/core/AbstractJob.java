package eu.w4.dsscli.core;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import eu.w4.common.configuration.ConfigurationParameter;
import eu.w4.engine.client.configuration.NetworkConfigurationParameter;
import eu.w4.engine.client.dss.service.DocumentStorageService;
import eu.w4.engine.client.dss.service.DocumentStorageServiceObjectFactory;
import eu.w4.engine.client.service.AuthenticationService;
import eu.w4.engine.client.service.EngineService;
import eu.w4.engine.client.service.EngineServiceFactory;

public abstract class AbstractJob implements Job {
	protected String login = "admin";
	protected String password = "admin";
	protected String hostname = "localhost";
	protected String portNumber = "7707";
	protected DocumentStorageService dssService = null;
	protected DocumentStorageServiceObjectFactory dssObjFactory = null;
	protected Principal usrPrincipal = null;
	protected EngineService engineService = null;
	protected AuthenticationService authService = null;
	public static Logger logger = Logger.getLogger("dsscli");
	public static String CLASS_NAME = TransferJob.class.getName();	
	protected String filename = "extractedData.xml";
			
	public AbstractJob() {
	}
	
	@Override
	public List<Option> getAvailableOptions() {
		List<Option> mainOptions = new ArrayList<Job.Option>();
		mainOptions.add(new Option() {
			{
				mainOption = true;
				name = "-host";
				description = "hostname without port number";
			}	
			@Override
			public void setValue(String optionValue) {
				hostname = optionValue;
			}
			
		});
		mainOptions.add(new Option() {
			{
				mainOption = true;
				name = "-port";
				description = "tcp port number";
			}	
			@Override
			public void setValue(String optionValue) {
				portNumber = optionValue;
			}
			
		});		
		mainOptions.add(new Option() {
			{
				mainOption = true;
				name = "-usr";
				description = "w4 user login";
			}	
			@Override
			public void setValue(String optionValue) {
				login = optionValue;
			}
			
		});
		mainOptions.add(new Option() {
			{
				mainOption = true;
				name = "-pwd";
				description = "w4 user password";
			}	
			@Override
			public void setValue(String optionValue) {
				password = optionValue;
			}
			
		});

		mainOptions.add(new Option() {
			{
				mainOption = true;
				name = "-trace";
				description = "trace level - (default) OFF, ALL, CONFIG, FINE, FINER, FINEST, INFO, SEVERE, WARNING";
			}	
			@Override
			public void setValue(String optionValue) {
				logger.setLevel(Level.parse(optionValue));
			}
			
		});
		
		return mainOptions; 
	}

	@Override
	public boolean openSession() {
		String methodName = "openSession";
		logger.entering(CLASS_NAME, methodName);
		try {
			final Map<ConfigurationParameter, String> connParams = new HashMap<ConfigurationParameter, String>();
			connParams.put(NetworkConfigurationParameter.RMI__REGISTRY_HOST, hostname);
			connParams.put(NetworkConfigurationParameter.RMI__REGISTRY_PORT, portNumber);
			engineService = EngineServiceFactory.getEngineService(connParams);
			authService = engineService.getAuthenticationService();
			usrPrincipal = authService.login(login, password, true);
			dssService = engineService.getExternalService(DocumentStorageService.class);
			dssObjFactory = engineService.getExternalService(DocumentStorageServiceObjectFactory.class);
		} catch (Throwable e) {
			logger.log(Level.WARNING, "error while trying to open connection to BPMN+ engine", e);
			return false;
		}
		logger.exiting(CLASS_NAME, methodName);
		return true;
	}

	@Override
	public void closeSession() {	
		String methodName = "closeSession";
		logger.entering(CLASS_NAME, methodName);
		try {
			authService.logout(usrPrincipal);
		} catch (Throwable e) {
			logger.log(Level.WARNING, "error while trying to close connection to BPMN+ engine", e);
		}
		logger.exiting(CLASS_NAME, methodName);		
	}

	@Override
	abstract public void launch() throws Exception;
	
	@Override
	public void setFileName(String fileName) {
		this.filename = fileName;
	}
	
	@Override
	abstract public String getDescription();
}
