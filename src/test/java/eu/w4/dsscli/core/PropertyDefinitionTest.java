package eu.w4.dsscli.core;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import eu.w4.common.configuration.ConfigurationParameter;
import eu.w4.dsscli.core.TransferJob;
import eu.w4.engine.client.configuration.NetworkConfigurationParameter;
import eu.w4.engine.client.dss.DefaultPropertyDefinitionDataType;
import eu.w4.engine.client.dss.PropertyDefinition;
import eu.w4.engine.client.dss.TypeIdentifier;
import eu.w4.engine.client.dss.service.DocumentStorageServiceObjectFactory;
import eu.w4.engine.client.service.EngineService;
import eu.w4.engine.client.service.EngineServiceFactory;

public class PropertyDefinitionTest {

	@Test
	public void testEquals() throws Exception {
		final Map<ConfigurationParameter, String> connParams = new HashMap<ConfigurationParameter, String>();
		connParams.put(NetworkConfigurationParameter.RMI__REGISTRY_HOST, "localhost");
		connParams.put(NetworkConfigurationParameter.RMI__REGISTRY_PORT, "7707");
		EngineService engineService = EngineServiceFactory.getEngineService(connParams);
		DocumentStorageServiceObjectFactory dssObjFactory = engineService
				.getExternalService(DocumentStorageServiceObjectFactory.class);
		TypeIdentifier ti = dssObjFactory.newTypeIdentifier();
		ti.setId(10l);
		ti.setName("test");
		ti.setPrefix("dsscli");
		PropertyDefinition propDef1 = dssObjFactory.newPropertyDefinition();
		PropertyDefinition propDef2 = dssObjFactory.newPropertyDefinition();
		propDef1.setDefaultValue("test");
		propDef1.setName("testProp");
		propDef1.setHidden(false);
		propDef1.setMandatory(false);
		propDef1.setReadOnly(false);
		propDef1.setDataType(DefaultPropertyDefinitionDataType.STRING);
		propDef1.setTypeIdentifier(ti);
		propDef2.setDefaultValue("test");
		propDef2.setName("testProp");		
		propDef2.setHidden(false);
		propDef2.setMandatory(false);
		propDef2.setReadOnly(false);
		propDef2.setDataType(DefaultPropertyDefinitionDataType.STRING);
		propDef2.setTypeIdentifier(ti);
		Assert.assertTrue(TransferJob.equals(propDef1, propDef2));
	}

}
