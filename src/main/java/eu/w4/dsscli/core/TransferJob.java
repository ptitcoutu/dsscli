package eu.w4.dsscli.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.rmi.RemoteException;
import java.security.Principal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import eu.w4.common.configuration.ConfigurationParameter;
import eu.w4.common.exception.CheckedException;
import eu.w4.common.exception.UnsupportedException;
import eu.w4.dsscli.core.TransferStructure.DSSFolder;
import eu.w4.dsscli.core.TransferStructure.ItemPropertyDef;
import eu.w4.dsscli.core.TransferStructure.ItemType;
import eu.w4.dsscli.core.TransferStructure.MDSimpleType;
import eu.w4.dsscli.core.TransferStructure.Part;
import eu.w4.dsscli.core.TransferStructure.Property;
import eu.w4.engine.client.configuration.NetworkConfigurationParameter;
import eu.w4.engine.client.dss.ContentType;
import eu.w4.engine.client.dss.DefaultContentType;
import eu.w4.engine.client.dss.DefaultHasContentType;
import eu.w4.engine.client.dss.DefaultPropertyDefinitionDataType;
import eu.w4.engine.client.dss.DefaultTypeType;
import eu.w4.engine.client.dss.Document;
import eu.w4.engine.client.dss.DocumentContentPart;
import eu.w4.engine.client.dss.DssException;
import eu.w4.engine.client.dss.Folder;
import eu.w4.engine.client.dss.FolderAlreadyExistsException;
import eu.w4.engine.client.dss.FolderAttachment;
import eu.w4.engine.client.dss.HasContentType;
import eu.w4.engine.client.dss.Item;
import eu.w4.engine.client.dss.ItemAlreadyExistsException;
import eu.w4.engine.client.dss.ItemAttachment;
import eu.w4.engine.client.dss.ItemIdentifier;
import eu.w4.engine.client.dss.PropertyDefinition;
import eu.w4.engine.client.dss.PropertyDefinitionDataType;
import eu.w4.engine.client.dss.Repository;
import eu.w4.engine.client.dss.RepositoryAttachment;
import eu.w4.engine.client.dss.RepositoryIdentifier;
import eu.w4.engine.client.dss.Type;
import eu.w4.engine.client.dss.TypeAttachment;
import eu.w4.engine.client.dss.TypeIdentifier;
import eu.w4.engine.client.dss.TypeType;
import eu.w4.engine.client.dss.service.DocumentStorageService;
import eu.w4.engine.client.dss.service.DocumentStorageServiceObjectFactory;
import eu.w4.engine.client.service.AuthenticationService;
import eu.w4.engine.client.service.EngineService;
import eu.w4.engine.client.service.EngineServiceFactory;

public class TransferJob {
	private String login = "admin";
	private String password = "admin";
	private String hostname = "localhost";
	private String portNumber = "7707";
	private DocumentStorageService dssService = null;
	private DocumentStorageServiceObjectFactory dssObjFactory = null;
	private Principal usrPrincipal = null;
	private EngineService engineService = null;
	private AuthenticationService authService = null;
	private String filename = "extractedData.xml";
	public static Logger logger = Logger.getLogger("RepositoryAdmin");
	public static String CLASS_NAME = TransferJob.class.getName();
	private long sizeLimit = Long.MAX_VALUE;

	public TransferJob() {
	}

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

	public void exportAll(boolean docExtracted) {
		String methodName = "exportAll";
		logger.entering(CLASS_NAME, methodName);
		try {
			JAXBContext context = JAXBContext.newInstance(TransferStructure.class);
			Marshaller marshaller = context.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

			TransferStructure extractResult = new TransferStructure();
			// Export types
			List<TransferStructure.ItemType> extractTypes = extractResult.itemTypes = new ArrayList<TransferStructure.ItemType>();
			TypeAttachment typeAtt = dssObjFactory.newTypeAttachment();
			typeAtt.setParentAttached(true);
			typeAtt.setPropertyDefinitionsAttached(true);
			exportTypes(extractTypes, dssService.getTypes(usrPrincipal, typeAtt), true);

			// Export repositories
			List<TransferStructure.Repository> extractReps = new ArrayList<TransferStructure.Repository>();
			extractResult.repositories = (extractReps);
			RepositoryAttachment repAtt = dssObjFactory.newRepositoryAttachment();
			for (Repository repository : dssService.getRepositories(usrPrincipal, repAtt)) {
				TransferStructure.Repository extractRepository = new TransferStructure.Repository();
				extractReps.add(extractRepository);
				final RepositoryIdentifier repId = repository.getIdentifier();
				final long repositoryLongId = repId.getId();
				final String repositoryName = repId.getName();
				extractRepository.id = repositoryLongId;
				extractRepository.name = repositoryName;
				List<TransferStructure.ItemType> extractDocTypes = new ArrayList<TransferStructure.ItemType>();
				extractRepository.itemTypes = (extractDocTypes);

				typeAtt.setParentAttached(false);
				typeAtt.setPropertyDefinitionsAttached(false);
				Collection<Type> itemTypes = dssService.getTypes(usrPrincipal, repId, typeAtt);

				// export document type
				exportTypes(extractDocTypes, itemTypes, false);

				// Export folders
				if (docExtracted) {
					FolderAttachment fldAtt = dssObjFactory.newFolderAttachment();
					fldAtt.setParentAttached(true);
					fldAtt.setPropertiesAttached(true);
					fldAtt.setTypeAttached(true);
					Folder dssRootFolder = dssService.getRootFolder(usrPrincipal, repId, fldAtt);
					extractRepository.rootFolder = extractDSSFolder(dssRootFolder, new File(repositoryName));
				}
			}

			System.setProperty("file.encoding", "UTF-8");
			OutputStreamWriter targetFW = new OutputStreamWriter(new FileOutputStream(filename), "UTF-8");
			marshaller.marshal(extractResult, targetFW);
			targetFW.close();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "error while executing commend", e);
		}
		logger.exiting(CLASS_NAME, methodName);
	}

	private void exportTypes(List<TransferStructure.ItemType> extractDocTypes, Collection<Type> itemTypes,
			boolean notOnlyIdentifier) {
		for (Type itemType : itemTypes) {
			TransferStructure.ItemType extractDocType = new TransferStructure.ItemType();
			extractDocTypes.add(extractDocType);
			TypeIdentifier typeIdent = itemType.getIdentifier();
			String typeName = typeIdent.getName();
			String typePrefix = typeIdent.getPrefix();
			long typeId = typeIdent.getId();
			extractDocType.id = (typeId);
			extractDocType.name = (typeName);
			extractDocType.prefix = typePrefix;
			if (notOnlyIdentifier) {
				Type parentType = itemType.getParentType();
				TypeIdentifier parentTypeId = (parentType == null) ? null : parentType.getIdentifier();
				extractDocType.parentPrefix = (parentTypeId == null) ? "" : parentTypeId.getPrefix();
				extractDocType.parentName = (parentTypeId == null) ? "" : parentTypeId.getName();
				ContentType contentType = itemType.getContentType();
				TypeType typeType = itemType.getType();
				HasContentType hasContent = itemType.getHasContent();
				extractDocType.contentType = (contentType == null) ? "" : (contentType.toString());
				extractDocType.typeType = (typeType == null) ? "" : (typeType.toString());
				extractDocType.hasContentType = (hasContent == null) ? null : hasContent.toString();
				List<TransferStructure.ItemPropertyDef> itemPropertyDefs = new ArrayList<TransferStructure.ItemPropertyDef>();
				extractDocType.propertyDefs = (itemPropertyDefs);
				Map<String, PropertyDefinition> propertiesDef = itemType.getPropertyDefinitions();
				for (Entry<String, PropertyDefinition> propertyDefEntry : propertiesDef.entrySet()) {
					TransferStructure.ItemPropertyDef itemPropertyDef = new TransferStructure.ItemPropertyDef();
					itemPropertyDefs.add(itemPropertyDef);
					PropertyDefinition propertyDef = propertyDefEntry.getValue();
					String propertyDefName = propertyDefEntry.getKey();
					Object propDefaultValue = propertyDef.getDefaultValue();
					itemPropertyDef.id = propertyDef.getId();
					itemPropertyDef.name = (propertyDefName);
					itemPropertyDef.defaultValue = ((propDefaultValue == null) ? null : propDefaultValue.toString());

					itemPropertyDef.mandatory = (propertyDef.isMandatory());
					itemPropertyDef.hidden = (propertyDef.isHidden());
					itemPropertyDef.readonly = (propertyDef.isReadOnly());
					itemPropertyDef.valueType = (propertyDef.getDataType().toString());
				}
			}
		}
	}

	private DSSFolder extractDSSFolder(Folder dssFolder, File currentDir)
			throws DssException, CheckedException, IOException {
		TransferStructure.DSSFolder extractedFolder = new TransferStructure.DSSFolder();
		TypeIdentifier folderTypeId = dssFolder.getType().getIdentifier();
		extractedFolder.itemType = folderTypeId.getPrefix() + ":" + folderTypeId.getName();
		extractedFolder.properties = extractMetadata(dssFolder);
		String folderName = (String) dssFolder.getName();
		if (folderName == null || folderName.length() <= 0) {
			folderName = "/";
		} else {
			currentDir = new File(currentDir, folderName);
		}
		extractedFolder.name = folderName;
		List<TransferStructure.DSSFolder> folders = extractedFolder.folders = new ArrayList<TransferStructure.DSSFolder>();
		List<TransferStructure.Document> documents = extractedFolder.documents = new ArrayList<TransferStructure.Document>();
		ItemAttachment itemAtt = dssObjFactory.newItemAttachment();
		itemAtt.setParentAttached(false);
		itemAtt.setPropertiesAttached(true);
		itemAtt.setTypeAttached(true);
		for (Item childItem : dssService.getChildItems(usrPrincipal, dssFolder.getIdentifier(), itemAtt)) {
			// if (ecmService.isFolder(sessionId, childItem.getId())) {
			if (childItem instanceof Folder) {
				TransferStructure.DSSFolder childFolder = extractDSSFolder((Folder) childItem, currentDir);
				folders.add(childFolder);
			} else {
				TransferStructure.Document childDocument = extractDocument((Document) childItem, currentDir);
				documents.add(childDocument);
			}
		}
		return extractedFolder;
	}

	private TransferStructure.Document extractDocument(Document dssDocument, File currentDir)
			throws DssException, CheckedException, IOException {
		TransferStructure.Document extractedDoc = new TransferStructure.Document();
		extractedDoc.name = (String) dssDocument.getName();
		File docFile = new File(currentDir, extractedDoc.name);

		TypeIdentifier docTypeId = dssDocument.getType().getIdentifier();
		extractedDoc.itemType = docTypeId.getPrefix() + ":" + docTypeId.getName();
		extractedDoc.properties = extractMetadata(dssDocument);
		List<DocumentContentPart> docContentParts = dssService.getDocumentContentParts(usrPrincipal,
				dssDocument.getIdentifier());
		boolean onlyOnePart = docContentParts != null && docContentParts.size() == 1;
		List<Part> parts = new ArrayList<Part>();
		for (DocumentContentPart docContentPart : docContentParts) {
			byte[] docContent = docContentPart.getContent();
			if (!currentDir.exists()) {
				currentDir.mkdirs();
			}
			if (docFile.exists()) {
				docFile.delete();
			}
			final String docName = extractedDoc.name;
			final String partName = docContentPart.getName();
			final String partFileName;
			if (onlyOnePart && docName.equals(partName)) {
				partFileName = docName;
			} else {
				partFileName = docName + "__part__" + partName;
			}
			File partFile = new File(currentDir, partFileName);
			FileOutputStream fileOut = new FileOutputStream(partFile);
			fileOut.write(docContent);
			fileOut.close();
			Part part = new Part();
			part.name = partName;
			part.path = partFile.getPath();
			part.length = docContentPart.getLength();
			part.mimeType = docContentPart.getMimeType();
			parts.add(part);
		}
		extractedDoc.parts = parts;
		return extractedDoc;
	}

	private List<TransferStructure.Property> extractMetadata(Item item) throws DssException {
		List<TransferStructure.Property> properties = new ArrayList<TransferStructure.Property>();
		Map<String, Object> itemMetadatas = item.getProperties();
		Map<String, PropertyDefinition> propDefMap = item.getType().getPropertyDefinitions();
		for (Entry<String, Object> itemMetadata : itemMetadatas.entrySet()) {
			String propName = itemMetadata.getKey();
			PropertyDefinition propDef = propDefMap.get(propName);
			if (propDef == null) {
				throw new UnsupportedException("can't property " + propName + " without its definition");
			}
			PropertyDefinitionDataType propDataType = propDef.getDataType();
			if (propDataType.isList()) {
				throw new UnsupportedException("list property xml conversion not implemented yet");
			}
			Converter converter = propValConverter.get(propDataType);

			TransferStructure.Property property = new TransferStructure.Property();
			property.name = propName;
			Object val = itemMetadata.getValue();
			try {
				property.value = converter.convertToString(val);
			} catch (ClassCastException e) {
				logger.log(Level.WARNING, "unable to convert property " + propName + " of type "
						+ converter.getSimpleType() + " the value is converted using toString()", e);
				property.value = (val == null) ? "" : val.toString();
			}
			property.valueType = converter.getSimpleType();

			properties.add(property);
		}
		return properties;
	}

	public String getLogin() {
		return login;
	}

	public void setLogin(String login) {
		this.login = login;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getPortNumber() {
		return portNumber;
	}

	public void setPortNumber(String portNumber) {
		this.portNumber = portNumber;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String fileName) {
		this.filename = fileName;
	}

	public long getSizeLimit() {
		return sizeLimit;
	}

	public void setSizeLimit(long sizeLimit) {
		this.sizeLimit = sizeLimit;
	}

	public void setSizeLimit(String sizeLimit) {
		if (sizeLimit.matches("^\\d+$")) {
			setSizeLimit(Long.parseLong(sizeLimit));
		} else {
			Pattern p = Pattern.compile("^(\\d+)([kKmMgG])$");
			Matcher m = p.matcher(sizeLimit);
			m.matches();
			long numericSizeLimit = Long.parseLong(m.group(1));
			if ("G".equals(m.group(2).toUpperCase())) {
				numericSizeLimit *= 1024;
			}
			if ("M".equals(m.group(2).toUpperCase())) {
				numericSizeLimit *= 1024;
			}
			if ("K".equals(m.group(2).toUpperCase())) {
				numericSizeLimit *= 1024;
			}
			setSizeLimit(numericSizeLimit);
		}
	}

	public void importAll(boolean docImported) {
		String methodName = "importAll";
		logger.entering(CLASS_NAME, methodName);

		try {
			JAXBContext context = JAXBContext.newInstance(TransferStructure.class);
			Unmarshaller unmarshaller = context.createUnmarshaller();
			TransferStructure structure = (TransferStructure) unmarshaller.unmarshal(new File(filename));
			RepositoryAttachment repAtt = dssObjFactory.newRepositoryAttachment();
			Collection<Repository> repositories = dssService.getRepositories(usrPrincipal, repAtt);
			Map<String, Repository> repMap = new HashMap<String, Repository>();
			for (Repository rep : repositories) {
				repMap.put(rep.getIdentifier().getName(), rep);
			}
			TypeAttachment typeAtt = dssObjFactory.newTypeAttachment();
			typeAtt.setParentAttached(true);
			typeAtt.setPropertyDefinitionsAttached(true);

			Collection<Type> itemTypes = dssService.getTypes(usrPrincipal, typeAtt);
			Map<String, Type> typeMap = new HashMap<String, Type>();
			for (Type itemType : itemTypes) {
				typeMap.put(itemType.getIdentifier().getPrefix() + ":" + itemType.getIdentifier().getName(), itemType);
			}
			List<DelayedOp> delayedImports = new LinkedList<TransferJob.DelayedOp>();
			for (TransferStructure.ItemType importedItemType : structure.itemTypes) {
				String itemTypeIdStr = importedItemType.prefix + ":" + importedItemType.name;
				if (typeMap.containsKey(itemTypeIdStr)) {
					DelayedOp delayedImport = importExistingType(typeMap, typeMap.get(itemTypeIdStr), importedItemType);
					if (delayedImport != null) {
						delayedImports.add(delayedImport);
					}
				} else {
					DelayedOp delayedImport = importNewType(typeMap, importedItemType);
					if (delayedImport != null) {
						delayedImports.add(delayedImport);
					}
				}
			}
			int previousSize = 0;
			List<DelayedOp> newDelayedImports = new ArrayList<TransferJob.DelayedOp>();
			while (delayedImports.size() != previousSize) {
				previousSize = delayedImports.size();
				for (DelayedOp delayedImport : delayedImports) {
					DelayedOp newDelayedImport = delayedImport.doOp();
					if (newDelayedImport != null) {
						newDelayedImports.add(newDelayedImport);
					}
				}
				delayedImports = newDelayedImports;
			}

			for (TransferStructure.Repository repStructure : structure.repositories) {
				RepositoryIdentifier repParentId = null;
				if (repMap.containsKey(repStructure.name)) {
					repParentId = repMap.get(repStructure.name).getIdentifier();
				} else {
					repParentId = dssService.createRepository(usrPrincipal, repStructure.name);
				}
				if (repStructure.itemTypes != null && repStructure.itemTypes.size() > 0) {
					// pour chaque repository, création des types
					importDefinitions(repParentId, repMap, repStructure, typeMap);
				}
				if (docImported && repStructure.rootFolder != null) {
					// pour chaque repository, création des répertoires
					createNewRepository(repParentId, repMap, repStructure, typeMap);
				}
			}
		} catch (final JAXBException exception) {
			logger.log(Level.SEVERE, exception.getMessage(), exception);
			System.exit(-1);
		} catch (final DssException exception) {
			logger.log(Level.SEVERE, exception.getMessage(), exception);
			System.exit(-1);
		} catch (final Exception exception) {
			logger.log(Level.SEVERE, exception.getMessage(), exception);
			System.exit(-1);
		} finally {
			try {
				authService.logout(usrPrincipal);
			} catch (Exception logoutException) {
				logger.log(Level.WARNING, "Error while trying to logout", logoutException);
			}
		}

		logger.exiting(CLASS_NAME, methodName);
	}

	private DelayedOp importNewType(final Map<String, Type> typeMap, final ItemType importedItemType)
			throws DssException, RemoteException, CheckedException {
		String itPrefix = importedItemType.prefix;
		String itName = importedItemType.name;

		String itpPrefix = importedItemType.parentPrefix;
		itpPrefix = (itpPrefix == null) ? "" : itpPrefix;

		String itpName = importedItemType.parentName;
		itpName = (itpName == null) ? "" : itpName;

		String itContentType = importedItemType.contentType;
		String itHasContentType = importedItemType.hasContentType;
		String itType = importedItemType.typeType;
		Type newTypeParent = null;

		if (itpName.length() != 0) {
			String itpTypeStr = itpPrefix + ":" + itpName;
			if (typeMap.containsKey(itpTypeStr)) {
				newTypeParent = typeMap.get(itpTypeStr);
			} else {
				return new DelayedOp() {
					public DelayedOp doOp() throws DssException, RemoteException, CheckedException {
						return importNewType(typeMap, importedItemType);
					}
				};
			}
		}
		TypeIdentifier newTypeId = dssService.createType(usrPrincipal,
				(newTypeParent == null) ? null : newTypeParent.getIdentifier(), itPrefix, itName,
				DefaultContentType.valueOf(itContentType), DefaultHasContentType.valueOf(itHasContentType),
				DefaultTypeType.valueOf(itType), null);
		TypeAttachment typeAtt = dssObjFactory.newTypeAttachment();
		typeAtt.setParentAttached(true);
		typeAtt.setPropertyDefinitionsAttached(true);
		typeMap.put(newTypeId.getPrefix() + ":" + newTypeId.getName(),
				dssService.getType(usrPrincipal, newTypeId, typeAtt));
		List<ItemPropertyDef> propertyDefs = importedItemType.propertyDefs;
		computePropertiesDef(newTypeId, propertyDefs, null, null);
		return null;
	}

	private static interface DelayedOp {
		public DelayedOp doOp() throws DssException, RemoteException, CheckedException;
	}

	public static boolean equals(Object obj1, Object obj2) {
		if (obj1 == null) {
			return obj2 == null;
		} else {
			return obj1.equals(obj2);
		}
	}

	public static boolean equals(PropertyDefinition pDef1, PropertyDefinition pDef2) {
		boolean eq = true;
		if (pDef1 == null && pDef2 == null) {
			eq = true;
		} else if (pDef1 == null) {
			eq = false;
		} else {
			eq = eq && equals(pDef1.getName(), pDef2.getName());
			eq = eq && equals(pDef1.getDefaultValue(), pDef2.getDefaultValue());
			eq = eq && equals(pDef1.getDataType(), pDef2.getDataType());
			eq = eq && equals(pDef1.getTypeIdentifier(), pDef2.getTypeIdentifier());
			eq = eq && (pDef1.isHidden() == pDef2.isHidden());
			eq = eq && (pDef1.isMandatory() == pDef2.isMandatory());
			eq = eq && (pDef1.isReadOnly() == pDef2.isReadOnly());
		}
		return eq;
	}

	private DelayedOp importExistingType(final Map<String, Type> typeMap, final Type existingType,
			final ItemType importedItemType) throws DssException, RemoteException, CheckedException {
		boolean changedData = false;
		String importedItemTypeContentType = importedItemType.contentType;

		String itPrefix = importedItemType.prefix;
		String itName = importedItemType.name;

		String itpPrefix = importedItemType.parentPrefix;
		itpPrefix = (itpPrefix == null) ? "" : itpPrefix;

		String itpName = importedItemType.parentName;
		itpName = (itpName == null) ? "" : itpName;

		String itContentType = importedItemType.contentType;
		String itHasContentType = importedItemType.hasContentType;
		String itType = importedItemType.typeType;

		String tPrefix = existingType.getIdentifier().getPrefix();
		String tName = existingType.getIdentifier().getName();

		Type parentType = existingType.getParentType();

		String tpPrefix = (parentType == null) ? "" : parentType.getIdentifier().getPrefix();
		String tpName = (parentType == null) ? "" : parentType.getIdentifier().getName();

		ContentType tContentTypeObj = existingType.getContentType();
		String tContentType = (tContentTypeObj == null) ? "" : tContentTypeObj.toString();
		HasContentType tHasContentTypeObj = existingType.getHasContent();
		String tHasContentType = (tHasContentTypeObj == null) ? "" : tHasContentTypeObj.toString();
		TypeType tTypeObject = existingType.getType();
		String tType = (tTypeObject == null) ? "" : tTypeObject.toString();
		if (!tpPrefix.equals(itpPrefix) || !tpName.equals(itpName)) {
			if (itpName.length() == 0) {
				existingType.setParentType(null);
				changedData = true;
			} else {
				String itpTypeStr = itpPrefix + ":" + itpName;
				if (typeMap.containsKey(itpTypeStr)) {
					existingType.setParentType(typeMap.get(itpTypeStr));
				} else {
					return new DelayedOp() {
						public DelayedOp doOp() throws DssException, RemoteException, CheckedException {
							return importExistingType(typeMap, existingType, importedItemType);
						}
					};
				}
			}
		}
		if(!changedData) {
			// TODO : Implement HasContentType comparison and tType comparison
		}
		if (changedData) {
			logger.warning("type " + tPrefix + ":" + tName + " definition is different but could not be changed");
		}
		List<ItemPropertyDef> propertyDefs = importedItemType.propertyDefs;
		int[] nbChange = new int[1];
		nbChange[0] = 0;
		computePropertiesDef(existingType.getIdentifier(), propertyDefs, existingType.getPropertyDefinitions(),
				nbChange);
		if (nbChange[0] > 0) {
			// TODO : Add log
		}
		return null;
	}

	static public interface Converter {
		public Object convertToObject(String objStringValue);

		public MDSimpleType getSimpleType();

		public String convertToString(Object objValue);
	}

	private Map<PropertyDefinitionDataType, Converter> propValConverter = new HashMap<PropertyDefinitionDataType, TransferJob.Converter>();

	{
		propValConverter.put(DefaultPropertyDefinitionDataType.BINARY, new Converter() {

			@Override
			public Object convertToObject(String objStringValue) {
				return (objStringValue == null) ? null : Base64.getDecoder().decode(objStringValue);
			}

			@Override
			public MDSimpleType getSimpleType() {
				return MDSimpleType.BINARY;
			}

			@Override
			public String convertToString(Object objValue) {
				return (objValue == null) ? null : Base64.getEncoder().encodeToString((byte[]) objValue);
			}
		});
		propValConverter.put(DefaultPropertyDefinitionDataType.BOOLEAN, new Converter() {

			@Override
			public Object convertToObject(String objStringValue) {
				return (objStringValue == null) ? null : new Boolean(objStringValue);
			}

			@Override
			public MDSimpleType getSimpleType() {
				return MDSimpleType.BOOLEAN;
			}

			@Override
			public String convertToString(Object objValue) {
				return (objValue == null) ? null : Boolean.toString((Boolean) objValue);
			}
		});
		propValConverter.put(DefaultPropertyDefinitionDataType.DATE, new Converter() {

			@Override
			public Object convertToObject(String objStringValue) {
				try {
					return (objStringValue == null) ? null : isoDateFormat.parse(objStringValue);
				} catch (ParseException e) {
					throw new RuntimeException("error while trying to parse date " + objStringValue, e);
				}
			}

			@Override
			public MDSimpleType getSimpleType() {
				return MDSimpleType.DATE;
			}

			@Override
			public String convertToString(Object objValue) {
				return (objValue == null) ? null : isoDateFormat.format((Date) objValue);
			}
		});
		propValConverter.put(DefaultPropertyDefinitionDataType.DOUBLE, new Converter() {

			@Override
			public Object convertToObject(String objStringValue) {
				return (objStringValue == null) ? null : new Double(objStringValue);
			}

			@Override
			public MDSimpleType getSimpleType() {
				return MDSimpleType.DOUBLE;
			}

			@Override
			public String convertToString(Object objValue) {
				return (objValue == null) ? null : ((Double) objValue).toString();
			}
		});

		propValConverter.put(DefaultPropertyDefinitionDataType.LONG, new Converter() {

			@Override
			public Object convertToObject(String objStringValue) {
				return (objStringValue == null) ? null : new Long(objStringValue);
			}

			@Override
			public MDSimpleType getSimpleType() {
				return MDSimpleType.LONG;
			}

			@Override
			public String convertToString(Object objValue) {
				return (objValue == null) ? null : objValue.toString();
			}
		});

		propValConverter.put(DefaultPropertyDefinitionDataType.STRING, new Converter() {

			@Override
			public Object convertToObject(String objStringValue) {
				return objStringValue;
			}

			@Override
			public MDSimpleType getSimpleType() {
				return MDSimpleType.STRING;
			}

			@Override
			public String convertToString(Object objValue) {
				return (String) objValue;
			}
		});

	}

	private Object computeStringValue(String strValue, PropertyDefinitionDataType dataType) {
		Object res = null;
		if (dataType != null) {
			if (dataType.isList()) {
				/*
				 * PropertyDefinitionDataType dt =
				 * DefaultPropertyDefinitionDataType
				 * .valueOf(dataType.getElementType().getName().toUpperCase());
				 * Converter valConverter = propValConverter.get(dt);
				 */
				throw new UnsupportedOperationException("this code path is not yet achieved...");
			} else {
				Converter valConverter = propValConverter.get(dataType);
				res = valConverter.convertToObject(strValue);
			}
		} else {
			throw new UnsupportedOperationException("cannot convert data " + strValue + " without its data type");
		}
		return res;
	}

	private Collection<PropertyDefinition> computePropertiesDef(TypeIdentifier typeId,
			List<ItemPropertyDef> importedPropertiesDef, Map<String, PropertyDefinition> existingPropertiesDef,
			int[] nbChange) throws DssException, RemoteException, CheckedException {
		Collection<PropertyDefinition> res = new ArrayList<PropertyDefinition>();
		Map<String, PropertyDefinition> existingPropDefMap = new HashMap<String, PropertyDefinition>();
		if (existingPropertiesDef != null) {
			existingPropDefMap.putAll(existingPropertiesDef);
		}
		for (ItemPropertyDef impPropertyDef : importedPropertiesDef) {
			String valueType = impPropertyDef.valueType;
			String defaultValue = impPropertyDef.defaultValue;
			String propName = impPropertyDef.name;
			PropertyDefinition propDef = dssObjFactory.newPropertyDefinition();
			PropertyDefinitionDataType propDataType = DefaultPropertyDefinitionDataType.valueOf(valueType);
			propDef.setDataType(propDataType);
			propDef.setDefaultValue(computeStringValue(defaultValue, propDataType));
			propDef.setHidden(impPropertyDef.hidden);
			propDef.setMandatory(impPropertyDef.mandatory);
			propDef.setName(propName);
			propDef.setReadOnly(impPropertyDef.readonly);
			res.add(propDef);
			if (existingPropertiesDef != null && existingPropDefMap.containsKey(propName)) {
				// check existing property def
				boolean hasDiff = false;
				PropertyDefinition existingPropDef = existingPropDefMap.get(propName);
				hasDiff = equals(existingPropDef, propDef);
				if (hasDiff) {
					dssService.modifyPropertyDefinition(usrPrincipal, existingPropDef.getId(), typeId, propName,
							propDataType, propDef.isMandatory(), propDef.isReadOnly(), propDef.isHidden(),
							propDef.getDefaultValue());
					nbChange[0]++;
				}
				existingPropDefMap.remove(propName);
			} else {
				// add new property def
				dssService.addPropertyDefinition(usrPrincipal, typeId, propName, propDataType, propDef.isMandatory(),
						propDef.isReadOnly(), propDef.isHidden(), propDef.getDefaultValue());
				if (nbChange != null) {
					nbChange[0]++;
				}
			}
		}
		if (existingPropertiesDef != null && existingPropDefMap.size() > 0) {
			// the remaining properties definition have to be removed
			for (PropertyDefinition propDef : existingPropDefMap.values()) {
				dssService.removePropertyDefinition(usrPrincipal, typeId, propDef.getName());
			}
			nbChange[0]++;
		}
		return res;
	}

	private void importDefinitions(RepositoryIdentifier repParentId, Map<String, Repository> repositories,
			TransferStructure.Repository repStructure, Map<String, Type> typeMap) throws Exception {
		Collection<Type> types = dssService.getTypes(usrPrincipal, repParentId, null);
		Map<String, Type> repositoryTypesMap = new HashMap<String, Type>();
		for (Type type : types) {
			repositoryTypesMap.put(type.getIdentifier().getPrefix() + ":" + type.getIdentifier().getName(), type);
		}
		for (TransferStructure.ItemType extractDocType : repStructure.itemTypes) {
			String typeIdStr = extractDocType.prefix + ":" + extractDocType.name;
			if (!repositoryTypesMap.containsKey(typeIdStr)) {
				if (typeMap.containsKey(typeIdStr)) {
					TypeIdentifier typeId = dssObjFactory.newTypeIdentifier();
					typeId.setName(extractDocType.name);
					typeId.setPrefix(extractDocType.prefix);
					dssService.addRepositoryType(usrPrincipal, repParentId, typeId);
				} else {
					logger.log(Level.WARNING, "can't add type " + typeIdStr + " to repository " + repParentId.getName()
							+ ". Type is not defined nor in target dss nor in source file ");
				}
			}
		}
	}

	private void createNewRepository(RepositoryIdentifier repParentId, Map<String, Repository> repositories,
			TransferStructure.Repository repStructure, Map<String, Type> typeMap) throws Exception {
		FolderAttachment folderAtt = dssObjFactory.newFolderAttachment();
		folderAtt.setPropertiesAttached(true);
		folderAtt.setParentAttached(true);
		folderAtt.setTypeAttached(true);
		Folder parentFolder = dssService.getRootFolder(usrPrincipal, repParentId, folderAtt);
		createNewFolder(usrPrincipal, true, parentFolder, repStructure.rootFolder, typeMap);
	}

	final SimpleDateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	private void createNewFolder(Principal usrPrincipal, boolean isRoot, Folder parentFolder,
			TransferStructure.DSSFolder folderStructure, Map<String, Type> typeMap) throws Exception {
		Map<String, Object> folderProperties = new HashMap<String, Object>();
		final Type itemType;
		final TypeIdentifier itemTypeId;
		final String folderName = folderStructure.name;

		if (folderStructure.itemType != null && !folderStructure.itemType.equals("")
				&& typeMap.containsKey(folderStructure.itemType)) {
			itemType = typeMap.get(folderStructure.itemType);
			itemTypeId = itemType.getIdentifier();
		} else {
			itemType = null;
			itemTypeId = null;
		}
		if (itemTypeId == null) {
			throw new RuntimeException("invalid folder type :" + folderStructure.itemType);
		}
		if (folderStructure.properties != null) {
			parseProperties(folderStructure.properties, folderProperties, itemType);
		}

		Folder folder = null;
		try {
			if (isRoot) {
				folder = parentFolder;
			} else {
				ItemIdentifier folderId = dssService.createFolder(usrPrincipal, parentFolder.getIdentifier(),
						itemTypeId, folderProperties);
				FolderAttachment folderAtt = dssObjFactory.newFolderAttachment();
				folderAtt.setParentAttached(false);
				folderAtt.setPropertiesAttached(true);
				folderAtt.setTypeAttached(true);
				folder = dssService.getFolder(usrPrincipal, folderId, folderAtt);
			}
		} catch (FolderAlreadyExistsException e) {
			try {
				ItemAttachment itemsAtt = dssObjFactory.newItemAttachment();
				itemsAtt.setPropertiesAttached(true);
				itemsAtt.setTypeAttached(true);
				Collection<Item> items = dssService.getChildItems(usrPrincipal, parentFolder.getIdentifier(), itemsAtt);
				for (Item item : items) {
					if (item instanceof Folder) {
						String itemName = item.getName();
						if (folderName.equals(itemName)) {
							folder = (Folder) item;
							break;
						}
					}
				}
				if (folder == null) {
					throw new RuntimeException("can't find folder " + folderName);
				}
			} catch (DssException ee) {
				logger.log(Level.WARNING, ee.getMessage(), ee);
				return;
			}
		}
		if (folderStructure.folders != null) {
			for (TransferStructure.DSSFolder childFolder : folderStructure.folders) {
				createNewFolder(usrPrincipal, false, folder, childFolder, typeMap);
			}
		}

		if (folderStructure.documents != null) {
			for (final TransferStructure.Document document : folderStructure.documents) {
				createNewDocument(usrPrincipal, folder, document, typeMap);
			}
		}

	}

	private void createNewDocument(final Principal usrPrincipal, final Folder parentFolder,
			final TransferStructure.Document documentStructure, final Map<String, Type> typeMap) throws Exception {
		Map<String, Object> docProperties = new HashMap<String, Object>();

		final Type docType;
		final TypeIdentifier itemTypeId;
		if (documentStructure.itemType != null && !documentStructure.itemType.equals("")) {
			docType = typeMap.get(documentStructure.itemType);
			itemTypeId = docType.getIdentifier();
		} else {
			throw new UnsupportedException("can't add document " + documentStructure.name + " without type");
		}
		if (documentStructure.properties != null) {
			parseProperties(documentStructure.properties, docProperties, docType);
		}

		try {
			final DocumentContentPart[] docParts;
			if (documentStructure.parts == null || documentStructure.parts.isEmpty()) {
				docParts = null;
			} else {
				docParts = new DocumentContentPart[documentStructure.parts.size()];
				int docPartsIndex = 0;
				for (Part part : documentStructure.parts) {
					final byte[] content;

					File contentFile = new File(part.path);
					if (contentFile.length() > sizeLimit) {
						content = null;
						logger.severe("File " + part.path + " exceeds size limit (" + contentFile.length() + ")");
					} else {
						if (!contentFile.exists()) {
							contentFile = new File(part.path);
							if (!contentFile.exists()) {
								throw new Exception("Cannot load content for document [" + documentStructure.name
										+ "], looking at [" + contentFile.getAbsolutePath() + "/" + part.path + "] or ["
										+ part.path + "]");
							}
						}
						content = new byte[(int) contentFile.length()];
						FileInputStream fis = null;
						try {
							fis = new FileInputStream(contentFile);
							int totalBytes = 0;
							while (totalBytes < contentFile.length()) {
								final int readedBytes = fis.read(content);
								totalBytes += readedBytes;
							}
						} finally {
							if (fis != null) {
								fis.close();
							}
						}
					}
					DocumentContentPart docContentPart = dssObjFactory.newDocumentContentPart();
					docContentPart.setContent(content);
					docContentPart.setMimeType(part.mimeType);
					docContentPart.setName(part.name);
					docParts[docPartsIndex] = docContentPart;
					docPartsIndex++;
				}
			}
			try {
				dssService.createDocument(usrPrincipal, parentFolder.getIdentifier(), itemTypeId, docProperties,
						docParts);
			} catch (Throwable t) {
				if (t instanceof ItemAlreadyExistsException) {
					throw (ItemAlreadyExistsException) t;
				} else {
					logger.log(Level.SEVERE, "can't create " + documentStructure.name, t);
				}
			}
		} catch (final ItemAlreadyExistsException exception) {
			logger.log(Level.WARNING, exception.getMessage());
		}
	}

	private void parseProperties(final List<Property> itemProperties, Map<String, Object> properties, Type itemType)
			throws ParseException {
		Map<String, PropertyDefinition> propDefMap = itemType.getPropertyDefinitions();
		for (Property tmpProperty : itemProperties) {
			PropertyDefinition propDef = propDefMap.get(tmpProperty.name);
			if (!propDef.isReadOnly()) {
				Converter converter = propValConverter
						.get(DefaultPropertyDefinitionDataType.valueOf(tmpProperty.valueType.name()));

				////// THIS PORTION IS A WORKAROUND ABOUT LAST_VERSION_CREATOR
				////// WHICH SHOULD BE A STRING
				if (tmpProperty.name.equals(DocumentStorageService.PROPERTY__LAST_VERSION_CREATOR)) {
					properties.put(tmpProperty.name, tmpProperty.value);
				} else {
					////// END OF WORKAROUND

					properties.put(tmpProperty.name, converter.convertToObject(tmpProperty.value));
				}
			}
		}
	}

}
