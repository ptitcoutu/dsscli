package eu.w4.dsscli.core;

import java.io.File;
import java.io.FileInputStream;
import java.rmi.RemoteException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import eu.w4.common.exception.CheckedException;
import eu.w4.common.exception.UnsupportedException;
import eu.w4.dsscli.core.TransferStructure.ItemPropertyDef;
import eu.w4.dsscli.core.TransferStructure.ItemType;
import eu.w4.dsscli.core.TransferStructure.Part;
import eu.w4.engine.client.dss.ContentType;
import eu.w4.engine.client.dss.DefaultContentType;
import eu.w4.engine.client.dss.DefaultHasContentType;
import eu.w4.engine.client.dss.DefaultTypeType;
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
import eu.w4.engine.client.dss.Repository;
import eu.w4.engine.client.dss.RepositoryAttachment;
import eu.w4.engine.client.dss.RepositoryIdentifier;
import eu.w4.engine.client.dss.Type;
import eu.w4.engine.client.dss.TypeAttachment;
import eu.w4.engine.client.dss.TypeIdentifier;
import eu.w4.engine.client.dss.TypeType;

public class ImportJob extends TransferJob {
	@Override
	public String getDescription() {
		return "import file content to DSS. Just repository and item types are imported. To import also documents and folders use importAll";
	}

	public void launch() throws Exception {
		importFromFileToDSS(false);
	}

	public void importFromFileToDSS(boolean docImported) throws Exception {
		String methodName = "importAll";
		logger.entering(CLASS_NAME, methodName);

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
				(itContentType == null || itContentType.isEmpty())?null:DefaultContentType.valueOf(itContentType), (itHasContentType==null || itHasContentType.isEmpty())?null:DefaultHasContentType.valueOf(itHasContentType),
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
		if (!changedData) {
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

}
