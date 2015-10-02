package eu.w4.dsscli.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import eu.w4.common.exception.CheckedException;
import eu.w4.common.exception.UnsupportedException;
import eu.w4.dsscli.core.TransferStructure.DSSFolder;
import eu.w4.dsscli.core.TransferStructure.Part;
import eu.w4.engine.client.dss.ContentType;
import eu.w4.engine.client.dss.Document;
import eu.w4.engine.client.dss.DocumentContentPart;
import eu.w4.engine.client.dss.DssException;
import eu.w4.engine.client.dss.Folder;
import eu.w4.engine.client.dss.FolderAttachment;
import eu.w4.engine.client.dss.HasContentType;
import eu.w4.engine.client.dss.Item;
import eu.w4.engine.client.dss.ItemAttachment;
import eu.w4.engine.client.dss.PropertyDefinition;
import eu.w4.engine.client.dss.PropertyDefinitionDataType;
import eu.w4.engine.client.dss.Repository;
import eu.w4.engine.client.dss.RepositoryAttachment;
import eu.w4.engine.client.dss.RepositoryIdentifier;
import eu.w4.engine.client.dss.Type;
import eu.w4.engine.client.dss.TypeAttachment;
import eu.w4.engine.client.dss.TypeIdentifier;
import eu.w4.engine.client.dss.TypeType;

public class ExportJob extends TransferJob {
	@Override
	public String getDescription() {
		return "export DSS content to xml file. Just repository and item types are exported. To export also documents and folders use exportAll";
	}

	public void launch() throws Exception {
		exportAll(false);
	}

	public void exportAll(boolean docExtracted) throws Exception {
		String methodName = "exportAll";
		logger.entering(CLASS_NAME, methodName);
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
		Type itemType = item.getType();
		Map<String, PropertyDefinition> propDefMap = getPropDefinitions(itemType);

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

}
