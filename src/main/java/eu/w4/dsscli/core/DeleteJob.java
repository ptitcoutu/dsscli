package eu.w4.dsscli.core;

import java.util.Collection;
import java.util.List;

import eu.w4.engine.client.dss.Document;
import eu.w4.engine.client.dss.Folder;
import eu.w4.engine.client.dss.Item;
import eu.w4.engine.client.dss.ItemIdentifier;
import eu.w4.engine.client.dss.Repository;
import eu.w4.engine.client.dss.RepositoryIdentifier;
import eu.w4.engine.client.dss.Type;
import eu.w4.engine.client.dss.TypeIdentifier;

public class DeleteJob extends AbstractJob {
	String targetType = "item";

	@Override
	public String getDescription() {
		return "delete the object designated by the file name (if content type is as type the file name is the type name)";
	}

	@Override
	public List<Option> getAvailableOptions() {
		List<Option> options = super.getAvailableOptions();
		options.add(new Job.Option() {
			{
				name = "-t";
				description = "target type : item, item_type, repository. By default target is an item";
			}

			@Override
			public void setValue(String optionValue) {
				if ("item".equals(optionValue) || "item_type".equals(optionValue) || "repository".equals(optionValue)) {
					targetType = optionValue;
				} else {
					throw new RuntimeException("invalid option value : " + optionValue + " must be item or item_type");
				}
			}
		});
		return options;
	}

	@Override
	public void launch() throws Exception {
		if ("item".equals(targetType) || "repository".equals(targetType)) {
			String analysedPath = filename;
			if (analysedPath.startsWith("/")) {
				analysedPath = analysedPath.substring(1);
			}
			String[] path = analysedPath.split("\\/");
			ItemIdentifier itemId = null;
			RepositoryIdentifier repId = null;
			ItemIdentifier rootFolderId = null;
			Repository rep = null;
			boolean isRoot = true;
			Boolean isDocument = null;
			for (String targetName : path) {
				if (isRoot) {
					repId = dssObjFactory.newRepositoryIdentifier();
					repId.setName(targetName);
					rep = dssService.getRepository(usrPrincipal, repId, null);
					repId = rep.getIdentifier();
					Folder folder = dssService.getRootFolder(usrPrincipal, repId, null);
					itemId = folder.getIdentifier();
					rootFolderId = itemId;
					isDocument = false;
					isRoot = false;
					if ("repository".equals(targetType)) {
						Collection<Item> childItems = dssService.getChildItems(usrPrincipal, rootFolderId, null);
						for (Item item : childItems) {
							if (item instanceof Document) {
								dssService.deleteDocument(usrPrincipal, item.getIdentifier());
							} else {
								dssService.deleteFolder(usrPrincipal, item.getIdentifier());
							}
						}
						Collection<Type> itemTypes = dssService.getTypes(usrPrincipal, repId, null);
						for (Type itemType : itemTypes) {
							dssService.removeRepositoryType(usrPrincipal, repId, itemType.getIdentifier());
						}
						dssService.deleteRepository(usrPrincipal, repId);
						return;
					}
				} else {
					Collection<Item> childItems = dssService.getChildItems(usrPrincipal, itemId, null);
					boolean childItemFound = false;
					for (Item childItem : childItems) {
						String childName = childItem.getName();
						if (childName != null && childName.equals(targetName)) {
							itemId = childItem.getIdentifier();
							isDocument = childItem instanceof Document;
							childItemFound = true;
							break;
						}
					}
					if (!childItemFound) {
						throw new RuntimeException("invalid path " + filename + " " + targetName + " not found");
					}
				}
			}
			if (itemId == rootFolderId) {
				throw new RuntimeException("cannot delete root folder of a repository. use -t repository");
			}
			if (isDocument) {
				dssService.deleteDocument(usrPrincipal, itemId);
			} else {
				dssService.deleteFolder(usrPrincipal, itemId);
			}
		} else {
			if (filename == null || filename.trim().length() == 0) {
				throw new RuntimeException("Precise the type id which is defined as prefix:name");
			}
			if (!filename.contains(":")) {
				throw new RuntimeException("Wrong type id : " + filename + " type id is defined as prefix:name");
			}
			String[] typeName = filename.split(":");
			TypeIdentifier typeId = dssObjFactory.newTypeIdentifier();
			typeId.setPrefix(typeName[0]);
			typeId.setName(typeName[1]);
			dssService.deleteType(usrPrincipal, typeId);
		}
	}
}
