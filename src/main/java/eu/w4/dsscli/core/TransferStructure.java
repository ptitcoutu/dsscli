package eu.w4.dsscli.core;

import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="dssTransfer")
public class TransferStructure {
	@XmlElements(@XmlElement(name = "itemType", type = TransferStructure.ItemType.class))
	public List<ItemType> itemTypes;
	
	@XmlElements(@XmlElement(name = "repository", type = TransferStructure.Repository.class))
	public List<Repository> repositories;
	
	public TransferStructure() {
	}

	static public class Repository {
		@XmlAttribute
		public String name;

		@XmlAttribute
		public long id;

		@XmlElements(@XmlElement(name = "itemType", type = TransferStructure.ItemType.class))
		public List<ItemType> itemTypes;
		
		@XmlElements(@XmlElement(name = "rootFolder", type = TransferStructure.DSSFolder.class))
		public DSSFolder rootFolder;
	}

	static public class ItemType {
		@XmlAttribute
		public long id;
		
		@XmlAttribute
		public String prefix;

		@XmlAttribute
		public String name;

		@XmlAttribute
		public String parentPrefix;

		@XmlAttribute
		public String parentName;

		
		@XmlAttribute
		public String contentType;

		@XmlAttribute
		public String hasContentType;

		@XmlAttribute
		public String typeType;		

		@XmlElements(@XmlElement(name = "propertyDef", type = TransferStructure.ItemPropertyDef.class))
		public List<ItemPropertyDef> propertyDefs;
	}

	static public class ItemPropertyDef {
		@XmlAttribute
		public long id;

		@XmlAttribute
		public String name;
		
		@XmlAttribute
		public String valueType;

		@XmlAttribute
		public String defaultValue;

		@XmlAttribute
		public boolean hidden;

		@XmlAttribute
		public boolean mandatory;

		@XmlAttribute
		public boolean readonly;
	}

	static public class DSSFolder {
		@XmlAttribute
		public String name;

		@XmlAttribute
		public String itemType;

		@XmlElements(@XmlElement(name = "property", type = TransferStructure.Property.class))
		public List<Property> properties;
		
		@XmlElements(@XmlElement(name = "folder", type = TransferStructure.DSSFolder.class))
		public List<DSSFolder> folders;

		@XmlElements(@XmlElement(name = "document", type = TransferStructure.Document.class))
		public List<Document> documents;


	}

	static public class Document {
		@XmlAttribute
		public String name;

		@XmlAttribute
		public String itemType;

		@XmlElements(@XmlElement(name = "property", type = TransferStructure.Property.class))
		public List<Property> properties;
		
		@XmlElements(@XmlElement(name = "part", type = Part.class))
		public List<Part> parts;
	}

	static public class Part {
		@XmlAttribute
		public String name;

		@XmlAttribute
		public long length;
		
		@XmlAttribute
		public String path;

		@XmlAttribute
		public String mimeType;
		
	}
	static public class Property {
		@XmlAttribute
		public String name;

		@XmlAttribute
		public String value;

		@XmlAttribute
		public MDSimpleType valueType;
	}

	public enum MDSimpleType {
		LONG, DOUBLE, DATE, STRING, BOOLEAN, BINARY, LONG_LIST, DOUBLE_LIST, DATE_LIST,STRING_LIST, BOOLEAN_LIST, BINARY_LIST
	}
}
