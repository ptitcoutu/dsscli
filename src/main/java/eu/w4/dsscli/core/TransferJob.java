package eu.w4.dsscli.core;

import java.rmi.RemoteException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.w4.common.exception.CheckedException;
import eu.w4.dsscli.core.TransferStructure.ItemPropertyDef;
import eu.w4.dsscli.core.TransferStructure.MDSimpleType;
import eu.w4.dsscli.core.TransferStructure.Property;
import eu.w4.engine.client.dss.DefaultPropertyDefinitionDataType;
import eu.w4.engine.client.dss.DssException;
import eu.w4.engine.client.dss.PropertyDefinition;
import eu.w4.engine.client.dss.PropertyDefinitionDataType;
import eu.w4.engine.client.dss.Type;
import eu.w4.engine.client.dss.TypeIdentifier;
import eu.w4.engine.client.dss.service.DocumentStorageService;

abstract public class TransferJob extends AbstractJob {
	public static String CLASS_NAME = TransferJob.class.getName();
	protected long sizeLimit = Long.MAX_VALUE;
	final SimpleDateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	public TransferJob() {
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

	public static interface DelayedOp {
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

	static public interface Converter {
		public Object convertToObject(String objStringValue);

		public MDSimpleType getSimpleType();

		public String convertToString(Object objValue);
	}

	protected Map<PropertyDefinitionDataType, Converter> propValConverter = new HashMap<PropertyDefinitionDataType, TransferJob.Converter>();

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

	protected Object computeStringValue(String strValue, PropertyDefinitionDataType dataType) {
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

	protected Collection<PropertyDefinition> computePropertiesDef(TypeIdentifier typeId,
			List<ItemPropertyDef> importedPropertiesDef, Map<String, PropertyDefinition> existingPropertiesDef,
			int[] nbChange) throws DssException, RemoteException, CheckedException {
		Collection<PropertyDefinition> res = new ArrayList<PropertyDefinition>();
		Map<String, PropertyDefinition> existingPropDefMap = new HashMap<String, PropertyDefinition>();
		if (existingPropertiesDef != null) {
			existingPropDefMap.putAll(existingPropertiesDef);
		}
		if (importedPropertiesDef != null) {
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
					dssService.addPropertyDefinition(usrPrincipal, typeId, propName, propDataType,
							propDef.isMandatory(), propDef.isReadOnly(), propDef.isHidden(), propDef.getDefaultValue());
					if (nbChange != null) {
						nbChange[0]++;
					}
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

	protected void parseProperties(final List<Property> itemProperties, Map<String, Object> properties, Type itemType)
			throws ParseException {
		Map<String, PropertyDefinition> propDefMap = getPropDefinitions(itemType);
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

	public Map<String, Map<String, PropertyDefinition>> itemTypePropDefsMap = new HashMap<String, Map<String, PropertyDefinition>>();

	public Map<String, PropertyDefinition> getPropDefinitions(Type itemType) {
		TypeIdentifier typeId = itemType.getIdentifier();
		String typeIdStr = typeId.getPrefix() + ":" + typeId.getName();
		if (itemTypePropDefsMap.containsKey(typeIdStr)) {
			return itemTypePropDefsMap.get(typeIdStr);
		}
		Map<String, PropertyDefinition> propDefMap = new HashMap<String, PropertyDefinition>();
		Type parentType = itemType;
		do {
			propDefMap.putAll(parentType.getPropertyDefinitions());
			parentType = parentType.getParentType();
		} while (parentType != null);
		itemTypePropDefsMap.put(typeIdStr, propDefMap);
		return propDefMap;
	}
}
