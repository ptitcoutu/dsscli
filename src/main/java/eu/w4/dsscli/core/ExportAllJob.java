package eu.w4.dsscli.core;

import java.util.List;

public class ExportAllJob extends ExportJob {
	public String getDescription() {
		return "export DSS content to xml file and for each repository create a folder structure. Repository, item types, Folders and documents are exported and each part of a document are exported as a file in the related folder. To export only repositories and item types use export command";
	}

	@Override
	public List<Option> getAvailableOptions() {
		List<Option> availableOptions = super.getAvailableOptions();
		availableOptions.add(new Option() {
			{
				name = "-limit";
				description = "content part (file imported to DSS or exported from DSS) size limit. Use G, M, K in order to precise respectively Go, Mo, Ko the default unit is Octet";
			}

			@Override
			public void setValue(String optionValue) {
				setSizeLimit(optionValue);
			}
		});
		return availableOptions;
	}

	public void launch() throws Exception {
		exportAll(true);
	}
}
