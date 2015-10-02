package eu.w4.dsscli.core;

import java.util.List;

public class ImportAllJob extends ImportJob {
	public String getDescription() {
		return "import file content to DSS. Repository, item types, Folders and documents are imported. To import only repositories and item types use import command";
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
		importFromFileToDSS(true);
	}
}
