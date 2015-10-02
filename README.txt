dsscli allow to import and export repositories, types, folders and/our documents to and from DSS. The command is accessible from the home folder.
in order to install dsscli :
   gradlew build
   (or gradle if gradle is already installed)

usage:dsscli <command> [-host hostname] [-port portNumber] [-usr user] [-pwd password] [-trace trace level] [specific command options] fileName
<command> : command to launch or help to display this message or version to have version number of the DSS CLI
 -host : hostname without port number
 -port : tcp port number
 -usr  : w4 user login
 -pwd  : w4 user password
 -trace : trace level - (default) OFF, ALL, CONFIG, FINE, FINER, FINEST, INFO, SEVERE, WARNING
available commands are : 
delete : delete the object designated by the file name (if content type is as type the file name is the type name)
  -t : target type : item, item_type, repository. By default target is an item
export : export DSS content to xml file. Just repository and item types are exported. To export also documents and folders use exportAll
exportAll : export DSS content to xml file and for each repository create a folder structure. Repository, item types, Folders and documents are exported and each part of a document are exported as a file in the related folder. To export only repositories and item types use export command
  -limit : content part (file imported to DSS or exported from DSS) size limit. Use G, M, K in order to precise respectively Go, Mo, Ko the default unit is Octet
import : import file content to DSS. Just repository and item types are imported. To import also documents and folders use importAll
importAll : import file content to DSS. Repository, item types, Folders and documents are imported. To import only repositories and item types use import command
  -limit : content part (file imported to DSS or exported from DSS) size limit. Use G, M, K in order to precise respectively Go, Mo, Ko the default unit is Octet
