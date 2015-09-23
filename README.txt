dsscli allow to import and export repositories, types, folders and/our documents to and from DSS. The command is accessible from the bin folder.

usage:dsscli [-cmd admin command] [-com communication mode] [-host hostname] [-port portNumber] [-usr user] [-pwd password] [-trace trace level] fileName
 -cmd : admin command - (default)export (export only types and repositories without folders and docs), import (import types and repositories), exportAll (export types, repositories, folders, documents and attached files), importAll (import types, repositories, folders and documents) 
 -host : hostname without port number
 -port : rmi port number
 -usr  : w4 user login
 -pwd  : w4 user password
 -trace : trace level - (default) OFF, ALL, CONFIG, FINE, FINER, FINEST, INFO, SEVERE, WARNING
