#!/usr/bin/python

import sys
import re
import os
from os.path import getsize
from azure.storage.blob import BlobService
from azure.storage.sharedaccesssignature import SharedAccessSignature, SharedAccessPolicy
from azure.storage.models import AccessPolicy
import datetime
from urlparse import urlparse

FILE_TYPE_LOCAL = 'local'
FILE_TYPE_BLOB = 'blob'
FILE_TYPE_TABLE = 'table'
FILE_TYPE_UNKNOWN = 'UNKNOWN'

ENDPOINT_BLOB = 'blob'
ENDPOINT_TABLE = 'table'

helptext = '------------------------------------------------------------------------------\n'
helptext += 'AzCopy 5.0.0 Copyright (c) 2015 Microsoft Corp. All Rights Reserved.\n'
helptext += '------------------------------------------------------------------------------\n'
helptext += '# AzCopy is designed for high-performance uploading, downloading, and copying data to and from Microsoft Azure Blob, File, and Table storage.\n'
helptext += '\n'
helptext += '# Command Line Usage:\n'
helptext += '	AzCopy /Source:<source> /Dest:<destination> [options]\n'
helptext += '\n'
helptext += '# Options:\n'
helptext += '    [/SourceKey:] [/DestKey:] [/SourceSAS:] [/DestSAS:] [/V:] [/Z:] [/@:] [/Y]\n'
helptext += '    [/NC:] [/SourceType:] [/DestType:] [/S] [/Pattern:] [/CheckMD5] [/L] [/MT]\n'
helptext += '    [/XN] [/XO] [/A] [/IA] [/XA] [/SyncCopy] [/SetContentType] [/BlobType:]\n'
helptext += '    [/Delimiter:] [/Snapshot] [/PKRS:] [/SplitSize:] [/EntityOperation:]\n'
helptext += '    [/Manifest:] [/PayloadFormat:]\n'
helptext += '\n'
helptext += '------------------------------------------------------------------------------\n'
helptext += 'For AzCopy command-line help, type one of the following commands:\n'
helptext += '# Detailed command-line help for AzCopy      ---   AzCopy /?\n'
helptext += '# Detailed help for any AzCopy option        ---   AzCopy /?:SourceKey\n'
helptext += '# Command line samples                       ---   AzCopy /?:Sample\n'
helptext += 'You can learn more about AzCopy at http://aka.ms/azcopy.\n'
helptext += '------------------------------------------------------------------------------\n'

helptextlong = 'long help text\n'
helptextlong += 'no long text'

ARG_HELP = '?'
ARG_VERBOSE = 'verbose'
ARG_SOURCE = 'Source'
ARG_DEST = 'Dest'
ARG_SRCKEY = 'SourceKey'
ARG_DESTKEY = 'DestKey'
ARG_SRCSAS = 'SourceSAS'
ARG_DESTSAS = 'DestSAS'

verboselogfile = 'V'
journalfilefolder = 'Z'
paramfile = '@'
autoconfirm = 'Y'
numberofthreads = 'NC'

#
ARG_SRCTYPE = 'SourceType'
ARG_DESTTYPE = 'DestType'
#
ARG_RECURSIVE = 'S'
ARG_PATTERN = 'Pattern'
checkmd5 = 'CheckMD5'
listing = 'L'
modifyTime = 'MT'
excludeNewe = 'XN'
excludeOlder = 'XO'
archiveOnly = 'A'
includeAttributes = 'IA'
excludeAttributes = 'XA'
synCopy = 'SyncCopy'
contentType = 'SetContentType'
# blob
blobType = 'BlobType'
delimiter = 'Delimiter'
snapshot = 'Snapshot'
#table
splitKey = 'PKRS'
splitsize ='SplitSize'
entityOperation = 'EntityOperation'
manifest = 'Manifest'
payloadFormat = 'PayloadFormat'

def argcontains(argarray, argument):	
	if (('/' + str(argument)) in argarray):
		return True
	else:
		for arg in argarray:
			argmatch = re.match('/' + str(argument) + ':(.*)', str(arg))
			if (argmatch):
				return True
	return False

def log(message, verbose):
	if (verbose and argcontains(sys.argv, ARG_VERBOSE)):
		print message
	elif (not verbose):
		print message

def getFileType(filename):
	filematch = re.match('(.*?)://(.*)', str(filename))
	if (filematch):
		log('matching regex', True)
		protocol = filematch.group(1)
		if (protocol == 'http' or protocol == 'https'):
			log('protocol is http(s), endpoint is ' + str(filematch.group(2)), True)
			typeMatch = re.match('(\S*?)\.(\S*?)\..*', str(filematch.group(2)))
			if typeMatch and typeMatch.group(2) == ENDPOINT_TABLE:
				return FILE_TYPE_TABLE
			elif typeMatch and typeMatch.group(2) == ENDPOINT_BLOB:
				return FILE_TYPE_BLOB
			elif typeMatch:
				log('unknown endpoint ' + str(typeMatch.group(1)), False)
		else:
			return FILE_TYPE_UNKNOWN
	else:
		return FILE_TYPE_LOCAL
	
def getArgument(argarray, argument):
	log('getting arg ' + str(argument), True)
	if (argcontains(argarray, argument)):
		for arg in argarray:
			argmatch = re.match('/' + str(argument) + ':(.*)', str(arg))
			if (argmatch):
				log('found', True)
				return argmatch.group(1)

def split_storage_url(url):
    parsed = urlparse(url)
    h=parsed.hostname.split(".", 1)
    p=parsed.path.split("/", 1)
    if len(p) < 3:
        p.append(None)
    return (h[0], h[1], p[1], p[2])

def copyLocalFileToAzure(sourceFile, destUrl, destKey, recurse, pattern):
    if os.path.isdir(sourceFile):
        if recurse:
            for root, directories, files in os.walk(sourceFile):
                for file in files:
                    fullpath = os.path.join(root, file)
                    if pattern:
                        filenamematch = re.match(pattern, str(file))
                        if not filenamematch:
                            continue
                    
                    uploadFile(fullpath, dest, destKey)
        else:
            for file in os.listdir(sourceFile):
                fullpath = os.path.join(sourceFile, file)
                if (os.path.isfile(fullpath)):
                    if pattern:
                        filenamematch = re.match(pattern, str(file))
                        if not filenamematch:
                            continue
                    
                    uploadFile(fullpath, dest, destKey)
    else:
        uploadFile(sourceFile, dest, destKey)
                    
def uploadFile(sourceFile, destUrl, destKey):
    storageparts = split_storage_url(destUrl)
    blobservice = BlobService(storageparts[0], destKey)
    try:
        fh=open(sourceFile, "r")
    except:
        print "No such file", sourceFile
        return
    log('uploading ' + str(sourceFile), True)
    blobservice.put_page_blob_from_file(storageparts[2], sourceFile, fh, getsize(sourceFile))

def copyBlobToBlob(sourceUrl, sourceKey, destUrl, destKey):
    blobservice = BlobService(destUrl, destkey)
    srcblobservice = BlobService(SourceUrl, srckey)
    today = datetime.datetime.utcnow()
    todayPlusMonth = today + datetime.timedelta(1)
    todayPlusMonthISO = todayPlusMonth.replace(microsecond=0).isoformat() + 'Z'
    srcSasParam = srcblobservice.generate_shared_access_signature(container,
            filename, SharedAccessPolicy(AccessPolicy(None, todayPlusMonthISO, "r"), None))
    srcUrl = srcblobservice.make_blob_url(container, filename,
            sas_token=srcSasParam)
    print srcUrl
    blobservice.copy_blob(container, filename, srcUrl)

def copyTableToTable(source, dest):
	log('copyTableToTable(' + str(source) + ',' + str(dest) + ')', False)

#print 'starting'

if (len(sys.argv) == 1):
	print helptext
	exit(0)
elif (argcontains(sys.argv, ARG_HELP)):
	print helptextlong
	exit(0)

# TODO: check correct call

source = getArgument(sys.argv, ARG_SOURCE)
dest = getArgument(sys.argv, ARG_DEST)
sourceType = getArgument(sys.argv, ARG_SRCTYPE)
if not sourceType:
	sourceType = getFileType(source)
destType = getArgument(sys.argv, ARG_DESTTYPE)
if not destType:
	destType = getFileType(dest)

sourceKey = getArgument(sys.argv, ARG_SRCKEY)
destKey = getArgument(sys.argv, ARG_DESTKEY)
recurse = argcontains(sys.argv, ARG_RECURSIVE)
pattern = getArgument(sys.argv, ARG_PATTERN)

log('source:' + str(source), False)
log('srctype:' + str(sourceType), False)
log('dest:' + str(dest), False)
log('desttype:' + str(destType), False)

if (sourceType == FILE_TYPE_LOCAL) and (destType == FILE_TYPE_BLOB):
	copyLocalFileToAzure(source, dest, destKey, recurse, pattern)
elif (sourceType == FILE_TYPE_BLOB) and (destType == FILE_TYPE_BLOB):
	copyBlobToBlob(source, dest)
elif (sourceType == FILE_TYPE_TABLE) and (destType == FILE_TYPE_TABLE):
	copyTableToTable(source, dest)

#print 'Number of arguments:', len(sys.argv), 'arguments.'
#print 'Argument List:', str(sys.argv)
