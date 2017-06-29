#!/usr/bin/env python
'''
This is one of the steps used in automation of archiver configuraiton.
This assumes the requests for the archiver are somehow stored in the IOC (perhaps in INFO fields) and then submitted as archive files.
Archive files are consolidated together into a IOC_DATA like folder and can be identified using a bash glob expression.
Archive files are space/tab separated text files with these rules
1) They can have blank lines
2) They can have comment lines beginning with a #
3) Space/tab separated lines are interpreted as PVName, sampling period (in secs), sampling method (one of scan/monitor).
4) The PVName is mandatory; the other two are optional in which case defaults (optional arguments to this script)  are applied.
'''

import os
import sys
import argparse
import json
import subprocess
import shutil
import datetime
import shlex
import urllib
import urllib2
import logging
import multiplePVCheck


def getAllExpandedNames(bplURL):
    '''Get all expanded PV names (.VAL, .HIHI, aliases etc) from the archiver.'''
    url = bplURL + '/getAllExpandedPVNames'
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    the_page = response.read()
    expandedNames = set()
    expandedNames.update(json.loads(the_page))
    return expandedNames
    

def getUnarchivedPVs(bplURL, pvNames):
    '''Use the archivers unarchivedPVs call to determine those PVs that need processing.'''
    url = bplURL + '/unarchivedPVs'
    req = urllib2.Request(url, json.dumps(pvNames), {'Content-Type': 'application/json'})
    response = urllib2.urlopen(req)
    the_page = response.read()
    unarchivedPVs = json.loads(the_page)
    return unarchivedPVs

def archivePVs(bplURL, pvConfigs):
    '''Submit unarchived PVs to the archiver using archivePV's and a JSON batch submit'''
    try:
        url = bplURL + '/archivePV'
        req = urllib2.Request(url, json.dumps(pvConfigs), {'Content-Type': 'application/json'})
        response = urllib2.urlopen(req)
        the_page = response.read()
        submittedPVs = json.loads(the_page)
        return submittedPVs
    except Exception as e:
        logger.error("Exception submitting PVs to the archiver. Perhaps we have some invalid characters in the PV names")

def findChangedFiles(args):
    ''' Find all the archive files that have changed. We determine this by saving off a cached copy and then comparing the file the next time around'''
    dataFolder = args.dataFolder
    if not os.path.exists(dataFolder):
        raise Exception("Datafolder {} does not exists".format(dataFolder))
    rootFolder = args.rootFolder

    changedfiles = []
    files = subprocess.check_output("shopt -s globstar && cd " + args.rootFolder + " && ls -1 " + args.filenamepattern, shell=True).split()
    for fname in files:
        absoluteFName = os.path.join(rootFolder, fname)
        cachedArchiveFileName = os.path.join(dataFolder, fname)
        logger.debug("Cached file name %s", cachedArchiveFileName)
        if os.path.exists(cachedArchiveFileName) and abs((datetime.datetime.fromtimestamp(os.path.getmtime(absoluteFName)) - datetime.datetime.fromtimestamp(os.path.getmtime(cachedArchiveFileName))).total_seconds()) < 2:
            logger.debug("File %s has not changed", fname)
        else:
            changedfiles.append(fname)

    return changedfiles
            
        

def checkForLivenessAndSubmitToArchiver(args, expandedNames, batchedPVConfig):
    '''Check for liveness of PV and submit those PV's that are live to the archiver'''
    if not batchedPVConfig:
        return
    url = args.url
    logger.debug("Checking for liveness of %s PVs", len(batchedPVConfig.keys()))
    unarchivedLivePvs = multiplePVCheck.checkMultiplePVs(batchedPVConfig.keys(), float(args.timeout))
    if unarchivedLivePvs:
        logger.info("Submitting %s new PVs to the archiver", len(unarchivedLivePvs))
        logger.debug("Submitting these PVs to the archiver %s", ",".join(unarchivedLivePvs))
        unarchivedPVsConfig = [ batchedPVConfig[x] for x in unarchivedLivePvs ]
        archivePVs(url, unarchivedPVsConfig)
        expandedNames.update(unarchivedLivePvs)
    else:
        logger.debug("Skipped potentially %s stale PVs", len(batchedPVConfig.keys()))
    batchedPVConfig.clear()

def processFile(fname, args, expandedNames, batchedPVConfig):
    ''' Process the archive request file f and submit unarchived PV's to the archiver'''
    dataFolder = args.dataFolder
    rootFolder = args.rootFolder
    absoluteFName = os.path.join(rootFolder, fname)
    cachedArchiveFileName = os.path.join(dataFolder, fname)
   
    logger.debug("Processing file %s", absoluteFName) 

    with open(absoluteFName, 'r') as f:
        lines = f.readlines()

    pvConfigEntries = [shlex.split(x) for x in filter(lambda x : x.strip() and not x.startswith("#"), lines)]
    pvNames = set()
    pvNames.update([x[0] for x in pvConfigEntries])
    pvName2Config = { x[0] : { 'pv': x[0], 'samplingperiod': x[1] if len(x) > 1 else args.defaultSamplingPeriod, 'samplingmethod' : x[2].upper() if len(x) > 2 else args.defaultSamplingMethod } for x in pvConfigEntries}
    unarchivedPVs = pvNames.difference(expandedNames)
    if unarchivedPVs:
        batchedPVConfig.update({ x : pvName2Config[x] for x in unarchivedPVs })
        if len(batchedPVConfig) > args.batchsize:
            checkForLivenessAndSubmitToArchiver(args, expandedNames, batchedPVConfig)
    else:
        logger.debug("All %s PVs from %s are in the archiver", len(pvNames), fname)
    
    if not os.path.exists(os.path.dirname(cachedArchiveFileName)):
        os.makedirs(os.path.dirname(cachedArchiveFileName))
    if os.path.exists(cachedArchiveFileName):
        os.remove(cachedArchiveFileName)
    shutil.copy2(absoluteFName, cachedArchiveFileName)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('-v', "--verbose", action="store_true",  help="Turn on verbose logging")
    parser.add_argument('-b', "--batchsize", default=1000, type=int,  help="Batch size for submitting PV's to the archiver")
    parser.add_argument('-m', "--defaultSamplingMethod", default="monitor", help="If the IOC engr has not specified a sampling method, use this as the sampling method", choices=['MONITOR', 'SCAN'])
    parser.add_argument('-p', "--defaultSamplingPeriod", default=1, help="If the IOC engineer has not specified a sampling period, use this as the sampling period", type=int)
    parser.add_argument('-t', "--timeout", default="5", help="Specify the timeout to wait for all the PV's to connect")
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    parser.add_argument("dataFolder", help="Folder where we cache the archive files; we compare the new version against the cached copy to determine changes.")
    parser.add_argument("rootFolder", help="The root folder of all the IOC archive request files")
    parser.add_argument("filenamepattern", help="The extended shell matching pattern used to determine archive request files, for example, */archive/*.archive", default="*/archive/*.archive")

    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    files = findChangedFiles(args)
    batchedPVConfig = {}
    if files:
        expandedNames = getAllExpandedNames(args.url)
        for f in files:
            try:
                processFile(f, args, expandedNames, batchedPVConfig)
            except Exception as ex:
                logger.exception("Exception processing {}".format(f))
        checkForLivenessAndSubmitToArchiver(args, expandedNames, batchedPVConfig)

