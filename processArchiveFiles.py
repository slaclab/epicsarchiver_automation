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
import logging
import multiplePVCheck
import requests

from utils import configureLogging

logger = logging.getLogger(__name__)


def getAllExpandedNames(bplURL):
    '''Get all expanded PV names (.VAL, .HIHI, aliases etc) from the archiver.'''
    expandedNames = set()
    url = bplURL + '/getAllExpandedPVNames'
    expandedNames.update(requests.get(url).json())
    return expandedNames


def getUnarchivedPVs(bplURL, pvNames):
    '''Use the archivers unarchivedPVs call to determine those PVs that need processing.'''
    url = bplURL + '/unarchivedPVs'
    unarchivedPVs = requests.post(url, json=pvNames).json()
    return unarchivedPVs

def archivePVs(bplURL, pvConfigs):
    '''Submit unarchived PVs to the archiver using archivePV's and a JSON batch submit'''
    try:
        url = bplURL + '/archivePV'
        submittedPVs = requests.post(url, json=pvConfigs).json()
        return submittedPVs
    except Exception as e:
        logger.error("Exception submitting PVs to the archiver. Perhaps we have some invalid characters in the PV names")

def findChangedFiles(rootFolder, filenamepattern, ignoreolder):
    ''' Find all the archive files that have changed. We determine this by checking the file's last modified timestamp.'''
    changedfiles = []
    files = subprocess.check_output("shopt -s globstar && cd " + rootFolder + " && ls -1 " + filenamepattern, shell=True).split()
    now = datetime.datetime.now()
    nIgnoredFiles = 0
    for fname in files:
        fname = fname.decode("utf-8")
        absoluteFName = os.path.join(rootFolder, fname)
        if abs((now - datetime.datetime.fromtimestamp(os.path.getmtime(absoluteFName))).total_seconds()) > ignoreolder*86400:
            nIgnoredFiles = nIgnoredFiles + 1
        else:
            logger.debug("Adding file %s that has been modified in less than %s days", absoluteFName, ignoreolder)
            changedfiles.append(fname)

    logger.debug("Ignored %d files older than %s days", nIgnoredFiles, ignoreolder)
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
        logger.info("Submitting these PVs to the archiver %s", ",".join(unarchivedLivePvs))
        unarchivedPVsConfig = [ batchedPVConfig[x] for x in unarchivedLivePvs ]
        archivePVs(url, unarchivedPVsConfig)
        expandedNames.update(unarchivedLivePvs)
    else:
        logger.debug("Skipped potentially %s stale PVs", len(batchedPVConfig.keys()))
    batchedPVConfig.clear()

def processFile(fname, args, expandedNames, batchedPVConfig):
    ''' Process the archive request file f and submit unarchived PV's to the archiver'''
    rootFolder = args.rootFolder
    absoluteFName = os.path.join(rootFolder, fname)
    logger.info("Processing file %s", absoluteFName)

    with open(absoluteFName, 'r') as f:
        lines = f.readlines()

    pvConfigEntries = [shlex.split(x) for x in filter(lambda x : x.strip() and not x.startswith("#"), lines)]
    pvNames = set()
    pvNames.update([x[0] for x in pvConfigEntries])
    pvName2Config = { x[0] : { 'pv': x[0], 'samplingperiod': str(x[1]) if len(x) > 1 else str(args.defaultSamplingPeriod), 'samplingmethod' : x[2].upper() if len(x) > 2 else args.defaultSamplingMethod } for x in pvConfigEntries}
    unarchivedPVs = pvNames.difference(expandedNames)
    if unarchivedPVs:
        batchedPVConfig.update({ x : pvName2Config[x] for x in unarchivedPVs })
        if len(batchedPVConfig) > args.batchsize:
            checkForLivenessAndSubmitToArchiver(args, expandedNames, batchedPVConfig)
    else:
        logger.debug("All %s PVs from %s are in the archiver", len(pvNames), fname)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', "--verbose", action="store_true",  help="Turn on verbose logging")
    parser.add_argument('-b', "--batchsize", default=1000, type=int,  help="Batch size for submitting PV's to the archiver")
    parser.add_argument('-m', "--defaultSamplingMethod", default="monitor", help="If the IOC engr has not specified a sampling method, use this as the sampling method", choices=['MONITOR', 'SCAN'])
    parser.add_argument('-p', "--defaultSamplingPeriod", default=1, help="If the IOC engineer has not specified a sampling period, use this as the sampling period", type=int)
    parser.add_argument('-i', "--ignoreolder", default="30", help="Ignore archive files whose last modified date is older than this many days.", type=int)
    parser.add_argument('-t', "--timeout", default="5", help="Specify the timeout to wait for all the PV's to connect")
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    parser.add_argument("rootFolder", help="The root folder of all the IOC archive request files")
    parser.add_argument("filenamepattern", help="The extended shell matching pattern used to determine archive request files, for example, */archive/*.archive", default="*/archive/*.archive")

    args = parser.parse_args()
    configureLogging(args.verbose)

    files = findChangedFiles(args.rootFolder, args.filenamepattern, args.ignoreolder)
    batchedPVConfig = {}
    if files:
        expandedNames = getAllExpandedNames(args.url)
        for f in files:
            try:
                processFile(f, args, expandedNames, batchedPVConfig)
            except Exception as ex:
                logger.exception("Exception processing {}".format(f))
        checkForLivenessAndSubmitToArchiver(args, expandedNames, batchedPVConfig)
