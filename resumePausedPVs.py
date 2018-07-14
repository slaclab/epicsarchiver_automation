#!/usr/bin/env python
'''This script gets a list of PVS that are paused and then resumes the archive request for each active PV.
Optionally, we can pass in a folder with a list of .archive files.
Only those paused PV's which are present in archive files that have been modified recently will be checked.
'''

import os
import sys
import argparse
import json
import requests
import logging
import multiplePVCheck
import shlex
import datetime

from utils import configureLogging
from processArchiveFiles import findChangedFiles

logger = logging.getLogger(__name__)


def getCurrentlyPausedPVs(bplURL):
    '''Get a list of PVs that are currently disconnected'''
    url = bplURL + '/getPausedPVsReport'
    currentlyPausedPVs = requests.get(url).json()
    return currentlyPausedPVs

def resumePVs(bplURL, pvNames):
    '''Bulk resume PVs specified in the pvNames'''
    url = bplURL + '/resumeArchivingPV'
    resumeResponse = requests.post(url, json=pvNames).json()
    return resumeResponse

def getPVsFromRecentlyChangedArchiveFiles(rootFolder, filenamepattern, ignoreolder):
    changedFiles = findChangedFiles(rootFolder, filenamepattern, ignoreolder)
    recentlyChangedPVs = set()
    for changedFile in changedFiles:
        lastModifiedTS = datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(rootFolder, changedFile)))
        logger.info("Processing recently changed file %s", changedFile)

        with open(os.path.join(rootFolder, changedFile), 'r') as f:
            lines = f.readlines()

        pvConfigEntries = [shlex.split(x) for x in filter(lambda x : x.strip() and not x.startswith("#"), lines)]
        pvNames = set()
        pvNames.update([x[0] for x in pvConfigEntries])
        recentlyChangedPVs = recentlyChangedPVs.union(pvNames)
    return recentlyChangedPVs

def checkForLivenessAndResume(args, batch):
    '''Check for liveness of PV's and then bulk resume those that are alive'''
    if not batch:
        return
    logger.debug("Checking for liveness of %s PVs", len(batch))
    livePVs = multiplePVCheck.checkMultiplePVs(batch, float(args.timeout))
    if livePVs:
        logger.info("Resuming %s live PVs", len(livePVs))
        resumePVs(args.url, list(livePVs))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', "--verbose", action="store_true",  help="Turn on verbose logging")
    parser.add_argument('-b', "--batchsize", default=1000, type=int,  help="Batch size for submitting PV's to the archiver")
    parser.add_argument('-t', "--timeout", default="5", help="Specify the timeout to wait for all the PV's to connect")
    parser.add_argument('-r', "--rootFolder", help="The root folder of all the IOC archive request files")
    parser.add_argument('-p', "--filenamepattern", help="The extended shell matching pattern used to determine archive request files, for example, */archive/*.archive", default="*/archive/*.archive")
    parser.add_argument('-i', "--ignoreolder", default="30", help="Ignore archive files whose last modified date is older than this many days.", type=int)
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")

    args = parser.parse_args()
    configureLogging(args.verbose)

    if not args.url.endswith('bpl'):
        logger.error("The URL %s needs to point to the mgmt bpl; for example, http://arch.slac.stanford.edu/mgmt/bpl. ", args.url)
        sys.exit(1)
    pausedPVs = getCurrentlyPausedPVs(args.url)
    if not pausedPVs:
        logger.info("There are no paused PVs")
        sys.exit(0)

    logger.info("%s PVs are paused", len(pausedPVs))

    if args.rootFolder and args.filenamepattern:
        pvNames = set([ x['pvName'] for x in pausedPVs ])
        recentlyChangedPVs = getPVsFromRecentlyChangedArchiveFiles(args.rootFolder, args.filenamepattern, args.ignoreolder)
        pvNames = pvNames.intersection(recentlyChangedPVs)
        logger.info("%s recently changed PVs are paused", len(pvNames))
        pvList = list(pvNames)
        if not pvList:
            logger.info("There are no recently changed paused PVs")
            sys.exit(0)
    else:
        pvList = [ x['pvName'] for x in pausedPVs ]

    def breakIntoBatches(l, n):
        '''Check for liveness and resume in batches specified by the batch size'''
        for i in range(0, len(l), n):
            yield l[i:i + n]
    for batch in breakIntoBatches(pvList, args.batchsize):
        checkForLivenessAndResume(args, batch)
