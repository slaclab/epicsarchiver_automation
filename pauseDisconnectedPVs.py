#!/usr/bin/env python
'''This script gets a list of PVS that are disconnected and then pauses those that have not connected for a specified amount of time'''

import os
import sys
import argparse
import json
import datetime
import requests
import logging
import multiplePVCheck

from utils import configureLogging

logger = logging.getLogger(__name__)

def getCurrentlyDisconnectedPVs(bplURL):
    '''Get a list of PVs that are currently disconnected'''
    url = bplURL + '/getCurrentlyDisconnectedPVs'
    currentlyDisconnectedPVs = requests.get(url).json()
    return currentlyDisconnectedPVs

def pausePVs(bplURL, pvNames):
    '''Bulk pause PVs specified in the pvNames'''
    url = bplURL + '/pauseArchivingPV'
    pauseResponse = requests.post(url, json=pvNames).json()
    return pauseResponse


def checkForLivenessAndPause(args, batch):
    '''Check for liveness of PV's and then bulk pause those that have not yet connected'''
    if not batch:
        return
    logger.debug("Checking for liveness of %s PVs", len(batch))
    livePVs = multiplePVCheck.checkMultiplePVs(batch, float(args.timeout))
    disConnPVs = list(set(batch) - set(livePVs))
    if disConnPVs:
        logger.info("Detected %s disconnected PVs", len(disConnPVs))
        pausePVs(args.url, disConnPVs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', "--verbose", action="store_true",  help="Turn on verbose logging")
    parser.add_argument('-b', "--batchsize", default=1000, type=int,  help="Batch size for submitting PV's to the archiver")
    parser.add_argument('-t', "--timeout", default="5", help="Specify the timeout to wait for all the PV's to connect")
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    parser.add_argument("disconnTimeout", help="Pause those PV's that have not connected for this amount of time (in minutes")

    args = parser.parse_args()
    configureLogging(args.verbose)

    if not args.url.endswith('bpl'):
        logger.error("The URL %s needs to point to the mgmt bpl; for example, http://arch.slac.stanford.edu/mgmt/bpl", args.url)
        sys.exit(1)
    pvList = getCurrentlyDisconnectedPVs(args.url)
    if not pvList:
        logger.info("There are no disconnected PVs")
        sys.exit(0)

    logger.info("%s PVs are disconnected", len(pvList))
    pvsThatHaveNotConnectedForTimeout = list(filter(lambda x : (abs((datetime.datetime.fromtimestamp(float(x['noConnectionAsOfEpochSecs'])) - datetime.datetime.now()).total_seconds()) > int(args.disconnTimeout)*60), pvList))
    if not pvsThatHaveNotConnectedForTimeout:
        logger.info("There are no PV's that have been disconnected for more than %s minute(s)", args.disconnTimeout)
        sys.exit(0)

    logger.info("%s PVs have been disconnected for more than %s minutes", len(pvsThatHaveNotConnectedForTimeout), args.disconnTimeout)
    pvNames = [ x['pvName'] for x in pvsThatHaveNotConnectedForTimeout]
    def breakIntoBatches(l, n):
        '''Check for liveness and pause in batches specified by the batch size'''
        for i in range(0, len(l), n):
            yield l[i:i + n]
    for batch in breakIntoBatches(pvNames, args.batchsize):
        checkForLivenessAndPause(args, batch)
