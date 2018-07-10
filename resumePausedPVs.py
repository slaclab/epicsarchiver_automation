#!/usr/bin/env python
'''This script gets a list of PVS that are paused and then resumes the archive request for each active PV'''

import os
import sys
import argparse
import json
import requests
import logging
import multiplePVCheck


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
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('-v', "--verbose", action="store_true",  help="Turn on verbose logging")
    parser.add_argument('-b', "--batchsize", default=1000, type=int,  help="Batch size for submitting PV's to the archiver")
    parser.add_argument('-t', "--timeout", default="5", help="Specify the timeout to wait for all the PV's to connect")
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")

    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        #import http.client as http_client
        #http_client.HTTPConnection.debuglevel = 1

    if not args.url.endswith('bpl'):
        logger.error("The URL %s needs to point to the mgmt bpl; for example, http://arch.slac.stanford.edu/mgmt/bpl. ", args.url)
        sys.exit(1)
    pvList = getCurrentlyPausedPVs(args.url)
    if not pvList:
        logger.info("There are no paused PVs")
        sys.exit(0)

    logger.info("%s PVs are paused", len(pvList))
    pvNames = [ x['pvName'] for x in pvList ]
    def breakIntoBatches(l, n):
        '''Check for liveness and resume in batches specified by the batch size'''
        for i in range(0, len(l), n):
            yield l[i:i + n]
    for batch in breakIntoBatches(pvNames, args.batchsize):
        checkForLivenessAndResume(args, batch)
