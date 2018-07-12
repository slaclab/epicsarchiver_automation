
import os
import sys
import logging

def configureLogging(verbose):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)
    #import http.client as http_client
    #http_client.HTTPConnection.debuglevel = 1
