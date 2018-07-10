#!/usr/bin/env python
'''Check the validity of multiple PV's; similar to caInfo1'''
import os
import epics
import time
import argparse
import fileinput

@epics.ca.withCA
def checkMultiplePVs(pvs, timeout):
    '''Connects to multiple PV's within the specified timeout. If we managed to connect to the PV, we add to the collection that is returned'''
    pv2chids = {}
    os.environ['EPICS_CA_CONN_TMO'] = str(timeout)
    def connect_cb(pvname, chid, conn):
        pv2chids[pvname] = chid
    for pv in pvs:
        channel = epics.ca.create_channel(pv, connect=False, callback=connect_cb)
    time.sleep(timeout)
    return pv2chids.keys()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Similar to caInfo1, this utility reads a list of PVs from the specified file and then checks to see if they are alive.")
    parser.add_argument('-v', '--verbose',   action='store_true', help='Enable verbose logging')
    parser.add_argument('-t', "--timeout", default="5", help="Specify the timeout to wait for all the PV's to connect")
    parser.add_argument('-c', "--connectedonly", action='store_true', help="Print only the connected PV's")
    parser.add_argument('-u', "--unconnectedonly", action='store_true', help="Print only the unconnected PV's")
    parser.add_argument("filename", help="The name of the file comtaining a list of PV's - one PV per line")

    args = parser.parse_args()

    pvs = []
    if args.filename and args.filename != "-":
        with open(args.filename, 'r') as f:
            lines = f.readlines()
            for line in lines:
                pvs.append(line.strip())
    else:
        for line in fileinput.input():
            pvs.append(line.strip())


    timeout = 5
    if 'timeout' in args:
        timeout = int(args.timeout)

    connectedPVs = checkMultiplePVs(pvs, timeout)

    if args.connectedonly:
        for pv in pvs:
            if pv in connectedPVs:
                print(pv)
    elif args.unconnectedonly:
        for pv in pvs:
            if pv not in connectedPVs:
                print(pv)
    else:
        for pv in pvs:
            if pv in connectedPVs:
                print("Connected %s" % pv)
            else:
                print("Not connected %s" % pv)
