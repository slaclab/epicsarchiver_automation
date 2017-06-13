# epicsarchiver_automation
A set of scripts that can be used to automate maintenance of an EPICS Archiver Appliance.

### Background
The EPICS Archiver Appliance is an archiver for EPICS control systems that seeks to archive millions of PVs.
One of the goals of the project is to support automated maintenance of the system.
In an ideal world, the archive requests are part of the IOC package itself; either as INFO fields or as a separate archive file.
This package contains a few scripts that use archive files to automate submission of new PVs to the archivers. 
In addition, there are few other scripts to
- Automatically resume paused PV's that are now live.
- Automatically pause PV's that have not connected for more than a certain amount of time.
Most of these scripts are expected to run as cron jobs periodically on machines that have access to the PV's in the control system (perhaps using a EPICS_CA_ADDR_LIST different from the archiver).




