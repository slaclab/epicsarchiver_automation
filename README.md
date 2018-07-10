# epicsarchiver_automation
A set of python scripts that can be used to automate maintenance of an EPICS Archiver Appliance.

### Background
The EPICS Archiver Appliance is an archiver for EPICS control systems that seeks to archive millions of PVs.
One of the goals of the project is to support automated maintenance of the system.
In an ideal world, the archive requests are part of the IOC package itself; either as INFO fields or as a separate archive file.
This package contains a few scripts that use archive files to automate submission of new PVs to the archivers.
In addition, there are few other scripts to
- Automatically resume paused PV's that are now live.
- Automatically pause PV's that have not connected for more than a certain amount of time.


Most of these scripts are expected to run as cron jobs periodically on machines that have access to the PV's in the control system (ideally using an EPICS_CA_ADDR_LIST that is different from the archiver).

### Dependencies
This is a Python 3 project; we've made minimal assumptions on what's available in the Python environment.
The main requirement is [ PyEpics ](http://cars9.uchicago.edu/software/python/pyepics/).
We also use `requests`; this package makes it much easier to use/debug client side HTTP calls.
Not included in this package is the script code that sets up the EPICS environment and perhaps activates the Python environment.

### Automated PV submission
The automated PV submission script `processArchiveFiles.py` assumes that the IOC engineer communicates list of PV's using `.archive` files.
These are gathered into a common folder into a reasonably simple hierarchy.
For example, in all our facilities, we have a top level folder called `IOC_DATA`.
Every IOC has its own subfolder in `IOC_DATA`.
In addition to storing various IOC related lists like autosave files, lists of PV's in `IOC.pvlist` files etc., the deployment process also places the archive request file in `IOC_DATA`/`iocName`/archive/`iocName`.archive. For example,
```
IOCData
       /ioc-hpl-beckhoff
                        /autosave
                        /archive
                                /ioc-hpl-beckhoff.archive
       /ioc-sxr-las1
                        /autosave
                        /archive
                                /ioc-sxr-las1.archive
```
Each archive file is a space/tab separated text file with a archiver config for a PV per line.
* Blank lines are permitted; comments are lines beginning with the `#` character.
* The name of the PV is the first column in the tab/space separated file.
* The second column is the sampling period (in seconds).
* The third column is the samping method (one of scan/monitor).
* The sampling period and sampling method are optional; in which case, defaults specified as arguments to the processArchiveFiles.py script are applied.

For example, here are a few lines from one if the archive files.

```
# Archive these values only when they change

R32:IOC:01:STARTTOD 30 monitor
R32:IOC:01:SYSRESET 15 scan
R32:IOC:01:SUSP_TASK_CNT 86400
R32:IOC:01:APP_DIR1 30 monitor
R32:IOC:01:APP_DIR2 30 monitor
```

The `processArchiveFiles.py` takes as an argument the path of the `IOCData` folder and a glob expression to match archive files in the IOCData folder.
It uses these two to determine the applicable IOC archive files.
It keeps a copy of these files in the specified data folder.
Each time it run, it compares the current .archive file with the previous version and only considers files that have changed.
For each changed file
* It parses the file to determine the list of PVs and their configuration information.
* It then uses CA to determine the connectivity of these PVs and only considers those than can be connected to.
* It then determines those PVs that are unarchived and submits them to the archiver using a batch process.
* Finally, the cached copy of the .archive file is updated.

This approach has the side effect that the it could take some hours for the script to complete the first time you run the script in your installation.
However, subsequently, as long as the data folder that contains the cached copies of the script is valid, the script should complete in a couple of minutes.
So, it is suggested that you run the script manually the first few times and then add a cron job once the data folder has stabilized.

### Auto pause
In many facilities, there are IOC's that come and go at a rapid pace.
This has an impact on the archiver in that within a short amount of time there are lot of disconnected PVs in the archiver.
Having a lot of disconnected PVs in the archiver has some impact on the entire control system as a whole, so it is desirable to pause those PV's that have not connected for a while.
The script `pauseDisconnectedPVs.py` takes a timeout (in minutes) as an argument and pauses those PV's that have not connected for the specified timeout.

### Auto resume
But the IOC's can come back; so the previous script would have paused PV's that may now be active.
The script `resumePausedPVs.py` undoes the work of the previous script.
It gets a list of paused PVs from the archiver and then resumes that are now live.

Thus, you can use a combination of the auto resume and auto pause script to maintain a reasonably clean system.
In addition to the benefits to ths control system, this should also make monitoring of the archiver significantly easier.
