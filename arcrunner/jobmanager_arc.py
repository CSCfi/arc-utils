#!/v/linux26_x86_64/lang/python/2.6.5-gcc/bin/python

"""
Copyright 2012 by CSC - It Center for Science

authors: Olli Tourunen, Kimmo Mattila 
email:   <first.last@csc.fi>

Generic grid job manager script. 

"""
import time
import commands

from logging import debug,info,warn,error,critical #@UnusedImport
from jobmanager import *
from optparse import OptionParser
import sys


def parseOptions(argv):
    parser = OptionParser()
    parser.add_option("--xrsl",dest="xrslfile",
                      help="xrsl file name that defines the job", metavar="FILE")
    parser.add_option("-R","--resources",dest="clusters",
                      help="file to define available clusters", default="None", metavar="FILE")
    parser.add_option("-W","--max_waiting",dest="max_waiting",
                      help="maximum number of jobs in the grid waiting to run", default="200", metavar="NUMJOBS")
    parser.add_option("-Q","--max_queuetime",dest="max_qtime",
                     help="the maximum time a jobs stays in a queue before being resubmitted", default="3600", metavar="SECS")
    parser.add_option("-S","--max_submittedtime",dest="max_stime",
                     help="the maximum time a jobs stays in submitted state before being resubmitted", default="3600", metavar="SECS")
    parser.add_option("-J","--max_jobs",dest="max_jobs",
                      help="maximum number of jobs in the grid", default=1000, metavar="NUMJOBS")
    (options, argv) = parser.parse_args(argv)
    
    return options

if __name__ == '__main__':
    
    # Always start with this before doing anything else. If you wish to turn 
    # debugging off, pass False or nothing to the initLogging()   
    #common.initLogging(True)
    common.initLogging(False)
    
    # Optional check for credentials right the beginning
    if not gridcommands.checkCredentials():
        warn('No valid credentials found, maybe you need to run arc-proxy?. Exiting')
        exit(1)
    
    # Add the runtime parameters to the global config. 

    # Start by parsing the command line options
    options=parseOptions(sys.argv)

    # 'xrslname' is the name for the job description files to look for.
    #  This parameter is mandatory.
    common.gConfig['xrslname']=options.xrslfile

    # 'clusters' is a list of potential resources for SimpleBroker and Graylisting broker
    print options.clusters
    
    if options.clusters == "None":
        print "Using default cluster selection"
        common.gConfig['clusters']=\
         'korundi.grid.helsinki.fi, vuori-arc.csc.fi'
    else:
        clusterList= ""
        clusterFile=open(options.clusters , 'r' )
        for line in clusterFile:
            if clusterList != "":
                clusterList += ", " 
            line=line.split( )
            if len(line) > 0:
                clusterList += line[0]
        common.gConfig['clusters']=clusterList

    # 'max_grid_jobs_waiting' is the maximum number of jobs in the grid waiting
    #  to run (just submitted + preparing + queuing)
    common.gConfig['max_grid_jobs_waiting']=int(options.max_waiting)
    
    # 'max_grid_jobs_total' is the maximum total number of jobs in the grid
    common.gConfig['max_grid_jobs_total']=int(options.max_jobs)

    # 'max_retries' is the per job limit for retrying a failed grid job
    common.gConfig['max_retries']=3
    
    # 'max_secs_in_state_<state>' is the limit for controlling the limit for each job
    # being stuck in 'submitted', 'queuing' or 'running' -state. If the limit is exceeded,
    # the job will be cancelled and retried
    common.gConfig['max_secs_in_state_submitted']=int(options.max_stime)

    common.gConfig['max_secs_in_state_queuing']=int(options.max_qtime)
    #common.gConfig['max_secs_in_state_running']=86400

    # use a custom broker to affect the job submission
    #manager.gCustomBroker=brokers.SimpleBroker()
    manager.gCustomBroker=brokers.GreylistingBroker()
    #manager.gCustomBroker=brokers.GreylistingExcludeBroker()
        
    # change to the test root directory
    #os.chdir('/home/csc/tourunen/tmp/jmtest')    
   
    # load the jobs from the directories
    jobs=manager.loadJobs()
    
    # record the start time for statistics
    starttime=time.time()

    # Stick in the main loop until manager returns True to indicate we are done 
    done=False
    while not done:
        # one call iterates through all the jobs once
        done=manager.processJobs(jobs)

        # print state summary per host
        clusterStats=common.gJobStatistics.getClusterMap()
        array=common.gJobStatistics.clusterMapToPrintableArray(clusterStats, True)
        info('State summary per host')
        for line in array:
            info(" ".join(line))
            
        # print encountered errors
        errorCounts=common.gJobStatistics.getErrorCounts()
        if len(errorCounts)>0:
            info("Error summary per host")
            maxLen=max([len(str(item)) for item in errorCounts.keys()])
            for host in errorCounts.keys():
                info(("  %"+str(maxLen)+"s %s") % (host, errorCounts[host]))
        
        # Check that grid proxy is still valid
        proxyTimeLeft=commands.getoutput("arcproxy -I | grep 'Time left for proxy'")
        if proxyTimeLeft == "Time left for proxy: Proxy expired":
            print "\nYour grid proxy has expired! \nPlease refresh your proxy by logging in to this server and running commands:\n"
            print "  module load nordugrid-arc \n  arcproxy \n \nYou can keep this process running though."
        else: 
            print proxyTimeLeft 
            
        # and sleep for a while before the next round
        if not done:
            time.sleep(30)
    
    # all done, record the endtime
    endtime=time.time()
    
    info("Job directories processed in %s seconds" % (int(endtime-starttime)))
    
