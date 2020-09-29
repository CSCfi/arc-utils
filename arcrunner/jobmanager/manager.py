""" 
Copyright 2012 by CSC - It Center for Science

author: Olli Tourunen <first.last@csc.fi>
"""

from logging import debug,info,warn,error,critical #@UnusedImport
import os
import common
import gridcommands
#import gridcommands_arclib as gridcommands
from common import gJobStatistics, gConfig
import time

# The default maximum number of jobs managed in the grid
MAX_JOBS_IN_GRID=200
# The default maximum number of jobs waiting to run in the grid
MAX_JOBS_WAITING=10

# the custom broker to use (if any)
gCustomBroker=None

# the object (if any) that is called after job state changes. See 
# jm_plugin_example.py for docs and examples.
gStateChangeCallback=None

def loadJobs():
    xrslname=gConfig.get('xrslname')
    jobs=[]
    # load all the jobs' metadata from their directories
    dirs=os.listdir('.')
    dirs.sort()
    for d in dirs:
        if os.path.isfile(d+'/'+xrslname):
            jobs.append(common.loadJob(d))
            
    gJobStatistics.setJobs(jobs)
    
    return jobs

def changeJobState(job, newState):
    if job.state==newState:
        debug('Job %s still in state %s' % (job.id, job.state))
        return
    
    info ('Job %s changing state from %s to %s' % (job.id, job.state, newState))
    gJobStatistics.removeState(job.state)
    gJobStatistics.addState(newState)
    job.state=newState
    job.lastStateChange=time.time()
    
    #TODO: safety mechanisms (cwd, exceptions, timeout(?) )
    if gStateChangeCallback!=None:
        gStateChangeCallback.stateChange(job.id, job.state)
    
    
def processJob(job):
    
    if job.state in (common.STATE_SUCCESS, common.STATE_FAILURE):
        return
    
    debug("Job %s in state %s" % (job.id, job.state))
    
    prevDir=os.getcwd()
    os.chdir(job.id)

    # This is the main state machine for the processing. We simply take the job
    # state and act accordinly

    try:
        
        # Jobs that are not yet submitted
        if job.state==common.STATE_NEW:
            # To keep the manager from spamming the clusters with excess jobs,
            # we limit the number of queueing and total jobs in the grid 
            maxWaitingJobs=gConfig.get('max_grid_jobs_waiting',MAX_JOBS_WAITING)
            maxJobsInGrid=gConfig.get('max_grid_jobs_total',MAX_JOBS_IN_GRID)
            
            if gJobStatistics.getJobsWaitingToRun()>=maxWaitingJobs:
                debug("Job %s submit delayed, too many jobs waiting to run" % job.id)
            elif gJobStatistics.getJobsInGrid()>=maxJobsInGrid:
                debug("Job %s submit delayed, total grid job limit exceeded" % job.id)
            else:
                if gridcommands.submit(job, gCustomBroker):
                    changeJobState(job, common.STATE_SUBMITTED)
                else:
                    warn('Job %s submission attempt failed. The job will be re-submitted.' % (job.id))
                    job.retries=job.retries+1
            

        # Jobs currently waiting or running in the grid
        elif job.state in (common.STATE_SUBMITTED, common.STATE_QUEUING, 
                           common.STATE_RUNNING):
            state=gridcommands.getGridJobState(job)
            if state is not None:
                changeJobState(job, state)
                
            # check that the per state time limits are not exceeded
            limit=gConfig.get('max_secs_in_state_%s' % job.state,0)
            if limit > 0 and (time.time() - job.lastStateChange) > limit:
                warn("Job %s time in state %s exceeds specified limit %s" %\
                    (job.id, job.state, limit))
                gridcommands.cancel(job)
                common.gJobStatistics.addJobError(job)
                if job.retries < common.gConfig.getInt('max_retries', 1):
                    info("Job %s is going to be retried, retry count %d" % (job.id, job.retries))
                    # no retry counts for rescheduling queued jobs
                    if job.state!=common.STATE_QUEUING:
                        job.retries=job.retries+1
                    changeJobState(job, common.STATE_NEW)
                else:
                    changeJobState(job, common.STATE_FAILURE)

        
        # Jobs that have completed successfully
        elif job.state==common.STATE_FINISHED:
            if gridcommands.download(job):
                changeJobState(job, common.STATE_SUCCESS) 
            else:
                warn("Job %s results download failed" % (job.id))
                job.errors=job.errors+1
        
        
        # Jobs that have failed
        elif job.state==common.STATE_FAILED:
            reason=gridcommands.getFailureReason(job)
            info("Job %s has failed, reason %s" % (job.id,reason))
            common.gJobStatistics.addJobError(job)
            if job.retries < common.gConfig.getInt('max_retries', 1):
                info("Job %s is going to be retried" % job.id)
                gridcommands.cancel(job)
                job.retries=job.retries+1
                changeJobState(job, common.STATE_NEW)
            else:
                if gConfig.getInt('download_job_evidence', 1)==1:
                    warn("Job %s has no retries left, registering failure and downloading evidence" % job.id)
                    gridcommands.download(job)
                else:
                    warn("Job %s has no retries left, registering failure" % job.id)
                
                changeJobState(job, common.STATE_FAILURE)
                f=open('FAILURE','a')
                f.write('%s %s\n' % (job.getCurrentGID(), reason))
                f.close()
                
    except Exception, e:
        warn("Job %s error in state '%s': %s" % (job.id, job.state, e))
        job.errors=job.errors+1
        common.gJobStatistics.addJobError(job)
        
    if job.errors > 5:
        warn("Job %s has too many errors, failing it" % job.id)
        changeJobState(job, common.STATE_FAILURE)
        
    os.chdir(prevDir)
    common.saveJob(job)
    
def processJobs(jobs):

    # process all the jobs in the set
    for job in jobs:
        processJob(job)

    # Are we done yet or just ready to wait a bit for jobs to progress?
    if gJobStatistics.getJobsInGrid() + gJobStatistics.getCount(common.STATE_NEW) == 0:
        return True
    else:
        return False
