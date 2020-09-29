""" 
Copyright 2012 by CSC - It Center for Science

author: Olli Tourunen <first.last@csc.fi>
"""
import sys
import os, pickle
import logging
from logging import debug,info,warn,error,critical #@UnusedImport


###############################################################################
# Auxiliary stuff first

""" state definitions """
STATE_NEW='new'
STATE_SUBMITTED='submitted'
STATE_QUEUING='queuing'
STATE_RUNNING='running'
STATE_FINISHED='finished'
STATE_FAILED='failed'
STATE_SUCCESS='success'
STATE_FAILURE='failure'

""" list of known job states """
STATE_NAMES = [STATE_NEW,
               STATE_SUBMITTED, STATE_QUEUING, STATE_RUNNING, STATE_FINISHED, STATE_FAILED,
               STATE_SUCCESS, STATE_FAILURE]

""" the filename job pickle is written into """
JOB_PICKLE_FILENAME='.job_saved'

""" simple class to hold the process wide configuration"""
class ConfigDict(dict):
    def getInt(self, key, default=None):
        res=self.get(key, default)
        try:
            res=int(res)
            return res
        except:  
            return int(default)

""" Global configuration dictionary instance"""
gConfig = ConfigDict()

def initLogging(enableDebug=False):
    if enableDebug:
        logging.basicConfig(
            level=logging.DEBUG, 
            format='%(asctime)s %(levelname)-7s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            stream=sys.stdout
            );
        debug('turning debugging info on')
    else:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)-7s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            stream=sys.stdout
            );

def extractHost(gridJobId):
    if gridJobId.rfind(':')<0:
        raise RuntimeError("Illegal grid job id %s" % gridJobId)

    if not gridJobId.startswith('gsiftp://'):
        if not gridJobId.startswith('https://'):
            raise RuntimeError("Illegal grid job id %s" % gridJobId)

    # gsiftp://<part_we_want>:
    if gridJobId.startswith('gsiftp://'):
        return gridJobId[9:gridJobId.rfind(':')]

    # https://<part_we_want>:
    if gridJobId.startswith('https://'):
        return gridJobId[8:gridJobId.rfind(':')]


###############################################################################
# Business logic from here on

class Job(object):
    def __init__(self, jid):
        self.id=jid
        self.state=None
        self.gridJobIds=[]
        self.errors=0
        self.retries=0
        self.lastStateChange=0

    def getCurrentGID(self):
        if len(self.gridJobIds)>0:
            return self.gridJobIds[-1]
        else:
            return None

    def __repr__(self):
        return "<Job('%s', '%s', '%s')>" % ( 
            self.id, self.state, self.gridJobIds)
        
class JobStatistics(object):
    def __init__(self):
        self.reset()

    def reset(self):
        self.statmap={}
        for state in STATE_NAMES:
            self.statmap[state]=0
        
        self.clusterErrorCounts={}
        
    def setJobs(self, jobs):
        self.reset()
        self.jobs=jobs
        self.rebuildStatistics()
        
    def addState(self,state):
        self.statmap[state]=self.statmap[state]+1

    def removeState(self,state):
        self.statmap[state]=self.statmap[state]-1
        
    def rebuildStatistics(self):
        for job in self.jobs:
            self.addState(job.state)

    def getCount(self, state):
        return self.statmap[state]
    
    def getJobsInGrid(self):
        total=0
        for state in STATE_SUBMITTED, STATE_QUEUING, STATE_RUNNING,\
                     STATE_FINISHED, STATE_FAILED:
            total = total + self.statmap[state]
        return total

    def getJobsWaitingToRun(self):
        total=0
        for state in STATE_SUBMITTED, STATE_QUEUING:
            total = total + self.statmap[state]
        return total

    def getClusterMap(self):
        res={}
        for job in self.jobs:
            gid=job.getCurrentGID()
            if gid is None:
                host="N/A"
            else:
                host=extractHost(gid)
            if not res.has_key(host):
                res[host]=JobStatistics()
            res[host].addState(job.state)
            
        return res
    
    def clusterMapToPrintableArray(self, cm, pad=False):
        rows=[]
        # make header
        col=[]
        col.append('host')
        col=col+STATE_NAMES
        rows.append(col)
        for host in cm.keys():
            col=[]
            col.append(host)
            for state in STATE_NAMES:
                col.append(cm[host].getCount(state))
            rows.append(col)

        # compute the totals
        col=[]
        col.append('TOTAL')
        for c in range(1,len(rows[0])):
            csum=0
            for r in range(1,len(rows)):
                csum=csum+rows[r][c]
            col.append(csum)
        rows.append(col)

        if not pad:
            return rows
        
        # add padding
        
        # calculate the max cell sizes per column and pad
        for c in range(0,len(rows[0])):
            mx=0
            for r in range(0,len(rows)):
                mx=max(len(str(rows[r][c])),mx)
            for r in range(0,len(rows)):
                rows[r][c]=("%"+str(mx)+"s") % rows[r][c] 
        
        return rows
        
    def addJobError(self, job):
        gid=job.getCurrentGID()
        if gid is None:
            return
        host=extractHost(gid)
        self.addClusterError(host)

    def addClusterError(self, host):
        if not self.clusterErrorCounts.has_key(host):
            self.clusterErrorCounts[host]=1
        else:
            self.clusterErrorCounts[host]=self.clusterErrorCounts[host] + 1
    
    def getErrorCounts(self):
        return self.clusterErrorCounts.copy()

    def getRelativeErrorCounts(self):
        res={}
        cm=self.getClusterMap()
        for cluster in self.clusterErrorCounts.keys():
            errors=self.clusterErrorCounts[cluster]
            successes=0
            if cm.has_key(cluster):
                successes=cm[cluster].getCount(STATE_SUCCESS)
            if successes>=10:
                errors=errors/(successes/10)
            res[cluster]=errors
        
        return res  
    
    def __repr__(self):
        res=""
        for state in STATE_NAMES:
            res=res+"%s=%s " %(state, self.getCount(state))
        return res


# Create the global job statistics object 
gJobStatistics=JobStatistics()

# loads a job from the given directory (which also acts as job id)
def loadJob(jobId):
    pfn=jobId+'/'+JOB_PICKLE_FILENAME
    if(os.path.exists(pfn) and os.path.getsize(pfn)>0):
        job=pickle.load(open(pfn))
        debug ('Job %s loaded' % job.id)
    else:
        job=Job(jobId)
        job.state=STATE_NEW
        debug ('Job %s created' % job.id)

    return job
    
# saves a job
def saveJob(job):
    pfn=job.id+'/'+JOB_PICKLE_FILENAME
    pfnTmp=pfn+'.tmp'
    pickle.dump(job, open(pfnTmp,'w'))
    os.system('mv %s %s' % (pfnTmp,pfn))
    debug ('Job %s saved' % job.id)



