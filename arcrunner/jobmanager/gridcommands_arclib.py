""" 
Copyright 2009 by CSC - It Center for Science

author: Olli Tourunen <first.last@csc.fi>
"""
import traceback
import arclib
import time
import sys
import subprocess

from jobmanager.common import * #@UnusedWildImport

#arclib.SetNotifyLevel(arclib.DEBUG)

class GridStatusCache(object):
    def __init__(self):
        self.cachemap={}
        self.refreshTime=0
        self.qinfo=None
        self.TTL=50
        
    def load(self):
        newmap={}

        debug('refreshing qinfo')
        self.qinfo=arclib.GetQueueInfo()
        
        debug('refreshing job statuses')
        jobIds=arclib.GetJobIDsList()
        jobs=arclib.GetJobInfo(jobIds, arclib.MDS_FILTER_JOBINFO, True, "", 30)
        
        job=arclib.Job()
        for job in jobs:
            newmap[job.id]=job.status
            
        self.cachemap=newmap
        self.refreshTime=time.time()
        
    def getStatus(self, jobId):
        if (time.time()-self.refreshTime) > self.TTL:
            self.load()

        if self.cachemap.has_key(jobId):
            return self.cachemap[jobId]
        
        return None

    def getQueueInfo(self):
        if (time.time()-self.refreshTime) > self.TTL:
            self.load()
        return self.qinfo
        

# create the global grid status caching object
gGridStatusCache=GridStatusCache()    



def checkCredentials():
    # get the remaining lifetime of the proxy
    commandString='grid-proxy-info -timeleft' 
    # we could also let grid-proxy-info do the check with 
    # 'grid-proxy-info -exists -valid 0:01'
    
    output=[]
    proc=subprocess.Popen(commandString, stdout=subprocess.PIPE, shell=True)
    for l in proc.stdout.readlines():
        output.append(l)
    proc.wait()
    
    if len(output) > 0 and int(output[0]) > 0:
        debug ('Proxy lifetime: %s' % int(output[0])) 
        return True
    
    debug ('No valid proxy found') 
    return False


def submit(job, broker=None):
    
    try:
        xrslStr="".join(open(gConfig.get('xrslname')).readlines())
        xrsl=arclib.Xrsl(xrslStr)
        
        qi = gGridStatusCache.getQueueInfo()
        #for q in qi:
        #    debug("%s@%s q:%s r:%s" % (q.name, q.cluster.hostname, q.grid_queued, q.grid_running))

        targets = arclib.ConstructTargets(qi, xrsl)
        # let ARC do the brokering first
        brokeredTargets = arclib.PerformStandardBrokering(targets)
        # then get our view of the best cluster
        topCluster=broker.getCluster()
        debug('Internal broker selected cluster %s' % topCluster)
        # go through the ARC targets and put our candidate on the top of the list
        # (we need a separate list because arclib returns a immutable tuple of targets)
        tgtList=[]
        for t in brokeredTargets:
            if t.cluster.hostname==topCluster:
                tgtList.insert(0, t)
            else:
                tgtList.append(t)
        
        jsub=arclib.JobSubmission(xrsl,tgtList)
        debug('Job %s being submitted' % job.id)
        gid=jsub.Submit()
        # the following does not seem to work (or have any effect):
        # jsub.RegisterJobsubmission(qi)
        
        joblist="%s/.ngjobs" % os.getenv('HOME')
        arclib.LockFile(joblist)
        jlf=open(joblist,"a")
        jlf.write("%s#%s" % (gid,xrsl.GetRelation("jobname").GetSingleValue()))
        jlf.write('\n')
        jlf.close()
        arclib.UnlockFile(joblist)
        
        info('Job %s submitted with gid %s' % (job.id, gid))
        job.gridJobIds.append(gid)
    
    except:
        err=sys.exc_info()[0]
        traceback.print_exc(file=sys.stdout)
        warn('Job %s submit failed: %r ' % (job.id, err))
        return False

    return True


def getGridJobState(job):
    
    status=gGridStatusCache.getStatus(job.getCurrentGID())
    
    if status is None:
        return None
    
    if status in ('FINISHED'):
        return STATE_FINISHED

    if status in ('FAILED','INLRMS:0'):
        return STATE_FAILED
    
    if status in ('INLRMS:Q', 'INLRMS:S'):
        return STATE_QUEUING

    if status in ('INLRMS:R'):
        return STATE_RUNNING

    return None


def download(job):
 
    gridJobId=job.getCurrentGID()

    if os.path.isdir('results'):
        os.system('mv results results.%s' % time.time())
    os.system('mkdir results')
    try:
        jctrl=arclib.FTPControl()
        debug ('DownloadDirectory(%s)' % gridJobId) 
        jctrl.DownloadDirectory(gridJobId,'results')
        debug ('CleanJob(%s)' % gridJobId) 
        arclib.CleanJob(gridJobId)
        debug ('RemoveJobID(%s)' % gridJobId) 
        arclib.RemoveJobID(gridJobId)
    except arclib.FTPControlError:
        err=sys.exc_info()[0]
        warn('Job %s downloading failed: %r ' % (job.id, err))
        return False
    
    
    return True

def cancel(job):
    gridJobId=job.getCurrentGID()
    
    try:
        debug ('CancelJob(%s)' % gridJobId) 
        arclib.CancelJob(gridJobId)
        debug ('RemoveJobID(%s)' % gridJobId) 
        arclib.RemoveJobID(gridJobId)
        return True
    except:
        debug('Job %s could not be cancelled, trying to clean' % (job.id))
        pass

    try:
        debug ('CleanJob(%s)' % gridJobId) 
        arclib.CleanJob(gridJobId)
        debug ('RemoveJobID(%s)' % gridJobId) 
        arclib.RemoveJobID(gridJobId)
        return True
    except:
        debug('Job %s could not be cleaned' % (job.id))
        
    
    warn('Job %s could not be cancelled or cleaned' % (job.id))
    return False
    
def clean(job):
    gridJobId=job.getCurrentGID()

    try:
        debug ('CleanJob(%s)' % gridJobId) 
        arclib.CleanJob(gridJobId)
        debug ('RemoveJobID(%s)' % gridJobId) 
        arclib.RemoveJobID(gridJobId)
        return True
    except:
        pass        
    
    warn('Job %s could not be cleaned' % (job.id))
    return False

def getFailureReason(job):
    
    gridJobId=job.getCurrentGID()
    
    try:    
        gridJobInfo=arclib.GetJobInfo(gridJobId, arclib.MDS_FILTER_JOBINFO, True, "", 30)
    except:
        err=sys.exc_info()[0]
        debug(err)
        info('Job %s failure reason unknown' % job.id)
        return ''
    
    return gridJobInfo.errors




