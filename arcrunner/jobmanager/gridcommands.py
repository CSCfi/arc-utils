""" 
Copyright 2012 by CSC - It Center for Science

author: Olli Tourunen <first.last@csc.fi>
"""
import time

import subprocess

from jobmanager.common import * #@UnusedWildImport

class GridStatusCache(object):
    def __init__(self):
        self.cachemap={}
        self.refreshTime=0

    def load(self):
        newmap={}
        commandString='arcstat -a'
        debug('Calling "%s"' % commandString) 
        
        jobId=''
        proc=subprocess.Popen(commandString, stdout=subprocess.PIPE, shell=True)
    
        for l in proc.stdout.readlines():
            # searching for jobid
            if jobId=='':
                if l.startswith('Job: gsiftp://'):
                    jobId=l[4:].strip()
                if l.startswith('Job: https://'):
                    jobId=l[4:].strip()
            # taking the next status row for the detected job
            else:
                idx=l.find('State:')
                if idx >= 0:
                    idx = l.find(':')
                    jobStatus=l[idx+1:].strip() 
                    newmap[jobId]=jobStatus
                    jobId=jobStatus=''

        proc.wait()
        self.cachemap=newmap
        self.refreshTime=time.time()
        
    def getStatus(self, jobId):
        if (time.time()-self.refreshTime) > 20:
            self.load()

        if self.cachemap.has_key(jobId):
            return self.cachemap[jobId]
        
        return None

# create the global grid status caching object
gGridStatusCache=GridStatusCache()    

def checkCredentials():
    # get the remaining lifetime of the proxy
    #commandString='grid-proxy-info -timeleft' 
    commandString='arcproxy -I | grep "Time left for proxy" | cut -d " " -f5 ' 
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
    gid=''
    
    # if a broker is given, query it for the cluster decision
    if broker is not None:
        cluster = broker.getCluster()
    else:
        cluster=''
    
    # did we get a directive?    
    if cluster is not None and cluster!='':
        if cluster.startswith('-c'):
            clusterParam=cluster
        else:
            clusterParam="-c %s" % cluster
    else:
        clusterParam=""
    
    commandString='arcsub %s %s -f %s' % \
        (gConfig.get('ngsub_params',''), clusterParam, gConfig.get('xrslname'))
    debug ('Job %s Calling "%s"' % (job.id,commandString))
    output=[]
    proc=subprocess.Popen(commandString, stdout=subprocess.PIPE, shell=True)
    for l in proc.stdout.readlines():
        output.append(l)
        if not l.startswith('Job submitted with'):
            continue
        
        idx=l.find('gsiftp://')
        if idx < 0:
           idx=l.find('https://')

        if idx >= 0:
            gid=l[idx:].strip()
            break
        else:
            raise Exception('Job id parsing failed. Line: %s' % output)
    proc.wait()
    
    # Did submission fail? 
    if gid=='':
        # Here we have to record the failure by the intended cluster, if
        # one was specified by the broker. This is because there is no GID in
        # the job and later on there is no way of knowing which was the
        # problematic target.
        # TODO: better handling of brokers returning multiple clusters
        if cluster is not None and cluster!='' and cluster[0]!='-':
            gJobStatistics.addClusterError(cluster)
            
        return False
    
    info('Job %s submitted with gid %s' % (job.id, gid))
    job.gridJobIds.append(gid)
    
    return True

def getGridJobState(job):
    
    status=gGridStatusCache.getStatus(job.getCurrentGID())
    
    if status is None:
        return None

    if status in ('Finishing (FINISHING)'):
        return STATE_RUNNING

    if status in ('Finished (FINISHED)'):
        return STATE_FINISHED

    if status in ('Finished (Finished)'):
        return STATE_FINISHED

    if status in ('Failed (FAILED)'):
        return STATE_FAILED
 
    if status in ('Queuing (INLRMS:Q)'):
        return STATE_QUEUING

    if status in ('Queuing (INLRMS:E)'):
        return STATE_QUEUING    

    if status in ('Queuing (INLRMS:S)'):
        return STATE_QUEUING

    if status in ('Running (INLRMS:R)'):
        return STATE_RUNNING

    return None


def download(job):
 
    gridJobId=job.getCurrentGID()
    jobDownloadDir=gridJobId[gridJobId.rfind('/')+1:]
    # clear old partially downloaded results, if any
    if os.path.isdir(jobDownloadDir):
        os.system('rm -rf %s' % jobDownloadDir)
    
    commandString='arcget %s' % (gridJobId) 
    debug ('Calling "%s"' % commandString) 
    success=True
    output=[]
    proc=subprocess.Popen(commandString, stdout=subprocess.PIPE, shell=True)
    for l in proc.stdout.readlines():
        output.append(l)
        if l.find('WARNING') >=0:
            success = False
        if l.find('ERROR') >=0:
            success = False
        if l.find('FATAL') >=0:
            success = False
    proc.wait()

    if success:
        # move the previous results out of the way if any
        if os.path.isdir('results'):
            os.system('mv results results.%s' % time.time())
        # rename the download dir to 'results'
        os.system('mv %s results' % jobDownloadDir)
    else:
        warn("Job %s download error: %s" % (job.id, output))
    return success

def cancel(job):
    commandString='arckill %s' % job.getCurrentGID() 
    debug ('Calling "%s"' % commandString) 
    success=False
    output=[]
    proc=subprocess.Popen(commandString, stdout=subprocess.PIPE, shell=True)
    for l in proc.stdout.readlines():
        output.append(l)
        if l.find('killed: 1') >=0:
            success = True
        if l.find('already finished') >=0:
            success=clean(job)
    proc.wait()
   
    if not success:
        warn('Job %s %s' %(job.id, output))
        
    return success  

def clean(job):
    commandString='arcclean %s' % job.getCurrentGID() 
    debug ('Calling "%s"' % commandString) 
    success=False
    output=[]
    proc=subprocess.Popen(commandString, stdout=subprocess.PIPE, shell=True)
    for l in proc.stdout.readlines():
        output.append(l)
        if l.find('deleted: 1') >=0:
            success = True
    proc.wait()
   
    if not success:
        warn('Job %s %s' %(job.id, output))
        
    return success  

def getFailureReason(job):
    commandString='arcstat %s' % job.getCurrentGID() 
    debug ('Calling "%s"' % commandString) 
    errorStr=''
    output=[]
    proc=subprocess.Popen(commandString, stdout=subprocess.PIPE, shell=True)
    for l in proc.stdout.readlines():
        output.append(l)
        if l.find('Error:') >=0:
            errorStr = l[l.find(':')+1:].strip(' \n')
    proc.wait()
    
    return errorStr
