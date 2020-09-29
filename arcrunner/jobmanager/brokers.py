""" 
Copyright 2012 by CSC - It Center for Science

author: Olli Tourunen <first.last@csc.fi>
"""
from common import gJobStatistics, gConfig
from logging import debug,info,warn,error,critical #@UnusedImport

# This file has some example brokers that can be used to affect the
# job submission targets.


# Simple broker class that picks a cluster based on a list of potential
# clusters given in the config that has the least queuing jobs managed
# by the current job manager. No cluster status information is used.    
class SimpleBroker(object):
    
    def getCluster(self):
        # get the statistics for the clusters
        clusterStats=gJobStatistics.getClusterMap()
        # get the list of configured resources
        clusterNames=[item.strip() for item in gConfig.get('clusters','').split(",")]
        bestCluster=None
        bestWaiting=9999999
        for cluster in clusterNames:
            if clusterStats.has_key(cluster):
                nWaiting=clusterStats[cluster].getJobsWaitingToRun()
            else:
                nWaiting=0
            
            if nWaiting<bestWaiting:
                bestWaiting=nWaiting
                bestCluster=cluster
            
        return bestCluster

# Simple broker class that excludes a cluster based on the number of queuing
# jobs managed by the current job manager. No cluster status information is used.    
class SimpleExcludeBroker(object):
    
    def getCluster(self):
        clusterStats=gJobStatistics.getClusterMap()
        clusterNames=clusterStats.keys()
        worstCluster=None
        worstWaiting=0
        for cluster in clusterNames:
            if clusterStats[cluster].getJobsWaitingToRun()>worstWaiting:
                worstWaiting=clusterStats[cluster].getJobsWaitingToRun()
                worstCluster=cluster

        if worstCluster != None:
            return '-%s' % worstCluster
        else:
            return None            
    
    
class GreylistingBroker(object):
    
    def getCluster(self):
        
        # get the error counts for the clusters
        #clusterErrors=gJobStatistics.getErrorCounts()
        clusterErrors=gJobStatistics.getRelativeErrorCounts()
        
        # get the statistics for the clusters
        clusterStats=gJobStatistics.getClusterMap()
        # get the list of configured resources
        clusterNames=[item.strip() for item in gConfig.get('clusters','').split(",")]
        bestCluster=None
        bestWaiting=9999999
        for cluster in clusterNames:
            if clusterStats.has_key(cluster):
                nWaiting=clusterStats[cluster].getJobsWaitingToRun()
            else:
                nWaiting=0
            
            # Here is the graylisting part: we simply count each error as a 
            # waiting job so the more errors there are the more unlikely it is
            # that a job will end up in the cluster
            if clusterErrors.has_key(cluster):
                nWaiting = nWaiting + clusterErrors[cluster]
            
            if nWaiting<bestWaiting:
                bestWaiting=nWaiting
                bestCluster=cluster
            
        return bestCluster

class GreylistingExcludeBroker(object):
    
    def getCluster(self):
        
        # get the error counts for the clusters
        #clusterErrors=gJobStatistics.getErrorCounts()
        clusterErrors=gJobStatistics.getRelativeErrorCounts()
 
        # get the statistics for the clusters
        clusterStats=gJobStatistics.getClusterMap()
        
        # use the current set of known clusters as the candidate for excluding 
        clusterNames=clusterStats.keys()
        worstCluster=None
        worstWaiting=0
        for cluster in clusterNames:
            if clusterStats.has_key(cluster):
                nWaiting=clusterStats[cluster].getJobsWaitingToRun()
            else:
                nWaiting=0
            
            # Here is the greylisting part: we simply count each error as a 
            # waiting job so the more errors there are the more unlikely it is
            # that a job will end up in the cluster
            if clusterErrors.has_key(cluster):
                nWaiting = nWaiting + clusterErrors[cluster]
                
            if nWaiting>worstWaiting:
                worstWaiting=nWaiting
                worstCluster=cluster
        if worstCluster != None:
            return '-%s' % worstCluster
        else:
            return None

class ExcludingARCBroker(object):
    
    def getCluster(self):
        
        # get the error counts for the clusters
        #clusterErrors=gJobStatistics.getErrorCounts()
        clusterErrors=gJobStatistics.getRelativeErrorCounts()
 
        # get the statistics for the clusters
        clusterStats=gJobStatistics.getClusterMap()
        
        # use the current set of clusters as the candidate for excluding 
        clusterNames=clusterStats.keys()
        worstCluster=None
        worstWaiting=0
        for cluster in clusterNames:
            if clusterStats.has_key(cluster):
                nWaiting=clusterStats[cluster].getJobsWaitingToRun()
            else:
                nWaiting=0
            
            # Here is the blacklisting part: we simply count each error as a 
            # waiting job so the more errors there are the more unlikely it is
            # that a job will end up in the cluster
            if clusterErrors.has_key(cluster):
                nWaiting = nWaiting + clusterErrors[cluster]
                
            if nWaiting>worstWaiting:
                worstWaiting=nWaiting
                worstCluster=cluster
        
        clusterNames=[item.strip() for item in gConfig.get('clusters','').split(",")]
        if '' in clusterNames:
            clusterNames.remove('')
        
        if worstCluster != None:
            # We have the worst cluster pinpointed, construct a list of all the others and
            # let ARC do the selection
            debug('ExcludingARCBroker: excluding %s' % worstCluster)
            if worstCluster in clusterNames:
                clusterNames.remove(worstCluster)
        
        if len(clusterNames)<1:
            return None
        
        res=""
        for name in clusterNames:
            res="%s -c %s" % (res,name)
        return res.strip()

