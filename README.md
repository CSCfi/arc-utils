# arc-utils

## INSTALLING ARCRUNNER

Arcrunner is a program for exiecuting large amounts of similar jobs in ARC based grid environments like FGCI. You can use it in the puhti.csc.fi server or 
you can download it to your local computer. 

To run arcrunner you need to have ARC 11-5 or newer and python 2.6 or newer installed to you computer.

Once you have downloaded the _arc-util_  repository, move to the `arcrunner/bin` directory
and modify the fifth row of the file `arcrunner` so that the _jobmanagerpath_ variable
corresponds the location of your arcrunner installation. For example if you have 
downloaded _arc-utils_ to directory `/opt/grid` the jobmanagerpath definining line shouild be:

```
jobmanagerpath="/opt/grid/arc-utils/arcrunner"
```

After this the only thing left to do is to add _arcrunner/bin_ to your command path.


## USING ARCRUNNER

The minimum input for _arcrunner_ command is:

```
arcrunner -xrsl job_descriptionfile.xrsl
```

When the _arcrunner_ is launched, it first checks all the sub-directories of 
the current directory. If a xrsl formatted job description file, defined with 
the option `-xrsl`, is found from the subfolder then the script submits the
job to be executed in FGCI environment.

In the cases of large amounts of grid jobs, all jobs are not submitted at once.
In these cases arcrunner tries to optimize the usage of the grid environment. 
It monitors how many jobs are queuing in the clusters and sends more jobs only
when there are free resources available. The command also keeps a track on the 
executed grid jobs and starts sending more jobs to those clusters that execute 
the jobs most efficiently. If you don't want to use all the FGCI clusters, 
you can use cluster list file and option `-R` to define what clusters will be used.

The maximum number of jobs, waiting to be executed, can be defined with option: `-W`. 
If some job stays queuing for too long time, it is withdrawn from this stalled 
queue and submitted to another cluster. The maximum  queuing time (in seconds) 
can be set with option `-Q`.

As some of the clusters may not work properly, part of the jobs may fail 
due to technical reasons.  If this happens, the failed grid jobs are re-submitted 
to other clusters three times before they are considered as failed sub-jobs.  

During execution, arcrunner checks the status of the sub-jobs once in a 
minute and prints you the status of each active sub-job. Finally it writes 
out a summary table about sub-jobs.

When a job finishes successfully, the job-manager retrieves the result files 
from the grid to the grid job directory.


## arcrunner options:

| Option             |            Description|
|--------------------|-----------------------|
| -xrsl file_name    | The common xrsl file name that defines the jobs. |
| -R file_name       | Text file containing the names of the clusters to be used. |
| -W integer         | Maximum number of jobs in the grid waiting to run. (Default 200).|
| -Q integer         | The maximum time a jobs stays in a queue before being resubmitted. (Default 3600s). |
| -S integer         | The maximum time a jobs stays in submitted state before being resubmitted. (Default 3600s). |
| -J integer         | Maximum number of simultaneous jobs running in the grid. (Default 1000 jobs). |

