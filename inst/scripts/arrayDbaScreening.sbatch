#!/bin/bash
#SBATCH -p hpc                         # queue
#SBATCH -t 00-24:00                    # time DD-HH:MM
#SBATCH -e RVarDir/sbatch_array_%A_%a.err      # write stderr to 
#SBATCH -o RVarDir/sbatch_array_%A_%a.out      # write stdout to 
#SBATCH --mail-user=RVarEmail     # mail address for notifications
#SBATCH --mail-type=BEGIN,END,FAIL     # mail messages at
#SBATCH --get-user-env                 # get the standard user env (~/.bashrc, ~/.bash_profile)
#SBATCH --nodes=1                      # number of requested nodes (max = 7)
#SBATCH --tasks-per-node=1             # number of tasks per node
#SBATCH --ntasks=1                     # number of tasks
#SBATCH --cpus-per-task=1              # number of cpus per task (max = 48)
#SBATCH --array=1-RVarNumBatches%10


#----------------------------------------------------------------
# Output Environment Variables
#----------------------------------------------------------------
echo "=========================================="
echo " Slurm Environmental Variables"
echo "=========================================="
echo " ID of ArrayJobsr -------- " $SLURM_ARRAY_JOB_ID
echo " ID of ArrayTasks -------- " $SLURM_ARRAY_TASK
echo " Cluster Name ------------ " $SLURM_CLUSTER_NAME
echo " Number of Tasks --------- " $SLURM_NTASKS
echo " Cpus per Task ----------- " $SLURM_CPUS_PER_TASK
echo " Account Name ------------ " $SLURM_JOB_ACCOUNT
echo " Job ID ------------------ " $SLURM_JOB_ID
echo " Job Name ---------------- " $SLURM_JOB_NAME
echo " Job allocated Nodes ----- " $SLURM_JOB_NODELIST
echo " Number of nodes requested " $SLURM_JOB_NUM_NODES
echo " Parititon / Queue of Job  " $SLURM_JOB_PARTITION
echo " User UID  Job Owner ----- " $SLURM_JOB_UID
echo " User Job Owner ---------- " $SLURM_JOB_USER
echo " Job restart count ------- " $SLURM_RESTART_COUNT
echo " Job Task ID ------------- " $SLURM_PROCID
echo " Job step ID ------------- " $SLURM_STEP_ID
echo " number of MPI ranks ----- " $SLURM_STEP_NUM_TASKS
#----------------------------------------------------------------
# Output of job information
#----------------------------------------------------------------
echo -e "\nJob: $SLURM_JOB_NAME   JobID: $SLURM_JOB_ID"
echo "=========================================="
echo "Number of nodes requested: "$SLURM_JOB_NUM_NODES
echo "Number of cores requested: "$SLURM_CPUS_ON_NODE
echo "Time: `date`"
echo "Running on master node: `hostname`"
echo "PATH: " $PATH
echo "LD_LIBRARY_PATH: " $LD_LIBRARY_PATH
echo "R_USER_LIB: `Rscript -e '.libPaths()'`"
echo -e "Current directory:" $PWD
echo "=========================================="
echo ""


Rscript RVarDir/dbaScreeningSlurm.R RVarDir/recordsInBatches.RDS $SLURM_ARRAY_TASK_ID


echo -e "------------------------------------------ \n"

