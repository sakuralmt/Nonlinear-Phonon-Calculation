#!/bin/bash
#SBATCH --job-name=my_python_job
#SBATCH --nodes=1
#SBATCH --cpus-per-task=10
#SBATCH --time=3-00:00:00
#SBATCH --error=python_error_%j.err
#SBATCH --partition=long
#SBATCH --qos=normal

export OMP_NUM_THREADS=1
python step1_simple.py

