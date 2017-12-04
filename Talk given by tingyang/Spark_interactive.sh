# Start the Spark
## Select a python version and add it to the init model. Here, we use python 3.4.3
module initadd python/3.4.3
## Load the modules for spark
module load zlib/1.2.8 openssl/1.0.1e java/1.8.0_31 protobuf/2.5.0 myhadoop/Sep012015 spark/2.1.0 python/3.4.3
## you should see like:
# $ module list
# Currently Loaded Modulefiles:
#   1) null                 4) java/1.8.0_31        7) spark/2.1.0
#   2) zlib/1.2.8           5) protobuf/2.5.0       8) python/3.4.3
#   3) openssl/1.0.1e       6) myhadoop/Sep012015

## Start an interactive sparkUI on BECAT
# sifo
fisbatch.hadoop --ntasks-per-node=1 -c 16 -N 2 -p general_requeue
## here we chose partition general_requeue because it has free nodes. -c 16 means each spark worker (task in slurm) need 16 CPUs. --ntasks-per-node means each node only has one spark worker (task in slurm). -N 1 means we need one node.

## create a working folder or go to the working folder
mkdir -p /scratch/[NetID]/spark
cd /scratch/[NetID]/spark
## Copy the data to here. Here we copy the data from my folder
cp -r /scratch/tix11001/spark/{*.txt,*.csv,*.xlsx,*.snp} ./

# Run sparkUI
pyspark

# Combine the results of output
cat output/part-* > final_results.txt
