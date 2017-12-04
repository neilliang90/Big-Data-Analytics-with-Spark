import numpy as np
# read file
file = sc.textFile('file:///scratch/tix11001/spark/HDgenotype.txt') \
    .map(lambda line: (line.split()[0], np.array([int(c) for c in line.split()[1]])))
# find the first line
file.first()

# read file 2 try read csv files


































file2 = sc.textFile('file:///scratch/tix11001/spark/HD_SNP_Name_seq2.csv') \
.map(lambda line: (line.split(',')[1],np.array(line.split(','))[[0,2,3]]))

file2.first()

# remove header
file2 = sc.textFile('file:///scratch/tix11001/spark/HD_SNP_Name_seq2.csv')
header = file2.first()
file2 = file2.filter(lambda row: row != header) \
.map(lambda line: np.array(line.split(',')))
file2.first()

# get third column (It is better not to get column from file2 because the order may be wrong.)
#column = file2.map(lambda row: row[2]).collect()
column = np.genfromtxt("/scratch/tix11001/spark/HD_SNP_Name_seq2.csv",usecols = (2,),skip_header = 1, delimiter = ',',dtype = np.str)
# Test with one element in file
test = file.first()
np.array([snp[0]*2 if id ==0 else snp if id == 1 else snp[1]*2 for id,snp in zip(test[1],column)])
# array(['AA', 'AG', 'AA', ..., 'AG', 'AA', 'AG'],
#       dtype='<U2')
data = file.map(lambda row: (row[0],np.array([snp[0]+snp[0] if id ==0 else snp if id == 1 else snp[1]+snp[1] for id,snp in zip(row[1],column)])))
data.first()

# filter SNPs from file2
## Read the filter list as unordered unique set (Much faster)
filter_list = set(np.genfromtxt("/scratch/tix11001/spark/HD_50K.snp",dtype = np.str)) 














data2 = file2.filter(lambda row: row[1] in filter_list)
data2.first()
data2.count()

# Filter SNPs from file
# The filter indices have to be stored in the master
SNP_names = np.genfromtxt("/scratch/tix11001/spark/HD_SNP_Name_seq2.csv",usecols = (1,),skip_header = 1, delimiter = ',',dtype = np.str)
filter_ind = np.array([ele in filter_list for ele in SNP.names])
# try filter data columns


















data_filtered = data.map(lambda row: (row[0],row[1][filter_ind]))
data_filtered.first()
data_filtered.map(lambda row: len(row[1])).collect()

# Write the data into file
data_filtered.map(lambda row: row[0] + ' ' + ' '.join(row[1])).saveAsTextFile("file:///scratch/tix11001/spark/output")


