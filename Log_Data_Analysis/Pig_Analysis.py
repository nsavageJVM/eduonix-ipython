#!/usr/bin/python
# -*- coding: utf-8 -*-
# snippets for ipython notebook, do not try to run this code
# assumes we have tsv data in '/root/temp.tsv'
# needs sklearn matplotlib

import warnings
warnings.filterwarnings('ignore')

import sys
import random
import numpy as np
import csv
from sklearn import linear_model, cross_validation, metrics, svm
from sklearn.metrics import confusion_matrix, precision_recall_fscore_support, accuracy_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import pandas as pd
import matplotlib.pyplot as plt
get_ipython().magic(u'matplotlib inline')



def read_tsv( cols, col_types=None):
  fhandle = open('/root/temp.tsv', 'r')
  pieces = []
  pieces.append(pd.read_csv(fhandle, names=cols, dtype=col_types,header=None, delimiter="\t"))
  fhandle.close()
  return pd.concat(pieces, ignore_index=True)



cols = ['time', 'ip', 'country', 'status'];
pHandle = read_tsv( cols)

pHandle.shape

df = pHandle[pHandle['country']=='US'].dropna(subset=['status'])
df

grouped = pHandle.groupby('country').count()
grouped.plot(kind='bar')


get_ipython().run_cell_magic(u'writefile', u'preprocess1.pig', u"DEFINE preprocess( ) returns data {\n    \n       tmp = LOAD '/root/data_1' using PigStorage(',')\n    \n        as ( time: chararray, ip: chararray, country: chararray, status: chararray);\n    \n      $data = filter tmp by country=='US';\n    \n};\n\ndata = preprocess();\n\nSTORE data INTO '/root/output';\n\n\ncopyToLocal /root/output /root/output")

get_ipython().run_cell_magic(u'bash', u'--err pig_out --bg ', u'pig -f preprocess1.pig')



while True:
    line = pig_out.readline()
    if not line: 
        break
    sys.stdout.write("%s" % line)
    sys.stdout.flush()


def read_dataset(cols):
  fhandle = open('/root/output/part-m-00000', 'r')
  pieces = []
  pieces.append(pd.read_csv(fhandle, names=cols, dtype=None,header=None, delimiter="\t"))
  fhandle.close()
  return pd.concat(pieces, ignore_index=True)


cols = ['time', 'ip', 'country', 'status'];
pHandle = read_dataset( cols)

pHandle.shape


values = ['153.222.218.103', '149.103.25.161']
df2 = pHandle[pHandle['ip'].isin(values)]
df2


%%writefile preprocess1.pig
DEFINE preprocess( ) returns data {
    
       tmp = LOAD '/root/data_1' using PigStorage(',')
    
        as ( time: chararray, ip: chararray, country: chararray, status: chararray);
    
      $data = filter tmp by country=='US';
    
};

data = preprocess();

STORE data INTO '/root/output';


copyToLocal /root/output /root/output


%%bash --err pig_out --bg 
pig -f preprocess1.pig


while True:
    line = pig_out.readline()
    if not line: 
        break
    sys.stdout.write("%s" % line)
    sys.stdout.flush()

def read_dataset(cols):
  fhandle = open('/root/output/part-m-00000', 'r')
  pieces = []
  pieces.append(pd.read_csv(fhandle, names=cols, dtype=None,header=None, delimiter="\t"))
  fhandle.close()
  return pd.concat(pieces, ignore_index=True)


cols = ['time', 'ip', 'country', 'status'];
pHandle = read_dataset( cols)

pHandle.shape

values = ['153.222.218.103', '149.103.25.161']
df2 = pHandle[pHandle['ip'].isin(values)]
df2










