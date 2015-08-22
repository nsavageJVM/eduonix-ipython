
# cell for IPython notbook only

# assumes we have tsv data in '/root/temp.tsv' 

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
%matplotlib inline


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



