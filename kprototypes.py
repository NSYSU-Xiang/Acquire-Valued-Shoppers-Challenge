# -*- coding: utf-8 -*-
import os
import pandas as pd
import pickle
import time
from sklearn.preprocessing import StandardScaler
from kmodes.kprototypes import KPrototypes
path = "C:\\Users\\Xiang\\Desktop\\Shopper"
os.chdir(path) #change dir
# feature normalization (feature scaling)
z_scaler = StandardScaler()
with open ('wanted.pickle', 'rb') as df:
    wanted = pickle.load(df)

item = ['brand', 'category', 'company']
df = {}
for i in item:
    deal = wanted[i].drop(['offerdate', 'date', 'id'], axis = 1)
    z_scale = pd.DataFrame(z_scaler.fit_transform(deal.iloc[:,1:]), columns=deal.columns[1:])
    deal.iloc[:,1:] = z_scale
    deal.index = wanted[i]['id']
    df[i] = deal

cost = {}
start = time.time()
K = range(1, 7)
for i in item:
    cost.update({i : []})
    for k in K:
        print('進入k = %s'%(k))
        result = KPrototypes(n_clusters = k, init='Cao', verbose = 1,
                             n_init = 2, n_jobs = -1).fit(df[i], categorical = [0])
        print(result.cost_)
        cost[i].append(result.cost_)

print("花了 %s"%( round(time.time() - start, 2)))