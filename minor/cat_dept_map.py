# -*- coding: utf-8 -*-
import pandas as pd
import pickle
cat_dept_map = {}
transaction = pd.read_csv('./transactions.csv', chunksize=500000)
for chunksize_data in transaction:
    if chunksize_data.empty:
        break    
    check = chunksize_data
    if check.empty:
        continue
    # 建立所有消費者紀錄所購買的category所對應的dept
    cat_dept_map.update(zip(check['category'], check['dept']))
     
output_file = 'cat_dept_map.pickle'
with open(output_file, 'wb') as fd:
    pickle.dump(cat_dept_map, fd)