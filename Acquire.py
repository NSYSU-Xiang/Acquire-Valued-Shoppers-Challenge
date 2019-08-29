# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
from dateutil.parser import parse
from itertools import chain, repeat
import pickle
import time
import logging
import argparse

path = ".\\Desktop"
logger = logging.getLogger('Acquire')

class Pickle:
    def __init__(self):
        self.site = 'Shopper'
        
    def prepare_dump(self):
        dirname = os.path.join(path, self.site)
        os.makedirs(dirname, exist_ok = True)
        ts = time.time()
        filename = os.path.join(dirname, '{}.{}.pickle'.format(self.site, ts))
        return filename
    
    def syncbuf(self, obj):
        if len(obj) > 0:          
            output_file = self.prepare_dump()
            with open(output_file, 'wb') as fd:
                pickle.dump(obj, fd)
            logger.info("Convert to pickle file successfully")
        else:
            logger.warning("Fail to convert since the file is empty")
    
    def loadall(self, filename):
        pkl_list = []
        for pkl in filename:
            with open(pkl, "rb") as f:
                pkl_list.append(pickle.load(f))
        return pkl_list
    
class Acquire:
    def __init__(self):
        self.ccb = ['category', 'company', 'brand']
        self.wanted = {}
        self.total_cost = pd.DataFrame()
        self.rep_ccb = list(chain.from_iterable(repeat(n, 7) for n in self.ccb)) #在30 60 90 120 150 180 all 天數內購買此category, company, brand
        self.days = [30, 60, 90, 120, 150, 180, 1000] * 7 #共生成7(時間段) * 3(購買次數 購買量 購買花費) * 3(category, company, brand) = 63個變數
        self.offers_file = "offers.csv"
        self.transactions_file = "transactions.csv"
        self.train = "trainHistory.csv"
        self.test = "testHistory.csv"
        self.loc_reduced = "loc_reduced.csv"
        self.check_red = True
        
    def merge_data(self):    
        transactions_offer = pd.read_csv(self.train) #Basis matrix
        offers = pd.read_csv(self.offers_file)
        ###第一種合併方式###
        offers["offer"] = offers["offer"].astype(str)
        transactions_offer["offer"] = transactions_offer["offer"].astype(str)
        #trainHistory跟offer藉由offer合併
        transactions_offer_new = pd.merge(transactions_offer, offers, how = 'inner', on='offer')
        transactions_offer_new = transactions_offer_new.sort_values(by = 'id').reset_index(drop=True)
        transactions_offer_new.rename(columns={'repeater':'label'}, inplace = True)
        transactions_offer_new["label"].replace({"t": "1", "f": "0"}, inplace=True)
        return transactions_offer_new
                
    def intersection(self, df1, df2):
        return list (set(df1) & set(df2))
    
    def mask(self, df, key, value):
        return df[key] <= value
    
    def creat(self):
        start = time.time()
        transactions_offer_new = self.merge_data() #productquantity and purchaseamount 有負值代表退貨
        #pd.DataFrame.mask = self.mask#過濾函數並定義在Dataframe中，返回布林值
        while self.check_red:
            try:
                #os.path.isfile(self.loc_reduced)
                open(self.loc_reduced) #check reduce file exists or not
            except FileNotFoundError:
                self.logger.warning("Could not find compression data. Re-compress!!")
                os.chdir(path)
                from reduced_data import Reduce
                os.chdir('..')
                Reduce().reduced_data()
            else:
                self.logger.info("Start to creat base-features!!")
                for item in self.ccb:
                    loc_reduced = pd.read_csv(self.loc_reduced, chunksize=500000)
                    self.wanted.update({item : pd.DataFrame()}) #建立名字為item的空列表
                    #在list中，append可以直接改變內容，但在df中要創建新對象
                    #a.append(~~)、 a = a.append(~~)
                    #若用Dataframe形式合併會讓效率變低而且會佔用更多內存
                    #因此若需要大量合併資料，可以先創建List中append，最後再合併成df
                    df_list = []
                    for chunksize_data in loc_reduced:
                        if chunksize_data.empty:
                            break    
                        check = pd.merge(transactions_offer_new[['id', item, 'offerdate']],
                                         chunksize_data[['id', item, 'date', 'purchasequantity', 'purchaseamount']],
                                         how = 'inner',
                                         on = ['id', item])
                        if check.empty:
                            continue
                        df_list.append(check)
                    self.wanted[item] = pd.concat(df_list)
                    #self.wanted[item] = self.wanted.append(df_list)
                    #轉換日期形式計算時間差
                    date = self.wanted[item]['date'].apply(parse)
                    offerdate = self.wanted[item]['offerdate'].apply(parse)
                    self.wanted[item]['day_diff'] = (offerdate - date).dt.days  
                    self.logger.info('%s is Done!!', item)
                self.logger.info("Spend %s sec.", (round(time.time()-start, 2)))
                
                transaction = pd.read_csv(self.transactions_file, chunksize=500000)
                df_list = []
                for chunksize_data in transaction:
                    if chunksize_data.empty:
                        break    
                    check = chunksize_data[['id', 'purchaseamount']]
                    if check.empty:
                        continue
                    df_list.append(pd.DataFrame(check.groupby('id')['purchaseamount'].sum()))
                self.total_cost = pd.concat(df_list)
                self.logger.info('total_cost is Done!!')
                self.logger.info("Spend %s sec.", (round(time.time()-start, 2)))
                
                #X1:提供offerdate之前有購買此category的ID
                length_X1 = self.intersection(transactions_offer_new['id'], self.wanted['category']['id'])
                #1:之前有購買, 0:之前沒有購買
                transactions_offer_new = pd.merge(transactions_offer_new,
                                                  pd.DataFrame({'id':length_X1, 'X1':1.}),
                                                  how='outer', on = ['id']).fillna(0)
                #X2:提供offerdate之前有購買此company的ID
                length_X2 = self.intersection(transactions_offer_new['id'], self.wanted['company']['id'])
                #1:之前有購買, 0:之前沒有購買
                transactions_offer_new = pd.merge(transactions_offer_new,
                                                  pd.DataFrame({'id':length_X2, 'X2':1.}),
                                                  how='outer', on = ['id']).fillna(0)
                #X3:提供offerdate之前有購買此brand的ID
                length_X3 = self.intersection(transactions_offer_new['id'], self.wanted['brand']['id'])
                #1:之前有購買, 0:之前沒有購買
                transactions_offer_new = pd.merge(transactions_offer_new,
                                                  pd.DataFrame({'id':length_X3, 'X3':1.}),
                                                  how='outer', on = ['id']).fillna(0)
                
                # X4 = X1*X2*X3
                # X5 = X2*X3
                # X6 = X1*X3
                transactions_offer_new['X4'] = transactions_offer_new[['X1', 'X2', 'X3']].apply(np.prod, axis = 1)
                transactions_offer_new['X5'] = transactions_offer_new[['X2', 'X3']].apply(np.prod, axis = 1)
                transactions_offer_new['X6'] = transactions_offer_new[['X1', 'X3']].apply(np.prod, axis = 1)
                
                # X7 : 計算每一個ID的過往消費總額
                #在分組計算總合，避免同個ID橫跨兩個chunksize
                self.total_cost = round(self.total_cost.groupby(['id']).sum(), 2)
                #加入ID(原先的index)
                self.total_cost['id'] = self.total_cost.index
                self.total_cost = self.total_cost.rename(columns={'purchaseamount': 'X7'}).reset_index(drop=True)
                transactions_offer_new = pd.merge(transactions_offer_new, self.total_cost, on = ['id'])
                # X8 = offervalue
                transactions_offer_new = transactions_offer_new.rename(columns={'offervalue': 'X8'})
                
                # X9 ~ X29
                #購買次數(True個數)
                colnames = ['X' + str(i) for i in list(range(9, 30))]
                #for cat, colname, day in zip(rep_ccb, colnames, days):
                #    print(cat, colname, day)
                for cat, colname, day in zip(self.rep_ccb, colnames, self.days): 
                    days_check = pd.concat([self.wanted[cat]['id'],
                                            self.mask(self.wanted[cat], 'day_diff', day)], axis = 1).groupby(['id']).sum()
                    days_check['id'] = days_check.index
                    days_check = days_check.rename(columns={'day_diff': colname}).reset_index(drop=True)
                    transactions_offer_new = pd.merge(transactions_offer_new,
                                                      days_check,
                                                      how='outer', on = ['id']).fillna(0)
                
                # X30 ~ X50
                #購買花費(purchaseamount)
                colnames = ['X' + str(i) for i in list(range(30, 51))]
                for cat, colname, day in zip(self.rep_ccb, colnames, self.days): 
                    days_check = pd.concat([self.wanted[cat][['id', 'purchaseamount']],
                                            self.mask(self.wanted[cat], 'day_diff', day)], axis = 1)
                    purchase = pd.DataFrame(days_check[days_check['day_diff'] == True].groupby(['id']).sum()['purchaseamount'])
                    purchase['id'] = purchase.index
                    purchase = purchase.rename(columns={'purchaseamount': colname}).reset_index(drop=True)
                    transactions_offer_new = pd.merge(transactions_offer_new,purchase,
                                                      how='outer',
                                                      on = ['id']).fillna(0)
                
                # X51 ~ X71
                # 購買量(purchasequantity)
                colnames = ['X' + str(i) for i in list(range(51, 72))]
                for cat, colname, day in zip(self.rep_ccb, colnames, self.days): 
                    days_check = pd.concat([self.wanted[cat][['id', 'purchasequantity']],
                                            self.mask(self.wanted[cat], 'day_diff', day)], axis = 1)
                    purchase = pd.DataFrame(days_check[days_check['day_diff'] == True].groupby(['id']).sum()['purchasequantity'])
                    purchase['id'] = purchase.index
                    purchase = purchase.rename(columns={'purchasequantity': colname}).reset_index(drop=True)
                    transactions_offer_new = pd.merge(transactions_offer_new,purchase,
                                                      how='outer',
                                                      on = ['id']).fillna(0)
                self.check_red = False
                self.logger.info("It totally spends: %s sec. creating all base-features !", (round(time.time()-start, 2)))
                Pickle().syncbuf(self.total_cost)
                Pickle().syncbuf(self.wanted)
                Pickle().syncbuf(self.transactions_offer_new)
                return self.total_cost, self.wanted, transactions_offer_new      

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--debug", help="getall ruten result", action="store_true")
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    total_cost, wanted, transactions_offer_new = Acquire().creat()