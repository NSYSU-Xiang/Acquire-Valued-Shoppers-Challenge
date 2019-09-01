# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
from itertools import chain, repeat
import pickle
import time
import logging
import argparse
from reduced_data import Reduce

logger = logging.getLogger('Acquire')
os.chdir('..')

class Pickle:
    def __init__(self):
        self.site = 'Shopper'
        
    def prepare_dump(self):
        dirname = os.path.join(os.getcwd(), self.site)
        os.makedirs(dirname, exist_ok = True)
        ts = time.time()
        filename = os.path.join(dirname, '{}.{}.pickle'.format(self.site, ts))
        return filename
    
    def syncbuf(self, obj):
        if len(obj) > 0:          
            output_file = self.prepare_dump()
            with open(output_file, 'wb') as fd:
                pickle.dump(obj, fd)
            logger.info("Convert to pickle file successfully !!")
        else:
            logger.warning("Fail to convert since the file is empty !!")
    
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
        
    def merge_data(self):    
        global off_cat, off_com
        transactions_offer = pd.read_csv(self.train) #Basis matrix
        offers = pd.read_csv(self.offers_file)
        off_cat = set(offers['category'])
        off_com = set(offers['company'])
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
        try: #check reduce file exists or not
            open(self.loc_reduced) 
        except FileNotFoundError: #Not exist, then run this block
            logger.warning("Could not find compression data. Re-compress!!")
            Reduce().reduced_data()
        finally: #Finally, run this block
            logger.info("Start to creat base-features!!")
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
                date = pd.to_datetime(self.wanted[item]['date'])
                offerdate = pd.to_datetime(self.wanted[item]['offerdate'])
                self.wanted[item]['day_diff'] = (offerdate - date).dt.days 
                logger.info('%s is Done!!', item)
            logger.info("Spend %s sec.", (round(time.time()-start, 2)))

            # 匯入所有消費者紀錄所購買的category所對應的dept
            with open('cat_dept_map.pickle', "rb") as f:
                cat_dept_map = pickle.load(f)
            off_dep = []
            #建立優惠券所屬的種類所對應的dept
            for i in transactions_offer_new['category']:
                off_dep.append(cat_dept_map[i])
            transactions_offer_dep = pd.concat([transactions_offer_new,
                                                pd.DataFrame({'dept':off_dep})], axis = 1)

            transaction = pd.read_csv(self.transactions_file, chunksize=500000)
            total = []
            total_cc_cost = []
            total_30_cost = []
            total_30_ccb = []
            total_ccb = []
            total_30_c_dept = []
            visit_30 = []
            for chunksize_data in transaction:
                if chunksize_data.empty:
                    break    
                check = chunksize_data
                if check.empty:
                    continue                         
                # X72 : 計算每一個ID的消費過優惠券所屬的company、category總額
                #只要購買的商品所屬的category屬於優惠券中的category就加入計算消費金額
                #只要購買的商品所屬的company屬於優惠券中的category就加入計算消費金額
                c1 = check[check['category'].apply(lambda x: x in off_cat)]
                c2 = check[check['company'].apply(lambda x: x in off_com)]
                #因有可能購買的某項商品同屬於category、company會重複計算
                ##### 因此只計算"有(True)"的，需考慮買的商品同時不屬於的#####
                c3 = c1.append(c2).drop_duplicates()
                total_cc_cost.append(pd.DataFrame(c3.groupby('id')['purchaseamount'].sum())['purchaseamount'])
                
                # X73 : 提供優惠券前30天內消費總額
                #提供優惠券前30天內消費總額
                check_block = pd.merge(transactions_offer_new[['id', 'offerdate']],
                                       check[['id', 'date', 'purchaseamount']],
                                       how = 'inner')
                offerdate = pd.to_datetime(check_block['offerdate'])
                date = pd.to_datetime(check_block['date'])
                check_block['day_diff'] = (offerdate - date).dt.days 
                check_30_cost = pd.concat([check_block[['id', 'purchaseamount']], self.mask(check_block, 'day_diff', 29)], axis = 1)
                #注意，此僅捕捉True，也就是在30天內有消費紀錄的，之後合併時要考慮未消費的(要補0)
                total_30_cost.append(pd.DataFrame(check_30_cost[check_30_cost['day_diff'] == True].groupby(['id']).sum()['purchaseamount']))
               
                # X74 : 30天內購買優惠券所屬的ccb的金額
                # X75 : 30天內購買優惠券所屬的ccb的量
                # 提供優惠券前30天有消費過優惠券的商品('category', 'company', 'brand'要一樣)的總額、數量
                check_ccb = pd.merge(transactions_offer_new[['id', 'category', 'company', 'brand', 'offerdate']],
                                       check[['id', 'date','category', 'company', 'brand', 'purchasequantity', 'purchaseamount']],
                                       how = 'inner')
                offerdate = pd.to_datetime(check_ccb['offerdate'])
                date = pd.to_datetime(check_ccb['date'])
                check_ccb['day_diff'] = (offerdate - date).dt.days
                check_30_ccb = pd.concat([check_ccb[['id', 'purchasequantity', 'purchaseamount']], self.mask(check_ccb, 'day_diff', 29)], axis = 1)
                total_30_ccb.append(pd.DataFrame(check_30_ccb[check_30_ccb['day_diff'] == True].groupby(['id']).sum()[['purchasequantity', 'purchaseamount']]))
                
                # X76 : 購買優惠券所屬的ccb的金額
                # X77 : 購買優惠券所屬的ccb的量
                # 有消費過優惠券的商品('category', 'company', 'brand'要一樣)的總額、數量
                total_ccb.append(pd.DataFrame(check_30_ccb.groupby(['id']).sum()[['purchasequantity', 'purchaseamount']]))

                # X78 : 30天內購買優惠券所屬的category所對應的dept的商品的金額
                # X79 : 30天內購買優惠券所屬的category所對應的dept的商品的量
                c_dep = pd.merge(transactions_offer_dep[['id', 'offerdate', 'dept']],
                                 check[['id', 'date', 'dept', 'purchasequantity', 'purchaseamount']],
                                 on = ['id', 'dept'])
                offerdate = pd.to_datetime(c_dep['offerdate'])
                date = pd.to_datetime(c_dep['date'])
                c_dep['day_diff'] = (offerdate - date).dt.days
                c_dep_check = pd.concat([c_dep[['id', 'purchasequantity', 'purchaseamount']],
                                         self.mask(c_dep, 'day_diff', 29)], axis = 1)
                total_30_c_dept.append(pd.DataFrame(c_dep_check[c_dep_check['day_diff'] == True].groupby(['id']).sum()[['purchasequantity', 'purchaseamount']]))
                
                # X80: 30天內購買了幾天
                # 先算出每筆消費紀錄跟提供優惠券的日期差，然後再刪掉重複
                # 就可以看出一位消費者在30天內消費了幾天
                visit_30_block  = check_block[self.mask(check_block, 'day_diff', 29)].drop(['purchaseamount'], axis = 1).drop_duplicates()
                visit_30.append(pd.value_counts(visit_30_block['id']))
                
                # X7 : 計算每一個ID的過往消費總額
                total.append(pd.DataFrame(check.groupby('id')['purchaseamount'].sum())['purchaseamount'])
            
            total_cc_cost = pd.concat(total_cc_cost) #it's Series
            total_30_cost = pd.concat(total_30_cost)
            total_30_ccb = pd.concat(total_30_ccb)
            total_ccb = pd.concat(total_ccb)
            total_30_c_dept = pd.concat(total_30_c_dept)
            visit_30 = pd.concat(visit_30)
            self.total_cost = pd.concat(total) #it's Series
            logger.info('cost related work are Done!!')
            logger.info("Spend %s sec.", (round(time.time()-start, 2)))
            
            #X1:提供offerdate之前有購買此category的ID
            length_X1 = self.intersection(transactions_offer_new['id'], self.wanted['category']['id'])
            #1:之前有購買, fillna(0):之前沒有購買
            transactions_offer_new = pd.merge(transactions_offer_new,
                                              pd.DataFrame({'id':length_X1, 'X1':1.}),
                                              how='outer', on = ['id']).fillna(0)
            #X2:提供offerdate之前有購買此company的ID
            length_X2 = self.intersection(transactions_offer_new['id'], self.wanted['company']['id'])
            #1:之前有購買, fillna(0):之前沒有購買
            transactions_offer_new = pd.merge(transactions_offer_new,
                                              pd.DataFrame({'id':length_X2, 'X2':1.}),
                                              how='outer', on = ['id']).fillna(0)
            #X3:提供offerdate之前有購買此brand的ID
            length_X3 = self.intersection(transactions_offer_new['id'], self.wanted['brand']['id'])
            #1:之前有購買, fillna(0):之前沒有購買
            transactions_offer_new = pd.merge(transactions_offer_new,
                                              pd.DataFrame({'id':length_X3, 'X3':1.}),
                                              how='outer', on = ['id']).fillna(0)
            
            # X4 = X1*X2*X3
            # X5 = X2*X3
            # X6 = X1*X3
            transactions_offer_new['X4'] = transactions_offer_new[['X1', 'X2', 'X3']].apply(np.prod, axis = 1)
            transactions_offer_new['X5'] = transactions_offer_new[['X2', 'X3']].apply(np.prod, axis = 1)
            
            transactions_offer_new['X6'] = transactions_offer_new[['X1', 'X3']].apply(np.prod, axis = 1)

            ##### 再進行一次分組計算總合，避免同個ID橫跨兩個chunksize ####
            ##### 針對Series reset_index，ID index會變成一個變數 ####
            # X7 : 計算每一個ID的過往消費總額
            self.total_cost = round(self.total_cost.groupby(['id']).sum(), 2)
            self.total_cost = self.total_cost.reset_index().rename(columns={'purchaseamount': 'X7'})
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
            # secondary features
            # See also X7
            # X72 : 計算每一個ID的消費過優惠券所屬的company、category總額
            total_cc_cost = round(total_cc_cost.groupby(['id']).sum(), 2)
            total_cc_cost = total_cc_cost.reset_index().rename(columns={'purchaseamount': 'X72'})
            ##### 考慮買的商品同時不屬於的 #####
            not_cc = list(set(transactions_offer_new['id']).difference(set(total_cc_cost['id'])))
            total_cc_cost = total_cc_cost.append(pd.DataFrame({'id':not_cc, 'X72':0}))
            transactions_offer_new = pd.merge(transactions_offer_new, total_cc_cost, on = ['id'])
            
            # X73 : 提供優惠券前30天內消費總額
            total_30_cost = round(total_30_cost.groupby(['id']).sum(), 2)
            total_30_cost = total_30_cost.reset_index().rename(columns={'purchaseamount': 'X73'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_30_cost,
                                              on = ['id'], how = 'outer').fillna(0)
            
            # X74 : 30天內購買優惠券所屬的ccb的金額
            total_30_ccb_A = round(total_30_ccb.groupby(['id']).sum()['purchaseamount'], 2)
            total_30_ccb_A = total_30_ccb_A.reset_index().rename(columns={'purchaseamount': 'X74'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_30_ccb_A,
                                              on = ['id'], how = 'outer').fillna(0)
            # X75 : 30天內購買優惠券所屬的ccb的量
            total_30_ccb_Q = round(total_30_ccb.groupby(['id']).sum()['purchasequantity'], 2)
            total_30_ccb_Q = total_30_ccb_Q.reset_index().rename(columns={'purchasequantity': 'X75'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_30_ccb_Q,
                                              on = ['id'], how = 'outer').fillna(0)
            
            # X76 : 購買優惠券所屬的ccb的金額
            total_ccb_A = round(total_ccb.groupby(['id']).sum()['purchaseamount'], 2)
            total_ccb_A = total_ccb_A.reset_index().rename(columns={'purchaseamount': 'X76'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_ccb_A,
                                              on = ['id'], how = 'outer').fillna(0)
            # X77 : 購買優惠券所屬的ccb的量
            total_ccb_Q = round(total_ccb.groupby(['id']).sum()['purchasequantity'], 2)
            total_ccb_Q = total_ccb_Q.reset_index().rename(columns={'purchasequantity': 'X77'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_ccb_Q,
                                              on = ['id'], how = 'outer').fillna(0)
            
            # X78 : 30天內購買優惠券所屬的category所對應的dept的商品的金額
            total_30_c_cept_Q = round(total_30_c_dept.groupby(['id']).sum()['purchaseamount'], 2)
            total_30_c_cept_Q = total_30_c_cept_Q.reset_index().rename(columns={'purchaseamount': 'X78'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_30_c_cept_Q,
                                              on = ['id'], how = 'outer').fillna(0)
            
            # X79 : 30天內購買優惠券所屬的category所對應的dept的商品的量
            total_30_c_cept_A = round(total_30_c_dept.groupby(['id']).sum()['purchasequantity'], 2)
            total_30_c_cept_A = total_30_c_cept_A.reset_index().rename(columns={'purchasequantity': 'X79'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_30_c_cept_A,
                                              on = ['id'], how = 'outer').fillna(0)
            # X80: 30天內購買了幾天
            visit_30 = visit_30.reset_index().groupby(['index']).sum()
            visit_30 = visit_30.reset_index().rename(columns={'index' : 'id', 'id' : 'X80'})
            transactions_offer_new = pd.merge(transactions_offer_new, visit_30,
                                              on = ['id'], how = 'outer').fillna(0)
            
            logger.info("It totally spends: %s sec. creating all features !", (round(time.time()-start, 2)))
            Pickle().syncbuf(self.total_cost)
            Pickle().syncbuf(self.wanted)
            Pickle().syncbuf(transactions_offer_new)
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