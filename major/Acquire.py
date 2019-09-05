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
        global off_cat, off_com, offers
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
    
    def trans(self, x):#>=0轉成0(無退貨)、<0轉成1(退貨)
        if x >= 0:
            x = 0
        else:
            x = 1
        return x
    
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
            transactions_offer_new = pd.concat([transactions_offer_new,
                                                pd.DataFrame({'dept':off_dep})], axis = 1)

            transaction = pd.read_csv(self.transactions_file, chunksize=500000)
            users = [] #總顧客人數
            total = []
            total_cc_cost = []
            total_30_cost = []
            total_30_ccb = []
            total_ccb = []
            total_30_c_dept = []
            visit_30 = []
            days_since_first_transaction = []
            days_from_lastdata_until_offerdate = []
            return_product = []
            
            #ccb_price = [] #購買屬於全部優惠券其中的商品的價錢
            ccb_count = [] #購買屬於全部優惠券其中的商品的次數
            #cat_price = []
            cat_count = []
            productcounts_B = []
            categorycounts_B = []
            for chunksize_data in transaction:
                if chunksize_data.empty:
                    break    
                check = chunksize_data
                if check.empty:
                    continue
                users.append(check.id.drop_duplicates())                     
                # X72 : 計算每一個ID消費過所有優惠券所屬的company、category總額
                #只要購買的商品所屬的category屬於優惠券中的category的集合就加入計算消費金額
                #只要購買的商品所屬的company屬於優惠券中的company的集合就加入計算消費金額
                c1 = check[check['category'].apply(lambda x: x in off_cat)]
                c2 = check[check['company'].apply(lambda x: x in off_com)]
                #因有可能購買的某項商品同屬於category、company會重複計算
                ##### 因此只計算"有(True)"的，需考慮買的商品同時不屬於的#####
                c3 = c1.append(c2).drop_duplicates()
                total_cc_cost.append(pd.DataFrame(c3.groupby('id')['purchaseamount'].sum())['purchaseamount'])
                
                # X73 : 每個ID在獲得自己的優惠券前30天內的消費總額
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
               
                # X74 : 每個ID在30天內購買自己的優惠券所屬的ccb的金額
                # X75 : 每個ID在30天內購買自己的優惠券所屬的ccb的量
                # 提供優惠券前30天有消費過優惠券的商品('category', 'company', 'brand'要一樣)的總額、數量
                check_ccb = pd.merge(transactions_offer_new[['id', 'category', 'company', 'brand', 'offerdate']],
                                       check[['id', 'date','category', 'company', 'brand', 'purchasequantity', 'purchaseamount']],
                                       how = 'inner')
                
                offerdate = pd.to_datetime(check_ccb['offerdate'])
                date = pd.to_datetime(check_ccb['date'])
                check_ccb['day_diff'] = (offerdate - date).dt.days
                check_30_ccb = pd.concat([check_ccb[['id', 'purchasequantity', 'purchaseamount']], self.mask(check_ccb, 'day_diff', 29)], axis = 1)
                total_30_ccb.append(pd.DataFrame(check_30_ccb[check_30_ccb['day_diff'] == True].groupby(['id']).sum()[['purchasequantity', 'purchaseamount']]))
                
                # X76 : 購買每個ID自己的優惠券所屬的ccb的金額
                # X77 : 購買每個ID自己的優惠券所屬的ccb的量
                # 有消費過優惠券的商品('category', 'company', 'brand'要一樣)的總額、數量
                total_ccb.append(pd.DataFrame(check_30_ccb.groupby(['id']).sum()[['purchasequantity', 'purchaseamount']]))

                # X78 : 每個ID在30天內購買自己的優惠券所屬的category所對應的dept的商品的金額
                # X79 : 每個ID在30天內購買自己的優惠券所屬的category所對應的dept的商品的量
                c_dep = pd.merge(transactions_offer_new[['id', 'offerdate', 'dept']],
                                 check[['id', 'date', 'dept', 'purchasequantity', 'purchaseamount']],
                                 on = ['id', 'dept'])
                offerdate = pd.to_datetime(c_dep['offerdate'])
                date = pd.to_datetime(c_dep['date'])
                c_dep['day_diff'] = (offerdate - date).dt.days
                c_dep_check = pd.concat([c_dep[['id', 'purchasequantity', 'purchaseamount']],
                                         self.mask(c_dep, 'day_diff', 29)], axis = 1)
                total_30_c_dept.append(pd.DataFrame(c_dep_check[c_dep_check['day_diff'] == True].groupby(['id']).sum()[['purchasequantity', 'purchaseamount']]))
                
                # X80: 每個ID在30天內購買了幾天
                # 先算出每筆消費紀錄跟提供優惠券的日期差，然後再刪掉重複
                # 就可以看出一位消費者在30天內消費了幾天
                visit_30_block  = check_block[self.mask(check_block, 'day_diff', 29)].drop(['purchaseamount'], axis = 1).drop_duplicates()
                visit_30.append(pd.value_counts(visit_30_block['id']))
                
                # X81: 每個消費者從第一次購買到發放自己的優惠券的時間間隔
                days_since_first_transaction.append(check_block.drop(['purchaseamount'], axis = 1).drop_duplicates().groupby('id').max()['day_diff'])
                
                # X82: 每個消費者最後一次購買到發放自己的優惠券的時間間隔
                days_from_lastdata_until_offerdate.append(check_block.drop(['purchaseamount'], axis = 1).drop_duplicates().groupby('id').min()['day_diff'])
                
                # X83 : 是否有退貨(每個ID自己的優惠券所屬的商品) 找出花費最少再判斷是>=0還<0
                return_product.append(pd.DataFrame(check_ccb.groupby(['id']).min()['purchaseamount']))
                
                # X84 : 各個優惠券所提供的商品的市場占有率(優惠券所屬的商品('category', 'company', 'brand')的銷售次數/優惠券所屬的種類('category')的銷售次數)
                # 因offers有重複ccb，merge時會重複產生，需去重複
                check_ccb_count = pd.merge(offers[self.ccb].drop_duplicates(), check)
                ccb_count.append(check_ccb_count[self.ccb].groupby(self.ccb).size().reset_index(name="counts_ccb"))
                check_cat_count=  pd.merge(pd.DataFrame(offers['category']), check).drop_duplicates()
                cat_count.append(pd.value_counts(check_cat_count['category']).reset_index(name="counts_cat").rename(columns = {'index':'category'}))
                
                # X86 :  share_of_cust_bought_prod
                productcounts_check = pd.merge(offers[self.ccb].drop_duplicates(), check[['id', 'category', 'company', 'brand']])
                productcounts_B.append(productcounts_check)
                
                # X87 :  share_of_cust_bought_cat
                categorycounts_check = pd.merge(pd.DataFrame(offers['category']).drop_duplicates(), check[['id', 'category']])
                categorycounts_B.append(categorycounts_check)
                
                # X7 : 計算每一個ID的過往消費總額
                total.append(pd.DataFrame(check.groupby('id')['purchaseamount'].sum())['purchaseamount'])
            
            users = len(pd.concat(users).drop_duplicates())
            total_cc_cost = pd.concat(total_cc_cost) #it's Series
            total_30_cost = pd.concat(total_30_cost)
            total_30_ccb = pd.concat(total_30_ccb)
            total_ccb = pd.concat(total_ccb)
            total_30_c_dept = pd.concat(total_30_c_dept)
            visit_30 = pd.concat(visit_30)
            days_since_first_transaction = pd.concat(days_since_first_transaction)
            days_from_lastdata_until_offerdate = pd.concat(days_from_lastdata_until_offerdate)
            return_product = pd.concat(return_product)
            ccb_count = pd.concat(ccb_count)
            cat_count = pd.concat(cat_count)
            productcounts_B = pd.concat(productcounts_B)
            categorycounts_B = pd.concat(categorycounts_B)
            self.total_cost = pd.concat(total) #it's Series
            logger.info('cost related work are Done!!')
            logger.info("Spend %s sec.", (round(time.time()-start, 2)))
            
            #X1:提供優惠券之前有購買此優惠券所屬的category的ID
            length_X1 = self.intersection(transactions_offer_new['id'], self.wanted['category']['id'])
            #1:之前有購買, fillna(0):之前沒有購買
            transactions_offer_new = pd.merge(transactions_offer_new,
                                              pd.DataFrame({'id':length_X1, 'X1':1.}),
                                              how='outer', on = ['id']).fillna(0)
            #X2:提供優惠券之前有購買此優惠券所屬的company的ID
            length_X2 = self.intersection(transactions_offer_new['id'], self.wanted['company']['id'])
            #1:之前有購買, fillna(0):之前沒有購買
            transactions_offer_new = pd.merge(transactions_offer_new,
                                              pd.DataFrame({'id':length_X2, 'X2':1.}),
                                              how='outer', on = ['id']).fillna(0)
            #X3:提供優惠券之前有購買此優惠券所屬的brand的ID
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
            #下載base-features
            Pickle().syncbuf(transactions_offer_new.drop(['offerdate', 'category',
                                                       'company', 'brand', 'dept', 'quantity'], axis = 1))
            # secondary features
            # See also X7
            # X72 : 計算每一個ID消費過所有優惠券所屬的company、category總額
            total_cc_cost = round(total_cc_cost.groupby(['id']).sum(), 2)
            total_cc_cost = total_cc_cost.reset_index().rename(columns={'purchaseamount': 'X72'})
            ##### 考慮買的商品同時不屬於的 #####
            not_cc = list(set(transactions_offer_new['id']).difference(set(total_cc_cost['id'])))
            total_cc_cost = total_cc_cost.append(pd.DataFrame({'id':not_cc, 'X72':0}))
            transactions_offer_new = pd.merge(transactions_offer_new, total_cc_cost, on = ['id'])
            
            # X73 : 每個ID在獲得自己的優惠券前30天內的消費總額
            total_30_cost = round(total_30_cost.groupby(['id']).sum(), 2)
            total_30_cost = total_30_cost.reset_index().rename(columns={'purchaseamount': 'X73'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_30_cost,
                                              on = ['id'], how = 'outer').fillna(0)
            
            # X74 : 每個ID在30天內購買自己的優惠券所屬的ccb的金額
            total_30_ccb_A = round(total_30_ccb.groupby(['id']).sum()['purchaseamount'], 2)
            total_30_ccb_A = total_30_ccb_A.reset_index().rename(columns={'purchaseamount': 'X74'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_30_ccb_A,
                                              on = ['id'], how = 'outer').fillna(0)
            # X75 : 每個ID在30天內購買自己的優惠券所屬的ccb的量
            total_30_ccb_Q = round(total_30_ccb.groupby(['id']).sum()['purchasequantity'], 2)
            total_30_ccb_Q = total_30_ccb_Q.reset_index().rename(columns={'purchasequantity': 'X75'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_30_ccb_Q,
                                              on = ['id'], how = 'outer').fillna(0)
            
            # X76 : 購買每個ID自己的優惠券所屬的ccb的金額
            total_ccb_A = round(total_ccb.groupby(['id']).sum()['purchaseamount'], 2)
            total_ccb_A = total_ccb_A.reset_index().rename(columns={'purchaseamount': 'X76'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_ccb_A,
                                              on = ['id'], how = 'outer').fillna(0)
            # X77 : 購買每個ID自己的優惠券所屬的ccb的量
            total_ccb_Q = round(total_ccb.groupby(['id']).sum()['purchasequantity'], 2)
            total_ccb_Q = total_ccb_Q.reset_index().rename(columns={'purchasequantity': 'X77'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_ccb_Q,
                                              on = ['id'], how = 'outer').fillna(0)
            
            # X78 : 每個ID在30天內購買自己的優惠券所屬的category所對應的dept的商品的金額
            total_30_c_cept_Q = round(total_30_c_dept.groupby(['id']).sum()['purchaseamount'], 2)
            total_30_c_cept_Q = total_30_c_cept_Q.reset_index().rename(columns={'purchaseamount': 'X78'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_30_c_cept_Q,
                                              on = ['id'], how = 'outer').fillna(0)
            
            # X79 : 每個ID在30天內購買自己的優惠券所屬的category所對應的dept的商品的量
            total_30_c_cept_A = round(total_30_c_dept.groupby(['id']).sum()['purchasequantity'], 2)
            total_30_c_cept_A = total_30_c_cept_A.reset_index().rename(columns={'purchasequantity': 'X79'})
            transactions_offer_new = pd.merge(transactions_offer_new, total_30_c_cept_A,
                                              on = ['id'], how = 'outer').fillna(0)
            # X80: 每個ID在30天內購買了幾天
            visit_30 = visit_30.reset_index().groupby(['index']).sum()
            visit_30 = visit_30.reset_index().rename(columns={'index' : 'id', 'id' : 'X80'})
            transactions_offer_new = pd.merge(transactions_offer_new, visit_30,
                                              on = ['id'], how = 'outer').fillna(0)
            
            # X81: 每個消費者從第一次購買到發放自己的優惠券的時間間隔
            days_since_first_transaction = days_since_first_transaction.groupby(['id']).max()
            days_since_first_transaction = days_since_first_transaction.reset_index().rename(columns={'day_diff': 'X81'})
            transactions_offer_new = pd.merge(transactions_offer_new, days_since_first_transaction,
                                              on = ['id'])
            
            # X82: 每個消費者最後一次購買到發放自己的優惠券的時間間隔
            days_from_lastdata_until_offerdate = days_from_lastdata_until_offerdate.groupby(['id']).min()
            days_from_lastdata_until_offerdate = days_from_lastdata_until_offerdate.reset_index().rename(columns={'day_diff': 'X82'})
            transactions_offer_new = pd.merge(transactions_offer_new, days_from_lastdata_until_offerdate,
                                              on = ['id'])
            
            # X83 : 是否有退貨(每個ID自己的優惠券所屬的商品) 找出花費最少再判斷是>=0還<0
            return_product = round(return_product.groupby(['id']).min(), 2)
            return_product = return_product.reset_index().rename(columns={'purchaseamount': 'X83'})
            return_product['X83'] = return_product['X83'].apply(self.trans)
            transactions_offer_new = pd.merge(transactions_offer_new, return_product,
                                              on = ['id'], how = 'outer').fillna(0)
            
            # X84 : 各個優惠券所提供的商品的市場占有率(優惠券所屬的商品('category', 'company', 'brand')的銷售次數/優惠券所屬的種類('category')的銷售次數)
            ccb_count = ccb_count.groupby(self.ccb).sum().reset_index()
            cat_count = cat_count.groupby('category').sum().reset_index()
            marketshare_in_cat = pd.merge(ccb_count, cat_count, on = 'category')
            marketshare_in_cat['X84'] = marketshare_in_cat.counts_ccb/marketshare_in_cat.counts_cat
            transactions_offer_new = pd.merge(transactions_offer_new, marketshare_in_cat[['category', 'company', 'brand', 'X84']], on = self.ccb)
            
            # X85 : prodid_spend_corr
            prodid_spend_corr = pd.DataFrame({'X85':np.where((transactions_offer_new['X76'] >= transactions_offer_new['X84']) ,
                                                             transactions_offer_new['X76'], transactions_offer_new['X84']*(-100))})
            transactions_offer_new = pd.concat([transactions_offer_new, prodid_spend_corr], axis = 1)
            
            # X86 :  share_of_cust_bought_prod
            productcounts_B = productcounts_B.drop_duplicates().groupby(self.ccb).size().reset_index(name="productcounts")
            productcounts_B['X86'] = productcounts_B['productcounts']/users #share_of_cust_bought_prod
            transactions_offer_new = pd.merge(transactions_offer_new, productcounts_B.drop('productcounts', axis = 1), on = self.ccb)
           
            # X87 :  share_of_cust_bought_cat
            categorycounts_B = categorycounts_B.drop_duplicates().groupby('category').size().reset_index(name="categorycounts")
            categorycounts_B['X87'] = categorycounts_B['categorycounts']/users
            transactions_offer_new = pd.merge(transactions_offer_new, categorycounts_B.drop('categorycounts', axis = 1), on = 'category')
            
            logger.info("It totally spends: %s sec. creating all features !", (round(time.time()-start, 2)))
            #Pickle().syncbuf(self.total_cost)
            #Pickle().syncbuf(self.wanted)
            Pickle().syncbuf(transactions_offer_new.drop(['offerdate', 'category',
                                                       'company', 'brand', 'dept', 'quantity'], axis = 1))#下載all-features
            return self.total_cost, self.wanted, transactions_offer_new.sort_values('id').reset_index(drop=True)   

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--debug", help="getall ruten result", action="store_true")
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    Acquire().creat()
    #total_cost, wanted, transactions_offer_new = Acquire().creat()