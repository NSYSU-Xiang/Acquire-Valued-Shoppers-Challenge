# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import pandas as pd
import os
os.chdir('..')
class Plot_offer:
    def __init__(self):
        self.train = "trainHistory.csv"
        self.test = "testHistory.csv"
        
    def plot(self):
        df = pd.value_counts(pd.read_csv(self.train)['offer'])
        df1 = pd.value_counts(pd.read_csv(self.test)['offer'])
        ticks = set(list(df.index.values) + list(df1.index.values))#totally 37 offers
        #Acquire().intersection(df.index.values, df1.index.values)
        #only 16 offers in common
        df = pd.DataFrame({'train_offer':df}, index = ticks).fillna(0)
        df1 = pd.DataFrame({'test_offer':df1}, index = ticks).fillna(0)
        pd.concat([df, df1], axis = 1).plot(kind='bar', stacked=True, color = ['#bb0000', '#000000'])
        plt.ylabel("number of shoppers")
        plt.title("Common offers in traing and test set")
        plt.show()