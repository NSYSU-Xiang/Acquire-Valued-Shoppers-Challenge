# -*- coding: utf-8 -*-
import os
import time
import logging
import re
path = "C:\\Users\\Xiang\\Desktop"
os.chdir(path) #change dir
class Reduce:
    logger = logging.getLogger('Reduce')
    def __init__(self):
        self.offers_cat = {}
        self.offers_com = {}
        self.offers_brand = {}
        self.offers_file = "offers.csv"
        self.transactions_file = "transactions.csv"
        self.loc_reduced = "loc_reduced.csv"
        
    def get(self):
        for e, line in enumerate(open(self.offers_file)):
            if e == 0:
                continue
            else:
                self.offers_cat[line.split(",")[1]] = 1
                self.offers_com[line.split(",")[3]] = 1
                self.offers_brand[re.sub('\n', '', line.split(",")[5])] = 1
        self.logger.info("searched: %s category, searched: %s company, searched: %s brand",
                    len(self.offers_cat), len(self.offers_com), len(self.offers_brand))
    
    def reduced_data(self):
        with open(self.loc_reduced, "w") as outfile:
            self.get()
            reduced = 0
            start = time.time()
            for index, line in enumerate(open(self.transactions_file)): 
                if index == 0:
                    outfile.write(line) #header
                else:
                    if line.split(",")[3] in self.offers_cat or line.split(",")[4] in self.offers_com or line.split(",")[5] in self.offers_brand:
                        outfile.write(line)
                        reduced += 1
                if index % 5000000 == 0:
                    self.logger.info("searched: %s, inserted: %s, spend: %s sec" , index, reduced, round(time.time() - start, 2))
            self.logger.info("Reduced Done!! searched: %s, inserted: %s, spend: %s sec" , index, reduced, round(time.time() - start, 2))