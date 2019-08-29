# Acquire-Valued-Shoppers-Challenge
### Since the transactions is about 20G, it is a trouble. We need to make a trick on it.
* We can filter those records which do not contain any of the companies, brands, or categories compare to the offer.
* Then it reduced to about 1.74G.
* It runs in about 5 ~ 15 minutes and will reduce the 35 million rows to 27 million rows.
* The efficiency depends on the computer.
### Then we start to creat base-features.
* X1 = 1/0 : if the customer ever/never bought the same category product of the offer before the offer date.
* X2 = 1/0 : if the customer ever/never bought the same company product of the offer before the offer date.
* X3 = 1/0 : if the customer ever/never bought the same brand product of the offer before the offer date.
* X4 : X1$\times$X2$\times$X3.
* X5 : X2$\times$X3.
* X6 : X1$\times$X3.
* X7 : historical total spend of a customer ID for all product.
* X8 : offervalue.

#### Source: https://www.kaggle.com/c/acquire-valued-shoppers-challenge
