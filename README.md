# Acquire-Valued-Shoppers-Challenge
## Since the transactions is about 20G, it is a trouble. We need to make a trick on it.
* We can filter those records which do not contain any of the **companies**, **brands**, or **categories** compare to the offer.
* Then it reduced to about 1.74G.
* It runs in about 5 ~ 15 minutes and will reduce the 35 million rows to 27 million rows.
* The efficiency depends on the computer.
## Then we start to creat base-features as below:
* X1 = 1/0 : if a customer ever/never bought the same **category** product of the offer before the offer date.
* X2 = 1/0 : if a customer ever/never bought the same **company** product of the offer before the offer date.
* X3 = 1/0 : if a customer ever/never bought the same **brand** product of the offer before the offer date.
* X4 : X1 * X2 * X3.
* X5 : X2 * X3.
* X6 : X1 * X3.
* X7 : historical total amount of a customer ID for all product.
* X8 : offervalue.
##### ccb = categoty, company, brand.
##### days = 30, 60, 90, 120, 150, 180, inf.
* X9,..., X29 : CD(ccb, days)
  * CD :　the purchase number of times for the same ccb and days.
* X30,..., X50 : A(ccb, days)
  * A :　the total amount of CD.
* X51,..., X71 : Q(ccb, days)
  * Q :　the total quantity of CD.
## Let start to creat secondary features as below:
* X72 : the total amount a customer has bought the product that its **category/company** is on offer before the offer date.
* X73 : the total amount a customer has bought for all product in last 30 days before the offer date.
* X74 : the total amount a customer has bought the same **category, company, brand** product of the offer in last 30 days before the offer date.
* X75 : the total quantity a customer has bought the same **category, company, brand** product of the offer in last 30 days before the offer date.
* X76 : the total amount a customer has bought the same **category, company, brand** product of the offer before the offer date. 
* X77 : the total quantity a customer has bought the same **category, company, brand** product of the offer before the offer date.
* X78 : the total amount a customer has bought the product that its **dept of the category** is on offer in last 30 days before the offer date.
* X79 : the total quantity a customer has bought the product that its **dept of the category** is on offer in last 30 days before the offer date.
* X80 : how many visits in last 30 days before the offer date.
* X81 : The interval between a customer's first transaction and the offer date.
* X82 : The interval between a customer's last transaction and the offer date.
#### Source: [Acquire Valued Shoppers Challenge](https://www.kaggle.com/c/acquire-valued-shoppers-challenge)
