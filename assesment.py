
import pandas as pd
import json
import os
from sqlalchemy import create_engine
import pymysql



customer = pd.read_csv("customers.csv")
product = pd.read_csv("products.csv")

 
# Get the list of all files and directories

dir_list = os.listdir("transactions/")
 




jsonData = []
for folder in dir_list:
    for line in open("transactions/"+folder+"/transactions.json", 'r'):
        jsonData.append(json.loads(line))

jsonDf = pd.DataFrame(jsonData)


basket = pd.DataFrame(jsonDf['basket'].to_list())

customer_product = pd.concat([jsonDf['customer_id'],basket],axis =1)

customer_product.columns = ['customer_id','product_1','product_2','product_3']



class my_dictionary(dict):
  
    # __init__ function
    def __init__(self):
        self = dict()
          
    # Function to add key:value
    def add(self, key, value):
        self[key] = value




newList = []
for row, series in customer_product.iterrows():
    newDict = my_dictionary()
    if series['product_1'] is not None:
            newDict.add('customer_id',series['customer_id'])
            newDict.add('product_id',series['product_1']['product_id'])
            newDict.add('price',series['product_1']['price'])
            newList.append(newDict)
            
    if series['product_2'] is not None:
            newDict.add('customer_id',series['customer_id'])
            newDict.add('product_id',series['product_2']['product_id'])
            newDict.add('price',series['product_2']['price'])
            newList.append(newDict)
    if series['product_3'] is not None:
            newDict.add('customer_id',series['customer_id'])
            newDict.add('product_id',series['product_3']['product_id'])
            newDict.add('price',series['product_3']['price'])
            newList.append(newDict)


custProductPrice = pd.DataFrame(newList)



cust_custPro = pd.DataFrame.merge(custProductPrice,customer,on='customer_id')


cust_pro = pd.DataFrame.merge(cust_custPro,product,on='product_id')



finalDf = cust_pro[['customer_id','loyalty_score','product_id','product_category']]


assessment = finalDf.groupby(['customer_id','loyalty_score','product_id','product_category']).size().sort_values(ascending=False).reset_index(name='count') 

assessment.columns = ["customer_id","loyalty_score","product_id","product_category","purchase_count"]

out = assessment.to_json(orient = 'records')[1:-1].replace('},{', '} {')

with open('assessment.json', 'w') as f:
    f.write(out)


# for testing
for row, series in finalDf.iterrows():
    if series['customer_id'] == 'C116':
        if series['product_id'] == 'P62':
            print(series['product_id'])
