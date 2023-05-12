#libraries 

import pandas as pd
import numpy as np
import glob
import plotly.express as px
import warnings 
warnings.filterwarnings('ignore')


path = 'C:/Users/z031844/Downloads/raw_dile/'
allfiles = glob.glob(path+"*.parquet")


for k in allfiles:
    f = k.replace("\\", "/")
    print('Reading file:', f[36:],'\n')
    #print('Year',f[52:56],'\n')
    df = pd.read_parquet(f)
    
     """ ************ Creating file as DataFrame ************ """  
    
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('amt','amount')
    
    if df.payment_type.dtypes == 'O':
    
        cash = df[(df['payment_type']==2) & (df['tip_amount']!=0)]
        avg = df['total_amount'].sum()/df['total_amount'].count()
        #print('Calculating the average price spend by the customers: \n',avg,"\n")
        df['avg_cost_per_miles'] = round(avg/df['trip_distance'],2)
        #print(df.head())

        """ ************  """

        pay = pd.DataFrame(df['tip_amount'].groupby(df['payment_type']).count())
        """pay[5] = np.NaN
        pay[6] = np.NaN
        pay = pd.DataFrame(pay)
        pay['payment_mode'] = ['Not defined','Credit card','Cash','No charge', 'Dispute','Unknown','Voided trip']
        #print(pay)"""

        """ ************  """

        # Calculated as per BODMAS

        #df['fare_&_extra_per_trip'] = (df['fare_amount'] + df['extra']) / df['trip_distance']
        #print(df.head())

        df['count_of_tips_by_payment'] = 0

        for i,j in zip(pay.index,pay.tip_amount):
            df['count_of_tips_by_payment'][df['payment_type']==i] = j

        #print(df[['count_of_tips_by_payment','payment_type','payment_mode']].head())
        mode = {'payment_type': [0,1,2,3,4,5,6],
         'payment_mode':['Not defined','Credit card','Cash','No charge', 'Dispute','Unknown','Voided trip']}
        mode = pd.DataFrame(mode)
        mode

        final = df[['avg_cost_per_miles','count_of_tips_by_payment','payment_type']]
        #print(final.head())

        z = f.replace("-", "")
        final_loc = z[52:58]+"01_yellow_taxi_kpis.json"

        final.to_json("C:/Users/z031844/Downloads/final_sub/"+final_loc, 
                      orient='records')

        print('Output Saved Successfully as '+ final_loc)
        print("\n")
        
        
    
                             #  """ ************  #   Else Statement  #   ************  """  #
    
    
    else:
        
        cash = df[(df['payment_type']==2) & (df['tip_amount']!=0)]
        avg = df['total_amount'].sum()/df['total_amount'].count()
        #print('Calculating the average price spend by the customers: \n',avg,"\n")
        df['avg_cost_per_miles'] = round(avg/df['trip_distance'],2)
        #print(df.head())

        """ ************  """

        pay = pd.DataFrame(df['tip_amount'].groupby(df['payment_type']).count())
        """pay[5] = np.NaN
        pay[6] = np.NaN
        pay = pd.DataFrame(pay)
        pay['payment_mode'] = ['Not defined','Credit card','Cash','No charge', 'Dispute','Unknown','Voided trip']
        #print(pay)"""

        """ ************  """

        # Calculated as per BODMAS

        df['fare_&_extra_per_trip'] = (df['fare_amount'] + df['extra']) / df['trip_distance']
        #print(df.head())

        df['count_of_tips_by_payment'] = 0

        for i,j in zip(pay.index,pay.tip_amount):
            df['count_of_tips_by_payment'][df['payment_type']==i] = j

        #print(df[['count_of_tips_by_payment','payment_type','payment_mode']].head())
        mode = {'payment_type': [0,1,2,3,4,5,6],
         'payment_mode':['Not defined','Credit card','Cash','No charge', 'Dispute','Unknown','Voided trip']}
        mode = pd.DataFrame(mode)
        mode

        draft = df[['avg_cost_per_miles','fare_&_extra_per_trip','count_of_tips_by_payment','payment_type']]
        #print(final.head())

        final = pd.merge(draft,mode,on='payment_type')

        final.drop('payment_type',1,inplace=True)

        z = f.replace("-", "")
        final_loc = z[52:58]+"01_yellow_taxi_kpis.json"

        final.to_json('C:/Users/z031844/Downloads/final_sub/'+final_loc, 
                      orient='records')

        print('Output Saved Successfully as '+ final_loc)
        print("\n")
        