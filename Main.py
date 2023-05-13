#libraries 

import pandas as pd
import numpy as np
import glob
import plotly.express as px
import warnings 
warnings.filterwarnings('ignore')

#reading file from directory

path = 'C:/Users/z031844/Downloads/raw_file/'
allfiles = glob.glob(path+"*.parquet")


for k in allfiles:
    f = k.replace("\\", "/")
    print('Reading file:', f[36:],'\n')
    #print('Year',f[52:56],'\n')
    df = pd.read_parquet(f)
    
    """ ************ Creating file as DataFrame ************ """  
    
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('amt','amount')
    
    if df.payment_type.dtypes == 'O' and len(df.payment_type.loc[1])>3:
    
        df['trip_pickup_datetime']= pd.to_datetime(df['trip_pickup_datetime'])
        df['trip_dropoff_datetime']= pd.to_datetime(df['trip_dropoff_datetime'])

        df['trip_pickup_datetime'] = df['trip_pickup_datetime'].dt.date
        df['trip_dropoff_datetime'] = df['trip_dropoff_datetime'].dt.date

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

        df['count_of_trips_by_payment'] = 0

        for i,j in zip(pay.index,pay.tip_amount):
            df['count_of_trips_by_payment'][df['payment_type']==i] = j

        #print(df[['count_of_trips_by_payment','payment_type','payment_mode']].head())
        """mode = {'payment_type': [0,1,2,3,4,5,6],
         'payment_mode':['Not defined','Credit card','Cash','No charge', 'Dispute','Unknown','Voided trip']}
        mode = pd.DataFrame(mode)
        mode"""

        #  NEW
        df = df.astype({"trip_pickup_datetime": 'string'})
        df = df.astype({'trip_dropoff_datetime':'string'})
        #df.info()


        dates = df.trip_pickup_datetime.unique()
        dates

        df.rename(columns={"payment_type": "payment_mode"}, inplace=True)
    
        for s in dates:

            dd = df[df['trip_pickup_datetime'] == s ]

            final = dd[['avg_cost_per_miles','count_of_trips_by_payment','payment_mode']]

            z = s.replace("-", "")

            final_loc = z+"_yellow_taxi_kpis.json"

            final.to_json('C:/Users/z031844/Downloads/final_sub/'+final_loc, 
                          orient='records')
            print('file saved scuccessfully', final_loc)
            
            
        print("\n")
        
        
                            #  """ ************  #   Elif #   ************  """  #
        
    
    
    elif df.payment_type.dtypes != 'O':
        
        df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].dt.date
        df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].dt.date
    
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

        df['count_of_trips_by_payment'] = 0

        for i,j in zip(pay.index,pay.tip_amount):
            df['count_of_trips_by_payment'][df['payment_type']==i] = j

        #print(df[['count_of_trips_by_payment','payment_type','payment_mode']].head())
        mode = {'payment_type': [0,1,2,3,4,5,6],
         'payment_mode':['Not defined','Credit card','Cash','No charge', 'Dispute','Unknown','Voided trip']}
        mode = pd.DataFrame(mode)
        mode

        #  NEW
        df = df.astype({"tpep_pickup_datetime": 'string'})
        df = df.astype({'tpep_dropoff_datetime':'string'})
        #df.info()


        dates = df.tpep_pickup_datetime.unique()
        dates


        for s in dates:

            dd = df[df['tpep_pickup_datetime'] == s ]

            draft = dd[['avg_cost_per_miles','fare_&_extra_per_trip','count_of_trips_by_payment','payment_type']]

            final = pd.merge(draft,mode,on='payment_type')

            final.drop('payment_type',1,inplace=True)

            z = s.replace("-", "")

            final_loc = z+"_yellow_taxi_kpis.json"

            final.to_json('C:/Users/z031844/Downloads/final_sub/'+final_loc, 
                          orient='records')
            print('file saved scuccessfully', final_loc)
    
    
        print("\n")
        
        
        
                               #  """ ************  #   Else #   ************  """  #
        
        
    else:
        
        
        df.rename(columns={"pickup_datetime": "trip_pickup_datetime",
                   "dropoff_datetime":"trip_dropoff_datetime"}, inplace=True)

        df['trip_pickup_datetime']= pd.to_datetime(df['trip_pickup_datetime'])
        df['trip_dropoff_datetime']= pd.to_datetime(df['trip_dropoff_datetime'])

        df['trip_pickup_datetime'] = df['trip_pickup_datetime'].dt.date
        df['trip_dropoff_datetime'] = df['trip_dropoff_datetime'].dt.date

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

        df['count_of_trips_by_payment'] = 0

        for i,j in zip(pay.index,pay.tip_amount):
            df['count_of_trips_by_payment'][df['payment_type']==i] = j

        #print(df[['count_of_trips_by_payment','payment_type','payment_mode']].head())
        """mode = {'payment_type': [0,1,2,3,4,5,6],
         'payment_mode':['Not defined','Credit card','Cash','No charge', 'Dispute','Unknown','Voided trip']}
        mode = pd.DataFrame(mode)
        mode"""

        #  NEW
        df = df.astype({"trip_pickup_datetime": 'string'})
        df = df.astype({'trip_dropoff_datetime':'string'})
        #df.info()


        dates = df.trip_pickup_datetime.unique()
        dates

        df.rename(columns={"payment_type": "payment_mode"}, inplace=True)

        for s in dates:

            dd = df[df['trip_pickup_datetime'] == s ]

            final = dd[['avg_cost_per_miles','count_of_trips_by_payment','payment_mode']]

            z = s.replace("-", "")

            final_loc = z+"_yellow_taxi_kpis.json"

            final.to_json('C:/Users/z031844/Downloads/final_sub/'+final_loc, 
                          orient='records')
            print('file saved scuccessfully', final_loc)


        print("\n")



path = 'C:/Users/z031844/Downloads/final_sub/'
allfiles = glob.glob(path+"202301*_yellow_taxi_kpis.json")

dataframes = []
name = allfiles[1][37:43]

for r in allfiles:
    f = r.replace("\\", "/")
    print('Reading file:', f[37:],'\n')
    #print('Year',f[52:56],'\n')
    df = pd.read_json(r)
    
    dataframes.append(df)
    
combined_df = pd.concat(dataframes, ignore_index=True)


combined_df.to_csv('C:/Users/z031844/Downloads/final_sub/'+name+'_yellow_taxi_kpis.csv',chunksize=1000)

print('A new CSV chunked is available and saved as '+name+'_yellow_taxi_kpis.csv')


