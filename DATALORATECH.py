import pandas as pd
from sqlalchemy import create_engine

#access to database 
DB_URL = "postgresql://loratech:loratech@localhost/dataloratech"
ENGINE = create_engine(DB_URL)
conn = ENGINE.connect()
try :
    print("connection is oke")
except:
    print("connection is not okey")


query_HK = """SELECT day, AVG(price) as AVARAGE_HK, MAX(price) as MAXIMUM_HK, MIN(price) as MINIMUM_HK FROM (SELECT * FROM imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='HK') AS price  GROUP by day ORDER BY day """
query_lowest_KOSPI2 = """SELECT day, MAX(nth_value) as LOWEST_FIVE_KOSPI2 from (SELECT day, price, NTH_VALUE(price, 5) OVER (PARTITION BY day ORDER BY day, price ASC) FROM (SELECT * 
    FROM imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='KOSPI2') AS price GROUP BY day, price ORDER BY day) AS nth_value GROUP BY day """
query_highest_KOSPI2 =  """SELECT day, MAX(nth_value) as HIGHEST_FIVE_KOSPI2 from (SELECT day, price, NTH_VALUE(price, 5) OVER (PARTITION BY day ORDER BY day, price DESC) FROM (SELECT * 
    FROM imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='KOSPI2') AS price GROUP BY day, price ORDER BY day) AS nth_value GROUP BY day """

#count avarage, maximum, minimum, HK index only
result_HK = pd.read_sql_query( query_HK,conn)
#count lowest five KOSPI2 only
result_lowest_five = pd.read_sql_query(query_lowest_KOSPI2,conn)
#count highest five KOSPI2 only
result_highest_five = pd.read_sql_query(query_highest_KOSPI2,conn)

data1 = pd.DataFrame(result_HK)
data2 = pd.DataFrame(result_lowest_five)
data3= pd.DataFrame(result_highest_five)
#concatination all data into result variable
result = pd.concat([data1, data2, data3], axis=1)
#save result to csv file
result.to_csv('DATALORATECH.csv', mode='a', header=True)

