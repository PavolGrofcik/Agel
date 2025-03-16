from dask.distributed import Client
import pandas as pd
import dask
import dask.dataframe as dd


def get_grade(marks: int) -> str:
  if marks >= 92:
   return "A"
  elif marks >= 83:
    return "B"
  elif marks >= 74:
   return "C"
  elif marks >= 65:
   return "D"
  elif marks >= 56:
   return "E"
  else:
   return "FX"

@dask.delayed
def lazy_evaluation(x, y):
    return x ** y

def dask_df_change_type(dataframe, col, type):
    df = dataframe[col].astype(type)
    return df


#Create Pandas Dataframe
columns = ['Student ID', 'Course ID', 'Marks']
data = [(103, 201, 67), (103, 203, 67), (103, 204, 89)]
df = pd.DataFrame(data, columns=columns)
df['Grade'] = df.apply(lambda x: get_grade(x['Marks']), axis=1)

print(df.head())


client = Client("localhost:8786")

with (client):
 a = lazy_evaluation(2, 2)
 b = lazy_evaluation(2, 2)
 d = lazy_evaluation(7, 20)
 c = lazy_evaluation(a, b)

 c.visualize(filename='graph.png', optimize_graph=True)

 c = c.compute()
 d = d.compute()

 ddf = dd.from_pandas(df, npartitions=2)
 # ddf = dask.delayed(dask_df_change_type)(ddf, 'Grade', 'str')
 # ddf = dask.delayed(dask_df_change_type)(ddf, 'Marks', 'int64')

 ddf['Grade'] = ddf['Grade'].astype(str)
 ddf['Marks'] = ddf['Marks'].astype('int64')
 ddf.visualize()

 ddf['Percentage'] = ddf['Marks'] / 100
 ddf = ddf.compute()

 ddf.to_csv("dask.csv", index=False)



 print(f"Final c is : {c}")


client.close()
