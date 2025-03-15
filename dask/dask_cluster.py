from dask.distributed import Client
import dask


@dask.delayed
def lazy_evaluation(x, y):
    return x ** y


client = Client("localhost:8786")

with client:
 a = lazy_evaluation(2, 2)
 b = lazy_evaluation(2, 2)
 d = lazy_evaluation(7, 20)
 c = lazy_evaluation(a, b)

 c.visualize(filename='graph.png', optimize_graph=True)
 c = c.compute()
 d = d.compute()

 print(f"Final c is : {c}")


client.close()
