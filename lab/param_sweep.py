import numpy as np
import pickle
import pandas as pd
from time import time, sleep
import networkx as nx

from lab.master.Upscaler import Upscaler
from lab.util.metrics import summarize
# from lab.master.Master import Master
# from lab.master.__init__ import main


dir = 'data/WS'
master_func = Upscaler
# for dirpath, dirnames, filenames in os.walk(dir):
# for f in filenames:
results = []
for _ in range(1):
    n_workers = 4

    # generate graph
    path = 'tmp/generated_graph.txt'
    G = nx.watts_strogatz_graph(n=1000, k=2, p=0.1)
    nx.readwrite.edgelist.write_edgelist(G, path, data=False)
    result = summarize(path)
    result['name'] = 'Original'
    result['dt'] = 0
    result['scale'] = 1
    results.append(result)

    for scale in [2, 10]:
        # DegreeDistrubution
        output_file = 'tmp/scaled_graph.txt'
        t0 = time()
        Upscaler(
            worker_hostnames=np.arange(n_workers), graph_path=path,
            worker_script='lab/upscaling/worker/__init__.py', split_graph=True,
            output_file=output_file, scale=scale, method='')

        dt = time() - t0
        result = summarize(output_file)
        result['name'] = f'Degree Distribution ({scale})'
        result['dt'] = dt
        result['scale'] = scale
        results.append(result)

result = pd.DataFrame(results)
with open(f'param_sweep_result.pkl', 'wb') as f:
    pickle.dump(results, f)

print(result.transpose())
