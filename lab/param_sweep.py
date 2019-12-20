import numpy as np
import pickle
import pandas as pd
from time import time, sleep
import networkx as nx

from lab.master.Upscaler import Upscaler
from lab.util.metrics import summarize
from lab.master.Master import Master

dir = 'data/WS'
extra_metrics = True
results = []

# generate graph
print('Generate graph')
path = 'tmp/generated_graph.txt'
G = nx.watts_strogatz_graph(n=10000, k=2, p=0.3)
nx.readwrite.edgelist.write_edgelist(G, path, data=False)
print('Summarize')
result = summarize(path, extra_metrics=extra_metrics)
result['name'] = 'Original'
result['dt'] = 0
result['scale'] = 1
results.append(result)

for n_workers in [4, 8]:
    for scale in [2]:
        print(f'\n\nn_workers: {n_workers}, scale: {scale}\n')
        output_file = 'tmp/scaled_graph.txt'
        t0 = time()
        if scale < 1:
            # downscaling
            name = f'Random edge ({scale}x)'
            Master(
                worker_hostnames=np.arange(n_workers), graph_path=path,
                worker_script='lab/downscaling/worker/__init__.py', split_graph=True,
                output_file=output_file, scale=scale, method='random_edge')
        else:
            # upscaling
            name = f'Degree Distribution ({scale}x, n: {n_workers})'
            print('scale', scale)
            Upscaler(
                worker_hostnames=np.arange(n_workers), graph_path=path,
                worker_script='lab/upscaling/worker/__init__.py', split_graph=True,
                output_file=output_file, scale=scale, method='DegreeDistribution')

        dt = time() - t0
        print('Summarize result')
        result = summarize(output_file, extra_metrics=extra_metrics)
        result['name'] = name
        result['dt'] = np.round(dt, 3)
        result['scale'] = scale
        results.append(result)
        sleep(0.1)

result = pd.DataFrame(results)
with open(f'param_sweep_result.pkl', 'wb') as f:
    pickle.dump(result, f)

print(result)
print('\n\n')
print(result.transpose())
