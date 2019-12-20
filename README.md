# Distributed systems
This repository contains the files of the course Distributed Systems taught at VU Amsterdam 2019-2020.


## Upscaling
Download a graph, e.g. to `data/graph.txt`. For `method`, use `random_walk` or `random_edge`.

```
python lab/master/__init__.py --graph data/graph.txt --master Master --worker-script lab/downscaling/worker/__init__.py --scale 0.1 --method random_walk

```

## Downscaling
Download a graph, e.g. to `data/graph.txt.

```
python lab/master/__init__.py --graph data/graph.txt --master Upscaling --worker-script lab/upscaling/worker/__init__.py --scale 10 --method DegreeDistribution
```
