# Distributed systems
This repository contains the files of the course Distributed Systems taught at VU Amsterdam 2019-2020.
(This is a fork form [_original repo_](https://github.com/r-hoffmann/distributed-systems))

# System setup

- Install python3.6 or higher
- Clone repo
- `cd distributed-systems`
- `virtualenv` venv
- `pip install -r requirement.txt`
- Copy `lab.util.ssh_connection_info_example.py` to `lab.util.ssh_connection_info.py` and edit if necessary.


# Directories

- `lab` contains the code
- `lab/downscaling` contains the code for the downscaling workers
- `lab/master` contains the code for the master
- `lab/upscaling` contains the code for the upscaling workers
- `lab/util` contains utility functions for the nodes

# Run system

Run master by
`python lab/master/__init__.py`

It can handle the following arguments:
- --worker-hostnames: The hostnames of the workers separated by commas (without spaces)
- --graph: The path to the graph to scale
- --worker-script: Python script for worker
- --split_graph: Flag to indicate to whether to split up the graph in smaller subgraphs or to copy the whole graph to every worker
- --output-file: File to save the scaled graph in
- --scale: The scale to which the graph should be up/down-sized, should be above zero
- --method: The method used for downscaling, `random_walk` or `random_edge`
- --random-walkers-per-worker: The number of random walker to start per worker
- --backup-size: Minimum size of the backup before it will be send to the master during a run
- --walking-iterations: The number of steps a random walker sets before the queue will be handled
- --debug: Show debug messages

## Downscaling
Download a graph, e.g. to `data/graph.txt`. For `method`, use `random_walk` or `random_edge`.

```
python lab/master/__init__.py --graph data/graph.txt --master Master --worker-script lab/downscaling/worker/__init__.py --scale 0.1 --method random_walk
```

## Upscaling
Download a graph, e.g. to `data/graph.txt.

```
python lab/master/__init__.py --graph data/graph.txt --master Upscaling --worker-script lab/upscaling/worker/__init__.py --scale 10 --method DegreeDistribution
```
