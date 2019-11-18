import subprocess


def run_python_script(script, *arguments):
    return subprocess.Popen(['python', script, *arguments])


def setup_worker(script, worker_id, host, port, graph_path):
    return run_python_script(script, '--worker-id', str(worker_id), '--master-host', host, '--master-port', str(port), '--graph', graph_path)