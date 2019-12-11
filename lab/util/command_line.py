import subprocess


def run_python_script(script, *arguments):
    return subprocess.Popen(['python', script, *arguments])


def setup_worker(script, worker_id, host, port, scale, method, output_file, number_of_random_walkers):
    return run_python_script(script,
                             '--worker-id', str(worker_id),
                             '--master-host', host,
                             '--master-port', str(port),
                             '--scale', str(scale),
                             '--method', method,
                             '--output', output_file,
                             '--n-random-walkers', str(number_of_random_walkers)
                             )
