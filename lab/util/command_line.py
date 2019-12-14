import subprocess
from multiprocessing import Process

# You should create this file yourself in order to run the program using ssh
from lab.util.ssh_connection_info import username, password

from spur import SshShell
from spur.ssh import MissingHostKey


def run_python_script(script, *arguments):
    return subprocess.Popen(['python', script, *arguments])


def run_ssh_script(hostname, *arguments):
    shell = SshShell(hostname=hostname, username=username, password=password, missing_host_key=MissingHostKey.accept)
    shell.spawn(['./start_scaler.sh', *arguments])

    return shell


def setup_worker(hostname_worker, script, worker_id, hostname_master, port_master, scale, method, load_backup, number_of_random_walkers):
    return run_python_script(
        script,
        '--worker-id', str(worker_id),
        '--master-host', hostname_master,
        '--master-port', str(port_master),
        '--scale', str(scale),
        '--method', method,
        '--load-backup', str(load_backup),
        '--n-random-walkers', str(number_of_random_walkers)
    )

    # return run_ssh_script(
    #     hostname_worker,
    #     script,
    #     '--worker-id', str(worker_id),
    #     '--master-host', hostname_master,
    #     '--master-port', str(port_master),
    #     '--scale', str(scale),
    #     '--method', method,
    #     '--load-backup', str(load_backup),
    #     '--n-random-walkers', str(number_of_random_walkers)
    # )

