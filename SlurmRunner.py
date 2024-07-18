import os
import sys
import subprocess
import signal
import re
import argparse
import time
import datetime
import io

parser = argparse.ArgumentParser(description='SLURM Runner written in Python for PyMOO.')
parser.add_argument("-s", "--script_path", help="Specify script path to be used for sbatch.")
parser.add_argument("-t", "--refresh", help="Specify how much time we refresh the SLURM job list.", default=2)
parser.add_argument("-i", "--status", help="Show output of job status.", default=True)
parser.add_argument("-o", "--output_dir", help="Output directory.", default="./")
parser.add_argument("-r", "--run", help="Run SLURM job.", default=True)
parser.add_argument("-j", "--jobs", help="Run multiple SLURM jobs.", default=1)
parser.add_argument("-wt", "--wall_time", help="Force running jobs to stop at a specified time formatted in HH:MM:SS.", default="")

args = parser.parse_args()
script_path = os.path.abspath(args.script_path)

try:
    with open(os.path.abspath(args.script_path), "rt") as slurm_file:
        lines = slurm_file.readlines()
        for line in lines:
            if "#SBATCH --job-name=" in line:
                job_name = line[ line.rfind('/') +1 : line.rfind('#') ].strip()
            elif "#SBATCH --output=" in line:
                output_file = line[ line.find('=') +1 : line.rfind('#') ].strip()
            elif "#SBATCH --error=" in line:
                error_file = line[ line.find('=') +1 : line.rfind('#') ].strip()
            elif "#SBATCH --time=" in line:
                time_limit = line[ line.find('=') +1 : line.rfind('#') ].strip()
except TypeError:
    print(f"File with path {os.path.abspath(args.script_path)} not found!")
    exit()

print(f"--> Using SLURM script: {os.path.abspath(args.script_path)}")
print(f"--> Output directory set to: {os.path.abspath(args.output_dir)}")
os.makedirs(os.path.abspath(args.output_dir),exist_ok=True)
print(f"--> Output file: {os.path.abspath(args.output_dir)}/{output_file}\n")
time.sleep(3)

def run_jobs(num_of_jobs=int(args.jobs), show_status=args.status):
    job_id_list = []
    cwd = os.getcwd()
    for job in range(num_of_jobs):
        path = os.path.abspath(args.output_dir) + "/ITER_" + str(job)
        os.makedirs(path, exist_ok=True)
        os.chdir(path)
        print(f"sbatch {os.path.abspath(script_path)}")
        result = subprocess.run(["sbatch", script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
        try:
            job_id = re.findall(r'\d+', result.stdout)[0]
            job_id_list.append(job_id)
        except IndexError:
            print("Cannot find job ID!")
        os.chdir(cwd)
            
    start_time = time.time()
    if args.wall_time:
        time_limit_val = args.wall_time
    else:
        time_limit_val = time_limit
    hh, mm, ss = time_limit_val.split(':')
    time_limit_int = int(hh) * 3600 + int(mm) * 60 + int(ss)

    try:
        with subprocess.run(["watch","-c","-d","-e","-n", str(args.refresh), "squeue", "--job", f"{','.join(map(str, job_id_list))}", " | grep $USER"], stdin=subprocess.PIPE, stdout=sys.stdout, stderr=sys.stderr, timeout=time_limit_int) as proc:
            try:
                sys.stdout.write(proc.stdout)
            except TypeError:
                time.sleep(1)
    except AttributeError:
        print(f"Jobs Completed\n")
    except subprocess.TimeoutExpired:
        sys.stdout.flush()
        time.sleep(5)
        #os.system('clear')
        os.system('reset')
        print(f"Current running time has exceeded wall-time limit for jobs!")
        print(f"Cancelling SLURM jobs!")
        time.sleep(1)
        try:
            check_output = subprocess.check_output(f"squeue --job {','.join(map(str, job_id_list)) } | grep $USER", shell=True, universal_newlines=True)
            time.sleep(2)
            check_output_list = list(filter(None, check_output.split('\n')))
            failed_job_id_list = []
            for line in check_output_list:
                try:
                    failed_job_id = re.search(r'\d+', line).group()
                    failed_job_id_list.append(failed_job_id)
                    print(f"CANCELLED JOB WITH ID {failed_job_id}")
                except AttributeError:
                    print(f"Cannot find job ID!")
            subprocess.check_output(f"scancel --job {','.join(map(str, failed_job_id_list)) } | grep $USER", shell=True, universal_newlines=True)
            #os.system('clear')
        except subprocess.CalledProcessError:
            print(f"Jobs finished or killed\n")
            time.sleep(10)

    print(f"===OUTPUT OF JOBS===\n")
    print("Output is available under these directories:")
    for job in range(num_of_jobs):
        print(os.path.abspath(args.output_dir) + "/ITER_" + str(job))

    print(f"\n===END OF OUTPUT FOR JOBS===\n")

if __name__ == "__main__":
        run_jobs()