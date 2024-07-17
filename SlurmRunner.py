import os
import sys
import subprocess
import re
import argparse
import time

parser = argparse.ArgumentParser(description='SLURM Runner written in Python for PyMOO.')
parser.add_argument("-s", "--script_path", help="Specify script path to be used for sbatch.")
parser.add_argument("-t", "--refresh", help="Specify how much time we refresh the SLURM job list.", default=30)
parser.add_argument("-i", "--status", help="Show output of job status.", default=True)
parser.add_argument("-o", "--output_dir", help="Output directory.", default="./")
parser.add_argument("-r", "--run", help="Run SLURM job.", default=True)
parser.add_argument("-j", "--jobs", help="Run multiple SLURM jobs.", default=1)
parser.add_argument("-c", "--config", help="Specify configuration file.")
parser.add_argument("-w", "--write", help="Write a SLURM job from a template.", default=False)
parser.add_argument("-wr", "--write_run", help="Write and run a SLURM job.", default=True)

args = parser.parse_args()
print(args.script_path)

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
except TypeError:
    print(f"File with path {os.path.abspath(args.script_path)} not found!")
    exit()

print("Output directory set to:", os.path.abspath(args.output_dir))
os.makedirs(os.path.abspath(args.output_dir),exist_ok=True)
print(f"Output file: {os.path.abspath(args.output_dir)}/{output_file}\n")

def run_multiple_jobs(num_of_jobs=int(args.jobs), show_status=args.status):

    time.sleep(2)
    job_id_list = []
    cwd = os.getcwd()
    for job in range(num_of_jobs):
        path = os.path.abspath(args.output_dir) + "/ITER_" + str(job)
        print("Creating path:", path)
        os.makedirs(path, exist_ok=True)
        os.chdir(path)
        result = subprocess.run(["sbatch", str(cwd) + '/' + args.script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
        try:
            job_id = re.findall(r'\d+', result.stdout)[0]
            job_id_list.append(job_id)
        except IndexError:
            print("Cannot find job id!")
        os.chdir(cwd)
            
    if show_status:
        print(f"===STATUS OF JOBS===\n")
        while True:
            try:
                check_output = subprocess.check_output(f"squeue --job {','.join(map(str, job_id_list)) } | grep $USER", shell=True, universal_newlines=True)
                if check_output is not None:
                    print(f"Jobs are running (refresh: {args.refresh} sec intervals):")
                    print(check_output)
                    time.sleep(int(args.refresh))
                else:
                    break
            except subprocess.CalledProcessError:
                print(f"Jobs Complete!\n")
                break

        while os.path.exists(output_file) is None:
            print("Waiting on SLURM to generate output...")
            time.sleep(15)
        
        print(f"===OUTPUT OF JOBS===\n")
        print("Output is available under these directories:")
        for job in range(num_of_jobs):
            print(os.path.abspath(args.output_dir) + "/ITER_" + str(job))

        print(f"===End OF OUTPUT FOR JOBS===\n")

def run_job(num_of_jobs=args.jobs, show_status=args.status):

    result = subprocess.run(["sbatch", args.script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
    job_id = re.findall(r'\d+', result.stdout)[0]

    if show_status:
        print(f"===STATUS of JOB {job_id}===\n")
        while True:
            try:
                check_output = subprocess.check_output(f'squeue | grep {job_id}', shell=True, universal_newlines=True)
                if check_output is not None:
                    print(f"Job {job_id} is running (refresh: {int(args.refresh)} sec intervals):")
                    print(check_output)
                    time.sleep(int(args.refresh))
                else:
                    break
            except subprocess.CalledProcessError:
                print(f"Job Complete!\n")
                break

        while os.path.exists(output_file) is None:
            print("Waiting on SLURM to generate output...")
            time.sleep(15)
        
        print(f"===OUTPUT OF JOB {job_id}===\n")

        if os.path.exists(error_file) and os.stat(error_file).st_size != 0:
            print("===ERROR===")
            print(f"Encountered error when running job {job_id}!")
            
            time.sleep(5)

            with open(error_file, "rt") as text_file:
                print("Error received from:", error_file)
                lines = text_file.readlines()
                for line in lines:
                    print(line)

        with open(output_file, "rt") as text_file:
            lines = text_file.readlines()
            for line in lines:
                print(line)

        print(f"===End OF OUTPUT FOR JOB {job_id}===\n")

def write_job(config=args.config):
    print("write_job")

if __name__ == "__main__":
    if int(args.jobs) > 1:
        run_multiple_jobs()
    elif args.run:
        run_job()
    elif args.write_run:
        write_job()
        run_job()