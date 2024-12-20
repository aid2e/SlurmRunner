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
parser.add_argument("-s", "--script_path", help="specify script path to be used for sbatch.")
parser.add_argument("-t", "--refresh", help="specify how much time we refresh the SLURM job list.", default=1)
parser.add_argument("-o", "--output_dir", help="output directory to send SLURM job output.", default="./")
parser.add_argument("-j", "--jobs", help="run multiple SLURM jobs.", default=1)
parser.add_argument("-wt", "--wall_time", help="overwrites time limit setting; jobs still running will be killed.", default="")

args = parser.parse_args()

def read_slurm_script():
    try:
        with open(os.path.abspath(args.script_path), "rt") as slurm_file:
            lines = slurm_file.readlines()
            for line in lines:
                if "#SBATCH --job-name=" in line:
                    job_name = line.split('#')[1][line.rfind('='): len(line)].strip()
                elif "#SBATCH --output=" in line:
                    output_file = line.split('#')[1][line.rfind('='): len(line)].strip()
                elif "#SBATCH --error=" in line:
                    error_file = line.split('#')[1][line.rfind('='): len(line)].strip()
                elif "#SBATCH --time=" in line:
                    time_limit = line.split('#')[1][line.rfind('='): len(line)].strip()
        print(f"--> Job Name: {output_file}")
        print(f"--> Output File: {output_file}")
        print(f"--> Error File: {error_file}")
        print(f"--> Time: {time_limit}")
        return job_name, output_file, error_file, time_limit
    except TypeError:
        print(f"File with path {os.path.abspath(args.script_path)} not found!")
        exit()
    except IndexError:
        print(f"Error extracting Slurm information!")
        exit()

def print_paths_and_make_output_dir():
    print(f"--> Using SLURM script: {os.path.abspath(args.script_path)}")
    print(f"--> Output directory set to: {os.path.abspath(args.output_dir)}")
    os.makedirs(os.path.abspath(args.output_dir),exist_ok=True)

def create_folder_and_change_dir_for_job(job):
    path = os.path.abspath(args.output_dir) + "/ITER_" + str(job)
    os.makedirs(path, exist_ok=True)
    os.chdir(path)

def convert_time_limit_to_sec(time_limit):
    if args.wall_time:
        hh, mm, ss = args.wall_time.split(':')
    else:
        hh, mm, ss = time_limit.split(':')
    time_limit_int = int(hh) * 3600 + int(mm) * 60 + int(ss)
    return time_limit_int

def create_jobs(num_of_jobs=int(args.jobs)):
    cwd = os.getcwd()
    job_id_list = []
    for job in range(num_of_jobs):
        create_folder_and_change_dir_for_job(job)
        cmd = f"sbatch {args.script_path}"
        result = subprocess.run(["sbatch", f"{args.script_path}"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        try:
            job_id = re.findall(r'\d+', result.stdout)[0]
            job_id_list.append(job_id)
        except IndexError:
            print("Cannot find job ID!")
        os.chdir(cwd) # Restore working directory where we ran the script
    return job_id_list

def poll_jobs(time_limit, job_id_list):
    time_limit_int = convert_time_limit_to_sec(time_limit)
    try:
        with subprocess.run(
                ["watch", "-c", "-d", "-e", "-n", f"{str(args.refresh)}", "squeue", "--job", f"{','.join(map(str, job_id_list))}", "| grep $USER"],
                stdin=subprocess.PIPE, 
                stdout=sys.stdout, 
                stderr=sys.stderr, 
                timeout=time_limit_int
            ) as proc:
            try:
                sys.stdout.write(proc.stdout)
            except TypeError:
                time.sleep(1)
    except AttributeError:  # Even though this is an error, this will tell us when there are no more jobs for $USER
        print(f"Jobs Completed\n")
    except subprocess.TimeoutExpired: # Jobs are still running after the wall-time limit
        sys.stdout.flush()
        os.system('reset')  # We have to reset otherwise frame buffer is contaminated
        if args.wall_time: print(f"Current running time has exceeded wall-time limit ({args.wall_time}) for jobs!")
        print(f"Cancelling SLURM jobs!")
        prompt_resubmit_jobs(kill_jobs(job_id_list))
        time.sleep(3)

def kill_jobs(job_id_list):
    try:
        check_output = subprocess.check_output(f"squeue --job {','.join(map(str, job_id_list)) } | grep $USER", shell=True, universal_newlines=True)
        check_output_list = list(filter(None, check_output.split('\n')))
        failed_job_id_list = []
        for line in check_output_list:
            try:
                failed_job_id = re.search(r'\d+', line).group()
                failed_job_id_list.append(failed_job_id)
                print(f"CANCELLED JOB WITH ID {failed_job_id}")
            except AttributeError:
                print(f"Cannot find job ID!")
        subprocess.run(
            ["scancel", f"{','.join(map(str, failed_job_id_list))}"], 
            stdin=subprocess.PIPE, 
            stdout=sys.stdout, 
            stderr=sys.stderr, 
            universal_newlines=True
                       )
        return failed_job_id_list
    except subprocess.CalledProcessError:
        time.sleep(5)

def prompt_resubmit_jobs(failed_job_id_list):
    time_limit = read_slurm_script()
    response = input("Do you want to resubmit the failed jobs? (Y/n): ")
    if response.lower() in ['y', '']:
        print("Resubmitting Jobs...")
        job_id_list = create_jobs(len(failed_job_id_list))
        poll_jobs(time_limit, job_id_list)
    elif response.lower() == 'n':
        return
    else:
        print("Invalid choice. Please enter Y or N.")

def print_job_output(num_of_jobs=int(args.jobs)):
    print(f"===OUTPUT OF JOBS===\n")
    print("Output (with logs) is available under these directories:")
    for job in range(num_of_jobs):
        print(os.path.abspath(args.output_dir) + "/ITER_" + str(job))
    print(f"\n===END OF OUTPUT FOR JOBS===\n")

def run_jobs():
    try:
        job_id_list = []
        job_name, output_file, error_file, time_limit = read_slurm_script()
        print_paths_and_make_output_dir()
        time.sleep(5)
        job_id_list = create_jobs()
        poll_jobs(time_limit, job_id_list)
        print_job_output()

    except KeyboardInterrupt:
        if job_id_list is not None:
            kill_jobs(job_id_list)
        print(f"\nProgram exited\n")

if __name__ == "__main__":
    run_jobs()
