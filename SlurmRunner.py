import os
import subprocess
import re
import argparse
import time

parser = argparse.ArgumentParser(description='Slurm Scheduler written in Python for PyMOO.')
parser.add_argument("-s", "--script_path", help="Specify script path to be used for sbatch.")
parser.add_argument("-r", "--refresh", help="Specify how much time we refresh the slurm job list.", default=60)
args = parser.parse_args()
print(args.script_path)

def run_job(script_path=args.script_path, refresh_job_list=int(args.refresh), show_output=True):

    with open(args.script_path, "rt") as slurm_file:
        lines = slurm_file.readlines()
        for line in lines:
            if "#SBATCH --job-name=" in line:
                job_name = line[ line.find('=') +1 : line.rfind('#') ].strip()
            elif "#SBATCH --output=" in line:
                output_file = line[ line.find('=') +1 : line.rfind('#') ].strip()
            elif "#SBATCH --error=" in line:
                error_file = line[ line.find('=') +1 : line.rfind('#') ].strip()

    print(f"Output File: {output_file}\n")
    time.sleep(2)
    result = subprocess.run(["sbatch",args.script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
    job_id = re.findall(r'\d+', result.stdout)[0]

    if show_output:
        print(f"===STATUS of JOB {job_id}===\n")
        while True:
            try:
                check_output = subprocess.check_output(f'squeue | grep {job_id}', shell=True, universal_newlines=True)
                if check_output is not None:
                    print(f"Job {job_id} is still running:")
                    print(check_output)
                    time.sleep(refresh_job_list)
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

if __name__ == "__main__":
    run_job()