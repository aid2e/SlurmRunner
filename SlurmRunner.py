import os
import subprocess
import argparse
import time

parser = argparse.ArgumentParser(description='Slurm Scheduler written in Python for PyMOO.')
parser.add_argument("-s", "--script_path", help="Specify script path to be used for sbatch.")
args = parser.parse_args()
print(args.script_path)

with open(args.script_path, "rt") as slurm_file:
    lines = slurm_file.readlines()
    for line in lines:
        if "#SBATCH --output=" in line:
            output_file = line[line.find('=') +1 : line.rfind('#')].strip()

print("Output File:", output_file)
#print("Running salloc and sbatch")
#os.subprocess("salloc --ntasks=2 --mem=4G --time=01:00:00")
#os.subprocess("sbatch" + str(script_file))
#subprocess.run("salloc",capture_output=True, text=True)
time.sleep(10)
result = subprocess.run(["sbatch",args.script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout = result.stdout.decode('utf-8')
stderr = result.stderr.decode('utf-8')

while os.path.exists(output_file) is None:
    print("Waiting on SLURM...")
    time.sleep(30)

print(output_file)
with open(output_file, "rt") as text_file:
    lines = text_file.readlines()
    for line in lines:
        print(line)