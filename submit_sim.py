#!/usr/bin/env python

import os, argparse, json, subprocess

def run_sim(configFile):
    """
    summary: This function is used to submit simulations for the FarForward detector
    """
    with open(configFile, "r") as f:
        pbsconfig = json.load(f)
    simulation = pbsconfig["simulation"]
    sim_name = simulation["name"]
    n_events = int(simulation["nEvents"])
    n_jobs = int(simulation["nJobs"])
    script_dir = pbsconfig["input"]["CodeDir"]
    run_sim_script = os.path.join(script_dir, pbsconfig["input"]["run_sim_template"])
    submit_script = os.path.join(script_dir, pbsconfig["input"]["submit_template"])
    container = pbsconfig["container"]["path"]
    
    outDir = pbsconfig["output"]["outDir"]
    if not os.path.exists(outDir):
        os.makedirs(outDir)
    workDir = pbsconfig["output"].get("workDir", outDir)
    if not os.path.exists(workDir):
        os.makedirs(workDir)
    logDir = pbsconfig["output"].get("logDir", None)
    if logDir and not os.path.exists(logDir):
        os.makedirs(logDir)
    if not logDir:
        logDir = None
    epic_install = pbsconfig["epic"]["epicDir"]
    eicrecon_install = pbsconfig["eicrecon"]["eicreconDir"]
    
    if os.path.exists(f"history_log_{sim_name}.txt"):
       os.system(f"rm history_log_{sim_name}.txt") 
    
    for it in range(n_jobs):
        jobName = f"{sim_name}_{it}"
        job_dir = os.path.join(workDir, f"ITER_{it}")
        if not os.path.exists(job_dir):
            os.makedirs(job_dir)
        # Modify SUBMIT.sh script
        with open(submit_script, "r") as f:
            submit_contents = f.read()
        submit_contents = submit_contents.replace("JOB_NAME", jobName)
        submit_contents = submit_contents.replace("WORK_DIR", job_dir)
        logDir = os.path.join(job_dir, "logs")
        if (not os.path.exists(logDir)):
            os.makedirs(logDir)
        submit_contents = submit_contents.replace("LOG_DIR", logDir)
        submit_contents = submit_contents.replace("OUTPUT_DIR", job_dir)
        submit_contents = submit_contents.replace("EIC_SHELL", container)
        submit_contents = submit_contents.replace("SCRIPTFILE", os.path.join(job_dir, pbsconfig["input"]["run_sim_template"]))
        
        with open(f"{job_dir}/"+pbsconfig["input"]["submit_template"], "w") as f:
            f.write(submit_contents)
            
        # Modify run_sim.sh script
        with open(run_sim_script, "r") as f:
            run_sim_contents = f.read()
        run_sim_contents = run_sim_contents.replace("epic_install", epic_install)
        run_sim_contents = run_sim_contents.replace("eicrecon_install", eicrecon_install)
        run_sim_contents = run_sim_contents.replace("code_dir", script_dir)
        run_sim_contents = run_sim_contents.replace("n_events", str(n_events))
        run_sim_contents = run_sim_contents.replace("out_dir", job_dir)
        run_sim_contents = run_sim_contents.replace("detector_name", sim_name)
        
        with open(f"{job_dir}/" + pbsconfig["input"]["run_sim_template"], "w") as f:
            f.write(run_sim_contents)
        command = ["sbatch", os.path.join(job_dir, pbsconfig["input"]["submit_template"])]
        with open(f"history_log_{sim_name}.txt", "a+") as f:
            f.write(": \t".join(command))
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout = result.stdout.decode('utf-8')
            stderr = result.stderr.decode('utf-8')
            f.write("\n \n" + stdout + ": \t ")
            f.write(stderr + "\n")
        
        
if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description="Submit simulations for the FarForward detector")
    argparser.add_argument("-c", "--configFile", 
                           help = "Path to the configuration file for the simulation", 
                           type = str, required = True
                           )
    args = argparser.parse_args()
    run_sim(args.configFile)