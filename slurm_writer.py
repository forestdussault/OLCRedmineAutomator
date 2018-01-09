import os


def create_template(issue, cpu_count, memory, work_dir):
    template = """
    #!/bin/bash
    
    #SBATCH -N 1
    #SBATCH --ntasks={cpu_count}
    #SBATCH --mem={memory}
    #SBATCH --time=1-00:00
    #SBATCH --job-name={jobid}
    #SBATCH -o {work_dir}/job_%j.out
    #SBATCH -e {work_dir}/job_%j.err
    
    """.format(cpu_count=cpu_count,
               memory=memory,
               jobid=issue.id,
               work_dir=work_dir)
    return template

