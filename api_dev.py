import os
import sys
import time
import pickle
import logging
from redminelib import Redmine
from settings_dev import AUTOMATOR_KEYWORDS, API_KEY, BIO_REQUESTS_DIR


def redmine_setup(api_key):
    """
    :param api_key: API key available from your Redmine user account settings. Stored in settings_dev.py.
    :return: instantiated Redmine API object
    """
    redmine_url = 'http://192.168.1.2:8080'
    redmine = Redmine(redmine_url,
                      key=api_key,
                      requests={
                          'verify': False,
                          'timeout': 10,
                      })
    return redmine


def retrieve_issues(redmine_instance):
    """
    :param redmine_instance: instantiated Redmine API object
    :return: returns an object containing all issues for the test Redmine environment
    """
    issues = redmine_instance.issue.filter(project_id='test')
    return issues


def new_automation_jobs(issues):
    """
    :param issues: issues object pulled from Redmine API
    :return: returns a new subset of issues that are Status: NEW and match a term in AUTOMATOR_KEYWORDS)
    """
    new_jobs = {}
    for issue in issues:
        # Only new issues
        if issue.status.name == 'New':
            # Strip whitespace and make lowercase ('subject' is the job type i.e. Diversitree)
            subject = issue.subject.lower().replace(' ', '')
            # Check for presence of an automator keyword in subject line
            if subject in AUTOMATOR_KEYWORDS:
                new_jobs[issue] = subject
                logging.debug('{id}:{subject}:{status}'.format(id=issue.id,
                                                               subject=issue.subject,
                                                               status=issue.status))
    return new_jobs


def bio_requests_setup(issue):
    """
    :param issue: issue object pulled from the Redmine API
    :return: path to newly created work directory
    """
    work_dir = os.path.join(BIO_REQUESTS_DIR, str(issue.id))
    try:
        os.makedirs(work_dir)
        logging.info('Created directory: {}'.format(work_dir))
    except OSError:
        logging.error('{} already exists'.format(work_dir))
    return work_dir


def issue_text_dump(issue):
    """
    Dumps Redmine issue details into a text file
    :param issue: object pulled from Redmine instance
    :return: path to text file
    """
    file_path = os.path.join(BIO_REQUESTS_DIR,
                             str(issue.id),
                             str(issue.id) + '_' + str(issue.subject) + '_redmine_details.txt')
    with open(file_path, 'w+') as file:
        for attr in dir(issue):
            file.write('{}: {}\n\n'.format(attr, getattr(issue, attr)))
    return file_path


def retrieve_issue_description(issue):
    """
    :param issue: object pulled from Redmine instance
    :return: parsed issue description as a list
    """
    description = issue.description.split('\n')
    for line in range(len(description)):
        description[line] = description[line].rstrip()
    return description


def pickle_redmine(redmine_instance, issue, work_dir, description):
    """
    Function to pickle our redmine instance and issue
    :param redmine_instance: instantiated Redmine API object
    :param issue: object pulled from Redmine instance
    :param work_dir: string path to working directory for Redmine job
    :param description: parsed redmine description list object
    :return: dictionary with paths to redmine instance, issue and description pickles
    """
    # Establish file paths
    pickled_redmine = os.path.join(work_dir, 'redmine.pickle')
    pickled_issue = os.path.join(work_dir, 'issue.pickle')
    pickled_description = os.path.join(work_dir, 'description.pickle')

    # Create dictionary
    pickles = {'redmine_instance': pickled_redmine,
               'issue': pickled_issue,
               'description': pickled_description}

    # Write pickle files
    with open(pickled_redmine, 'wb') as file:
        pickle.dump(redmine_instance, file)
    with open(pickled_issue, 'wb') as file:
        pickle.dump(issue, file)
    with open(pickled_description, 'wb') as file:
        pickle.dump(description, file)

    return pickles


def make_executable(path):
    """
    Takes a shell script and makes it executable (chmod +x)
    :param path: path to shell script
    """
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2
    os.chmod(path, mode)


def create_template(issue, cpu_count, memory, work_dir, cmd):
    """
    Creates a SLURM job shell script
    :param issue: object pulled from Redmine instance
    :param cpu_count: number of CPUs to allocate for slurm job
    :param memory: memory in MB to allocate for slurm job
    :param work_dir: string path to working directory for Redmine job
    :param cmd: string containing bash command
    :return: string file path to generated shell script
    """
    # Prepare SLURM shell script contents
    template = "#!/bin/bash\n" \
               "#SBATCH -N 1\n" \
               "#SBATCH --ntasks={cpu_count}\n" \
               "#SBATCH --mem={memory}\n" \
               "#SBATCH --time=1-00:00\n" \
               "#SBATCH --job-name={jobid}\n" \
               "#SBATCH -o {work_dir}/job_%j.out\n" \
               "#SBATCH -e {work_dir}/job_%j.err\n" \
               "source /mnt/nas/Redmine/.virtualenvs/OLCRedmineAutomator/bin/activate\n" \
               "{cmd}".format(cpu_count=cpu_count,
                              memory=memory,
                              jobid=issue.id,
                              work_dir=work_dir,
                              cmd=cmd)

    # Path to SLURM shell script
    file_path = os.path.join(BIO_REQUESTS_DIR, str(issue.id), str(issue.id) + '_slurm.sh')

    # Write SLURM job to shell script
    with open(file_path, 'w+') as file:
        file.write(template)

    make_executable(file_path)

    return file_path


def submit_slurm_job(redmine_instance, issue, work_dir, cmd, job_type, cpu_count=8, memory=12000):
    """
    Wrapper for several tasks necessary to submit a SLURM job.
    This function will update the issue, then create a shell script for SLURM, then run the shell script on the cluster.
    :param redmine_instance: instantiated Redmine API object
    :param issue: object pulled from Redmine instance
    :param work_dir: string path to working directory for Redmine job
    :param cmd: string containing bash command
    :param cpu_count: number of CPUs to allocate for slurm job
    :param memory: memory in MB to allocate for slurm job
    """
    # Set status of issue to In Progress
    redmine_instance.issue.update(resource_id=issue.id,
                                  status_id=2,
                                  notes='Your {} job has been submitted to the OLC Slurm cluster.'.format(
                                      job_type.upper()))

    logging.info('Updated job status for {} to In Progress'.format(issue.id))

    # Create shell script
    slurm_template = create_template(issue=issue, cpu_count=cpu_count, memory=memory, work_dir=work_dir, cmd=cmd)

    # Submit job to slurm
    logging.info('Submitting job {} to Slurm'.format(issue.id))
    os.system('sbatch ' + slurm_template)
    logging.info('Output for {} is available in {}'.format(issue.id, work_dir))


def prepare_automation_command(automation_script, pickles, work_dir):
    """
    Function for preparing the system call to an automation script
    :param automation_script: name of the script you'd like to call (i.e. 'autoclark.py')
    :param pickles: dictionary from the pickle_redmine() function
    :param work_dir: string path to working directory for Redmine job
    :return: string of completed command to pass to automation script
    """
    # Get path to script responsible for running automation job
    automation_script_path = os.path.join(os.path.dirname(__file__), 'automators_dev', automation_script)

    # Prepare command
    cmd = 'python ' \
          '{script} ' \
          '--redmine_instance {redmine_pickle} ' \
          '--issue {issue_pickle} ' \
          '--work_dir {work_dir} ' \
          '--description {description_pickle}'.format(script=automation_script_path,
                                                      redmine_pickle=pickles['redmine_instance'],
                                                      issue_pickle=pickles['issue'],
                                                      description_pickle=pickles['description'],
                                                      work_dir=work_dir)
    return cmd


def main():
    """
    USAGE:
    To suppress all irritating SSL warnings:
        python api.py 2> /dev/null

    To enjoy the wonderful SSL warnings:
        python api.py
    """

    logging.basicConfig(
        format='\033[92m \033[1m %(asctime)s \033[0m %(message)s ',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout) # Defaults to sys.stderr

    # Log into Redmine
    redmine = redmine_setup(API_KEY)

    # Greetings
    logging.info('####' * 13)
    logging.info('## The OLCRedmineAutomator-DEV is now operational ##')
    logging.info('####' * 13)

    # Continually monitor for new jobs
    while True:
        # Grab all issues belonging to CFIA
        issues = retrieve_issues(redmine)

        # Pull any new automation job requests from issues
        new_jobs = new_automation_jobs(issues)

        logging.info('Found {} jobs'.format(len(new_jobs)))
        if len(new_jobs) > 0:
            # Queue up a SLURM job for each new issue
            for job, job_type in new_jobs.items():
                logging.info('Detected {} job for Redmine issue {}'.format(job_type.upper(), job.id))

                # Grab work directory
                work_dir = bio_requests_setup(job)

                # Pull issue details from Redmine and dump to text file
                issue_text_dump(job)

                # Pull issue description
                description = retrieve_issue_description(job)

                # Pickle objects for usage by analysis scripts
                pickles = pickle_redmine(redmine_instance=redmine,
                                         issue=job,
                                         work_dir=work_dir,
                                         description=description)

                # Prepare command
                cmd = prepare_automation_command(automation_script=job_type + '.py',
                                                 pickles=pickles,
                                                 work_dir=work_dir)

                # Submit job
                submit_slurm_job(redmine_instance=redmine,
                                 issue=job,
                                 work_dir=work_dir,
                                 cmd=cmd,
                                 job_type=job_type,
                                 cpu_count=AUTOMATOR_KEYWORDS[job_type]['n_cpu'],
                                 memory=AUTOMATOR_KEYWORDS[job_type]['memory'])
                logging.info('----' * 12)

        # Pause for 5 seconds
        time.sleep(5)


if __name__ == '__main__':
    main()
