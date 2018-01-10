import os
import time
import logging
import pickle
from redminelib import Redmine
from setup import AUTOMATOR_KEYWORDS, API_KEY, BIO_REQUESTS_DIR


def redmine_setup(api_key):
    """
    :param api_key: API key available from your Redmine user account settings. Stored in setup.py.
    :return: instantiated Redmine API object
    """
    redmine = Redmine('http://redmine.biodiversity.agr.gc.ca/', key=api_key)
    return redmine


def retrieve_issues(redmine_instance):
    """
    :param redmine_instance: instantiated Redmine API object
    :return: returns an object containing all issues for OLC CFIA (http://redmine.biodiversity.agr.gc.ca/projects/cfia/)
    """
    issues = redmine_instance.issue.filter(project_id='cfia')
    return issues


def new_automation_jobs(issues):
    """
    :param issues: issues object pulled from Redmine API
    :return: returns a new subset of issues that are Status: NEW and match a term in AUTOMATOR_KEYWORDS)
    """
    new_jobs = []
    for issue in issues:
        # Only new issues
        if issue.status.name == 'New': # change this to new for production
            # Check for presence of an automator keyword in subject line
            if issue.subject.lower() in AUTOMATOR_KEYWORDS:
                new_jobs.append(issue)
                logging.debug('{id}:{subject}:{status}'.format(id = issue.id, subject = issue.subject, status = issue.status))
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
    :param issue: object pulled from Redmine instance
    :param destination: output path for text file
    :return: path to text file
    """
    file_path = os.path.join(BIO_REQUESTS_DIR, str(issue.id), str(issue.id)+'_'+str(issue.subject)+'_redmine_details.txt')
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
    :return: dictionary with paths to redmine instance, issue and description pickles
    """
    pickled_redmine = os.path.join(work_dir,'redmine.pickle')
    pickled_issue = os.path.join(work_dir,'issue.pickle')
    pickled_description = os.path.join(work_dir,'description.pickle')

    # Create dictionary
    pickles = {'redmine_instance' : pickled_redmine,
               'issue' : pickled_issue,
               'description' : pickled_description}

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
    Takes a shell script and makes it executable
    :param path: path to shell script
    """
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2
    os.chmod(path, mode)


def create_template(issue, cpu_count, memory, work_dir, cmd):
    """
    Creates a slurm job shell script
    :param issue: object pulled from Redmine instance
    :param cpu_count: number of CPUs to allocate for slurm job
    :param memory: memory in MB to allocate for slurm job
    :param work_dir: string path to working directory for Redmine job
    :param cmd: string containing bash command
    :return: string file path to generated shell script
    """
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

    # Path to slurm shell script
    file_path = os.path.join(BIO_REQUESTS_DIR, str(issue.id), str(issue.id) + '_slurm.sh')

    # Write slurm job to shell script
    with open(file_path, 'w+') as file:
        file.write(template)

    # chmod +x
    make_executable(file_path)

    return file_path


def submit_slurm_job(redmine_instance, resource_id, issue, work_dir, cmd, cpu_count=8, memory=12000):
    """
    :param redmine_instance: instantiated Redmine API object
    :param resource_id: ID pulled from issue instance
    :param issue: object pulled from Redmine instance
    :param work_dir: string path to working directory for Redmine job
    :param cmd: string containing bash command
    :param cpu_count: number of CPUs to allocate for slurm job
    :param memory: memory in MB to allocate for slurm job
    """
    # Set status of issue to In Progress
    redmine_instance.issue.update(resource_id=issue.id, status_id=2)
    logging.info('Updated job status for {} to In Progress'.format(issue.id))

    # Create shell script
    slurm_template = create_template(issue=issue, cpu_count=cpu_count, memory=memory, work_dir=work_dir, cmd=cmd)

    # Submit job to slurm
    logging.info('Submitting job {} to Slurm'.format(issue.id))
    os.system('sbatch ' + slurm_template)


def main():
    logging.basicConfig(level=logging.DEBUG)
    redmine = redmine_setup(API_KEY)

    # Continually monitor for new jobs
    while True:
        logging.info('{}: Scanning for new Redmine jobs...'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())))

        # Grab jobs
        issues = retrieve_issues(redmine)
        new_jobs = new_automation_jobs(issues)

        # Queue up a slurm job for each new issue
        for job in new_jobs:
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

            #############################################
            ### Plug in your new Redmine scripts here ###
            #############################################

            if job.subject.lower() == 'strainmash':
                logging.info('Detected STRAINMASH job for Redmine issue {}'.format(job.id))

                # Prepare command to call analysis script with pickled Redmine objects
                cmd = 'python ' \
                      '/mnt/nas/Redmine/OLCRedmineAutomator/automators/strainmash.py ' \
                      '--redmine_instance {redmine_pickle} ' \
                      '--issue {issue_pickle} ' \
                      '--work_dir {work_dir} ' \
                      '--description {description_pickle}'.format(redmine_pickle=pickles['redmine_instance'],
                                                                  issue_pickle=pickles['issue'],
                                                                  description_pickle=pickles['description'],
                                                                  work_dir=work_dir,)

                # Submit job to slurm
                submit_slurm_job(redmine_instance=redmine,
                                 resource_id=job.id,
                                 issue=job,
                                 work_dir=work_dir,
                                 cmd=cmd,
                                 cpu_count=8,
                                 memory=12000)

            elif job.subject.lower() == 'wgs assembly':
                logging.info('Detected WGS ASSEMBLY job for Redmine issue {}'.format(job.id))

            elif job.subject.lower() == 'autoclark':
                logging.info('Detected AUTOCLARK job for Redmine issue {}'.format(job.id))

            elif job.subject.lower() == 'snvphyl':
                logging.info('Detected SNVPHYL job for Redmine issue {}'.format(job.id))

            elif job.subject.lower() == 'diversitree':
                logging.info('Detected DIVIERSITREE job for Redmine issue {}'.format(job.id))

            elif job.subject.lower() == 'irida retrieve':
                logging.info('Detected IRIDA RETRIEVE job for Redmine issue {}'.format(job.id))

            elif job.subject.lower() == 'resfindr':
                logging.info('Detected RESFINDER job for Redmine issue {}'.format(job.id))

            elif job.subject.lower() == 'external retrieve':
                logging.info('Detected EXTERNAL RETRIEVE job for Redmine issue {}'.format(job.id))

            elif job.subject.lower() == 'autoroga':
                logging.info('Detected AUTOROGA job for Redmine issue {}'.format(job.id))

            elif job.subject.lower() == 'retrieve':
                logging.info('Detected RETRIEVE job for Redmine issue {}'.format(job.id))

            #############################################
            #############################################
            #############################################

        # Take a nap for 5 minutes
        logging.info('A new scan for issues will be performed in 5 minutes')
        time.sleep(300)


if __name__ == '__main__':
    main()
