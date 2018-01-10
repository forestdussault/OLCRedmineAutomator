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
    :param redmine_instance:
    :param issue:
    :param work_dir:
    :return: dictionary with paths to redmine instance, issue and issue description pickles
    """
    pickled_redmine = os.path.join(work_dir,'redmine.p')
    pickled_issue = os.path.join(work_dir,'issue.p')
    pickled_description = os.path.join(work_dir,'description.p')

    # Create dictionary
    pickles = {'redmine_instance' : pickled_redmine,
               'issue' : pickled_issue,
               'description' : pickled_description}

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
    :param issue:
    :param cpu_count:
    :param memory:
    :param work_dir:
    :param cmd:
    :return:
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


def submit_slurm_job(redmine_instance, resource_id, job, work_dir, cmd, cpu_count=8, memory=12000):
    # Set status of issue to In Progress
    redmine_instance.issue.update(resource_id=job.id, status_id=2)
    logging.info('Updated job status for {} to In Progress'.format(job.id))

    # Create shell script
    slurm_template = create_template(issue=job, cpu_count=cpu_count, memory=memory, work_dir=work_dir, cmd=cmd)

    # Submit job to slurm
    logging.info('Submitting job {} to Slurm'.format(job.id))
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
            work_dir = bio_requests_setup(job)
            issue_text_dump(job)
            description = retrieve_issue_description(job)

            pickles = pickle_redmine(redmine_instance=redmine,
                                     issue=job,
                                     work_dir=work_dir,
                                     description=description)

            #########################################
            # Plug in your new Redmine scripts here #
            #########################################

            if job.subject.lower() == 'strainmash_test':
                logging.info('Detected STRAINMASH job for Redmine issue {}'.format(job.id))

                cmd = 'python ' \
                      '/mnt/nas/Redmine/OLCRedmineAutomator/automators/strainmash.py ' \
                      '--redmine_instance {redmine_pickle} ' \
                      '--issue {issue_pickle} ' \
                      '--work_dir {work_dir} ' \
                      '--description {description_pickle}'.format(redmine_pickle=pickles['redmine_instance'],
                                                                  issue_pickle=pickles['issue'],
                                                                  work_dir=work_dir,
                                                                  description_pickle=pickles['description'])

                submit_slurm_job(redmine_instance=redmine,
                                 resource_id=job.id,
                                 job=job,
                                 work_dir=work_dir,
                                 cmd=cmd,
                                 cpu_count=8,
                                 memory=12000)

            elif job.subject == 'test2':
                pass
            else:
                pass

            #########################################

        # Take a nap for a minute
        time.sleep(60)


if __name__ == '__main__':
    main()
