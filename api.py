import os
import time
import logging
import pickle
import slurm_writer
from redminelib import Redmine
from setup import AUTOMATOR_KEYWORDS, API_KEY, BIO_REQUESTS_DIR
from automators import strainmash


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

    pickle.dump(redmine_instance, open(pickled_redmine, 'rb'))
    pickle.dump(issue, open(pickled_issue, 'rb'))
    pickle.dump(description, open(pickled_description, 'rb'))

    return pickles

def main():
    logging.basicConfig(level=logging.DEBUG)
    redmine = redmine_setup(API_KEY)

    # Continually monitor for new jobs
    while True:
        issues = retrieve_issues(redmine)
        new_jobs = new_automation_jobs(issues)

        logging.info('{}: Scanning for new Redmine jobs...'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())))

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

                slurm_template = slurm_writer.create_template(issue=job,
                                                              cpu_count=8,
                                                              memory=12000,
                                                              work_dir=work_dir
                                                              )


                strainmash.strainmash_redmine(redmine_instance=redmine,
                                              issue=job,
                                              work_dir=work_dir,
                                              description=description)
            elif job.subject == 'test2':
                pass
            else:
                pass

            #########################################

        # Take a nap for a minute
        time.sleep(60)


if __name__ == '__main__':
    main()
