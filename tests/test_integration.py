#!/usr/bin/env python

from redminelib import Redmine
import tempfile
import logging
import time
import os

"""
Test script to make sure that adding an automator/changing some dependency somewhere doesn't break all the things.
"""

DEV_REDMINE_URL = 'http://192.168.1.2:8080'
REDMINE_PROJECT = 'test'


def create_issue(redmine_instance, subject, description, attachments=None):
    if attachments is None:
        issue = redmine_instance.issue.create(project_id=REDMINE_PROJECT,
                                              subject=subject,
                                              description=description)
    else:
        issue = redmine_instance.issue.create(project_id=REDMINE_PROJECT,
                                              subject=subject,
                                              description=description,
                                              uploads=attachments)

    return issue.id


def create_test_issues(redmine):

    """
    Creates issues to be tested.
    :param redmine: instantiated redmine instance
    :return: dictionary with issue subjects as keys and redmine issue IDs as values.
    """

    logging.info('Creating Issues!')
    # CREATE ALL THE ISSUES - store the issue ID for each of the issues created in the dictionary.
    issues_created = dict()

    issues_created['amrsummary'] = create_issue(redmine_instance=redmine,
                                                subject='AMR Summary',
                                                description='2014-SEQ-0276')
    issues_created['autoclark'] = create_issue(redmine_instance=redmine,
                                               subject='autoclark',
                                               description='fasta\n2014-SEQ-0276')
    issues_created['ecgf'] = create_issue(redmine_instance=redmine,
                                          subject='ecgf',
                                          description='2018-LET-0106')
    issues_created['ec_typer'] = create_issue(redmine_instance=redmine,
                                              subject='ec_typer',
                                              description='2014-SEQ-0276')
    issues_created['externalretrieve'] = create_issue(redmine_instance=redmine,
                                                      subject='External Retrieve',
                                                      description='fasta\n2014-SEQ-0276')
    issues_created['diversitree'] = create_issue(redmine_instance=redmine,
                                                 subject='diversitree',
                                                 description='2\n2014-SEQ-0275\n2014-SEQ-0276\n2014-SEQ-0277\n2014-SEQ-0278')
    issues_created['geneseekr'] = create_issue(redmine_instance=redmine,
                                               subject='geneseekr',
                                               description='analysis=mlst\n2014-SEQ-0276')
    issues_created['pointfinder'] = create_issue(redmine_instance=redmine,
                                                 subject='pointfinder',
                                                 description='2014-SEQ-0276')
    issues_created['prokka'] = create_issue(redmine_instance=redmine,
                                            subject='prokka',
                                            description='2014-SEQ-0276')
    issues_created['resfinder'] = create_issue(redmine_instance=redmine,
                                               subject='resfinder',
                                               description='2014-SEQ-0276')
    issues_created['sipprverse'] = create_issue(redmine_instance=redmine,
                                                subject='sipprverse',
                                                description='analysis=mlst\n2014-SEQ-0276')
    issues_created['staramr'] = create_issue(redmine_instance=redmine,
                                             subject='staramr',
                                             description='2014-SEQ-0282')
    return issues_created


def monitor_issues(redmine, issue_dict, timeout):
    """
    Monitors issues for completion.
    :param redmine: Instantiated redmine instance.
    :param issue_dict: Dictionary created by create_test_issues (issue subjects as keys, issue IDs as values)
    :param timeout: Number of seconds to wait for all issues to finish running
    :return:
    """
    all_complete = False
    total_time_taken = 0
    increment = 30  # Check on issues every increment seconds
    # Create dictionary to track progress of each issue created.
    issues_validated = dict()
    for issue_subject in issue_dict:
        issues_validated[issue_subject] = 'Unknown'

    # Now loop through all the things!
    while all_complete is False and total_time_taken < timeout:
        all_complete = True  # Set flag to true - if any of our issues are not finished, this gets set back to False
        # Check all issues for completion.
        for issue_subject in issue_dict:
            issue_id = issue_dict[issue_subject]
            issue = redmine.issue.get(issue_id, include=['attachments'])

            if issue_subject == 'amrsummary' and issues_validated[issue_subject] == 'Unknown':
                if issue.status.id == 4:  # 4 corresponds to issue being complete.
                    if len(issue.attachments) > 0:  # Make sure attachments got uploaded
                        amr_summary_found = False
                        for attachment in issue.attachments:
                            if attachment.filename == 'amr_summary.csv':
                                amr_summary_found = True
                                with tempfile.TemporaryDirectory() as tmpdir:
                                    attachment.download(savepath=tmpdir, filename='amr_summary.csv')
                                    if os.path.getsize(os.path.join(tmpdir, 'amr_summary.csv')) > 0:
                                        issues_validated[issue_subject] = 'Validated'
                                        # Also validate that report has correct stuff
                        if amr_summary_found is False:
                            issues_validated[issue_subject] = 'No amr_summary.csv found, Validation Failed'

                    else:
                        issues_validated[issue_subject] = 'No Attachments, Validation Failed.'
                else:  # If hasn't finished yet, just wait!
                    all_complete = False

            elif issue_subject == 'autoclark':
                # Check for abundance.xlsx attachment, make sure size is nonzero
                # Also validate that report has correct stuff
                pass
            elif issue_subject == 'ecgf':
                # Check for ecgf_output_ISSUEID.zip, make sure size is nonzero
                # Also validate that report has correct stuff
                pass
            elif issue_subject == 'ec_typer':
                # Check for ec_typer_report.tsv, non-zero size
                # Also validate that report has correct stuff
                pass
            elif issue_subject == 'externalretrieve':
                # Check for ISSUEID.zip on outgoing FTP (or just parse updates for that address)
                pass
            elif issue_subject == 'diversitree':
                # Check for diversitree_report.html attachment, non-zero size
                # Also validate that report has correct stuff
                pass
            elif issue_subject == 'geneseekr':
                # Check for ??? (geneseekr_blastn I think)
                # Also validate that report has correct stuff
                pass
            elif issue_subject == 'pointfinder':
                # Figure out files expected once pointfinder gets fixed.
                # Also validate that report has correct stuff
                pass
            elif issue_subject == 'prokka':
                # Check for prokka_output_ISSUEID.zip on outgoing FTP
                pass
            elif issue_subject == 'resfinder':
                # Check for resfinder_blastn.xlsx attachment, nonzero size
                # Also validate that report has correct stuff
                pass
            elif issue_subject == 'sipprverse':
                # Check for sipprverse_output.zip, non-zero size
                # Also validate that report has correct stuff
                pass
            elif issue_subject == 'staramr':
                # Check for staramr_output.zip, non-zero size
                # Also validate that report has correct stuff
                pass
        total_time_taken += increment
        time.sleep(increment)

    return issues_validated


if __name__ == '__main__':
    logging.basicConfig(format='\033[92m \033[1m %(asctime)s \033[0m %(message)s ',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    api_key = input('Enter API Key for redmine dev: ')
    redmine = Redmine(DEV_REDMINE_URL,
                      key=api_key,
                      requests={
                          'verify': False,
                          'timeout': 10,
                      })

    """
    Need to: create an issue for each of the automators we actually care about testing, and then check on outputs
    for each of them after a reasonable timeframe.
    Automators to test - more to be added as we go: 
    AMRsummary
    AutoCLARK
    eCGF
    ECTyper
    External Retrieve
    DiversiTree
    GeneSeekr
    PointFinder
    Prokka
    ResFinder
    sipprverse
    staramr
    """

    issues_created = create_test_issues(redmine)

    issues_validated = monitor_issues(redmine=redmine,
                                      issue_dict=issues_created,
                                      timeout=600)
    for issue_subject in issues_validated:
        print('{}: {}'.format(issue_subject, issues_validated[issue_subject]))

