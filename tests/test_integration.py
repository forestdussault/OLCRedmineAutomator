#!/usr/bin/env python

from redminelib import Redmine
import logging

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
