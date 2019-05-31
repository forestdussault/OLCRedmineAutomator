import os
import glob
import click
import pickle
import shutil
import sentry_sdk
from automator_settings import SENTRY_DSN
from automator_settings import COWBAT_DATABASES
from nastools.nastools import retrieve_nas_files

@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def resfinder_redmine(redmine_instance, issue, work_dir, description):
    sentry_sdk.init(SENTRY_DSN)
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    try:
        # Parse description to figure out what SEQIDs we need to run on.
        seqids = list()
        for item in description:
            item = item.upper()
            seqids.append(item)

        retrieve_nas_files(seqids=seqids,
                           outdir=work_dir,
                           filetype='fasta',
                           copyflag=False)

        missing_fastas = verify_fasta_files_present(seqids, work_dir)
        if missing_fastas:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='WARNING: Could not find the following requested SEQIDs on'
                                                ' the OLC NAS: {}'.format(missing_fastas))

        # Run ResFindr
        cmd = 'GeneSeekr blastn -s {seqfolder} -t {targetfolder} -r {reportdir} -A'\
            .format(seqfolder=work_dir,
                    targetfolder=os.path.join(COWBAT_DATABASES, 'resfinder'),
                    reportdir=os.path.join(work_dir, 'reports'))
        print(cmd)
        os.system(cmd)
        # Get the output file uploaded.
        output_list = list()
        output_dict = dict()
        output_dict['path'] = os.path.join(work_dir, 'reports', 'resfinder_blastn.xlsx')
        output_dict['filename'] = 'resfinder_blastn.xlsx'
        output_list.append(output_dict)
        redmine_instance.issue.update(resource_id=issue.id, uploads=output_list, status_id=4,
                                      notes='resfinder process complete!')

        # Clean up all FASTA/FASTQ files so we don't take up too much space on the NAS
        os.system('rm {workdir}/*fasta'.format(workdir=work_dir))
        try:
            shutil.rmtree(os.path.join(work_dir, 'reports'))
        except IOError:
            pass
    except Exception as e:
        sentry_sdk.capture_exception(e)
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Something went wrong! We log this automatically and will look into the '
                                            'problem and get back to you with a fix soon.')


def verify_fasta_files_present(seqid_list, fasta_dir):
    missing_fastas = list()
    for seqid in seqid_list:
        if len(glob.glob(os.path.join(fasta_dir, seqid + '*.fasta'))) == 0:
            missing_fastas.append(seqid)
    return missing_fastas


if __name__ == '__main__':
    resfinder_redmine()
