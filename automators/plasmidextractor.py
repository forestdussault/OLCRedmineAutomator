import os
import glob
import click
import pickle
import subprocess
from biotools import mash

@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def plasmidextractor_redmine(redmine_instance, issue, work_dir, description):
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    # Parse description to get list of SeqIDs
    seqids = []
    for i in range(0, len(description)):
        item = description[i]
        item = item.upper()
        seqids.append(item)

    # Create folder to drop FASTQ files
    raw_reads_folder = os.path.join(work_dir, 'raw_reads')
    os.mkdir(raw_reads_folder)

    # Extract FASTQ files.
    if len(seqids) > 0:
        with open(os.path.join(work_dir, 'seqid.txt'), 'w') as f:
            for seqid in seqids:
                f.write(seqid + '\n')
        current_dir = os.getcwd()
        os.chdir('/mnt/nas/MiSeq_Backup')
        cmd = 'python2 file_linker.py {seqidlist} ' \
              '{output_folder}'.format(seqidlist=os.path.join(work_dir, 'seqid.txt'),
                                       output_folder=raw_reads_folder)
        os.system(cmd)
        os.chdir(current_dir)

    redmine_instance.issue.update(resource_id=issue.id, status_id=4)


# TODO: conda must be installed in order to call PlasmidExtractor
def call_plasmidextractor(work_dir):
    p = subprocess.Popen('python PlasmidExtractor.py')


if __name__ == '__main__':
    plasmidextractor_redmine()
