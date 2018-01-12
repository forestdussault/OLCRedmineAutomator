import os
import glob
import click
import ftplib
import pickle
import shutil
from setup import FTP_USERNAME, FTP_PASSWORD


@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def externalretrieve_redmine(redmine_instance, issue, work_dir, description):
    print('External retrieving!')
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    os.makedirs(os.path.join(work_dir, str(issue.id)))
    # Parse description to figure out what SEQIDs we need to run on.
    fasta_list = list()
    fastq_list = list()
    fasta = False
    fastq = True
    for item in description:
        item = item.upper()
        if 'FASTA' in item:
            fasta = True
            fastq = False
            continue
        if 'FASTQ' in item:
            fastq = True
            fasta = False
            continue
        if fasta:
            fasta_list.append(item)
        elif fastq:
            fastq_list.append(item)

    # Write SEQIDs to file to be extracted as FASTAs and copy them to the biorequest dir.
    if len(fasta_list) > 0:
        with open(os.path.join(work_dir, 'seqid.txt'), 'w') as f:
            for seqid in fasta_list:
                f.write(seqid + '\n')
        cmd = 'python2 /mnt/nas/WGSspades/file_extractor.py {seqidlist} ' \
              '{output_folder} /mnt/nas/'.format(seqidlist=os.path.join(work_dir, 'seqid.txt'),
                                                 output_folder=os.path.join(work_dir, str(issue.id)))
        os.system(cmd)

    # Extract FASTQ files.
    if len(fastq_list) > 0:
        with open(os.path.join(work_dir, 'seqid.txt'), 'w') as f:
            for seqid in fastq_list:
                f.write(seqid + '\n')
        current_dir = os.getcwd()
        os.chdir('/mnt/nas/MiSeq_Backup')
        cmd = 'python2 file_extractor.py {seqidlist} ' \
              '{output_folder}'.format(seqidlist=os.path.join(work_dir, 'seqid.txt'),
                                       output_folder=os.path.join(work_dir, str(issue.id)))
        os.system(cmd)
        os.chdir(current_dir)

    # Check that we got all the requested files.
    missing_fastas = check_fastas_present(fasta_list, os.path.join(work_dir, str(issue.id)))
    missing_fastqs = check_fastqs_present(fastq_list, os.path.join(work_dir, str(issue.id)))
    if len(missing_fastqs) > 0:
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='WARNING: Could not find the following requested FASTQ SEQIDs on'
                                            ' the OLC NAS: {}'.format(missing_fastqs))

    if len(missing_fastas) > 0:
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='WARNING: Could not find the following requested FASTA SEQIDs on'
                                            ' the OLC NAS: {}'.format(missing_fastqs))

    # Now make a zip folder that we'll upload to the FTP.
    shutil.make_archive(root_dir=os.path.join(work_dir, str(issue.id)),
                        format='zip',
                        base_name=os.path.join(work_dir, str(issue.id)))

    # Now need to login to the FTP to upload the zipped folder.
    s = ftplib.FTP('ftp.agr.gc.ca', user=FTP_USERNAME, passwd=FTP_PASSWORD)
    s.cwd('outgoing/cfia-ak')
    f = open(os.path.join(work_dir, str(issue.id) + '.zip'), 'rb')
    s.storbinary('STOR {}.zip'.format(str(issue.id)), f)
    f.close()
    s.quit()

    # And finally, do some file cleanup.
    try:
        shutil.rmtree(os.path.join(work_dir, str(issue.id)))
        os.remove(os.path.join(work_dir, str(issue.id) + '.zip'))
    except:
        pass

    redmine_instance.issue.update(resource_id=issue.id, status_id=4,
                                  notes='External Retrieve process complete!')


def check_fastas_present(fasta_list, fasta_dir):
    missing_fastas = list()
    for seqid in fasta_list:
        if len(glob.glob(os.path.join(fasta_dir, seqid + '*.fasta'))) == 0:
            missing_fastas.append(seqid)
    return missing_fastas


def check_fastqs_present(fastq_list, fastq_dir):
    missing_fastqs = list()
    for seqid in fastq_list:
        if len(glob.glob(os.path.join(fastq_dir, seqid + '*.fastq.gz'))) < 2:
            missing_fastqs.append(seqid)
    return missing_fastqs

if __name__ == '__main__':
    externalretrieve_redmine()
