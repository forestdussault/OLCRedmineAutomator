import os
import glob
import click
import pickle


@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def clark_redmine(redmine_instance, issue, work_dir, description):
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    # Parse description to figure out what SEQIDs we need to run on.
    seqids = list()
    fasta = False
    for item in description:
        item = item.upper()
        if 'FASTA' in item:
            fasta = True
            continue
        seqids.append(item)

    # Write SEQIDs to file to be extracted and CLARKed.
    with open(os.path.join(work_dir, 'seqid.txt'), 'w') as f:
        for seqid in seqids:
            f.write(seqid + '\n')

    # If FASTQ, run file linker and then make sure that all FASTQ files requested are present. Warn user if they
    # requested things that we don't have.
    if not fasta:
        current_dir = os.getcwd()
        os.chdir('/mnt/nas/MiSeq_Backup')
        cmd = 'python2 /mnt/nas/MiSeq_Backup/file_linker.py {}/seqid.txt {}'.format(work_dir, work_dir)
        os.system(cmd)
        os.chdir(current_dir)
        missing_fastqs = verify_fastq_files_present(seqids, work_dir)
        if missing_fastqs:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='WARNING: Could not find the following requested SEQIDs on'
                                                ' the OLC NAS: {}'.format(missing_fastqs))

    # If it's FASTA, extract them and make sure all are present.
    if fasta:
        cmd = 'python2 /mnt/nas/WGSspades/file_extractor.py {}/seqid.txt {} /mnt/nas/'.format(work_dir, work_dir)
        os.system(cmd)
        missing_fastas = verify_fasta_files_present(seqids, work_dir)
        if missing_fastas:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='WARNING: Could not find the following requested SEQIDs on'
                                                ' the OLC NAS: {}'.format(missing_fastas))

    # Run CLARK for classification.
    cmd = 'python -m metagenomefilter.automateCLARK -s {} -d /mnt/nas/Adam/RefseqDatabase/Bos_taurus/ ' \
          '-C /home/ubuntu/Programs/CLARKSCV1.2.3.2/ {}\n'.format(work_dir, work_dir)
    os.system(cmd)

    # Get the output file uploaded.
    output_list = list()
    output_dict = dict()
    output_dict['path'] = os.path.join(work_dir, 'reports', 'abundance.xlsx')
    output_dict['filename'] = 'abundance.xlsx'
    output_list.append(output_dict)
    redmine_instance.issue.update(resource_id=issue.id, uploads=output_list, status_id=4,
                                  notes='AutoCLARK process complete!')

    # Clean up all FASTA/FASTQ files so we don't take up too
    os.system('rm {workdir}/*fasta {workdir}/*fastq*'.format(workdir=work_dir))

def verify_fastq_files_present(seqid_list, fastq_dir):
    """
    Makes sure that FASTQ files specified in seqid_list have been successfully copied/linked to directory specified
    by fastq_dir.
    :param seqid_list: List with SEQIDs.
    :param fastq_dir: Directory that FASTQ files (both forward and reverse reads should have been copied to
    :return: List of SEQIDs that did not have files associated with them.
    """
    missing_fastqs = list()
    for seqid in seqid_list:
        # Check forward.
        if len(glob.glob(fastq_dir + '/' + seqid + '*R1*fastq*')) == 0:
            missing_fastqs.append(seqid)
        # Check reverse. Only add to list of missing if forward wasn't present.
        if len(glob.glob(fastq_dir + '/' + seqid + '*R2*fastq*')) == 0 and seqid not in missing_fastqs:
            missing_fastqs.append(seqid)
        return missing_fastqs


def verify_fasta_files_present(seqid_list, fasta_dir):
    missing_fastas = list()
    for seqid in seqid_list:
        if len(glob.glob(os.path.join(fasta_dir, seqid, '*.fasta'))) == 0:
            missing_fastas.append(seqid)
    return missing_fastas

if __name__ == '__main__':
    clark_redmine()
