import os
import glob
import click
import pickle
import shutil


@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def resfinder_redmine(redmine_instance, issue, work_dir, description):
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

        # Write SEQIDs to file to be extracted and CLARKed.
        with open(os.path.join(work_dir, 'seqid.txt'), 'w') as f:
            for seqid in seqids:
                f.write(seqid + '\n')

        # If it's FASTA, extract them and make sure all are present.
        cmd = 'python2 /mnt/nas/WGSspades/file_extractor.py {}/seqid.txt {} /mnt/nas/'.format(work_dir, work_dir)
        os.system(cmd)
        missing_fastas = verify_fasta_files_present(seqids, work_dir)
        if missing_fastas:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='WARNING: Could not find the following requested SEQIDs on'
                                                ' the OLC NAS: {}'.format(missing_fastas))

        # Run CLARK for classification.
        cmd = 'python -m spadespipeline.GeneSeekr -s {seqfolder} -t {targetfolder} -r {reportdir} -R'\
            .format(seqfolder=work_dir,
                    targetfolder='/mnt/nas/assemblydatabases/0.2.3/databases/resfinder',
                    reportdir=os.path.join(work_dir, 'reports'))
        print(cmd)
        os.system(cmd)
        # Get the output file uploaded.
        output_list = list()
        output_dict = dict()
        output_dict['path'] = os.path.join(work_dir, 'reports', 'resfinder.xlsx')
        output_dict['filename'] = 'resfinder.xlsx'
        output_list.append(output_dict)
        redmine_instance.issue.update(resource_id=issue.id, uploads=output_list, status_id=4,
                                      notes='resfinder process complete!')

        # Clean up all FASTA/FASTQ files so we don't take up too
        os.system('rm {workdir}/*fasta'.format(workdir=work_dir))
        try:
            shutil.rmtree(os.path.join(work_dir, 'reports'))
        except IOError:
            pass
    except Exception as e:
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Something went wrong! Send this error traceback to your friendly '
                                            'neighborhood bioinformatician: {}'.format(e))


def verify_fasta_files_present(seqid_list, fasta_dir):
    missing_fastas = list()
    for seqid in seqid_list:
        if len(glob.glob(os.path.join(fasta_dir, seqid + '*.fasta'))) == 0:
            missing_fastas.append(seqid)
    return missing_fastas


if __name__ == '__main__':
    resfinder_redmine()