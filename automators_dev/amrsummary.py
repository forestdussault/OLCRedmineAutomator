import os
import glob
import click
import pickle
import shutil
from nastools.nastools import retrieve_nas_files

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

        retrieve_nas_files(seqids=seqids,
                           outdir=work_dir,
                           filetype='fasta',
                           copyflag=False)

        missing_fastas = verify_fasta_files_present(seqids, work_dir)
        if missing_fastas:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='WARNING: Could not find the following requested SEQIDs on'
                                                ' the OLC NAS: {}'.format(missing_fastas))

        db_path = '/mnt/nas2/databases/assemblydatabases/0.3.4/'
        # Run ResFindr
        cmd = 'GeneSeekr blastn -s {seqfolder} -t {targetfolder} -r {reportdir} -R'\
            .format(seqfolder=work_dir,
                    targetfolder=os.path.join(db_path, 'resfinder'),
                    reportdir=os.path.join(work_dir, 'reports'))
        print(cmd)
        os.system(cmd)
        mob_cmd = 'docker run --rm -i -u $(id -u) -v /mnt/nas2:/mnt/nas2 cowbat:0.4.1 /bin/bash ' \
                  '-c "source activate cowbat && python -m spadespipeline.mobrecon -s {seqfolder} ' \
                  '-r {targetfolder}"'.format(seqfolder=work_dir,
                                              targetfolder=os.path.join(db_path, 'mobrecon'))
        print(mob_cmd)
        os.system(mob_cmd)
        # Get the output file uploaded.
        output_list = list()
        output_dict = dict()
        # Add the three reports separately to the output list
        output_dict['path'] = os.path.join(work_dir, 'reports', 'resfinder_blastn.xlsx')
        output_dict['filename'] = 'resfinder_blastn.xlsx'
        output_list.append(output_dict)
        output_dict = dict()
        output_dict['path'] = os.path.join(work_dir, 'reports', 'mob_recon_summary.csv')
        output_dict['filename'] = 'mob_recon_summary.csv'
        output_list.append(output_dict)
        output_dict = dict()
        output_dict['path'] = os.path.join(work_dir, 'reports', 'amr_summary.csv')
        output_dict['filename'] = 'amr_summary.csv'
        output_list.append(output_dict)
        redmine_instance.issue.update(resource_id=issue.id, uploads=output_list, status_id=4,
                                      notes='resfinder process complete!')

        # Clean up all FASTA/FASTQ files so we don't take up too much space on the NAS
        os.system('rm {workdir}/*fasta'.format(workdir=work_dir))
        try:
            # Remove all other folders
            for dirpath, dirnames, filenames in os.walk(work_dir):
                for dirname in dirnames:
                    shutil.rmtree(os.path.join(dirpath, dirname))
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
