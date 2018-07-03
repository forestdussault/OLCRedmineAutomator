import os
import glob
import click
import pickle
import shutil
import ftplib
import subprocess
from automator_settings import FTP_USERNAME, FTP_PASSWORD
from nastools.nastools import retrieve_nas_files

@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def mob_suite(redmine_instance, issue, work_dir, description):
    """
    """
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    try:
        # Description should just be a list of SEQIDs. Get the fasta files associated with them extracted
        # to the bio_request dir
        retrieve_nas_files(seqids=description,
                           outdir=os.path.join(work_dir, 'fastas'),
                           filetype='fasta',
                           copyflag=False)
        # Now we need to run mob_recon (and typing!) on each of the fasta files requested. Put all results into one
        # folder (this will need to be uploaded to FTP - will overwhelm max (10MB) file size limit on Redmine

        fasta_files = glob.glob(os.path.join(work_dir, 'fastas', '*.fasta'))
        # Verify that specified fasta files are actually there, warn user if they aren't.
        missing_fastas = verify_fasta_files_present(seqid_list=description,
                                                    fasta_dir=os.path.join(work_dir, 'fastas'))
        if len(missing_fastas) > 0:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='WARNING: Could not find the following requested SEQIDs on'
                                                ' the OLC NAS: {}'.format(missing_fastas))

        # Make output dir
        output_dir = os.path.join(work_dir, 'mob_suite_results')
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        for fasta in fasta_files:
            # os.system(cmd)
            seqid = os.path.split(fasta)[-1].split('.')[0]
            cmd = '/mnt/nas2/virtual_environments/mob_suite/bin/mob_recon -i {input_fasta} -o {output_dir}' \
                  ' --run_typer'.format(input_fasta=fasta,
                                        output_dir=os.path.join(output_dir, seqid))
            subprocess.run('source activate /mnt/nas2/virtual_environments/mob_suite && {cmd} && '
                           'source deactivate'.format(cmd=cmd), shell=True, executable='/bin/bash')

        # With mobsuite done, zip up the results folder and upload to the FTP.
        shutil.make_archive(root_dir=output_dir,
                            format='zip',
                            base_name=os.path.join(work_dir, str(issue.id)))

        s = ftplib.FTP('ftp.agr.gc.ca', user=FTP_USERNAME, passwd=FTP_PASSWORD)
        s.cwd('outgoing/cfia-ak')
        f = open(os.path.join(work_dir, str(issue.id) + '.zip'), 'rb')
        s.storbinary('STOR {}.zip'.format(str(issue.id)), f)
        f.close()
        s.quit()

        # And finally, do some file cleanup.
        try:
            shutil.rmtree(output_dir)
            os.remove(os.path.join(work_dir, str(issue.id) + '.zip'))
        except:
            pass

        redmine_instance.issue.update(resource_id=issue.id, status_id=4,
                                      notes='Mob-suite process complete!\n\n'
                                            'Results are available at the following FTP address:\n'
                                            'ftp://ftp.agr.gc.ca/outgoing/cfia-ak/{}'.format(str(issue.id) + '.zip'))

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
    mob_suite()
