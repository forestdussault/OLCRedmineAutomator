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
def ecgf(redmine_instance, issue, work_dir, description):
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
                           filetype='fasta')
        fasta_files = glob.glob(os.path.join(work_dir, 'fastas', '*.fasta'))
        # Verify that specified fasta files are actually there, warn user if they aren't.
        missing_fastas = verify_fasta_files_present(seqid_list=description,
                                                    fasta_dir=os.path.join(work_dir, 'fastas'))
        if len(missing_fastas) > 0:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='WARNING: Could not find the following requested SEQIDs on'
                                                ' the OLC NAS: {}'.format(missing_fastas))

        # Make output dir
        output_dir = os.path.join(work_dir, 'results')
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)
        # These unfortunate hard coded paths appear to be necessary
        activate = 'source /home/ubuntu/miniconda3/bin/activate /mnt/nas2/virtual_environments/ecgf'
        # As the files are processed one at a time, create a list of all the reports in order to create a summary report
        reports = list()
        for fasta in sorted(fasta_files):
            seqid = os.path.split(fasta)[-1].split('.')[0]
            report = os.path.join(output_dir, '{seqid}.csv'.format(seqid=seqid))
            reports.append(report)
            # Create the command line call to eCGF
            cmd = 'eCGF {fasta} {csv}'.format(fasta=fasta,
                                              csv=report)
            # Create another shell script to execute within the conda environment
            template = "#!/bin/bash\n{activate} && {cmd}".format(activate=activate,
                                                                 cmd=cmd)
            ecgf_script = os.path.join(work_dir, 'run_ecgf.sh')
            with open(ecgf_script, 'w+') as file:
                file.write(template)
            # Modify the permissions of the script to allow it to be run on the node
            make_executable(ecgf_script)
            # Run shell script
            os.system(ecgf_script)
        # Create a summary report of all the individual reports
        header = str()
        data = str()
        for report in reports:
            with open(report, 'r') as summary:
                if not header:
                    header = summary.readline()
                else:
                    next(summary)
                for line in summary:
                    # Create a list of the entries by splitting on ,
                    line_list = line.split(',')
                    # Remove the path and extension from the file name
                    line_list[0] = os.path.basename(os.path.splitext(line_list[0])[0])
                    data += ','.join(line_list)
        summary_report = os.path.join(output_dir, '{id}_summary_report.csv'.format(id=str(issue.id)))
        with open(summary_report, 'w') as summary:
            summary.write(header)
            summary.write(data)
        zip_filepath = os.path.join(work_dir, 'eCGF_output_{id}'.format(id=str(issue.id)))
        # With eCGF done, zip up the results folder
        shutil.make_archive(root_dir=output_dir,
                            format='zip',
                            base_name=zip_filepath)
        # Prepare upload
        output_list = [
            {
                'filename': os.path.basename(zip_filepath) + '.zip',
                'path': zip_filepath + '.zip'
            }
        ]
        # Wrap up issue
        redmine_instance.issue.update(resource_id=issue.id,
                                      uploads=output_list,
                                      status_id=4,
                                      notes='Analysis with eCGF complete!'
                                      )
        # And finally, do some file cleanup.
        try:
            shutil.rmtree(output_dir)
            shutil.rmtree(os.path.join(work_dir, 'fastas'))
            os.remove(zip_filepath + '.zip')
        except:
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


def make_executable(path):
    """
    Takes a shell script and makes it executable (chmod +x)
    :param path: path to shell script
    """
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2
    os.chmod(path, mode)


if __name__ == '__main__':
    ecgf()
