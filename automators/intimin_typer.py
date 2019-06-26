import os
import glob
import click
import pickle
import shutil
import sentry_sdk
from amrsummary import before_send
from automator_settings import SENTRY_DSN
from nastools.nastools import retrieve_nas_files


@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def intimin_typer_redmine(redmine_instance, issue, work_dir, description):
    sentry_sdk.init(SENTRY_DSN, before_send=before_send)
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    try:
        # Parse description to get list of SeqIDs
        seqids = list()
        for i in range(0, len(description)):
            item = description[i]
            item = item.upper()

            # Minimal check to make sure IDs provided somewhat resemble a valid sample ID
            if item.isalpha():
                pass
            else:
                seqids.append(item)

        # Create folder to drop FASTQ files
        fasta_folder = os.path.join(work_dir, 'fasta_files')

        # Extract FASTA files.
        retrieve_nas_files(seqids=seqids, outdir=fasta_folder, filetype='fasta', copyflag=False)

        # These unfortunate hard coded paths appear to be necessary
        activate = 'source /home/ubuntu/miniconda3/bin/activate /mnt/nas2/virtual_environments/phylotyper'
        phylotyper_py = '/mnt/nas2/virtual_environments/phylotyper/bin/phylotyper'

        output_dir = os.path.join(work_dir, 'intimin_subtype_output')
        # Prepare command
        fasta_files = sorted(glob.glob(os.path.join(fasta_folder, '*.fasta')))
        cmd = '{phylotyper_py} genome eae {output_dir} '.format(phylotyper_py=phylotyper_py, output_dir=output_dir)
        for fasta_file in fasta_files:
            cmd += fasta_file + ' '

        # Create another shell script to execute within the PlasmidExtractor conda environment
        template = "#!/bin/bash\n{} && {}".format(activate, cmd)
        phylotyper_script = os.path.join(work_dir, 'run_phylotyper.sh')
        with open(phylotyper_script, 'w+') as file:
            file.write(template)
        make_executable(phylotyper_script)

        # Run shell script
        os.system(phylotyper_script)

        # Clean up fairly large html files that phylotyper makes so we don't compe anywhere near redmine upload size
        # limit.
        os.system('rm {}'.format(os.path.join(output_dir, '*.html')))

        # Prepare upload
        output_list = [
            {
                'filename': 'intimin_predictions.tsv',
                'path': os.path.join(output_dir, 'subtype_predictions.tsv')
            }
        ]

        # Wrap up issue
        redmine_instance.issue.update(resource_id=issue.id, uploads=output_list, status_id=4,
                                      notes='Intimin subtyping complete!')
    except Exception as e:
        sentry_sdk.capture_exception(e)
        redmine_instance.issue.update(resource_id=issue.id, status_id=4,
                                      notes='Something went wrong! We log this automatically and will look into the '
                                            'problem and get back to you with a fix soon.')


def make_executable(path):
    """
    Takes a shell script and makes it executable (chmod +x)
    :param path: path to shell script
    """
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2
    os.chmod(path, mode)


def zip_folder(results_path, output_dir, output_filename):
    output_path = os.path.join(output_dir, output_filename)
    shutil.make_archive(output_path, 'zip', results_path)
    return output_path


if __name__ == '__main__':
    intimin_typer_redmine()
