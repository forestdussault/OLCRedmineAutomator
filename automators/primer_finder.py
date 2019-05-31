import os
import glob
import click
import pickle
import shutil
import zipfile
import sentry_sdk
from automator_settings import SENTRY_DSN
from nastools.nastools import retrieve_nas_files


@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def primer_finder_redmine(redmine_instance, issue, work_dir, description):
    sentry_sdk.init(SENTRY_DSN)
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))
    # Programs supported by the automator
    programs = [
        'legacy', 'supremacy'
    ]
    # Current list of analysis types that the primer finder can support
    analyses = [
        'vtyper', 'custom'
    ]
    # List of supported file formats
    formats = [
        'fasta', 'fastq'
    ]
    # Acceptable number of mismatches
    mismatches = [
        0, 1, 2, 3
    ]
    # Variable to hold supplied arguments
    argument_dict = {
        'program': str(),
        'analysis': str(),
        'mismatches': 2,
        'kmersize': '55,77,99,127',
        'format': 'fasta',
        'exportamplicons': False,
    }
    try:
        # Parse description to figure out what SEQIDs we need to run on.
        seqids = list()
        for item in description:
            item = item.upper().rstrip()
            if 'PROGRAM' in item:
                argument_dict['program'] = item.split('=')[1].lower()
                continue
            if 'ANALYSIS' in item:
                argument_dict['analysis'] = item.split('=')[1].lower()
                continue
            if 'MISMATCHES' in item:
                argument_dict['mismatches'] = int(item.split('=')[1].lower())
                continue
            if 'KMERSIZE' in item:
                argument_dict['kmersize'] = item.split('=')[1].lower()
                continue
            if 'FORMAT' in item:
                argument_dict['format'] = item.split('=')[1].lower()
                continue
            if 'EXPORTAMPLICONS' in item:
                argument_dict['exportamplicons'] = True
                continue
            # Otherwise the item should be a SEQID
            seqids.append(item)
        # Ensure that the analysis type is provided
        if not argument_dict['program']:
            redmine_instance.issue \
                .update(resource_id=issue.id,
                        notes='WARNING: No program type provided. Please ensure that issue contains '
                              '"program=requested_program", where requested_program is one of the '
                              'following keywords: {pts}. Please see the the usage guide: '
                              'https://olc-bioinformatics.github.io/redmine-docs/analysis/primerfinder/ '
                              'for additional details'.format(pts=','.join(programs)))
            return
        if argument_dict['program'] not in programs:
            redmine_instance.issue \
                .update(resource_id=issue.id,
                        notes='WARNING: Requested program type: {pt} not in list of supported analyses: {pts}. Please '
                              'see https://olc-bioinformatics.github.io/redmine-docs/analysis/primerfinder/ '
                              'for additional details'.format(pt=argument_dict['program'],
                                                              pts=','.join(programs)))
            return
        # Custom analyses must have an attached FASTA file of primer sequences
        if argument_dict['analysis'] == 'custom':
            # Download the attached FASTA file.
            # First, get the attachment id - this seems like a kind of hacky way to do this, but I have yet to figure
            # out a better way to do it.
            attachment = redmine_instance.issue.get(issue.id, include='attachments')
            attachment_id = 0
            for item in attachment.attachments:
                attachment_id = item.id
            # Set the name of and create the folder to store the targets
            target_file = os.path.join(work_dir, 'primers.txt')
            # Download if attachment id is not 0, which indicates that we didn't find anything attached to the issue.
            if attachment_id != 0:
                attachment = redmine_instance.attachment.get(attachment_id)
                attachment.download(savepath=work_dir, filename='primers.txt')
            else:
                redmine_instance.issue.update(resource_id=issue.id,
                                              notes='ERROR: Analysis type custom requires an attached FASTA file of '
                                                    'targets. The automator could not find any attached files. '
                                                    'Please create a new issue with the FASTA file attached and try '
                                                    'again.',
                                              status_id=4)
                return
        # Use the v-typer primer set included in the package for V-typer analyses
        else:
            target_file = \
                '/mnt/nas2/virtual_environments/in_silico_pcr/lib/python3.6/site-packages/spadespipeline/primers.txt'
        # Ensure that the analysis type is provided
        if not argument_dict['analysis']:
            redmine_instance.issue\
                .update(resource_id=issue.id,
                        notes='WARNING: No analysis type provided. Please ensure that issue contains '
                              '"analysistype=requested_analysis_type", where requested_analysis_type is one of the '
                              'following keywords: {ats}. Please see the the usage guide: '
                              'https://olc-bioinformatics.github.io/redmine-docs/analysis/primerfinder/ '
                              'for additional details'.format(ats=','.join(analyses)))
            return
        # Ensure that the supplied analysis type is valid
        if argument_dict['analysis'] not in analyses:
            redmine_instance.issue\
                .update(resource_id=issue.id,
                        notes='WARNING: Requested analysis type: {at} not in list of supported analyses: {ats}. Please '
                              'see https://olc-bioinformatics.github.io/redmine-docs/analysis/primerfinder/ '
                              'for additional details'.format(at=argument_dict['analysis'],
                                                              ats=','.join(analyses)))
            return
        # Make sure that the supplied file format is valid
        if argument_dict['format'] not in formats:
            redmine_instance.issue\
                .update(resource_id=issue.id,
                        notes='WARNING: Requested file format {ft} not in list of supported formats: {fts}. Please '
                              'see https://olc-bioinformatics.github.io/redmine-docs/analysis/primerfinder/ '
                              'for additional details'.format(ft=argument_dict['format'],
                                                              fts=','.join(formats)))
            return
        # Ensure that the number of mismatches is acceptable
        if argument_dict['mismatches'] not in mismatches:
            redmine_instance.issue\
                .update(resource_id=issue.id,
                        notes='WARNING: Requested number of mismatches, {nm}, is not in the acceptable range of allowed'
                              ' mismatches: {nms}. Please '
                              'see https://olc-bioinformatics.github.io/redmine-docs/analysis/primerfinder/ '
                              'for additional details'.format(nm=argument_dict['mismatches'],
                                                              nms=','.join([str(num) for num in mismatches])))
            return
        # Ensure that SEQIDs were included
        if not seqids:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='WARNING: No SEQIDs provided!')
        # Run file linker and then make sure that all files requested are present. Warn user if they
        # requested things that we don't have.
        retrieve_nas_files(seqids=seqids,
                           outdir=work_dir,
                           filetype=argument_dict['format'] if argument_dict['program'] == 'supremacy' else 'fasta',
                           copyflag=False)
        missing_files = verify_sequence_files_present(seqid_list=seqids,
                                                      seq_dir=work_dir,
                                                      file_type=argument_dict['format'])
        # Update the Redmine issue if one or more of the requested SEQIDs could not be located
        if missing_files:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='WARNING: Could not find the following requested SEQIDs on'
                                                ' the OLC NAS: {mf}'.format(mf=missing_files))
        # These unfortunate hard coded paths appear to be necessary
        activate = 'source /home/ubuntu/miniconda3/bin/activate /mnt/nas2/virtual_environments/in_silico_pcr'
        # Call the appropriate script depending on the requested program
        if argument_dict['program'] == 'legacy':
            # Run legacy primer locator with the necessary arguments
            primer_cmd = 'python -m spadespipeline.legacy_vtyper -s {seqpath} -m {mismatches}'\
                .format(seqpath=work_dir,
                        mismatches=argument_dict['mismatches'])
            # Add additional flags as required
            if argument_dict['analysis'] == 'custom':
                primer_cmd += ' -a {at}'.format(at=argument_dict['analysis'])
                primer_cmd += ' -pf {primer_file}'.format(primer_file=target_file)
            primer_cmd += ' -e' if argument_dict['exportamplicons'] else ''
        else:
            # Run legacy primer locator with the necessary arguments
            primer_cmd = 'python -m spadespipeline.primer_finder_bbduk -p {seqpath} -s {seqpath} -m {mismatches} ' \
                         '-k {kmerlength} -pf {primer_file}' \
                .format(seqpath=work_dir,
                        mismatches=argument_dict['mismatches'],
                        kmerlength=argument_dict['kmersize'],
                        primer_file=target_file
                        )
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Primer finder command:\n {cmd}'.format(cmd=primer_cmd))
        # Create another shell script to execute within the conda environment
        template = "#!/bin/bash\n{} && {}".format(activate, primer_cmd)
        primer_finder_script = os.path.join(work_dir, 'run_primer_finder.sh')
        with open(primer_finder_script, 'w+') as file:
            file.write(template)
        # Modify the permissions of the script to allow it to be run on the node
        make_executable(primer_finder_script)
        # Run shell script
        os.system(primer_finder_script)

        # Zip output
        output_filename = 'primer_finder_output'
        zip_filepath = zip_folder(output_dir=work_dir,
                                  output_filename=output_filename,
                                  program=argument_dict['program'])
        zip_filepath += '.zip'
        # Prepare upload
        output_list = [
            {
                'filename': os.path.basename(zip_filepath),
                'path': zip_filepath
            }
        ]
        # Create a list of all the folders - will be used to clean up the working directory
        folders = glob.glob(os.path.join(work_dir, '*/'))
        # Remove all the folders
        for folder in folders:
            if os.path.isdir(folder):
                shutil.rmtree(folder)
        # Wrap up issue
        redmine_instance.issue.update(resource_id=issue.id,
                                      uploads=output_list,
                                      status_id=4,
                                      notes='{at} analysis with primer finder complete!'
                                      .format(at=argument_dict['analysis'].lower()))
    except Exception as e:
        sentry_sdk.capture_exception(e)
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Something went wrong! We log this automatically and will look into the '
                                            'problem and get back to you with a fix soon.')


def verify_sequence_files_present(seqid_list, seq_dir, file_type):
    """
    Makes sure that FASTQ files specified in seqid_list have been successfully copied/linked to directory specified
    by fastq_dir.
    :param seqid_list: List with SEQIDs.
    :param seq_dir: Directory that sequence files
    :param file_type: File format used in the analyses. Options are fastq or fasta
    :return: List of SEQIDs that did not have files associated with them.
    """
    missing_files = list()
    if file_type == 'fastq':
        for seqid in seqid_list:
            # Check forward.
            if len(glob.glob(seq_dir + '/' + seqid + '*R1*fastq*')) == 0:
                missing_files.append(seqid)
            # Check reverse. Only add to list of missing if forward wasn't present.
            if len(glob.glob(seq_dir + '/' + seqid + '*R2*fastq*')) == 0 and seqid not in missing_files:
                missing_files.append(seqid)
    else:
        for seqid in seqid_list:
            # Check FASTA.
            if len(glob.glob(seq_dir + '/' + seqid + '*fasta')) == 0:
                missing_files.append(seqid)
    return missing_files


def make_executable(path):
    """
    Takes a shell script and makes it executable (chmod +x)
    :param path: path to shell script
    """
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2
    os.chmod(path, mode)


def zip_folder(output_dir, output_filename, program):
    """
    Compress a folder
    :param output_dir: The output directory
    :param output_filename: The output file name
    :param program: The program being used to analyse the files. Options are legacy and supremacy
    :return: Absolute path to the zip file minus the .zip extension
    """
    if program == 'legacy':
        folder_base = ['reports']
    else:
        folder_base = ['consolidated_report', 'detailed_reports']
    output_base = os.path.join(output_dir, output_filename)
    with zipfile.ZipFile(output_base + '.zip', 'w', zipfile.ZIP_DEFLATED) as zippo:
        for folder in folder_base:
            to_zip = os.path.join(output_dir, folder)
            for root, dirs, files in os.walk(to_zip):
                for filename in files:
                    dir_name = os.path.split(root)[-1]
                    # Don't upload the FASTA assemblies in the directory
                    if not filename.endswith('.fasta'):
                        zippo.write(os.path.join(root, filename), arcname=os.path.join(dir_name, filename))

    return output_base


if __name__ == '__main__':
    primer_finder_redmine()
