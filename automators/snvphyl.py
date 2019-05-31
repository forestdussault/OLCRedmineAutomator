import os
import glob
import click
import pickle
import shutil
import sentry_sdk
from automator_settings import SENTRY_DSN
from biotools import mash
from nastools.nastools import retrieve_nas_files


@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def snvphyl_redmine(redmine_instance, issue, work_dir, description):
    sentry_sdk.init(SENTRY_DSN)
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    try:
        query_list = list()
        reference = list()
        compare = False
        # Go through description to figure out what our query is and what the reference is.
        for item in description:
            item = item.upper()
            if item == '':
                continue
            if 'COMPARE' in item:
                compare = True
                continue
            if compare:
                query_list.append(item)
            else:
                if 'REFERENCE' not in item:
                    reference.append(item)

        # Retrieve our reference file. Error user if they selected anything but one reference and don't continue.
        if len(reference) != 1:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='ERROR: You must specify one reference strain, and you '
                                                'specified {} reference strains. Please create a new'
                                                ' issue and try again.'.format(len(reference)), status_id=4)
            return

        if reference[0].upper() != 'ATTACHED':
            # Extract our reference file to our working directory.
            retrieve_nas_files(seqids=reference,
                               outdir=work_dir,
                               filetype='fasta',
                               copyflag=True)
            # Check that the file was successfully extracted. If it wasn't boot the user.
            if len(glob.glob(os.path.join(work_dir, '*fasta'))) == 0:
                redmine_instance.issue.update(resource_id=issue.id,
                                              notes='ERROR: Could not find the specified reference file.'
                                                    ' Please verify it is a correct SEQID, create a new '
                                                    'issue, and try again.', status_id=4)
                return

        # If user specified attachment as the reference file, download it to our working directory.
        else:
            # Get the attachment ID, and download if it isn't equal to zero (meaning no attachment, so boot user with
            # appropriate error message)
            attachment = redmine_instance.issue.get(issue.id, include='attachments')
            attachment_id = 0
            for item in attachment.attachments:
                attachment_id = item.id

            # Download if we found an attachment, and use as our reference. Otherwise, exit and tell user to try again
            if attachment_id != 0:
                attachment = redmine_instance.attachment.get(attachment_id)
                attachment.download(savepath=work_dir, filename='reference.fasta')
            else:
                redmine_instance.issue.update(resource_id=issue.id,
                                              notes='ERROR: You specified that the reference would be in attached file,'
                                                    ' but no attached file was found. Please create a new issue and '
                                                    'try again.',
                                              status_id=4)
                return

        # Now extract our query files.
        retrieve_nas_files(seqids=query_list,
                           outdir=os.path.join(work_dir, 'fastqs'),
                           filetype='fastq',
                           copyflag=True)

        # With our query files extracted, verify that all the SEQIDs the user specified were able to be found.
        missing_fastqs = verify_fastqs_present(query_list, os.path.join(work_dir, 'fastqs'))
        if len(missing_fastqs) > 0:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='Warning! Could not find the following requested query SEQIDs: '
                                                '{}. \nYou may want to verify the SEQIDs, create a new issue, and try'
                                                ' again.'.format(str(missing_fastqs)))

        # Now check that the FASTQ files aren't too far away from the specified reference file.
        bad_fastqs = check_distances(ref_fasta=glob.glob(os.path.join(work_dir, '*fasta'))[0],
                                     fastq_folder=os.path.join(work_dir, 'fastqs'),
                                     work_dir=work_dir)
        if len(bad_fastqs) > 0:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='Warning! The following SEQIDs were found to be fairly'
                                                ' divergent from the reference file specified:{} \nYou may'
                                                ' want to start a new SNVPhyl issue without them and try '
                                                'again.'.format(str(bad_fastqs)))

        # With everything checked, time to actually run the SNVPhyl. Need to call a snvphyl-specific virtualenv.
        cmd = '/mnt/nas/Virtual_Environments/snvphylcli/bin/python /mnt/nas/slurmtest/snvphyl-galaxy-cli/bin/snvphyl.py' \
              ' --deploy-docker --fastq-dir {fastq_dir} ' \
              '--reference-file {ref_file} --min-coverage 5 --output-dir {output} ' \
              '--docker-port {port}'.format(fastq_dir=os.path.join(work_dir, 'fastqs'),
                                            ref_file=glob.glob(os.path.join(work_dir, '*fasta'))[0],
                                            output=os.path.join(work_dir, 'output'),
                                            port=issue.id)
        os.system(cmd)

        # Now need to create a zip archive of the results file, upload it, and clean up the fastq files.
        shutil.make_archive(os.path.join(work_dir, 'SNVPhyl_' + str(issue.id)), 'zip', os.path.join(work_dir, 'output'))
        output_list = list()
        output_dict = dict()
        output_dict['path'] = os.path.join(work_dir, 'SNVPhyl_' + str(issue.id) + '.zip')
        output_dict['filename'] = 'SNVPhyl_' + str(issue.id) + '.zip'
        output_list.append(output_dict)
        redmine_instance.issue.update(resource_id=issue.id, uploads=output_list, status_id=4,
                                      notes='SNVPhyl process complete!')

        shutil.rmtree(os.path.join(work_dir, 'fastqs'))
    except Exception as e:
        sentry_sdk.capture_exception(e)
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Something went wrong! We log this automatically and will look into the '
                                            'problem and get back to you with a fix soon.')


def check_distances(ref_fasta, fastq_folder, work_dir):
    bad_fastqs = list()
    # fastqs = glob.glob(os.path.join(fastq_folder, '*R1*'))
    mash.sketch(os.path.join(fastq_folder, '*R1*'), output_sketch=os.path.join(work_dir, 'sketch.msh'), threads=5)
    mash.dist(os.path.join(work_dir, 'sketch.msh'), ref_fasta, threads=48,
              output_file=os.path.join(work_dir, 'distances.tab'))
    mash_output = mash.read_mash_output(os.path.join(work_dir, 'distances.tab'))
    for item in mash_output:
        print(item.reference, item.query, str(item.distance))
        if item.distance > 0.15:  # Moved value from 0.06 to 0.15 - was definitely too conservative before.
            bad_fastqs.append(item.reference)
    return bad_fastqs


def verify_fastqs_present(query_list, fastq_folder):
    missing_fastqs = list()
    for query in query_list:
        # Check that forward reads are present.
        if len(glob.glob(os.path.join(fastq_folder, query + '*R1*fastq*'))) == 0:
            missing_fastqs.append(query)
        # Check that reverse reads are present, and add to list if forward reads weren't missing
        if len(glob.glob(os.path.join(fastq_folder, query + '*R2*fastq*'))) == 0 and query not in missing_fastqs:
            missing_fastqs.append(query)
    # Returns list of SEQIDs for which we couldn't find forward and/or reverse reads
    return missing_fastqs


if __name__ == '__main__':
    snvphyl_redmine()
