import os
import glob
import click
import pickle
import subprocess


@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def qiimegraph(redmine_instance, issue, work_dir, description):
    """
    Description should be parsed as follows:

    TAXONOMIC LEVEL
    CUTOFF
    SAMPLE

    Example:

    Family
    None
    SP1
    """
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    try:
        # Download the attached taxonomy_barplot.qzv
        redmine_instance.issue.update(resource_id=issue.id, notes='Initiated QIIMETAXREPORT...')
        attachment = redmine_instance.issue.get(issue.id, include='attachments')
        attachment_id = 0
        for item in attachment.attachments:
            attachment_id = item.id

        # Now download, if attachment id is not 0, which indicates that we didn't find anything attached to the issue.
        if attachment_id != 0:
            attachment = redmine_instance.attachment.get(attachment_id)
            attachment.download(savepath=work_dir, filename='taxonomy_barplot.qzv')
            attachment_file = os.path.join(work_dir, 'taxonomy_barplot.qzv')
        else:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='ERROR: Did not find any attached files. Please create a new issue with'
                                                ' a QIIME2 taxonomy_barplot.qzv file attached and try again.',
                                          status_id=4)
            return

        # DESCRIPTION PARSING
        # Taxonomy
        taxonomic_level = description[0].lower().strip()

        # Acceptable taxonomic levels
        check_taxonomy_list = ['kingdom',
                               'phylum',
                               'class',
                               'order',
                               'family',
                               'genus',
                               'species']
        if taxonomic_level not in check_taxonomy_list:
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Invalid taxonomic level provided!'
                                                'Please ensure the first line of your '
                                                'Redmine description specifies one of the following taxonomic levels:\n'
                                                '{}'.format(check_taxonomy_list))
            quit()

        # Filtering
        cutoff = description[1]
        cutoff = None if cutoff.lower().strip() == 'none' else float(cutoff)

        # Samples
        sample = description[2]

        cmd = '/mnt/nas/Redmine/QIIME2_CondaEnv/qiime2-2018.2/bin/python ' \
              '/mnt/nas/Redmine/OLCRedmineAutomator/automators_dev/qiimetaxreport_generate_report.py ' \
              '-i {} ' \
              '-o {} ' \
              '-s {} ' \
              '-t {}'.format(attachment_file, work_dir, sample, taxonomic_level)
        if cutoff is not None:
            cmd += ' -c {}'.format(cutoff)
        p = subprocess.Popen(cmd, shell=True)
        p.wait()

        # Zip up the ouput
        output_file = None
        try:
            output_file = glob.glob(os.path.join(work_dir, 'taxonomy_report*.csv'))[0]
        except IndexError:
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Something went wrong. Please verify the provided '
                                                'sample annotation is correct.')
            quit()

        # Output list containing dictionaries with file path as the key for upload to Redmine
        output_list = [
            {
                'path': os.path.join(work_dir, output_file),
                'filename': os.path.basename(output_file)
            }
        ]

        redmine_instance.issue.update(resource_id=issue.id, uploads=output_list, status_id=4,
                                      notes='QIIMETAXREPORT Complete! Output report attached.')
    except Exception as e:
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Something went wrong! Send this error traceback to your friendly '
                                            'neighborhood bioinformatician: {}'.format(e))


if __name__ == '__main__':
    qiimegraph()
