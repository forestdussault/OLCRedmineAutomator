import os
import glob
import click
import pickle
import zipfile
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
    FILTERING CRITERIA
    SAMPLES
    SAMPLES...

    Example:

    Family
    None
    SP1,SP2,SP3
    SP4,SP5,SP6
    """
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))


    try:
        # Download the attached taxonomy_barplot.qzv
        redmine_instance.issue.update(resource_id=issue.id, notes='Initiated QIIMEGRAPH...')
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
        filtering = description[1].capitalize()
        filtering = None if filtering.lower().strip() == 'none' else filtering

        # Samples
        sample_list = list()
        for item in description[2:]:
            item = item.strip().replace(' ', '')
            if item != '':
                sample_list.append(item)
        sample_list = tuple(sample_list)

        for samples in sample_list:
            cmd = '/mnt/nas/Redmine/QIIME2_CondaEnv/qiime2-2018.2/bin/python ' \
                  '/mnt/nas/Redmine/OLCRedmineAutomator/automators/qiimegraph_generate_chart.py ' \
                  '-i {} ' \
                  '-o {} ' \
                  '-s {} ' \
                  '-t {}'.format(attachment_file, work_dir, samples, taxonomic_level)
            if filtering is not None:
                cmd += ' -f {}'.format(filtering)
            p = subprocess.Popen(cmd, shell=True)
            p.wait()

        # Zip up all of the ouput
        output_files = glob.glob(os.path.join(work_dir, '*.png'))
        zipped = zipfile.ZipFile(os.path.join(work_dir, 'qiime2_graphs.zip'), 'w')
        for file in output_files:
            zipped.write(file, arcname=os.path.basename(file), compress_type=zipfile.ZIP_DEFLATED)
        zipped.close()

        # Glob zip
        output_zip = glob.glob(os.path.join(work_dir, '*.zip'))[0]

        # Output list containing dictionaries with file path as the key for upload to Redmine
        output_list = [
            {
                'path': os.path.join(work_dir, output_zip),
                'filename': os.path.basename(output_zip)
            }
        ]

        redmine_instance.issue.update(resource_id=issue.id, uploads=output_list, status_id=4,
                                      notes='QIIMEGRAPH Complete! Output graphs attached.')
    except Exception as e:
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Something went wrong! Send this error traceback to your friendly '
                                            'neighborhood bioinformatician: {}'.format(e))


if __name__ == '__main__':
    qiimegraph()
