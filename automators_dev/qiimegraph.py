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

    Qiime output folder
    TAXONOMIC LEVEL
    FILTERING CRITERIA
    SAMPLES
    SAMPLES...

    Example:

    180501_M05722
    None
    Family
    SP1,SP2,SP3
    SP4,SP5,SP6
    """
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    try:
        # DESCRIPTION PARSING
        # First line of description should specify output folder from qiime run.
        qiime_output_folder = description[0].upper().strip()
        # Verify that the qiime taxonomy barplot can be found for specified qiime output folder.
        qiime_taxonomy_barplot = os.path.join('/mnt/nas2/processed_sequence_data/miseq_assemblies',
                                              qiime_output_folder, 'qiime2', 'taxonomy_barplot.qzv')
        if not os.path.isfile(qiime_taxonomy_barplot):
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Could not find taxonomy_barplot.qvz for specified run.'
                                                ' Run specified was {}'.format(qiime_output_folder))

        # Taxonomy
        taxonomic_level = description[1].lower().strip()

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
        filtering = description[2].capitalize()
        filtering = None if filtering.lower().strip() == 'none' else filtering

        # Samples
        sample_list = list()
        for item in description[3:]:
            item = item.strip().replace(' ', '')
            if item != '':
                sample_list.append(item)
        sample_list = tuple(sample_list)

        for samples in sample_list:
            cmd = '/mnt/nas/Redmine/QIIME2_CondaEnv/qiime2-2018.2/bin/python ' \
                  '/mnt/nas/Redmine/OLCRedmineAutomator/automators_dev/qiimegraph_generate_chart.py ' \
                  '-i {} ' \
                  '-o {} ' \
                  '-s {} ' \
                  '-t {}'.format(qiime_taxonomy_barplot, work_dir, samples, taxonomic_level)
            if filtering is not None:
                cmd += ' -f {}'.format(filtering)
            p = subprocess.Popen(cmd, shell=True)
            p.wait()

        # Zip up all of the ouput
        output_files = glob.glob(os.path.join(work_dir, '*.png'))
        if len(output_files) == 0:
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Something went wrong. Please verify the provided Sample IDs are'
                                                'correct.')
            quit()

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
