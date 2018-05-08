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
    Sequence Run
    TAXONOMIC LEVEL
    CUTOFF
    SAMPLE,SAMPLE,SAMPLE,etc...

    Example:
    180501_M05722
    Family
    None
    SP1,SP2,SP3
    """
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    try:
        # First line of description should specify output folder from qiime run.
        qiime_output_folder = description[0].upper().strip()
        # Verify that the qiime taxonomy barplot can be found for specified qiime output folder.
        qiime_taxonomy_barplot = os.path.join('/mnt/nas2/processed_sequence_data/miseq_assemblies',
                                              qiime_output_folder, 'qiime2', 'taxonomy_barplot.qzv')
        if not os.path.isfile(qiime_taxonomy_barplot):
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Could not find taxonomy_barplot.qvz for specified run.'
                                                ' Run specified was {}'.format(qiime_output_folder))

        # DESCRIPTION PARSING
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
        cutoff = description[2]
        cutoff = None if cutoff.lower().strip() == 'none' else float(cutoff)

        # Samples
        samples = description[3]
        samples = samples.split(',')

        for sample in samples:
            cmd = '/mnt/nas/Redmine/QIIME2_CondaEnv/qiime2-2018.2/bin/python ' \
                  '/mnt/nas/Redmine/OLCRedmineAutomator/automators/qiimetaxreport_generate_report.py ' \
                  '-i {} ' \
                  '-o {} ' \
                  '-s {} ' \
                  '-t {}'.format(qiime_taxonomy_barplot, work_dir, sample, taxonomic_level)
            if cutoff is not None:
                cmd += ' -c {}'.format(cutoff)
            p = subprocess.Popen(cmd, shell=True)
            p.wait()

        # Zip up the ouput
        output_files = None
        try:
            output_files = glob.glob(os.path.join(work_dir, 'taxonomy_report*.csv'))
        except IndexError:
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Something went wrong. Please verify the provided '
                                                'sample annotation is correct.')
            quit()

        # Output list containing dictionaries with file path as the key for upload to Redmine
        output_list = []
        for f in output_files:
            tmp = {'path': os.path.join(work_dir, f), 'filename': os.path.basename(f)}
            output_list.append(tmp)

        redmine_instance.issue.update(resource_id=issue.id, uploads=output_list, status_id=4,
                                      notes='QIIMETAXREPORT Complete! Output report attached.')

    except Exception as e:
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Something went wrong! Send this error traceback to your friendly '
                                            'neighborhood bioinformatician: {}'.format(e))


if __name__ == '__main__':
    qiimegraph()
