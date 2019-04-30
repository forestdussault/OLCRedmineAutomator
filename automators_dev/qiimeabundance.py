import os
import csv
import glob
import click
import pickle
import zipfile
import pandas as pd


@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def qiimeabundance_redmine(redmine_instance, issue, work_dir, description):
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    """
    Description is expected to be in the following format.
    level=taxlevel
    taxonomy<percent  # Allow searching on more than one column header by adding more lines
    
    So an example would be:
    level=5
    Escherichia<10
    Salmonella>5
    """

    # Parse description. TODO
    level = description[0].split('=')[1]
    taxa_operations = dict()
    for i in range(1, len(description)):
        if '<' in description[i]:
            taxa = description[i].split('<')[0].upper()
            percentage = float(description[i].split('<')[1])
            operation = '<'
            taxa_operations[taxa] = (operation, percentage)
        elif '>' in description[i]:
            taxa = description[i].split('>')[0].upper()
            percentage = float(description[i].split('>')[1])
            operation = '>'
            taxa_operations[taxa] = (operation, percentage)

    tax_barplots = glob.glob('/mnt/nas2/processed_sequence_data/miseq_assemblies/*/qiime2/taxonomy_barplot.qzv')
    # As it turns out, qiime2 qzv files are actually just zip files with a bunch of data/metadata.
    # https://github.com/joey711/phyloseq/issues/830
    # Getting at data seems to be easiest if we just unzip and read in the relevant csv files with pandas
    for tax_barplot in tax_barplots:
        # Check the date of the assembly to see if it's in our range.
        cmd = 'cp {} {}'.format(tax_barplot, work_dir)
        os.system(cmd)
        output_dir = os.path.join(work_dir, tax_barplot.split('/')[-3])
        with zipfile.ZipFile(os.path.join(work_dir, 'taxonomy_barplot.qzv'), 'r') as zipomatic:
            zipomatic.extractall(output_dir)

    # Now we should have folders for all of our QIIME2 runs.
    # Grab the csv file for level of interest for all of them and create a combined CSV.
    dataframe_list = list()
    csv_files = glob.glob(os.path.join(work_dir, '*', '*', 'data', 'level-{}.csv'.format(level)))
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        dataframe_list.append(df)
    combined_df = pd.concat(dataframe_list, ignore_index=True, sort=False)
    combined_df.fillna(0, inplace=True)
    combined_df.to_csv(os.path.join(work_dir, 'all_results.csv'), index=False)

    # Now parse our combined csv to find abundances specified.
    with open(os.path.join(work_dir, 'all_results.csv')) as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)
    # Have to reopen file to refresh the reader.
    total_read_dict = dict()
    with open(os.path.join(work_dir, 'all_results.csv')) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # First, sum the number of reads assigned to each genus/family/order/whatever.
            total_reads = 0
            for header in headers:
                if str(header).startswith('D_0_'):
                    total_reads += int(row[header].split('.')[0])
            total_read_dict[row['index']] = total_reads

    indices_to_print = list()
    # Have to reopen file to refresh the reader. Now we figure out
    with open(os.path.join(work_dir, 'all_results.csv')) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            print(row['index'])
            row_is_good = True
            for header in headers:
                taxa = header.split('_')[-1].upper()
                if taxa in taxa_operations:
                    try:
                        proportion_of_total = 100.0 * float(row[header])/total_read_dict[row['index']]
                    except ZeroDivisionError:
                        proportion_of_total = 0
                    print('{},{}'.format(header, proportion_of_total))
                    if taxa_operations[taxa][0] == '>':
                        if proportion_of_total <= taxa_operations[taxa][1]:
                            row_is_good = False
                    elif taxa_operations[taxa][1] == '<':
                        if proportion_of_total >= taxa_operations[taxa][1]:
                            row_is_good = False
            if row_is_good:
                indices_to_print.append(row['index'])

    with open(os.path.join(work_dir, 'all_results.csv')) as infile:
        with open(os.path.join(work_dir, 'qiimeabundance_results.csv'), 'w') as outfile:
            for line in infile:
                if line.split(',')[0] == 'index' or line.split(',')[0] in indices_to_print:
                    outfile.write(line)

    output_list = list()
    output_dict = dict()
    output_dict['path'] = os.path.join(work_dir, 'qiimeabundance_results.csv')
    output_dict['filename'] = 'QIIMEabundance_results.csv'
    output_list.append(output_dict)
    # Clean up files.
    cmd = 'rm -r {}'.format(os.path.join(work_dir, '*/'))
    os.system(cmd)

    # Upload files, set status to Feedback
    redmine_instance.issue.update(resource_id=issue.id,
                                  status_id=4,
                                  uploads=output_list,
                                  notes='QIIMEAbundance complete. See attached file for results.')


if __name__ == '__main__':
    qiimeabundance_redmine()

