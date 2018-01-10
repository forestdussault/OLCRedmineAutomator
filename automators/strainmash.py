import os
import re
import glob
import click
import pickle
import pandas as pd
from Bio import Entrez
from biotools import mash


@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def strainmash_redmine(redmine_instance, issue, work_dir, description):
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    # Reference path
    typestrain_db_path = '/mnt/nas/Databases/GenBank/typestrains/typestrains_sketch.msh'

    # Parse description to get list of SeqIDs
    seqids = []
    for i in range(0, len(description)):
        item = description[i]
        item = item.upper()
        seqids.append(item)

    with open(os.path.join(work_dir, 'seqid.txt'), 'w') as f:
        for seqid in seqids:
            f.write(seqid + '\n')

    # Drop FASTA files into workdir
    cmd = 'python2 /mnt/nas/WGSspades/file_extractor.py {0}/seqid.txt {0} /mnt/nas/'.format(work_dir)
    os.system(cmd)

    # Create output directory
    os.mkdir(os.path.join(work_dir, 'output'))

    # Get all of the FASTA files
    fasta_list = glob.glob(os.path.join(work_dir, '*.fasta'))

    # Output list containing dictionaries with file path as the key
    output_list = []

    # Run mash_screen on everything
    for item in fasta_list:
        output_filename = os.path.join(work_dir, 'output', (item.replace('.fasta', '') + '_strainmash.txt'))

        # Setup dictionary for upload to Redmine
        output_dict = {}
        output_dict['path'] = output_filename
        output_dict['filename'] = os.path.basename(output_filename)

        output_list.append(output_dict)
        mash_screen(reference=typestrain_db_path,
                    queryfile=item,
                    outname=output_filename)

    # Upload files, set status to Feedback
    redmine_instance.issue.update(resource_id=issue.id, uploads=output_list, status_id=4)


def mash_screen(reference, queryfile, outname):
    mash.screen(reference, queryfile, output_file=outname, w='')
    mash2pandas(outname)


def mash2pandas(outname):
    # Pandas
    df = pd.read_csv(outname, sep='\t', header=None)
    try:
        df = df.drop(df.columns[[5]], axis=1)
    except:
        pass
    df = df.loc[df[0] >= 0.7]
    df = df.sort_values(df.columns[0], ascending=False)

    # Get rid of path from names in output (artifact of the straindb)
    df[4] = df[4].apply(lambda x: os.path.basename(x))

    # Grab only the accession with some regex
    df[4] = df[4].apply(lambda x: re.findall(r"^\w{3}_\d+\.\d", x)[0])

    # Reset the index
    df.reset_index(drop=True, inplace=True)

    # Take only the top 10 rows to significantly cut down on Entrez retrieval time
    df = df[:10]

    # Map the extract_entrez_data function on each accession
    df['Organism'] = df[4].map(extract_entrez_data)

    # Print out top hit
    tophit_name = df[4].iloc[0]
    tophit_score = df[1].iloc[0]
    print(df)
    print('\nTop Hit: {}\nScore: {}'.format(tophit_name, tophit_score))

    # Header names
    colnames = [
        'MashDistance',
        'NumMatchingHashes',
        'MedianMultiplicity',
        'Pvalue',
        'ReferenceStrain',
        'Organism'
    ]

    # Overwrite mash.screen output with a nice Pandas df
    try:
        df.to_csv(outname, sep='\t', header=colnames, index=False)
    except:
        df.to_csv(outname, sep='\t', header=None, index=False)


def extract_entrez_data(accession):
    """
    :param accession: GenBank assembly accession ID
    :return: organism name stored in NCBI
    """
    Entrez.email = 'forest.dussault@inspection.gc.ca'

    try:
        handle = Entrez.esearch(db='assembly', term=accession)
        record = Entrez.read(handle)
    except:
        pass

    for id in record['IdList']:
        # Get Assembly Summary
        print('\nWorking on {}...'.format(id))
        try:
            esummary_handle = Entrez.esummary(db="assembly", id=id, report="full")
            esummary_record = Entrez.read(esummary_handle)

            # Parse Accession Name from Summary
            accession_id = esummary_record['DocumentSummarySet']['DocumentSummary'][0]['AssemblyAccession']

            # Search Name for ID
            refseq_id = Entrez.read(Entrez.esearch(db="nucleotide", term=accession_id))['IdList'][0]

            # Fetch Record with ID from nucleotide db
            seq_record = Entrez.efetch(db="nucleotide", id=refseq_id, retmode='xml')

            record = Entrez.read(seq_record)
            organism = record[0]['GBSeq_organism']
        except:
            print('Failed')
            return None

        # Might want to implement these later
        primary_accession = record[0]['GBSeq_primary-accession']
        taxonomy = record[0]['GBSeq_taxonomy']

        print(organism)
        return organism

if __name__ == '__main__':
    strainmash_redmine()