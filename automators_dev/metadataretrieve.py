import os
import glob
import click
import csv
import ftplib
import pickle
import shutil
from automator_settings import FTP_USERNAME, FTP_PASSWORD


@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def metadataretrieve_redmine(redmine_instance, issue, work_dir, description):
    print('Metadata retrieving!')
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    try:
        os.makedirs(os.path.join(work_dir, str(issue.id)))
        # Parse description to figure out what SEQIDs we need to retrieve metadata for.
        seqid_list = list()
        for item in description:
            item = item.upper()
            seqid_list.append(item)

        # Now we have a list of SEQIDs - need to open up a metadatasheet with correct headers in biorequest dir.
        with open(os.path.join(work_dir, 'combinedMetadata.csv'), 'w') as f:
            f.write('SampleName,N50,NumContigs,TotalLength,MeanInsertSize,AverageCoverageDepth,'
                    'ReferenceGenome,RefGenomeAlleleMatches,16sPhylogeny,rMLSTsequenceType,'
                    'MLSTsequencetype,MLSTmatches,coregenome,Serotype,geneSeekrProfile,vtyperProfile,'
                    'percentGC,TotalPredictedGenes,predictedgenesover3000bp,predictedgenesover1000bp,'
                    'predictedgenesover500bp,predictedgenesunder500bp,SequencingDate,Investigator,'
                    'TotalClustersinRun,NumberofClustersPF,PercentOfClusters,LengthofForwardRead,'
                    'LengthofReverseRead,Project,PipelineVersion\n')

        # Now for the fun part - we need to go through all of the combinedMetadata sheets we have and pull relevant
        # data for upload.
        metadata_sheets = glob.glob('/mnt/nas/WGSspades/*/reports/combinedMetadata.csv')
        metadata_sheets += glob.glob('/mnt/nas/External_WGSspades/*/*/reports/combinedMetadata.csv')

        for metadata_sheet in metadata_sheets:
            with open(metadata_sheet) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if row['SampleName'] in seqid_list:
                        with open(os.path.join(work_dir, 'combinedMetadata.csv'), 'a+') as f:
                            f.write(row)

    except Exception as e:
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Something went wrong! Send this error traceback to your friendly '
                                            'neighborhood bioinformatician: {}'.format(e))


if __name__ == '__main__':
    metadataretrieve_redmine()
