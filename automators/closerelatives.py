import os
import glob
import click
import pickle
from redminelib import Redmine
from biotools import mash
from setup import API_KEY

@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def closerelatives_redmine(redmine_instance, issue, work_dir, description):
    # Unpickle Redmine objects
    # redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))
    redmine_url = 'https://redmine.biodiversity.agr.gc.ca/'
    redmine_instance = Redmine(redmine_url, key=API_KEY, requests={'verify': False})
    try:
        # First line of description should be number of close relatives desired.
        try:
            num_close_relatives = int(description[0])
        except ValueError:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='Error! The first line of the description must be the number'
                                                ' of strains you want to find. The first line of your '
                                                'description was: {}'.format(description[0]),
                                          status_id=4)
            return

        # Second line of description should be the SEQID of what you want to find a close reference for.
        seqid = description[1]

        # Try to extract FASTQ files for the specified SEQID.
        with open(os.path.join(work_dir, 'seqid.txt'), 'w') as f:
            f.write(seqid)

        cmd = 'python2 /mnt/nas/MiSeq_Backup/file_linker.py {seqidlist} {workdir}'.format(seqidlist=os.path.join(work_dir, 'seqid.txt'),
                                                                                          workdir=work_dir)
        os.system(cmd)
        if len(glob.glob(os.path.join(work_dir, '*.fastq.gz'))) != 2:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='Error! Could not find FASTQ files for the specified SEQID. The SEQID'
                                                ' that you specified was: {}'.format(seqid),
                                          status_id=4)
            return

        # Let the user know that it's MASH time.
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Beginning MASHING!',
                                      status_id=2)

        # Run mash dist with the FASTQ file specified against the sketch of all our stuff.
        query_fastq = glob.glob(os.path.join(work_dir, '*R1*'))[0]
        mash.dist(query_fastq, '/mnt/nas/bio_requests/10937/all_sequences.msh',
                  m='3', threads=48, output_file=os.path.join(work_dir, 'distances.tab'))
        mash_results = mash.read_mash_output(os.path.join(work_dir, 'distances.tab'))
        result_dict = dict()
        # Put all the results into a dictionary, where the key is the sequence file and the value is mash distance
        # between query fastq and reference fastq.
        for item in mash_results:
            seq_name = os.path.split(item.query)[-1].split('_')[0]
            result_dict[seq_name] = item.distance

        # Sort the results, store the sorted dictionary keys in a list.
        sorted_distance_results = sorted(result_dict, key=result_dict.get)

        # Prepare a string that lists the top hit SEQIDs to be posted to redmine.
        upload_string = ''
        for i in range(num_close_relatives):
            upload_string = upload_string + sorted_distance_results[i] + '\n'

        # Post the list of closely related SEQIDs to redmine.
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Process complete! Here is the list of the {num_relatives} closest strains '
                                            'to {query_strain}:\n{upload_string}'.format(num_relatives=str(num_close_relatives),
                                                                                         query_strain=seqid,
                                                                                         upload_string=upload_string),
                                      status_id=4)
    except Exception as e:
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Something went wrong! Send this error traceback to your friendly '
                                            'neighborhood bioinformatician: {}'.format(e))

if __name__ == '__main__':
    closerelatives_redmine()
