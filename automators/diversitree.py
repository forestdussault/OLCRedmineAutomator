import os
import glob
import click
import pickle
from biotools import mash


@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def diversitree_redmine(redmine_instance, issue, work_dir, description):
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    # Check that the first line of the request is a number. If it isn't, tell author they goofed and give up.
    try:
        desired_num_strains = int(description[0])
    except ValueError:
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Error! The first line of your request must be the number of'
                                            ' strains you want picked from the tree.',
                                      status_id=4)
        return

    # Parse description for SEQIDs, write list that file_extractor needs.
    seqids = list()
    for i in range(1, len(description)):
        item = description[i].upper()
        seqids.append(item)

    with open(os.path.join(work_dir, 'seqid.txt'), 'w') as f:
        for seqid in seqids:
            f.write(seqid + '\n')

    # Drop FASTA files into workdir
    cmd = 'python2 /mnt/nas/WGSspades/file_extractor.py {0}/seqid.txt {0} /mnt/nas/'.format(work_dir)
    os.system(cmd)

    # Run a mash to figure out if any strains are particularly far apart and likely to make PARSNP fail.
    reference_file = glob.glob(os.path.join(work_dir, '*.fasta'))[0]
    bad_fastas = check_distances(reference_file, work_dir)
    if bad_fastas:
        outstr = ''
        for fasta in bad_fastas:
            fasta = os.path.split(fasta)[-1]
            outstr += fasta + '\n'
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='Warning! MASH screening thinks that the following samples may be too'
                                                ' far from the reference: {samples}\nIn this case, the refence file'
                                                ' was {reference}. You may want to create a new issue and '
                                                'try again.'.format(samples=outstr,
                                                                    reference=os.path.split(reference_file)[-1]))

    cmd = 'python /mnt/nas/Redmine/OLCRedmineAutomator/automators/sampler.py -i {work_dir} ' \
          '-d {desired_tips} -o {output}'.format(work_dir=work_dir,
                                                 desired_tips=desired_num_strains,
                                                 output=os.path.join(work_dir, 'output'))
    os.system(cmd)
    output_list = list()
    output_dict = dict()
    output_dict['path'] = os.path.join(work_dir, 'output', 'strains.txt')
    output_dict['filename'] = 'strains.txt'
    output_list.append(output_dict)
    output_dict = dict()
    output_dict['path'] = os.path.join(work_dir, 'output', 'parsnp.tree')
    output_dict['filename'] = 'tree.nwk'
    output_list.append(output_dict)
    redmine_instance.issue.update(resource_id=issue.id, uploads=output_list, status_id=4,
                                  notes='DiversiTree process complete!')
    os.system('rm {fasta_files}'.format(fasta_files=os.path.join(work_dir, '*fasta')))


def check_distances(ref_fasta, fasta_folder):
    bad_fastqs = list()
    # fastqs = glob.glob(os.path.join(fastq_folder, '*R1*'))
    mash.sketch(os.path.join(fasta_folder, '*.fasta'), output_sketch=os.path.join(fasta_folder, 'sketch.msh'), threads=56)
    mash.dist(os.path.join(fasta_folder, 'sketch.msh'), ref_fasta, threads=56, output_file=os.path.join(fasta_folder, 'distances.tab'))
    mash_output = mash.read_mash_output(os.path.join(fasta_folder, 'distances.tab'))
    for item in mash_output:
        print(item.reference, item.query, str(item.distance))
        if item.distance > 0.06:  # May need to adjust this value.
            bad_fastqs.append(item.reference)
    return bad_fastqs

if __name__ == '__main__':
    diversitree_redmine()
