import os
import glob
import click
import pickle
from Bio import Phylo
from Bio import SeqIO
from biotools import mash
from collections import OrderedDict
from nastools.nastools import retrieve_nas_files

@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def neartree_redmine(redmine_instance, issue, work_dir, description):
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    try:
        if not os.path.isdir(os.path.join(work_dir, 'fastas')):
            os.makedirs(os.path.join(work_dir, 'fastas'))
        # Check that the first line of the request is a number. If it isn't, tell author they goofed and give up.
        try:
            desired_num_strains = int(description[0])
        except ValueError:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='Error! The first line of your request must be the number of'
                                                ' strains you want picked from the tree.',
                                          status_id=4)
            return

        # Go through description to figure out what our query is and what the reference is.
        query = False
        reference = False
        query_list = list()
        seqid_list = list()
        for i in range(1, len(description)):
            item = description[i].upper()
            if item == '':
                continue
            if 'QUERY' in item:
                query = True
                reference = False
                continue
            elif 'REFERENCE' in item:
                reference = True
                query = False
                continue
            if query:
                query_list.append(item)
            elif reference:
                seqid_list.append(item)

        # Only allowed to have one query file - boot the user if they tried to specify too many queries.
        if len(query_list) > 1:
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='ERROR: You specified {query_list_len} query files ({query_list}). Only'
                                                ' one query file is supposed to be specified. '
                                                'Please try again.'.format(query_list_len=len(query_list),
                                                                           query_list=query_list))

        # Drop FASTA files into workdir
        retrieve_nas_files(seqids=seqid_list,
                           outdir=os.path.join(work_dir, 'fastas'),
                           filetype='fasta',
                           copyflag=False)
        # Also retrieve the query file.
        retrieve_nas_files(seqids=query_list,
                           outdir=work_dir,
                           filetype='fasta',
                           copyflag=False)
        # TODO: Add check that specified files were able to be retrieved.
        # Run a mash to figure out if any strains are particularly far apart and likely to make PARSNP fail.
        reference_file = glob.glob(os.path.join(work_dir, '*.fasta'))[0]
        make_ref(reference_file, os.path.join(work_dir, 'reference.fasta'))
        bad_fastas = check_distances(reference_file, os.path.join(work_dir, 'fastas'))
        if bad_fastas:
            outstr = ''
            for fasta in bad_fastas:
                fasta = os.path.split(fasta)[-1]
                outstr += fasta + '\n'
            redmine_instance.issue.update(resource_id=issue.id,
                                          notes='Warning! MASH screening thinks that the following samples may be too'
                                                ' far from the reference: {samples}\nIn this case, the reference file'
                                                ' was {reference}. You may want to create a new issue and '
                                                'try again.'.format(samples=outstr,
                                                                    reference=os.path.split(reference_file)[-1]))

        # Remove distances.tab and sketch.msh from fastas folder, because sometimes they make
        # parsnp crash. Other times they don't. I have no idea why, so remove just to be safe.
        try:
            os.remove(os.path.join(work_dir, 'fastas', 'distances.tab'))
            os.remove(os.path.join(work_dir, 'fastas', 'sketch.msh'))
        except OSError:
            pass

        cmd = '/mnt/nas/Programs/Parsnp-Linux64-v1.2/parsnp -r {workdir}/reference.fasta -d {input} ' \
              '-c -o {output} -p {threads}'.format(threads=48,
                                                   workdir=work_dir,
                                                   input=os.path.join(work_dir, 'fastas'),
                                                   output=os.path.join(work_dir, 'parsnp_output'))
        os.system(cmd)

        tree = Phylo.read(os.path.join(work_dir, 'parsnp_output', 'parsnp.tree'), 'newick')
        ref_clades = tree.find_clades('reference.fasta.ref')
        for clade in ref_clades:
            ref_clade = clade
        clades = tree.get_terminals()
        distance_dict = dict()
        for clade in clades:
            distance = tree.distance(clade, ref_clade)
            distance_dict[clade.name] = distance

        # Use some stackoverflow magic to sort dict https://stackoverflow.com/questions/613183/how-do-i-sort-a-dictionary-by-value
        sorted_dict = OrderedDict(sorted(distance_dict.items(), key=lambda x: x[1]))

        i = 0
        outstr = ''
        for key in sorted_dict:
            if 'reference' not in key and i < desired_num_strains:
                outstr += key.replace('.fasta', '') + '\n'
                i += 1

        redmine_instance.issue.update(resource_id=issue.id, status_id=4,
                                      notes='NearTree process complete! Closest strains are:\n {}'.format(outstr))
        os.system('rm {fasta_files}'.format(fasta_files=os.path.join(work_dir, 'fasta', '*fasta')))
    except Exception as e:
        redmine_instance.issue.update(resource_id=issue.id,
                                      notes='Something went wrong! Send this error traceback to your friendly '
                                            'neighborhood bioinformatician: {}'.format(e))


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


def make_ref(input_file, output_file):
    """
    Parsnp doesn't like having multi-fastas as input files, so this method puts all contigs in a multi-fasta
    into a single contig.
    :param input_file: Path to your input multi-fasta
    :param output_file: Path to output fasta. Overwrites the file if it already exists.
    """
    contigs = SeqIO.parse(input_file, 'fasta')
    with open(output_file, 'w') as f:
        f.write('>reference\n')
        for s in contigs:
            f.write(str(s.seq) + '\n')


if __name__ == '__main__':
    neartree_redmine()
