#!/usr/bin/env python

import multiprocessing
import subprocess
import argparse
import shutil
import copy
import glob
import os
from Bio import SeqIO
from Bio import Phylo
from scipy import cluster


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


def prune_clade_from_tree(tree, clade_name):
    """
    Given a treefile, will prune out the specified clade.
    :param tree: Treefile read in with Bio.Phylo
    :param clade_name: Name of clade you want removed from the tree.
    :return: New Biopython tree object.
    """
    new_tree = copy.deepcopy(tree)
    if new_tree.find_any(clade_name) is not None:
        new_tree.prune(clade_name)
    else:
        raise AttributeError('Clade {} not found in tree. Cannot be pruned.'.format(clade_name))
    return new_tree


def create_clustering(treefile, desired_clusters=10):
    """
    :param treefile: Tree file, in newick format.
    :param desired_clusters: The total number of clusters you want to return.
    :return: A list of strains that represent each cluster.
    """
    num_clusters = -1
    tree = Phylo.read(treefile, "newick")
    terminals = list()
    clades = tree.find_clades()
    matrix = list()
    # Get a list of all the terminal branches
    for clade in clades:
        if clade.is_terminal():
            terminals.append(str(clade))

    # Create a matrix with distances between each tip and each other tip
    for i in range(len(terminals)):
        for j in range(i + 1, len(terminals)):
            dist = tree.distance(terminals[i], terminals[j])
            matrix.append(dist)

    count = 1
    print('Finding correct number of clusters.')
    # Create the linkage thingy so we can try clustering.
    z = cluster.hierarchy.linkage(matrix, method='average')
    # Try different cutoff levels for making clusters. Start at a very large cluster distance.
    # Check if enough clusters are created. If not enough, make the cutoff slightly smaller and repeat process.
    cluster_distance = 0.9
    while num_clusters < desired_clusters:
        clustering = cluster.hierarchy.fcluster(z, cluster_distance, criterion='distance')
        num_clusters = (max(clustering))
        cluster_distance -= 0.00003
        count += 1
    # Once we've found the desired number of clusters, create a 2d array where each element of the array is a cluster
    # and each element is made up of a list of strains that are part of that cluster.
    clusters = list()
    for i in range(num_clusters):
        clusters.append(list())
    for i in range(len(clustering)):
        clusters[clustering[i] - 1].append(terminals[i])

    # Now that we have a list of strains for each cluster, choose the strain that's the most like everything else
    # in each cluster (the most average-y thing I guess?) to be the representative.
    strains = list()
    for c in clusters:
        representative_strain = choose_representative_strain(c, tree)
        representative_strain = representative_strain.replace('.fasta', '')
        strains.append(representative_strain)

    return strains


def choose_representative_strain(cluster, tree):
    """
    Given a cluster, will find which tip in that cluster tends to be closest to all others (aka the best representative)
    and return that tip
    :param cluster: Cluster found.
    :param tree: Tree read in by Bio.Phylo
    :return: representative strain.
    """
    representative = 'NA'
    best_distance = 100000000.0  # Start off at an absolutely ridiculous value.
    # Iterate through
    for strain1 in cluster:
        total_length = 0.0
        for strain2 in cluster:
            if strain1 != strain2:
                total_length += tree.distance(strain1, strain2)
        if total_length < best_distance:
            best_distance = total_length
            representative = strain1
    return representative


def main(args):
    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)
    if not os.path.isdir(os.path.join(args.output_dir, 'tmp')):
        os.makedirs(os.path.join(args.output_dir, 'tmp'))
    tmpdir = os.path.join(args.output_dir, 'tmp')
    # Check that we haven't requested more strains than are present in the tree.
    if args.desired_strains > len(glob.glob(os.path.join(args.input_dir, '*fasta'))):
        raise ValueError('Number of requested strains cannot exceed number of input strains.')
    # Next step: Choose a reference. Parsnp will complain if there are multiple contigs, so make it into one contig.
    try:
        reference_file = glob.glob(os.path.join(args.input_dir, '*.fasta'))[0]
    except IndexError:
        raise FileNotFoundError('No fasta files found in specified directory. '
                         'Specified directory was: {}'.format(args.input_dir))
    # Create a copy of the tree that parsnp won't complain about.
    make_ref(reference_file, os.path.join(tmpdir, 'reference.fasta'))
    # Run PARSNP on everything to make a tree that we'll get to parsing to determine which species are far apart.
    cmd = '/mnt/nas/Programs/Parsnp-Linux64-v1.2/parsnp -r {tmpdir}/reference.fasta -d {input} -c -o {output} -p {threads}'.format(tmpdir=tmpdir,
                                                                                          threads=str(args.num_threads),
                                                                                             output=args.output_dir,
                                                                                             input=args.input_dir)
    subprocess.call(cmd, shell=True)
    # Remove the "reference" fasta that's actually the same as one of our strains and write the revised tree to file.
    pruned_tree = prune_clade_from_tree(Phylo.read(os.path.join(args.output_dir, 'parsnp.tree'), 'newick'),
                                        'reference.fasta.ref')
    Phylo.write(pruned_tree, os.path.join(args.output_dir, 'pruned.tree'), 'newick')
    # Now get the desired number of strains.
    strains = create_clustering(os.path.join(args.output_dir, 'pruned.tree'), desired_clusters=args.desired_strains)
    shutil.rmtree(tmpdir)
    with open(os.path.join(args.output_dir, 'strains.txt'), 'w') as output_file:
        for strain in strains:
            output_file.write(strain + '\n')


if __name__ == '__main__':
    cpu_count = multiprocessing.cpu_count()
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_dir',
                        type=str,
                        required=True,
                        help='Directory with sequences to be compared in uncompressed FASTA format.')
    parser.add_argument('-t', '--num_threads',
                        type=int,
                        default=cpu_count,
                        help='Number of threads to run analysis on.')
    parser.add_argument('-o', '--output_dir',
                        type=str,
                        required=True,
                        help='Output directory.')
    parser.add_argument('-d', '--desired_strains',
                        type=int,
                        required=True,
                        help='Number of strains you want to pick out.')
    args = parser.parse_args()
    main(args)
