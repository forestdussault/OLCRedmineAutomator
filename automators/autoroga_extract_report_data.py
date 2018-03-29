import os
import glob
import collections
import pandas as pd
from automator_settings import ASSEMBLIES_FOLDER


def create_report_dictionary(report_list, seq_list, id_column='SeqID'):
    """
    :param report_list: List of paths to report files
    :param seq_list: List of OLC Seq IDs
    :param id_column: Column used to specify primary key
    :return: Dictionary containing Seq IDs as keys and dataframes as values
    """
    report_dict = {}

    # Iterate over every metadata file (e.g. combinedMetadata.csv or GDCS.csv)
    for report in report_list:
        # Check if the sample we want is in file
        print(report)

        # Accomodating runs that might still be in progress
        try:
            df = pd.read_csv(report)
        except:  # from pandas.io.parser import CParserError try/except with CParserError for this
            continue

        # Accomodating old reports coming up
        try:
            samples = df[id_column]
        except KeyError:
            samples = df['SampleName']  # This was the old column name for SeqID

        # Check all of our sequences of interest to see if they are in the combinedMetadata file
        for seq in seq_list:
            if seq in samples.values:
                # Associate dataframe with sampleID
                report_dict[seq] = df

    ordered_dict = collections.OrderedDict(sorted(report_dict.items()))
    return ordered_dict


def get_combined_metadata(seq_list):
    """
    :param seq_list: List of OLC Seq IDs
    :return: Dictionary containing Seq IDs as keys and combinedMetadata dataframes as values
    """
    # Grab every single combinedMetadata.csv file we have
    all_reports = glob.glob(os.path.join(ASSEMBLIES_FOLDER, '*/reports/combinedMetadata.csv'))
    metadata_report_dict = create_report_dictionary(report_list=all_reports, seq_list=seq_list)
    return metadata_report_dict


def get_gdcs(seq_list):
    """
    :param seq_list: List of OLC Seq IDs
    :return: Dictionary containing Seq IDs as keys and GDCS dataframes as values
    """
    # Grab every single GDCS.csv file we have
    gdcs_reports = glob.glob(os.path.join(ASSEMBLIES_FOLDER, '*/reports/GDCS.csv'))
    gdcs_report_dict = create_report_dictionary(report_list=gdcs_reports, seq_list=seq_list, id_column='Strain')
    return gdcs_report_dict


def validate_genus(seq_list, genus):
    """
    Validates whether or not the expected genus matches the observed genus parsed from combinedMetadata.
    :param seq_list: List of OLC Seq IDs
    :param genus: String of expected genus (Salmonella, Listeria, Escherichia)
    :return: Dictionary containing Seq IDs as keys and a 'valid status' as the value
    """
    metadata_reports = get_combined_metadata(seq_list)

    valid_status = {}

    for seqid in seq_list:
        print('Validating {} genus'.format(seqid))
        df = metadata_reports[seqid]
        observed_genus = df.loc[df['SeqID'] == seqid]['Genus'].values[0]
        if observed_genus == genus:
            valid_status[seqid] = True  # Valid genus
        else:
            valid_status[seqid] = False  # Invalid genus

    return valid_status


def validate_ecoli(seq_list, metadata_reports):
    """
    Checks if the uidA marker and vt markers are present in the combinedMetadata sheets and stores True/False for
    each SeqID. Values are stored as tuples: (uida_present, verotoxigenic)
    :param seq_list: List of OLC Seq IDs
    :param metadata_reports: Dictionary retrieved from get_combined_metadata()
    :return: Dictionary containing Seq IDs as keys and (uidA, vt) presence or absence for values.
             Present = True, Absent = False
    """
    ecoli_seq_status = {}

    for seqid in seq_list:
        print('Validating {} uidA and vt marker detection'.format(seqid))
        df = metadata_reports[seqid]
        observed_genus = df.loc[df['SeqID'] == seqid]['Genus'].values[0]
        uida_present = False
        verotoxigenic = False

        if observed_genus == 'Escherichia':
            if 'uidA' in df.loc[df['SeqID'] == seqid]['GeneSeekr_Profile'].values[0]:
                uida_present = True
            if 'vt' in df.loc[df['SeqID'] == seqid]['Vtyper_Profile'].values[0]:
                verotoxigenic = True
            ecoli_seq_status[seqid] = (uida_present, verotoxigenic)

    return ecoli_seq_status


def validate_salmonella(seq_list, metadata_reports):
    """
    Checks combined metadata for presence of invA or stn in the GeneSeekr_Profile column
    :param seq_list:
    :param metadata_reports:
    :return: dict with pair of SeqID:boolean where True means GeneSeekr confirms the identity
    """
    salmonella_seq_status = {}

    for seqid in seq_list:
        print('Validating {} invA OR stn marker detection for Salmonella'.format(seqid))
        df = metadata_reports[seqid]
        observed_genus = df.loc[df['SeqID'] == seqid]['Genus'].values[0]
        inva_present = False
        stn_present = False

        if observed_genus == 'Salmonella':
            if 'invA' in df.loc[df['SeqID'] == seqid]['GeneSeekr_Profile'].values[0]:
                inva_present = True
            if 'stn' in df.loc[df['SeqID'] == seqid]['GeneSeekr_Profile'].values[0]:
                stn_present = True

        if stn_present is True or inva_present is True:
            salmonella_seq_status[seqid] = True
        else:
            salmonella_seq_status[seqid] = False
    return salmonella_seq_status


def validate_listeria(seq_list, metadata_reports):
    """
    Checks combined metadata for presence of IGS, inlJ, and hlyA in the GeneSeekr_Profile column

    To be confirmed Listeria, the following criteria must be met:
    IGS present + (inlJ present OR hlyA present) = True

    :param seq_list:
    :param metadata_reports:
    :return: dict with pair of SeqID:boolean where True means GeneSeekr confirms the identity
    """
    listeria_seq_status = {}

    for seqid in seq_list:
        print('Validating {} invA OR stn marker detection for Salmonella'.format(seqid))
        df = metadata_reports[seqid]
        observed_genus = df.loc[df['SeqID'] == seqid]['Genus'].values[0]
        igs_present = False
        hlya_present = False
        inlj_present = False

        if observed_genus == 'Listeria':
            if 'IGS' in df.loc[df['SeqID'] == seqid]['GeneSeekr_Profile'].values[0]:
                igs_present = True
            if 'hlyA' in df.loc[df['SeqID'] == seqid]['GeneSeekr_Profile'].values[0]:
                hlya_present = True
            if 'inlj' in df.loc[df['SeqID'] == seqid]['GeneSeekr_Profile'].values[0]:
                inlj_present = True

        if igs_present is True:
            if hlya_present is True or inlj_present is True:
                listeria_seq_status[seqid] = True
            else:
                listeria_seq_status[seqid] = False
        else:
            listeria_seq_status[seqid] = False
    return listeria_seq_status


def generate_validated_list(seq_list, genus):
    """
    :param seq_list: List of OLC Seq IDs
    :param genus: String of expected genus (Salmonella, Listeria, Escherichia)
    :return: List containing each valid Seq ID
    """
    validated_list = []
    validated_dict = validate_genus(seq_list=seq_list, genus=genus)

    for seqid, valid_status in validated_dict.items():
        if validated_dict[seqid]:
            validated_list.append(seqid)
        else:
            print('WARNING: '
                  'Seq ID {} does not match the expected genus of {} and was ignored.'.format(seqid, genus.upper()))
    return validated_list


def parse_geneseekr_profile(value):
    """
    Takes in a value from the GeneSeekr_Profile of combinedMetadata.csv and parses it to determine which markers are
    present. i.e. if the cell contains "invA;stn", a list containing invA, stn will be returned
    :param value: String delimited by ';' character containing markers
    :return: List of markers parsed from value
    """
    detected_markers = []
    marker_list = ['invA', 'stn', 'IGS', 'hlyA', 'inlJ', 'VT1', 'VT2', 'VT2f', 'uidA', 'eae']
    markers = value.split(';')
    for marker in markers:
        if marker in marker_list:
            detected_markers.append(marker)
    return detected_markers


def generate_gdcs_dict(gdcs_reports):
    """
    :param gdcs_reports: Dictionary derived from get_gdcs() function
    :return: Dictionary containing parsed GDCS values
    """
    gdcs_dict = {}
    for sample_id, df in gdcs_reports.items():
        matches = df.loc[df['Strain'] == sample_id]['Matches'].values[0]
        passfail = df.loc[df['Strain'] == sample_id]['Pass/Fail'].values[0]
        gdcs_dict[sample_id] = (matches, passfail)
    return gdcs_dict
