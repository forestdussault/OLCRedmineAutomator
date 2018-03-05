import glob
import pandas as pd


def create_report_dictionary(report_list, seq_list, id_column):
    """
    :param report_list: List of paths to report files
    :param seq_list: List of OLC Seq IDs
    :param id_column: Column used to specify primary key
    :return: Dictionary containing Seq IDs as keys and dataframes as values
    """
    # Create empty dict to store reports of interest
    report_dict = {}

    # Iterate over every metadata file
    for report in report_list:
        # Check if the sample we want is in file
        df = pd.read_csv(report)
        samples = df[id_column]
        # Check all of our sequences of interest to see if they are in the combinedMetadata file
        for seq in seq_list:
            if seq in samples.values:
                # Associate dataframe with sampleID
                report_dict[seq] = df
    return report_dict


def get_combined_metadata(seq_list):
    """
    :param seq_list: List of OLC Seq IDs
    :return: Dictionary containing Seq IDs as keys and combinedMetadata dataframes as values
    """
    # Grab every single combinedMetadata file we have
    all_reports = glob.glob('/mnt/nas/WGSspades/*/reports/combinedMetadata.csv')
    metadata_report_dict = create_report_dictionary(report_list=all_reports, seq_list=seq_list, id_column='SeqID')
    return metadata_report_dict


# TODO: Fix this for production to retrieve full list of GDCS reports
def get_gdcs(seq_list):
    """
    :param seq_list: List of OLC Seq IDs
    :return: Dictionary containing Seq IDs as keys and GDCS dataframes as values
    """
    # Grab every single combinedMetadata file we have
    gdcs_reports = glob.glob('/mnt/nas/WGSspades/*/reports/GDCS.csv')
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


def validate_mash(seq_list, metadata_reports, expected_species):
    """
    Takes a species name as a string (i.e. 'Salmonella enterica') and creates a dictionary with keys for each Seq ID
    and boolean values if the value pulled from MASH_ReferenceGenome matches the string or not
    :param seq_list: List of OLC Seq IDs
    :param metadata_reports: Dictionary retrieved from get_combined_metadata()
    :param expected_species: String containing expected species
    :return: Dictionary with Seq IDs as keys and True/False as values
    """
    seq_status = {}

    for seqid in seq_list:
        print('Validating MASH reference genome for {} '.format(seqid))
        df = metadata_reports[seqid]
        observed_species = df.loc[df['SeqID'] == seqid]['MASH_ReferenceGenome'].values[0]

        if observed_species == expected_species:
            seq_status[seqid] = True
        else:
            seq_status[seqid] = False

    return seq_status

def generate_validated_list(seq_list, genus):
    """
    :param seq_list: List of OLC Seq IDs
    :param genus: String of expected genus (Salmonella, Listeria, Escherichia)
    :return: List containing each valid Seq ID
    """
    # VALIDATION
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
        # Grab values
        matches = df.loc[df['Strain'] == sample_id]['Matches'].values[0]
        passfail = df.loc[df['Strain'] == sample_id]['Pass/Fail'].values[0]
        gdcs_dict[sample_id] = (matches, passfail)
    return gdcs_dict

