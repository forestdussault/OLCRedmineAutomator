# begin-doc-include

import os
import re
import click
import pickle
import pylatex as pl
import autoroga_extract_report_data as extract_report_data

from datetime import datetime
from pylatex.utils import bold, italic
from autoroga_database import update_db

"""
This script receives input from a CFIA Redmine issue and will generate a ROGA using associated assembly data.
A search is done for each of the input SEQ IDs in the WGSAssembly folders, then the combinedMetadata.csv and GDCS.csv 
files associated with each sample are parsed and formatted for the report. These two .csv files are generated via the 
COWBAT pipeline and should be available for every new assembly.

The Redmine issue description input must be formatted as follows:

    AMENDMENT:REPORTID [optional]
    LABID
    SOURCE
    GENUS
    SEQID
    SEQID
    SEQID
    ... etc.

A note on Sample IDs:
LSTS ID should be parsed from SampleSheet.csv by the COWBAT pipeline, and is available within the combinedMetadata.csv
file. The LSTS ID is available under the 'SampleName' column in combinedMetadata.csv


"""

lab_info = {
    'GTA': ('2301 Midland Ave., Scarborough, ON, M1P 4R7', '416-952-3203'),
    'BUR': ('3155 Willington Green, Burnaby, BC, V5G 4P2', '604-292-6028'),
    'OLC': ('960 Carling Ave, Building 22 CEF, Ottawa, ON, K1A 0Y9', '613-759-1267'),
    'FFFM': ('960 Carling Ave, Building 22 CEF, Ottawa, ON, K1A 0Y9', '613-759-1220'),
    'DAR': ('1992 Agency Dr., Dartmouth, NS, B2Y 3Z7', '902-536-1012'),
    'CAL': ('3650 36 Street NW, Calgary, AB, T2L 2L1', '403-338-5200'),
    'STH': ('3400 Casavant Boulevard W., St. Hyacinthe, QC, J2S 8E3', '450-768-6800')
}


# TODO: GDCS + GenomeQAML combined metric. Everything must pass in order to be listed as 'PASS'
@click.command()
@click.option('--redmine_instance', help='Path to pickled Redmine API instance')
@click.option('--issue', help='Path to pickled Redmine issue')
@click.option('--work_dir', help='Path to Redmine issue work directory')
@click.option('--description', help='Path to pickled Redmine description')
def redmine_roga(redmine_instance, issue, work_dir, description):
    """
    Main method for generating autoROGA
    """
    # Unpickle Redmine objects
    redmine_instance = pickle.load(open(redmine_instance, 'rb'))
    issue = pickle.load(open(issue, 'rb'))
    description = pickle.load(open(description, 'rb'))

    # Setup
    amended_report_id = None
    lab = None
    genus = None
    source = None
    seqids = None
    lstsids = None
    seq_lsts_dict = None

    # Remove all newlines/empty lines from the description items
    description = [x for x in description if x != '']

    # Amendment functionality
    amendment_flag = False
    amendment_check = description[0].upper()
    if 'AMENDMENT' in amendment_check:
        amendment_flag = True

    # Parse fields
    if amendment_flag is False:
        # Parse lab ID
        lab = description[0]
        # Verify lab ID
        if lab not in lab_info:
            valid_labs = str([x for x, y in lab_info.items()])
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Invalid Lab ID provided. Please ensure the first line of your '
                                                'Redmine description specifies one of the following labs:\n'
                                                '{}\n'
                                                'Your input: {}'.format(valid_labs, description))
            quit()

        # Parse source
        source = description[1].lower()
        # Quick verification check to make sure this line isn't a Seq ID. This is brittle and should be changed.
        if len(source.split('-')) > 2:
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Invalid source provided. '
                                                'Line 2 of the Redmine description must be a valid string e.g. "flour"')
            quit()

        # Parse genus
        genus = description[2].capitalize()
        if genus not in ['Escherichia', 'Salmonella', 'Listeria']:
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Input genus "{}" does not match any of the acceptable values'
                                                ' which include: "Escherichia", "Salmonella", "Listeria"'.format(genus))
            quit()

        # Parse Seq IDs
        # NOTE: Now should have SeqID;LSTSID format
        try:
            seqids, lstsids = parse_seqid_list(description)
        except:
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Could not pair Seq IDs and LSTS IDs from the provided '
                                                'description. Confirm that each sample follows the required format '
                                                'of [SEQID; LSTSID] or [SEQID   LSTSID] for each line.')
            quit()

        seq_lsts_dict = dict(zip(seqids, lstsids))

    elif amendment_flag:
        try:
            amended_report_id = amendment_check.split(':')[1]
        except IndexError:
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Could not parse AutoROGA ID from AMENDMENT field.\n'
                                                'Must be formatted as follows: AMENDMENT:ROGAID')
            quit()

        # Parse lab ID
        lab = description[1]
        if lab not in lab_info:
            valid_labs = str([x for x, y in lab_info.items()])
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Invalid Lab ID provided. Please ensure the first line of your '
                                                'Redmine description specifies one of the following labs:\n'
                                                '{}'.format(valid_labs))
            quit()

        # Parse source
        source = description[2].lower()
        if len(source.split('-')) > 2:
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Invalid source provided. '
                                                'Line 2 of the Redmine description must be a valid string e.g. "flour"')
            quit()

        # Parse genus
        genus = description[3].capitalize()
        if genus not in ['Escherichia', 'Salmonella', 'Listeria']:
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Input genus "{}" does not match any of the acceptable values'
                                                ' which include: "Escherichia", "Salmonella", "Listeria"'.format(genus))
            quit()

        # Parse Seq IDs
        try:
            seqids, lstsids = parse_seqid_list(description)
        except:
            redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                          notes='ERROR: Could not pair Seq IDs and LSTS IDs from the provided '
                                                'description. Confirm that each sample follows the required format '
                                                'of [SEQID; LSTSID] or [SEQID   LSTSID] for each line.')
            quit()
        seq_lsts_dict = dict(zip(seqids, lstsids))

    # Validate Seq IDs
    validated_list = []
    try:
        validated_list = extract_report_data.generate_validated_list(seq_list=seqids, genus=genus)
    except KeyError as e:
        redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                      notes='ERROR: Could not find one or more of the provided Seq IDs on the NAS.\n'
                                            'TRACEBACK: {}'.format(e))
        quit()

    if len(validated_list) == 0:
        redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                      notes='ERROR: No samples provided matched the expected genus '
                                            '"{}"'.format(genus.upper()))
        quit()

    if validated_list != seqids:
        redmine_instance.issue.update(resource_id=issue.id, status_id=3,
                                      notes='ERROR: Could not validate SeqIDs.\nValidated list: {}\nSeqList: {}'
                                      .format(validated_list, seqids))
        quit()

    # GENERATE REPORT
    pdf_file = generate_roga(seq_lsts_dict=seq_lsts_dict,
                             genus=genus,
                             lab=lab,
                             source=source,
                             work_dir=work_dir,
                             amendment_flag=amendment_flag,
                             amended_id=amended_report_id)

    # Output list containing dictionaries with file path as the key for upload to Redmine
    output_list = [
        {
            'path': os.path.join(work_dir, pdf_file),
            'filename': os.path.basename(pdf_file)
        }
    ]

    redmine_instance.issue.update(resource_id=issue.id, uploads=output_list, status_id=3,
                                  notes='Generated ROGA successfully. Completed PDF report is attached.')


def generate_roga(seq_lsts_dict, genus, lab, source, work_dir, amendment_flag, amended_id):
    """
    Generates PDF
    :param seq_lsts_dict: Dict of SeqIDs;LSTSIDs
    :param genus: Expected Genus for samples (Salmonella, Listeria, or Escherichia)
    :param lab: ID for lab report is being generated for
    :param source: string input for source that strains were derived from, i.e. 'ground beef'
    :param work_dir: bio_request directory
    :param amendment_flag: determined if the report is an amendment type or not (True/False)
    :param amended_id: ID of the original report that the new report is amending
    """

    # RETRIEVE DATAFRAMES FOR EACH SEQID
    seq_list = list(seq_lsts_dict.keys())

    metadata_reports = extract_report_data.get_combined_metadata(seq_list)
    gdcs_reports = extract_report_data.get_gdcs(seq_list)
    gdcs_dict = extract_report_data.generate_gdcs_dict(gdcs_reports)

    # DATE SETUP
    date = datetime.today().strftime('%Y-%m-%d')
    year = datetime.today().strftime('%Y')

    # PAGE SETUP
    geometry_options = {"tmargin": "2cm",
                        "lmargin": "1cm",
                        "rmargin": "1cm",
                        "headsep": "1cm"}

    doc = pl.Document(page_numbers=False, geometry_options=geometry_options)

    header = produce_header_footer()
    doc.preamble.append(header)
    doc.change_document_style("header")

    # DATABASE HANDLING
    report_id = update_db(date=date, year=year, genus=genus, lab=lab, source=source,
                          amendment_flag=amendment_flag, amended_id=amended_id)

    # MARKER VARIABLES SETUP
    all_uida = False
    all_vt = False
    all_mono = False
    all_enterica = False
    some_vt = False
    vt_sample_list = []

    # SECOND VALIDATION SCREEN
    if genus == 'Escherichia':
        validated_ecoli_dict = extract_report_data.validate_ecoli(seq_list, metadata_reports)
        vt_list = []
        uida_list = []

        for key, value in validated_ecoli_dict.items():
            ecoli_uida_present = validated_ecoli_dict[key][0]
            ecoli_vt_present = validated_ecoli_dict[key][1]

            uida_list.append(ecoli_uida_present)
            vt_list.append(ecoli_vt_present)

            # For the AMR table so only vt+ samples are shown
            if ecoli_vt_present is True:
                vt_sample_list.append(key)

            if not ecoli_uida_present:
                print('WARNING: uidA not present for {}. Cannot confirm E. coli.'.format(key))
            if not ecoli_vt_present:
                print('WARNING: vt probe sequences not detected for {}. '
                      'Cannot confirm strain is verotoxigenic.'.format(key))

        if False not in uida_list:
            all_uida = True
        if False not in vt_list:
            all_vt = True

        if True in vt_list:
            some_vt = True

    elif genus == 'Listeria':
        validated_listeria_dict = extract_report_data.validate_listeria(seq_list, metadata_reports)
        mono_list = []
        for key, value in validated_listeria_dict.items():
            mono_list.append(value)
        if False not in mono_list:
            all_mono = True

    elif genus == 'Salmonella':
        validated_salmonella_dict = extract_report_data.validate_salmonella(seq_list, metadata_reports)
        enterica_list = []
        for key, value in validated_salmonella_dict.items():
            enterica_list.append(value)
        if False not in enterica_list:
            all_enterica = True

    # MAIN DOCUMENT BODY
    with doc.create(pl.Section('Report of Genomic Analysis: ' + genus, numbering=False)):

        # REPORT ID AND AMENDMENT CHECKING
        if amendment_flag:
            doc.append(bold('Report ID: '))
            doc.append(report_id)
            doc.append(italic(' (This report is an amended version of '))
            doc.append(amended_id)
            doc.append(italic(')'))
            doc.append('\n')
            doc.append(pl.Command('TextField',
                                  options=["name=rdimsnumberbox",
                                           "multiline=false",
                                           pl.NoEscape("bordercolor=0 0 0"),
                                           pl.NoEscape("width=1.1in"),
                                           "height=0.2in"],
                                  arguments=bold('RDIMS ID: ')))
            doc.append(bold('\nReporting laboratory: '))
            doc.append(lab)
            doc.append('\n\n')

            # LAB SUMMARY
            with doc.create(pl.Tabular('lcr', booktabs=True)) as table:
                table.add_row(bold('Laboratory'),
                              bold('Address'),
                              bold('Tel #'))
                table.add_row(lab, lab_info[lab][0], lab_info[lab][1])

            # AMENDMENT FIELD
            with doc.create(pl.Subsubsection('Reason for amendment:', numbering=False)):
                with doc.create(Form()):
                    doc.append(pl.Command('noindent'))
                    doc.append(pl.Command('TextField',
                                          options=["name=amendmentbox",
                                                   "multiline=true",
                                                   pl.NoEscape("bordercolor=0 0 0"),
                                                   pl.NoEscape("width=7in"),
                                                   "height=0.43in"],
                                          arguments=''))
        else:
            doc.append(bold('Report ID: '))
            doc.append(report_id)
            doc.append('\n')
            doc.append(pl.Command('TextField',
                                  options=["name=rdimsnumberbox",
                                           "multiline=false",
                                           pl.NoEscape("bordercolor=0 0 0"),
                                           pl.NoEscape("width=1.1in"),
                                           "height=0.2in"],
                                  arguments=bold('RDIMS ID: ')))
            doc.append(bold('\nReporting laboratory: '))
            doc.append(lab)
            doc.append('\n\n')

            # LAB SUMMARY
            with doc.create(pl.Tabular('lcr', booktabs=True)) as table:
                table.add_row(bold('Laboratory'),
                              bold('Address'),
                              bold('Tel #'))
                table.add_row(lab, lab_info[lab][0], lab_info[lab][1])

        # TEXT SUMMARY
        with doc.create(pl.Subsection('Identification Summary', numbering=False)) as summary:

            summary.append('Whole-genome sequencing analysis was conducted on '
                           '{} '.format(len(metadata_reports)))
            summary.append(italic('{} '.format(genus)))

            if len(metadata_reports) == 1:
                summary.append('strain isolated from "{}". '.format(source.lower()))
            else:
                summary.append('strains isolated from "{}". '.format(source.lower()))

            if genus == 'Escherichia':
                if all_uida:
                    summary.append('The following strains are confirmed as ')
                    summary.append(italic('Escherichia coli '))
                    summary.append('based on 16S sequence and the presence of marker gene ')
                    summary.append(italic('uidA. '))
                elif not all_uida:
                    summary.append('Some of the following strains could not be confirmed to be ')
                    summary.append(italic('Escherichia coli '))
                    summary.append('as the ')
                    summary.append(italic('uidA '))
                    summary.append('marker gene was not detected. ')

                if all_vt:
                    summary.append('All strain(s) are confirmed to be VTEC based on detection of probe sequences '
                                   'indicating the presence of verotoxin genes.')

            elif genus == 'Listeria':
                if all_mono:
                    summary.append('The following strains are confirmed to be ')
                    summary.append(italic('Listeria monocytogenes '))
                    summary.append('based on GeneSeekr analysis: ')
                else:
                    summary.append('Some of the following strains could not be confirmed to be ')
                    summary.append(italic('Listeria monocytogenes.'))

            elif genus == 'Salmonella':
                if all_enterica:
                    summary.append('The following strains are confirmed to be ')
                    summary.append(italic('Salmonella enterica '))
                    summary.append('based on GeneSeekr analysis: ')
                else:
                    summary.append('Some of the following strains could not be confirmed to be ')
                    summary.append(italic('Salmonella enterica.'))

        # ESCHERICHIA TABLE
        if genus == 'Escherichia':
            genesippr_table_columns = (bold('ID'),
                                       bold(pl.NoEscape(r'uidA{\footnotesize \textsuperscript {a}}')),
                                       bold(pl.NoEscape(r'Serotype')),
                                       bold(pl.NoEscape(r'Verotoxin Profile')),
                                       bold(pl.NoEscape(r'eae{\footnotesize \textsuperscript {a}}')),
                                       bold(pl.NoEscape(r'MLST')),
                                       bold(pl.NoEscape(r'rMLST')),
                                       )

            with doc.create(pl.Subsection('GeneSeekr Analysis', numbering=False)) as genesippr_section:
                with doc.create(pl.Tabular('|c|c|c|c|c|c|c|')) as table:
                    # Header
                    table.add_hline()
                    table.add_row(genesippr_table_columns)

                    # Rows
                    for sample_id, df in metadata_reports.items():
                        table.add_hline()

                        # ID
                        # lsts_id = df.loc[df['SeqID'] == sample_id]['SampleName'].values[0]
                        lsts_id = seq_lsts_dict[sample_id]

                        # Genus (pulled from 16S)
                        genus = df.loc[df['SeqID'] == sample_id]['Genus'].values[0]

                        # Serotype
                        serotype = df.loc[df['SeqID'] == sample_id]['E_coli_Serotype'].values[0]

                        # Remove % identity
                        fixed_serotype = remove_bracketed_values(serotype)

                        # Verotoxin
                        verotoxin = df.loc[df['SeqID'] == sample_id]['Vtyper_Profile'].values[0]

                        # MLST/rMLST
                        mlst = str(df.loc[df['SeqID'] == sample_id]['MLST_Result'].values[0]).replace('-', 'New')
                        rmlst = str(df.loc[df['SeqID'] == sample_id]['rMLST_Result'].values[0]).replace('-', 'New')

                        marker_list = df.loc[df['SeqID'] == sample_id]['GeneSeekr_Profile'].values[0]

                        (uida, eae) = '-', '-'
                        if 'uidA' in marker_list:
                            uida = '+'
                        if 'eae' in marker_list:
                            eae = '+'

                        table.add_row((lsts_id, uida, fixed_serotype, verotoxin, eae, mlst, rmlst))
                    table.add_hline()

                create_caption(genesippr_section, 'a', "+ indicates marker presence : "
                                                       "- indicates marker was not detected")

        # LISTERIA TABLE
        if genus == 'Listeria':
            genesippr_table_columns = (bold('ID'),
                                       bold(pl.NoEscape(r'IGS{\footnotesize \textsuperscript {a}}')),
                                       bold(pl.NoEscape(r'hlyA{\footnotesize \textsuperscript {a}}')),
                                       bold(pl.NoEscape(r'inlJ{\footnotesize \textsuperscript {a}}')),
                                       bold(pl.NoEscape(r'MLST')),
                                       bold(pl.NoEscape(r'rMLST')),
                                       )

            with doc.create(pl.Subsection('GeneSeekr Analysis', numbering=False)) as genesippr_section:
                with doc.create(pl.Tabular('|c|c|c|c|c|c|')) as table:
                    # Header
                    table.add_hline()
                    table.add_row(genesippr_table_columns)

                    # Rows
                    for sample_id, df in metadata_reports.items():
                        table.add_hline()

                        # ID
                        # lsts_id = df.loc[df['SeqID'] == sample_id]['SampleName'].values[0]
                        lsts_id = seq_lsts_dict[sample_id]

                        # Genus
                        genus = df.loc[df['SeqID'] == sample_id]['Genus'].values[0]

                        # MLST/rMLST
                        mlst = str(df.loc[df['SeqID'] == sample_id]['MLST_Result'].values[0]).replace('-', 'New')
                        rmlst = str(df.loc[df['SeqID'] == sample_id]['rMLST_Result'].values[0]).replace('-', 'New')

                        # Markers
                        marker_list = df.loc[df['SeqID'] == sample_id]['GeneSeekr_Profile'].values[0]
                        (igs, hlya, inlj) = '-', '-', '-'
                        if 'IGS' in marker_list:
                            igs = '+'
                        if 'hlyA' in marker_list:
                            hlya = '+'
                        if 'inlJ' in marker_list:
                            inlj = '+'

                        table.add_row((lsts_id, igs, hlya, inlj, mlst, rmlst))
                    table.add_hline()
                create_caption(genesippr_section, 'a', "+ indicates marker presence : "
                                                       "- indicates marker was not detected")

        # SALMONELLA TABLE
        if genus == 'Salmonella':
            genesippr_table_columns = (bold('ID'),
                                       bold(pl.NoEscape(r'Serovar{\footnotesize \textsuperscript {a}}')),
                                       bold(pl.NoEscape(r'Serogroup{\footnotesize \textsuperscript {a,b}}')),
                                       bold(pl.NoEscape(r'H1{\footnotesize \textsuperscript {a}}')),
                                       bold(pl.NoEscape(r'H2{\footnotesize \textsuperscript {a}}')),
                                       bold(pl.NoEscape(r'invA{\footnotesize \textsuperscript {b}}')),
                                       bold(pl.NoEscape(r'stn{\footnotesize \textsuperscript {b}}')),
                                       bold(pl.NoEscape(r'MLST')),
                                       bold(pl.NoEscape(r'rMLST')),
                                       )

            with doc.create(pl.Subsection('GeneSeekr Analysis', numbering=False)) as genesippr_section:
                with doc.create(pl.Tabular('|c|c|c|c|c|c|c|c|c|')) as table:
                    # Header
                    table.add_hline()
                    table.add_row(genesippr_table_columns)

                    # Rows
                    for sample_id, df in metadata_reports.items():
                        table.add_hline()

                        # ID
                        # lsts_id = df.loc[df['SeqID'] == sample_id]['SampleName'].values[0]
                        lsts_id = seq_lsts_dict[sample_id]

                        # MLST/rMLST
                        mlst = str(df.loc[df['SeqID'] == sample_id]['MLST_Result'].values[0]).replace('-', 'New')
                        rmlst = str(df.loc[df['SeqID'] == sample_id]['rMLST_Result'].values[0]).replace('-', 'New')

                        # Serovar
                        serovar = df.loc[df['SeqID'] == sample_id]['SISTR_serovar'].values[0]

                        # SISTR Serogroup, H1, H2
                        sistr_serogroup = df.loc[df['SeqID'] == sample_id]['SISTR_serogroup'].values[0]
                        sistr_h1 = df.loc[df['SeqID'] == sample_id]['SISTR_h1'].values[0].strip(';')
                        sistr_h2 = df.loc[df['SeqID'] == sample_id]['SISTR_h2'].values[0].strip(';')

                        # Markers
                        marker_list = df.loc[df['SeqID'] == sample_id]['GeneSeekr_Profile'].values[0]
                        (inva, stn) = '-', '-'
                        if 'invA' in marker_list:
                            inva = '+'
                        if 'stn' in marker_list:
                            stn = '+'

                        table.add_row((lsts_id, serovar, sistr_serogroup, sistr_h1, sistr_h2, inva, stn, mlst, rmlst))
                    table.add_hline()

                create_caption(genesippr_section, 'a', "Predictions conducted using SISTR "
                                                       "(Salmonella In Silico Typing Resource)")
                create_caption(genesippr_section, 'b', "+ indicates marker presence : "
                                                       "- indicates marker was not detected")

        # SEQUENCE TABLE
        with doc.create(pl.Subsection('Sequence Quality Metrics', numbering=False)):
            with doc.create(pl.Tabular('|c|c|c|c|c|')) as table:
                # Columns
                sequence_quality_columns = (bold('ID'),
                                            bold(pl.NoEscape(r'Total Length')),
                                            bold(pl.NoEscape(r'Coverage')),
                                            bold(pl.NoEscape(r'GDCS')),
                                            bold(pl.NoEscape(r'Pass/Fail')),
                                            )

                # Header
                table.add_hline()
                table.add_row(sequence_quality_columns)

                # Rows
                for sample_id, df in metadata_reports.items():
                    table.add_hline()

                    # Grab values
                    # lsts_id = df.loc[df['SeqID'] == sample_id]['SampleName'].values[0]
                    lsts_id = seq_lsts_dict[sample_id]
                    total_length = df.loc[df['SeqID'] == sample_id]['TotalLength'].values[0]
                    average_coverage_depth = df.loc[df['SeqID'] == sample_id]['AverageCoverageDepth'].values[0]

                    # Fix coverage
                    average_coverage_depth = format(float(str(average_coverage_depth).replace('X', '')), '.0f')
                    average_coverage_depth = str(average_coverage_depth) + 'X'

                    # Matches
                    matches = gdcs_dict[sample_id][0]

                    passfail = gdcs_dict[sample_id][1]
                    if passfail == '+':
                        passfail = 'Pass'
                    elif passfail == '-':
                        passfail = 'Fail'

                    # Add row
                    table.add_row((lsts_id, total_length, average_coverage_depth, matches, passfail))
                table.add_hline()

        # PIPELINE METADATA TABLE
        pipeline_metadata_columns = (bold('ID'),
                                     bold('Seq ID'),
                                     bold('Pipeline Version'),
                                     bold('Database Version'))

        with doc.create(pl.Subsection('Pipeline Metadata', numbering=False)):
            with doc.create(pl.Tabular('|c|c|c|c|')) as table:
                # Header
                table.add_hline()
                table.add_row(pipeline_metadata_columns)

                # Rows
                for sample_id, df in metadata_reports.items():
                    table.add_hline()

                    # ID
                    # lsts_id = df.loc[df['SeqID'] == sample_id]['SampleName'].values[0]
                    lsts_id = seq_lsts_dict[sample_id]

                    # Pipeline version
                    pipeline_version = df.loc[df['SeqID'] == sample_id]['PipelineVersion'].values[0]
                    database_version = pipeline_version

                    # Add row
                    table.add_row((lsts_id, sample_id, pipeline_version, database_version))

                table.add_hline()

        # 'VERIFIED BY' FIELD
        with doc.create(pl.Subsubsection('Verified by:', numbering=False)):
            with doc.create(Form()):
                doc.append(pl.Command('noindent'))
                doc.append(pl.Command('TextField',
                                      options=["name=verifiedbybox",
                                               "multiline=false",
                                               pl.NoEscape("bordercolor=0 0 0"),
                                               pl.NoEscape("width=2.5in"),
                                               "height=0.3in"],
                                      arguments=''))

    # OUTPUT PDF FILE
    pdf_file = os.path.join(work_dir, '{}_{}_{}'.format(report_id, genus, date))

    try:
        doc.generate_pdf(pdf_file, clean_tex=False)
    except:
        pass

    pdf_file += '.pdf'
    return pdf_file


def parse_seqid_list(description):
    seqids = list()
    lstsids = list()

    # Remove whitespace
    description = [x.replace(' ', '') for x in description]

    try:
        for item in description[3:]:
            seqid_item = None
            lstsid_item = None

            # Accomodating pasting straight from Excel
            if '\t' in item:
                seqid_item = item.split('\t')[0]
                lstsid_item = item.split('\t')[1]

            # Manually delimiting IDs with a semicolon in the description
            elif ';' in item:
                seqid_item = item.split(';')[0]
                lstsid_item = item.split(';')[1]

            seqid_item = seqid_item.upper().strip()
            lstsid_item = lstsid_item.upper().strip()

            if seqid_item != '':
                seqids.append(seqid_item)
            if lstsid_item != '':
                lstsids.append(lstsid_item)
    except IndexError:
        return None

    seqids = tuple(seqids)
    lstsids = tuple(lstsids)
    return seqids, lstsids


def produce_header_footer():
    """
    Adds a generic header/footer to the report. Includes the date and CFIA logo in the header + legend in the footer.
    """
    header = pl.PageStyle("header", header_thickness=0.1)

    image_filename = get_image()
    with header.create(pl.Head("L")) as logo:
        logo.append(pl.StandAloneGraphic(image_options="width=110px", filename=image_filename))

    # Date
    with header.create(pl.Head("R")):
        header.append("Date Report Issued: " + datetime.today().strftime('%Y-%m-%d'))

    # Footer
    with header.create(pl.Foot("C")):
        with header.create(pl.Tabular('lcr')) as table:
            table.add_row('', bold('Data interpretation guidelines can be found in RDIMS document ID: 10401305'), '')
            table.add_row('', bold('This report was generated with OLC AutoROGA v1.1'), '')
    return header


def create_caption(section, superscript, text):
    """
    Adds a caption preceded by superscripted characters to a table
    :param section: LateX section object
    :param superscript: character(s) to superscript
    :param text: descriptive text
    """
    section.append('\n')

    # Superscript
    section.append(bold(pl.NoEscape(r'{\footnotesize \textsuperscript {' + superscript + '}}')))

    # Text
    section.append(italic(pl.NoEscape(r'{\footnotesize {' + text + '}}')))


def remove_bracketed_values(string):
    p = re.compile('\(.*?\)')  # Regex to remove bracketed terms
    new_string = re.sub(p, '', string).replace(' ', '')  # Remove bracketed terms and spaces
    return new_string


def get_image():
    """
    :return: full path to image file
    """
    image_filename = os.path.join(os.path.dirname(__file__), 'CFIA_logo.png')
    return image_filename


class Form(pl.base_classes.Environment):
    """A class to wrap hyperref's form environment."""
    _latex_name = 'Form'

    packages = [pl.Package('hyperref')]
    escape = False
    content_separator = "\n"


if __name__ == '__main__':
    redmine_roga()
