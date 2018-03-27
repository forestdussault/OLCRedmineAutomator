# automators_dev

This subfolder contains the development versions of all automator scripts.

Some of these scripts are dependent on a git ignored config file (automator_settings.py)
The contents of this file should resemble the following:

```python
FTP_USERNAME = ''
FTP_PASSWORD = ''
POSTGRES_PASSWORD = ''
POSTGRES_USERNAME = ''
ASSEMBLIES_FOLDER = ''
```


#### Strainmash
- strainmash.py
#### WGS Assembly
- wgsassembly.py
#### AutoCLARK
- autoclark.py
#### SNVPhyl
- snvphyl.py
#### Diversitree
- diversitree.py
- sampler.py
#### External Retrieve
- externalretrieve.py
#### PlasmidExtractor
- plasmidextractor.py
#### Merge
- merge.py
- merger.py
#### CloseRelatives
- closerelatives.py
#### AutoROGA
- autoroga.py
- autoroga_database.py
- autoroga_extract_report_data.py
- CFIA_logo.png
#### QIIMEgraph
- qiimegraph.py
- qiimegraph_generate_chart.py
- qiimegraph_taxonomic_color_dictionary.pickle
#### Metadata Retrieve
- metadataretrieve.py