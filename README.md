# OLCRedmineAutomator

The OLCRedmineAutomator allows for easy access to a number of
bioinformatics tools for use through Redmine.

The majority of our tools work by parsing a list of Sample IDs provided
in the description field of a Redmine issue submitted through the
[OLC Redmine portal](http://redmine.biodiversity.agr.gc.ca/projects/cfia/).
The requested tool to run the analysis must be specified in the subject line.

Below are a list of acceptable keywords that the OLCRedmineAutomator
will detect in an issue subject line. The amount of resources allocated
for a particular job type is also shown.

| Keyword          | CPUs |  RAM (GB)|
| ---------------  |:----:|:--------:|
| Strainmash       | 8    |  12      |
| WGS Assembly     | 12   |  192     |
| AutoCLARK        | 48   |  192     |
| SNVPhyl          | 48   |  192     |
| Diversitree      | 56   |  192     |
| External Retrieve| 1    |  6       |
| PlasmidExtractor | 48   |  192     |
| Merge            | 48   |  192     |


### Tool Descriptions (Work in progress)

#### Strainmash
**Description:** Reads a list of SeqIDs from a Redmine issue and calls Mash for each against a sketch of the entire GenBank strain database (Up to date as of Dec. 01, 2017). Returns a formatted Mash.screen output file per SeqID.

**Usage:** Set your Redmine issue topic to 'Strainmash'.
Then, enter a list of SeqIDs into the Description box. i.e.
```
2017-SEQ-0918
2017-SEQ-0919
2017-SEQ-0920
2017-SEQ-0921
```

---
#### Diversitree
**Description:**

**Usage:**

---
#### AutoCLARK
**Description:**

**Usage:**

---
#### CloseRelatives
**Description:**

**Usage:**

---
#### PlasmidExtractor
**Description:**

**Usage:**

---
#### WGSAssembly
**Description:**

**Usage:**

---
#### SNVPHyl
**Description:**

**Usage:**

---


### Internal Notes
1. Log into the head node (ubuntu@192.168.1.26)
2. Activate the virtual environment (see *requirements.txt*)
2. Call /mnt/nas/Redmine/OLCRedmineAutomator/api.py
3. Enjoy Redmine automation

Running this program requires a *setup.py* file that is not included in
this repository for security. Here's a censored example of setup.py:

```
API_KEY = 'super_secret'
BIO_REQUESTS_DIR = '/mnt/nas/bio_requests'
AUTOMATOR_KEYWORDS = {
    'strainmash':
        {'memory': 12000, 'n_cpu': 8},
    'autoclark':
        {'memory': 192000, 'n_cpu': 48},
    'diversitree':
        {'memory': 192000, 'n_cpu': 56},
}
```