# OLCRedmineAutomator

1. Log into the head node (ubuntu@192.168.1.26)
2. Activate the virtual environment (see *requirements.txt*)
2. Call /mnt/nas/Redmine/OLCRedmineAutomator/api.py
3. Enjoy Redmine automation

### Acceptable OLCRedmineAutomator keywords:
```
'strainmash',
'wgs assembly',
'autoclark',
'snvphyl',
'diversitree',
'irida retrieve',
'resfindr',
'external retrieve',
'autoroga',
'retrieve'
```

### Notes
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

### Tool Descriptions
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


#### Diversitree
**Description:**

**Usage:**


#### AutoCLARK
**Description:**

**Usage:**


... more Coming Soon™