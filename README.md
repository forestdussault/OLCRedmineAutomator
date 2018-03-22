# OLCRedmineAutomator

The OLCRedmineAutomator allows for easy access to a number of
bioinformatics tools for use through Redmine.

The majority of our tools work by parsing a list of Sample IDs provided
in the description field of a Redmine issue submitted through the
[OLC Redmine portal](http://redmine.biodiversity.agr.gc.ca/projects/cfia/).
The requested tool to run the analysis must be specified in the subject line.

Below are a list of acceptable keywords that the OLCRedmineAutomator
will detect in an issue subject line. The amount of resources allocated
to the OLC Slurm cluster for each respective job type is also displayed.

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
| CloseRelatives   | 48   |  192     |


### Internal Notes
The OLCRedmineAutomator is constantly running on the OLC head node (ubuntu@192.168.1.5).
To access recent output from the production log, type `redminelog` into the console.
Similarly, `redminelog-dev` will provide output from the development instance.

Production instance: https://redmine.biodiversity.agr.gc.ca/projects/cfia/issues
Development instance: http://192.168.1.2:8080/

#### Supervisor notes
The automator is controlled through a supervisor on the head node.
Configuration scripts for the supervisor can be found at `/etc/supervisor/conf.d`

Log files for STDERR and STDOUT can be found at the following locations:
- `/var/log/olcredmineautomator.err.log`
- `/var/log/olcredmineautomator.out.log`

If at any time an update is needed on the supervisor scripts,
register the changes with the following commands:
```
sudo supervisorctl reread
sudo supervisorctl update
```

#### Manual operation
1. Log into the head node (ubuntu@192.168.1.5)
2. Activate the virtual environment
    - ```source /mnt/nas/Redmine/.virtualenvs/OLCRedmineAutomator/bin/activate```
3. Call automation script
    - ```python /mnt/nas/Redmine/OLCRedmineAutomator/api.py 2> /dev/null```
4. Enjoy Redmine automation


#### Setup
Running this program requires a *settings.py* file that is not included in
this repository for security. Here's a censored example of settings.py:

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