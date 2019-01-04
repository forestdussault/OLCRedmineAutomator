# User Guide

User guide available at: https://olc-bioinformatics.github.io/redmine-docs/


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
sudo service supervisor restart
```

To account for intermittent connectivity issues with the Redmine instances,
there is a cronjob that regularly restarts the supervisor service.
This may be edited on the head node with the following command:
```
sudo crontab -e
```

#### Manual operation
1. Log into the head node (ubuntu@192.168.1.5)
2. Activate the virtual environment
    - ```source /mnt/nas2/redmine/applications/.virtualenvs/OLCRedmineAutomator/bin/activate```
3. Call automation script
    - ```python /mnt/nas2/redmine/applications/OLCRedmineAutomator/api.py 2> /dev/null```
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
