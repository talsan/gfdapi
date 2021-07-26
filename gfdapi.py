import config
import requests
import datetime
import os
import getpass

def gfd_auth(username=config.username, password=config.password):
    """
    Pulls a GFD API token and stores it as an environmental variable.

    Parameters
        username: GFD-approved email address.

        password: Password for GFD-approved email address.
    """
    if username is None:
        username = getpass.getpass('Please enter your GFD Finaeon username: ')

    if password is None:
        password = getpass.getpass('Please enter your GFD Finaeon password: ')

    url = 'https://api.globalfinancialdata.com/login/'
    parameters = {'username': username, 'password': password}
    resp = requests.post(url, data=parameters)

    # check for unsuccessful API returns
    if resp.status_code != 200:
        raise ValueError('GFD API request failed with HTTP status code %s' % resp.status_code)

    json_content = resp.json()
    os.environ['GFD_API_TOKEN'] = json_content['token'].strip('"')
    print("GFD API token recieved at %s" % str(datetime.datetime.now()))