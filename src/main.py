"""
Creates a new event trigger for a given configuration with the same tables as the last one and reset last-run time

"""

import os
import json
from datetime import datetime

import requests
import pandas as pd
from keboola import docker


def get_latest_trigger_tables(configuration_id, url, headers):
    """Returns the list of tables from the latest event trigger."""

    # Get all triggers info
    url_g = url + f'/?component=orchestrator&configurationId={configuration_id}'
    response_g = requests.request("GET", url_g, headers=headers).json()

    # Find the last trigger id
    trigger_ids = [int(item.get('id')) for item in response_g]
    last_trigger = max(trigger_ids)

    # Find last trigger tables
    for item in response_g:
        if item['id'] == str(last_trigger):
            trigger_tables0 = item['tables']
            trigger_tables = [table.get('tableId') for table in trigger_tables0]
    return trigger_tables


def delete_all_triggers(configuration_id, url, headers):
    """Deletes all triggers of the configuration."""
    # Find all trigger ids
    url_g = url + f'/?component=orchestrator&configurationId={configuration_id}'
    response_g = requests.request("GET", url_g, headers=headers).json()

    column_names = ['CONFIGURATION_ID', 'EVENT', 'TRIGGER_ID', 'TRIGGER_INFO']
    deleted_triggers = pd.DataFrame(columns=column_names)

    for item in response_g:
        output_dict = {'CONFIGURATION_ID': item['configurationId'], 'EVENT': 'DELETED', 'TRIGGER_ID': item['id'],
                       'TRIGGER_INFO': str(item)}
        deleted_triggers = deleted_triggers.append(pd.DataFrame(data=output_dict, index=[0]))

    # Delete all triggers
    for id in deleted_triggers.TRIGGER_ID:
        url_g = url + "/" + str(id)
        response_g = requests.request("DELETE", url_g, headers=headers)

    return (deleted_triggers)


def create_new_trigger(configuration_id, url, headers, token_id, tables):
    """Creates a new trigger for the configuration with the selected trigger tables."""

    column_names = ['CONFIGURATION_ID', 'EVENT', 'TRIGGER_ID', 'TRIGGER_INFO']
    created_trigger = pd.DataFrame(columns=column_names)

    trigger_tables_values = '&tableIds%5B0%5D='.join(tables)
    values = f'runWithTokenId={token_id}&component=orchestrator&configurationId={configuration_id}&coolDownPeriodMinutes=5&tableIds%5B1%5D={trigger_tables_values}'
    response = requests.request("POST", url, headers=headers, data=values)

    item = json.loads(response.text)

    output_dict = {'CONFIGURATION_ID': item['configurationId'], 'EVENT': 'CREATED', 'TRIGGER_ID': item['id'],
                   'TRIGGER_INFO': str(item)}

    created_trigger = created_trigger.append(pd.DataFrame(data=output_dict, index=[0]))

    return (created_trigger)


def main():
    # Get parameters
    datadir = os.getenv('KBC_DATADIR', '/data/')
    conf = docker.Config(datadir)
    params = conf.get_parameters()
    path = f'{os.getenv("KBC_DATADIR")}out/tables/results.csv'

    configuration_id = params.get('configuration_id')
    my_token_id = params.get('my_token_id')
    headers = {
        'X-StorageApi-Token': params.get('#X-StorageApi-Token'),
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    url = params.get('url')

    # Find latest trigger tables
    trigger_tables = get_latest_trigger_tables(configuration_id=configuration_id, url=url, headers=headers)

    # Delete all triggers
    del_triggers = delete_all_triggers(configuration_id=configuration_id, url=url, headers=headers)

    # Create a mew trigger
    created_trigger = create_new_trigger(configuration_id=configuration_id, url=url, headers=headers,
                                         token_id=my_token_id,
                                         tables=trigger_tables)

    output = del_triggers.append(created_trigger)
    output['TIMESTAMP'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output.to_csv(path, index=False)


if __name__ == '__main__':
    main()
