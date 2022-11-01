# %%
import logging
import datetime
import sys
import os
import boto3
import time
import yaml
import numpy as np
import pandas as pd

# %%
# Basic defaults
now = datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S.%f')

config_yaml_path = 'config.yml'
test_queries_path = 'test_queries'
log_path = 'logs'
csv_path = 'run_details'
show_recs = 3
status_failed = 'FAILED'
status_finished = 'FINISHED'

# %%
# Logging setup to display in console and save to file
log_formatter = logging.Formatter(
    '%(asctime)s:%(name)s:%(levelname)s:%(lineno)d:\t%(message)s')

file_handler = logging.FileHandler(os.path.join(log_path, f'{now}.log'))
file_handler.setFormatter(log_formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# %%
def read_args():
    '''
    Read arguments

    Flow
    1. Check if sufficient args are available
    '''

    logger.info('Check arguments')

    result = None

    # If insufficient arguments
    if len(sys.argv) != 3:
        logger.error('- Missing 2 arguments or more than 2 arguments provided')
        logger.error(
            '- Argument 1 - Configuration target in {config_yaml_path}')
        logger.error(
            '- Argument 2 - File name in {test_queries_path} directory')

    # If sufficient arguments
    else:
        logger.info(f'- Argument 1 - {sys.argv[1]}')
        logger.info(f'- Argument 2 - {sys.argv[2]}')

        result = {
            'config_yaml_target': sys.argv[1],
            'test_queries_file': sys.argv[2]
        }

    return result

# %%
def read_config(target):
    '''
    Read run configurations

    Flow
    1. Read all configurations
    2. Filter for configuration specified by args target
    3. Validation
       - Mandatory params
       - Valid data type
       - Valid values
    4. Apply default values for optional params
    '''

    logger.info('Check run configurations')

    result = None
    err = None

    param_checks = {
        'clusterid_or_workgroupname': {
            'mandatory': True,
            'data_type': str,
            'fixed_value': None,
            'num_lower': None,
            'num_upper': None,
            'default_value': None
        },
        'type': {
            'mandatory': True,
            'data_type': str,
            'fixed_value': [
                'provisioned',
                'serverless'
            ],
            'num_lower': None,
            'num_upper': None,
            'default_value': None
        },
        'dbname': {
            'mandatory': True,
            'data_type': str,
            'fixed_value': None,
            'num_lower': None,
            'num_upper': None,
            'default_value': None
        },
        'secret_arn': {
            'mandatory': True,
            'data_type': str,
            'fixed_value': None,
            'num_lower': None,
            'num_upper': None,
            'default_value': None
        },
        'attempts': {
            'mandatory': False,
            'data_type': int,
            'fixed_value': None,
            'num_lower': 1,
            'num_upper': 200,
            'default_value': 1
        },
        'wait_cycles': {
            'mandatory': False,
            'data_type': int,
            'fixed_value': None,
            'num_lower': 1,
            'num_upper': None,
            'default_value': 5
        },
        'sleep_time': {
            'mandatory': False,
            'data_type': int,
            'fixed_value': None,
            'num_lower': 1,
            'num_upper': None,
            'default_value': 5
        },
        'synchronous': {
            'mandatory': False,
            'data_type': bool,
            'fixed_value': None,
            'num_lower': None,
            'num_upper': None,
            'default_value': True
        },
        'silent': {
            'mandatory': False,
            'data_type': bool,
            'fixed_value': None,
            'num_lower': None,
            'num_upper': None,
            'default_value': True
        },
        'resultcache': {
            'mandatory': False,
            'data_type': bool,
            'fixed_value': None,
            'num_lower': None,
            'num_upper': None,
            'default_value': False
        },
        'mvrewrite': {
            'mandatory': False,
            'data_type': bool,
            'fixed_value': None,
            'num_lower': None,
            'num_upper': None,
            'default_value': False
        }
    }

    # Read all configurations
    if err is None:
        try:
            with open(config_yaml_path, 'r') as stream:
                result = yaml.safe_load(stream)
        except Exception as e:
            err = f'{e} while reading {config_yaml_path}'

    # Filter for configuration specified by args target
    if err is None:
        if target in result.keys():
            result = result[target]
        else:
            err = f'Missing target {target} in {config_yaml_path}'

    # Validate mandatory params
    if err is None:
        for param, details in param_checks.items():
            if param not in result.keys() and details['mandatory']:
                err = f'Missing mandatory param {param} in {config_yaml_path}'
                break

    # Validate data types
    if err is None:
        for param, details in param_checks.items():
            if param in result.keys() and not isinstance(result[param], details['data_type']):
                err = f'Missing valid data type {details["data_type"]} for {param} in {config_yaml_path}'
                break

    # Validate valid values
    if err is None:
        for param, details in param_checks.items():
            if param in result.keys() and details['fixed_value'] is not None and result[param] not in details['fixed_value']:
                err = f'Missing valid values {details["fixed_value"]} for {param} in {config_yaml_path}'
                break

    # Validate min numeric value
    if err is None:
        for param, details in param_checks.items():
            if param in result.keys() and details['num_lower'] is not None and result[param] < details['num_lower']:
                err = f'Missing greater than or equal to {details["num_lower"]} value for {param} in {config_yaml_path}'
                break
    
    # Validate max numeric value
    if err is None:
        for param, details in param_checks.items():
            if param in result.keys() and details['num_upper'] is not None and result[param] > details['num_upper']:
                err = f'Missing lesser than or equal to {details["num_upper"]} value for {param} in {config_yaml_path}'
                break

    # Apply default values for optional params
    if err is None:
        for param, details in param_checks.items():
            if param not in result.keys() and details['default_value'] is not None:
                result[param] = details['default_value']

    # Log run configurations or error
    if err is None:
        for k, v in result.items():
            logger.info(f'- {k.ljust(26)} : {v}')
    else:
        result = None
        logger.error(f'- {err}')

    return result

# %%
def read_queries(file_name):
    '''
    Read test queries

    Flow
    1. Read all queries specified by args file_name
    2. Validation
       - 1st level is list
       - 1st level items are str or list
       - 2nd level items are str, if 1st level item is list
    '''

    logger.info('Check test queries')

    file_path = os.path.join(test_queries_path, file_name)

    result = None
    err = None

    # Read all queries specified by args file_name
    if err is None:
        try:
            with open(file_path, 'r') as stream:
                result = yaml.safe_load(stream)
        except Exception as e:
            err = f'{e} while reading {file_path}'

    # Validate 1st level is list
    if err is None:
        if not isinstance(result, list):
            err = f'First level of {file_name} should be a list'

    # Validate 1st and 2nd level items
    if err is None:
        for first_level_item in result:

            # Validate 1st level items are str or list
            if not isinstance(first_level_item, list) and not isinstance(first_level_item, str):
                err = f'First level of {file_name} should contain list or str'
                break

            # If 1st level item is list, continue to validate 2nd level item
            if isinstance(first_level_item, list):
                for second_level in first_level_item:

                    # Validate 2nd level items are str
                    if not isinstance(second_level, str):
                        err = f'Second level of {file_name} should contain str'
                        break
                else:
                    continue
                break

    # Log number of test queries or error
    if err is None:
        logger.info(f'- {len(result)} test to run')
    else:
        result = None
        logger.error(f'- {err}')

    return result

# %%
def batch_test_queries(run_config, first_level_item):
    '''
    Batch related test queries together

    Flow
    1. Prefix enable_result_cache_for_session on/off
    2. Prefix mv_enable_aqmv_for_session on/off
    3. Add test queries
    '''

    result = []

    # Prefix enable_result_cache_for_session on/off
    if run_config['resultcache']:
        result += ['set enable_result_cache_for_session to on;']
    else:
        result += ['set enable_result_cache_for_session to off;']

    # Prefix mv_enable_aqmv_for_session on/off
    if run_config['mvrewrite']:
        result += ['set mv_enable_aqmv_for_session to on;']
    else:
        result += ['set mv_enable_aqmv_for_session to off;']

    # Add test queries
    if isinstance(first_level_item, list):
        result += first_level_item
    else:
        result += [first_level_item]

    # Log each test query in batch
    for each_query in result:
        logger.info(f'- {each_query.strip()}')

    return result

# %%
def run_batch_execute_statement(redshift_data_api_client, run_config, query):
    '''
    Send batched test queries to Redshift data api

    Flow
    1. If provisioned, supply ClusterIdentifier
    2. If serverless, supply WorkgroupName
    '''

    result = None

    try:
        if run_config['type'] == 'provisioned':
            result = redshift_data_api_client.batch_execute_statement(
                Database=run_config['dbname'],
                SecretArn=run_config['secret_arn'],
                Sqls=query,
                ClusterIdentifier=run_config['clusterid_or_workgroupname']
            )
        else:
            result = redshift_data_api_client.batch_execute_statement(
                Database=run_config['dbname'],
                SecretArn=run_config['secret_arn'],
                Sqls=query,
                WorkgroupName=run_config['clusterid_or_workgroupname']
            )
    except Exception as e:
        logger.error(e)

    return result

# %%
def run_describe_statement(redshift_data_api_client, data_api_id):
    '''
    Get sent batched test queries status from Redshift data api

    Flow
    1. Get sent batched test queries status
    2. Convert duration from nanoseconds to seconds
    '''

    result = None

    try:
        result = redshift_data_api_client.describe_statement(Id=data_api_id)

        result['Duration'] = result['Duration']/1000000000

        for substatement in result['SubStatements']:
            substatement['Duration'] = substatement['Duration']/1000000000

    except Exception as e:
        logger.error(e)

    return result

# %%
def run_sync_attempts(redshift_data_api_client, run_config, query):
    '''
    Synchronously run all attempts

    Flow
    1. For each required attempt
       - Send query
       - Poll until query status is FAILED or FINISHED or wait_cycles reached
         - If FAILED, log error
         - If FINISHED, log success
         - If wait_cycles reached, log timeout
       - Move to next attempt
    '''

    logger.info('Attempts')

    result = {}
    attempt = 1

    # For each required attempt
    while attempt <= run_config['attempts']:

        # Send query
        result[attempt] = run_batch_execute_statement(
            redshift_data_api_client, run_config, query)

        if result[attempt] is None:
            attempt += 1
            continue

        # Poll until query status is FAILED or FINISHED or wait_cycles reached
        wait_cycle = 0

        while wait_cycle < run_config['wait_cycles']:

            result[attempt] = run_describe_statement(
                redshift_data_api_client, result[attempt]['Id'])

            attempt_status = result[attempt]['Status']
            attempt_duration = result[attempt]['Duration']
            attempt_has_result = result[attempt]['HasResultSet']

            status_msg = f'- {attempt}, {attempt_status}, {round(attempt_duration, 4):.4f} s'

            if not run_config['silent']:
                logger.info(f'- {result[attempt]}')

            # If FAILED, log error
            if attempt_status == status_failed:
                logger.info(
                    f'{status_msg}, {result[attempt]["Error"]}')
                break

            # If FINISHED, get results/records and log success
            if attempt_status == status_finished:
                if attempt_has_result:
                    logger.info(f'{status_msg}, Has result')
                else:
                    logger.info(f'{status_msg}, No result')
                break

            time.sleep(run_config['sleep_time'])
            wait_cycle += 1

        # If wait_cycles reached, log timeout
        if wait_cycle >= run_config['wait_cycles']:
            logger.info(f'{status_msg}, wait_cycles limit reached')

        attempt += 1

    return result

# %%
def run_async_attempts(redshift_data_api_client, run_config, query):
    '''
    Asynchronously run all attempts

    Flow
    1. For each required attempt
       - Send query
       - Move to next attempt
    2. For all sent attempts
       - Poll until all queries status are FAILED or FINISHED or wait_cycles reached
    3. For all sent attempts
       - If FAILED, log error
       - If FINISHED, log success
       - If wait_cycles reached, log timeout
    '''

    logger.info('Attempts')

    result = {}
    attempt = 1

    # For each required attempt
    while attempt <= run_config['attempts']:

        # Send query
        result[attempt] = run_batch_execute_statement(
            redshift_data_api_client, run_config, query)

        if result[attempt] is None:
            attempt += 1
            continue

        logger.info(f'- {attempt}, SUBMITTED')

        attempt += 1

    # Poll until all queries status are FAILED or FINISHED or wait_cycles reached
    wait_cycle = 0

    while wait_cycle < run_config['wait_cycles']:

        for attempt in result.keys():

            if result[attempt] is not None and result[attempt].get('Status') not in [status_failed, status_finished]:

                result[attempt] = run_describe_statement(
                    redshift_data_api_client, result[attempt]['Id'])

                attempt_status = result[attempt]['Status']
                attempt_has_result = result[attempt]['HasResultSet']

                if not run_config['silent']:
                    logger.info(f'- {result[attempt]}')

        all_status = [
            attempt['Status']
            for attempt in result.values()
            if attempt is not None
        ]

        cnt_status = {
            status: all_status.count(status)
            for status in set(all_status)
        }

        if cnt_status:
            logger.info(
                f'- {", ".join(f"{k}: {v}" for k, v in cnt_status.items())}')

        if all(elem in [status_failed, status_finished] for elem in cnt_status.keys()):
            break

        time.sleep(run_config['sleep_time'])
        wait_cycle += 1

    # Log each attempt
    for attempt in result.keys():

        if result[attempt] is None:
            break

        attempt_status = result[attempt]['Status']
        attempt_duration = result[attempt]['Duration']
        attempt_has_result = result[attempt]['HasResultSet']

        status_msg = f'- {attempt}, {attempt_status}, {round(attempt_duration, 4):.4f} s'

        # If FAILED, log error
        if attempt_status == status_failed:
            logger.info(f'{status_msg}, {result[attempt]["Error"]}')

        # If FINISHED, log success
        if attempt_status == status_finished:
            if attempt_has_result:
                logger.info(f'{status_msg}, Has result')
            else:
                logger.info(f'{status_msg}, No result')

        # If wait_cycles reached, log timeout
        if attempt_status not in [status_failed, status_finished] and wait_cycle >= run_config['wait_cycles']:
            logger.info(f'{status_msg}, wait_cycles limit reached')

    return result

# %%
def calculate_duration_stats(attemps):
    '''
    Calculate aggregated stats of duration

    Flow
    1. Calculate
       - Minimun
       - Maximum
       - Average 
    '''

    duration = {
        'Total': [attempt['Duration'] for attempt in attemps.values()],
        'Last query': [attempt['SubStatements'][-1]['Duration'] for attempt in attemps.values()]
    }

    for k, v in duration.items():
        if v:
            logger.info(f'{k} duration stats ({status_finished})')

            logger.info(
                f'- Min: {round(np.min(v), 3):.3f} s')

            logger.info(
                f'- Max: {round(np.max(v), 3):.3f} s')

            logger.info(
                f'- Avg: {round(np.average(v), 3):.3f} s')

# %%
def show_sample_records(redshift_data_api_client, attempts):
    '''
    Show first N records of first successful attempt

    Flow
    1. For first attempt with results
       - Get records
       - Fit records into Pandas dataframe
       - Show first 3 records
    '''

    if attempts:

        logger.info('Sample records')

        for v in attempts.values():
            if v['HasResultSet']:
                recs = redshift_data_api_client.get_statement_result(
                    Id=v['SubStatements'][-1]['Id'])

                header_list = [col['name'] for col in recs['ColumnMetadata']]
                record_list = []

                for row in recs['Records']:
                    row_data = [v for col in row for v in col.values()]
                    record_list.append(row_data)

                df = pd.DataFrame(record_list, columns=header_list)

                logger.info(f'\n{df.head(show_recs)}')
            break

# %%
def run_details_output(attempts_df, test_num, attempts):
    '''
    Save substatements from data api describe_statement as CSV to capture Redshift Query ID

    Flow
    1. For each test
       - For each attempt
         - Fit details into Pandas dataframe
         - Save Pandas dataframe as CSV
    '''

    for attempt in attempts.keys():

        if attempts[attempt] is not None:

            for subStatement in attempts[attempt]['SubStatements']:
                subStatement['QueryString'] = subStatement['QueryString'].replace(
                    '\n', ' ')

            attempt_df = pd.DataFrame(attempts[attempt]['SubStatements'])

            attempt_df.insert(loc=0, column='Attempt', value=attempt)
            attempt_df.insert(loc=0, column='Test', value=test_num+1)

            attempts_df = pd.concat([attempts_df, attempt_df])

            attempts_df.to_csv(os.path.join(
                csv_path, f'{now}.csv'), index=False, header=True)

    return attempts_df

# %%
def main():

    read_args_result = read_args()

    if read_args_result is not None:

        read_config_result = read_config(
            read_args_result['config_yaml_target'])

        if read_config_result is not None:

            read_queries_result = read_queries(
                read_args_result['test_queries_file'])

            if read_queries_result is not None:

                attempts_df = pd.DataFrame()

                for test_num in range(len(read_queries_result)):

                    logger.info(f'Test {test_num+1}')

                    batch_test_queries_result = batch_test_queries(
                        read_config_result, read_queries_result[test_num])

                    redshift_data_api_client = boto3.client('redshift-data')

                    run_attempts_result = {}

                    if read_config_result['synchronous']:
                        run_attempts_result = run_sync_attempts(
                            redshift_data_api_client, read_config_result, batch_test_queries_result)

                    if not read_config_result['synchronous']:
                        run_attempts_result = run_async_attempts(
                            redshift_data_api_client, read_config_result, batch_test_queries_result)

                    success_run_attempts_result = {
                        k: v
                        for k, v in run_attempts_result.items()
                        if v is not None and v['Status'] == status_finished
                    }

                    calculate_duration_stats(success_run_attempts_result)

                    show_sample_records(
                        redshift_data_api_client, success_run_attempts_result)

                    attempts_df = run_details_output(
                        attempts_df, test_num, run_attempts_result)

                    redshift_data_api_client.close()


# %%
if __name__ == '__main__':
    main()
