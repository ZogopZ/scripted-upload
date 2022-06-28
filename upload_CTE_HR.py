import shlex
from multiprocessing import Pool
from pprint import pprint
# from sys import exit
from datetime import datetime as dt
import getpass
import glob
import hashlib
import json
import nest_asyncio
import os
# import pandas as pd
# import re
import subprocess
import xarray as xr
import paramiko
import constants as const

nest_asyncio.apply()

reason = 'CTE-HR'
archive_in = 'archive_in_nc.json'
archive_out = dict()
static_stilt = dict()
generated_files_directory = 'generated-files'
cookies = 'cookies.txt'
arrow = '\u2192'
gear = '\u2699 '
archive = '\U0001F4DA'
check = '\u2713'
# Todo: start comment
# Todo: Uncomment this line.
# nc_files_directory = os.path.join(generated_files_directory, 'nc-files')
# Todo: Remove this line.
nc_files_directory = os.path.join('../../../ctehires/upload/remco/')
# Todo: This is the original way to list nc files.
# nc_files = os.path.join(nc_files_directory, 'anthropogenic.persector.*.nc')
# Todo: below
nc_files = os.path.join(nc_files_directory, '*2022*.nc')
# end Todo
# Todo: New way of listing remote files.
# command = 'ls /home/remco/data/anthropogenic.persector.*.nc'
# nc_files = ssh_handler(command=command)
# Todo: end comment
json_files_directory = os.path.join(generated_files_directory, 'json-files')


def get_static_data(cli_flag):
    global archive_out
    user_input = None
    while True:
        if cli_flag:
            # Archive in file does not exist.
            if not os.path.exists(archive_in):
                print(f'- {archive} Creating file {archive_in}... ', end='')
                with open(file=archive_in, mode='w+'):
                    pass
            # Archive in file exists.
            else:
                # Archive in is empty.
                if os.stat(archive_in).st_size == 0:
                    print(f'- {archive} File {archive_in} exists but it is '
                          f'empty. Converting it to json... {check}')
                    with open(file=archive_in, mode='w') as archive_in_handle:
                        json.dump(dict(), archive_in_handle, indent=4)
                print(f'- {archive} Reading static {reason} data from file '
                      f'{archive_in}... ', end='')
                with open(file=archive_in, mode='r') as archive_in_handle:
                    archive_out = json.load(archive_in_handle)
            print(check)
            return archive_out
        elif not cli_flag:
            break
        elif user_input == 'e':
            exit('User exited')
        else:
            continue
    return {}


def store_current_archive():
    print(f'- {gear} Storing json information to {archive_in}...')
    if os.path.exists('archive_in_nc.json'):
        user_input = input(
            '\tBe carefull!!! You are trying to overwrite an already existing '
            'archive_in_nc.json file.\n\tAre you sure you want to overwrite?(Y/n)')
    else:
        user_input = 'Y'
    if user_input == 'Y':
        with open(file='archive_in_nc.json', mode='w') as archive_out_handle:
            json.dump(archive_out, archive_out_handle, indent=4)
    intermediate_output = 'Overwrite complete.' \
        if user_input == 'Y' else 'Skipping overwrite.'
    print(f'- {gear} {intermediate_output} {check}')
    return


def archive_files():
    """Archive file paths, names, and other information if needed."""
    print(f'- {gear} Archiving system information of .nc files... ', end='')
    for file_path in glob.glob(nc_files, recursive=True):
        file_name = file_path.split('/')[-1]
        year = file_name.split('.')[-2][0:4]
        month = file_name.split('.')[-2][4:6]
        dataset_type, data_object_spec = get_file_info(file_name)
        base_key = file_name.rstrip('.nc')
        archive_out[base_key] = dict({
            'file_path': file_path,
            'file_name': file_name,
            'dataset_type': dataset_type,
            'data_object_spec': data_object_spec,
            'month': month,
            'year': year,
            'try_ingest_command': build_try_ingest_command(file_path=file_path,
                                                           data_object_spec=data_object_spec)
        })
    print(check)
    return


def get_file_info(file_name=None):
    dataset_type = None
    data_object_spec = None
    if 'persector' in file_name:
        dataset_type = 'anthropogenic emissions per sector'
        data_object_spec = const.ANTHROPOGENIC_OBJECT_SPEC
    elif 'anthropogenic' in file_name:
        dataset_type = 'anthropogenic emissions'
        data_object_spec = const.ANTHROPOGENIC_OBJECT_SPEC
    elif 'nep' in file_name:
        dataset_type = 'biospheric fluxes'
        data_object_spec = const.BIOSPHERIC_OBJECT_SPEC
    elif 'fire' in file_name:
        dataset_type = 'fire emissions'
        data_object_spec = const.FIRE_OBJECT_SPEC
    elif 'ocean' in file_name:
        dataset_type = 'ocean fluxes'
        data_object_spec = const.OCEAN_OBJECT_SPEC
    return dataset_type, data_object_spec


def build_try_ingest_command(file_path=None, data_object_spec=None):
    """Build the try-ingest command for each file."""
    dataset = xr.open_dataset(file_path)
    variable_list = list(dataset.data_vars)
    variables = ', '.join(f'"{variable}"' for variable in variable_list)
    try_ingest_command = \
        ["curl", "-s", "-G",
         "--data-urlencode", f"\"specUri={data_object_spec}\"",
         "--data-urlencode", f"\'varnames=[{variables}]\'",
         "--upload-file", file_path,
         "https://data.icos-cp.eu/tryingest"]
    return shlex.split(' '.join(try_ingest_command))


def archive_json():
    """Generates standalone .json files and adds to archive.

    Generates the standalone .json files for each data file and updates
    the archive with the regenerated json content. This function needs
    to be rerun each time we need to change something in the meta-data.
    If we decide to rerun this then it is mandatory that we also
    overwrite the `archive_in_nc.json` file using the function
    `store_current_archive()` at the end of the script.

    """
    print(f'- {gear} Archiving meta-data... ', end='')
    for base_key, base_info in archive_out.items():
        dataset = xr.open_dataset(base_info['file_path'])
        creation_date = dt.strptime(dataset.creation_date, '%Y-%m-%d %H:%M')
        base_info['json'] = dict({
            'fileName': base_info['file_name'],
            'hashSum': get_hash_sum(file_path=base_info['file_path']),
            'isNextVersionOf': [],
            'objectSpecification': base_info['data_object_spec'],
            'references': {
                'keywords': [
                    'carbon flux'
                ],
                'licence': 'http://meta.icos-cp.eu/ontologies/cpmeta/icosLicence'
            },
            'specificInfo': {
                'description': dataset.comment,
                'production': {
                    'contributors': [
                        'http://meta.icos-cp.eu/resources/people/Ingrid_van%20der%20Laan-Luijkx',
                        'http://meta.icos-cp.eu/resources/people/Naomi_Smith',
                        'http://meta.icos-cp.eu/resources/people/Remco_de_Kok',
                        'http://meta.icos-cp.eu/resources/people/Wouter_Peters'
                    ],
                    'creationDate':
                        creation_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'creator': 'http://meta.icos-cp.eu/resources/people/Auke_van_der_Woude',
                    'hostOrganization': 'http://meta.icos-cp.eu/resources/organizations/WUR',
                    'sources': [],
                },
                'spatial': 'http://meta.icos-cp.eu/resources/latlonboxes/ctehrEuropeLatLonBox',
                'temporal': {
                    'interval': {
                        'start': dataset.time[0].dt.strftime('%Y-%m-%dT%H:%M:%SZ').item(),
                        'stop': dataset.time[-1].dt.strftime('%Y-%m-%dT%H:%M:%SZ').item(),
                    },
                    'resolution': 'hourly'
                },
                'title': f'High-resolution, near-real-time fluxes over Europe '
                         f'from CTE-HR: {base_info["dataset_type"]} '
                         f'{base_info["year"]}-{base_info["month"]}',
                'variables': [variable for variable in dataset.data_vars],
            },
            'submitterId': 'CP'
        })
        json_file_name = base_key + '.json'
        json_file_path = os.path.join(json_files_directory, json_file_name)
        base_info['json_file_path'] = json_file_path
        with open(file=json_file_path, mode='w+') as json_file_handle:
            json.dump(base_info['json'], json_file_handle, indent=4)
    print(check, flush=True)
    return


def get_hash_sum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file=file_path, mode='rb') as file_handle:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: file_handle.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def ssh_handler(command=None):
    """Executes command in a remote host."""
    # Todo: start comment
    # Todo: Some of this information should be classified.
    ssh_key_location = 'C:\\Users\\Zois\\Desktop\\zois\\keys_Z\\zKey.ppk'
    key = paramiko.RSAKey.from_private_key_file(filename=ssh_key_location,
                                                password='Monogiah5')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname='fsicos3.lunarc.lu.se',
                username='zois',
                pkey=key,
                port=60575)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(command)
    path_list = ssh_stdout.read().decode(encoding='utf-8').split('\n')
    if path_list[-1] == '':
        path_list.pop(-1)
    return path_list
    # Todo: end comment


def archive_json_curl():
    print(f'- {gear} Archiving json curl commands for meta-data and data '
          f'upload... ', end='')
    for base_key, base_info in archive_out.items():
        metadata_curl_command_list = \
            ["curl", "-s", "--cookie", "cookies.txt", "-H",
             '"Content-Type: application/json"',
             "-X", "POST",
             "-d", f"@{base_info['json_file_path']}",
             "https://meta.icos-cp.eu/upload"]
        data_curl_command_list = \
            ["curl", "-s", "-v", "--cookie", "cookies.txt", "-H",
             '"Transfer-Encoding: chunked"',
             "--upload-file", f"{base_info['file_path']}",
             "https://data.icos-cp.eu/objects/file_id"]
        base_info['curl'] = dict({
            'metadata_using_bash': ' '.join(metadata_curl_command_list),
            'metadata_using_python': metadata_curl_command_list,
            'data_using_bash': ' '.join(data_curl_command_list),
            'data_using_python': data_curl_command_list
        })
    print(check)
    return


def upload():
    print(f'- {gear} Uploading meta-data '
          f'(Expecting {len(archive_out.items())} checks)... ', end='')
    for base_key, base_info in archive_out.items():
        archive_out[base_key] = upload_metadata(base_info=base_info)
        # upload_data(base_info=base_info)
    print('')
    return


def check_permissions():
    valid_cookie = False
    print(f'- {gear} Authenticating user...')
    while not valid_cookie:
        if os.path.exists(cookies):
            user_input = input(
                f'\tFile {cookies} exists in the current working directory:\n'
                f'\t- Continue with current cookie {arrow} y\n'
                f'\t- Regenerate cookie {arrow} r\n'
                f'\tPlease enter a selection (y/r): ')
            if user_input == 'r':
                valid_cookie = True if curl_cookie() else False
            elif user_input == 'y':
                valid_cookie = True if validate_cookie() else False
                print(validate_cookie())
        else:
            curl_cookie()
    return


def curl_cookie():
    validation = False
    email = input('\t  Please enter your e-mail: ')
    password = getpass.getpass(prompt='\t  Please enter your password: ')
    cookie_command_list = ['curl', '-s', '--cookie-jar', 'cookies.txt',
                           '--data', f'"mail={email}&password={password}"',
                           'https://cpauth.icos-cp.eu/password/login']
    cookie_process = subprocess.Popen(' '.join(cookie_command_list),
                                      stdout=subprocess.PIPE, shell=True)
    cookie_output, cookie_error = cookie_process.communicate()
    print(f'\tSUCCESS: {cookie_output},\n\tERROR: {cookie_error}')
    if not cookie_error:
        print('\tAuthentication complete!')
        validation = True
    else:
        print(
            '\t\tWARNING! An error has occured during user authentication...',
            '\n')
        input('')
    return validation


def validate_cookie():
    validation = False
    url = 'https://cpauth.icos-cp.eu/whoami'
    cookie_command_list = ['curl', '-s', '--cookie', 'cookies.txt',
                           'https://cpauth.icos-cp.eu/whoami']
    cookie_process = subprocess.Popen(' '.join(cookie_command_list),
                                      stdout=subprocess.PIPE, shell=True)
    cookie_output, cookie_error = cookie_process.communicate()
    if not cookie_error:
        validation = True
    print(cookie_output, cookie_error)
    return validation


def upload_metadata():
    print(f'- {gear} Uploading meta-data '
          f'(Expecting {len(archive_out.items())} checks)... ', end='')
    for base_key, base_info in archive_out.items():
        # archive_out[base_key] = upload_metadata(base_info=base_info)
        # upload_data(base_info=base_info)
        meta_process = subprocess.Popen(base_info['curl']['metadata_using_bash'],
                                        stdout=subprocess.PIPE, shell=True)
        meta_output, meta_error = meta_process.communicate()
        meta_output = meta_output.decode('utf-8')
        if not meta_error and 'https://data.icos-cp.eu/objects' in meta_output:
            file_data_url = meta_output
            base_info['file_data_url'] = file_data_url
            base_info['file_metadata_url'] = file_data_url.replace('data', 'meta')
            base_info['curl']['data_using_python'][-1] = file_data_url
            base_info['curl']['data_using_bash'] = \
                ' '.join(base_info['curl']['data_using_python'])
            # print(f'\tSuccessfully uploaded meta-data. Check them out here: '
            #       f'{base_info["file_metadata_url"]}')
            print(f'{check}', end='')
        else:
            print('\t\tWARNING! An error has occurred during meta-data upload...')
            print('\t\t' + base_info['file_name'])
            input('You can press ctrl+c to stop this program or press any '
                  'other key to continue... ')
    print('')
    return


def upload_data():
    print(f'- {gear} Uploading data '
          f'(Expecting {len(archive_out.items())} checks)... ', end='')
    for base_key, base_info in archive_out.items():
        if base_info['json']['hashSum'] == get_hash_sum(base_info['file_path']):
            print(f'{check}', end='')
        else:
            print(f'\t\tWARNING! An error has occurred during hash-sum '
                  f'validation for file {base_info["file_name"]}')
            input('You can press ctrl+c to stop this program or press any '
                  'other key to continue...')
        data_process = subprocess.Popen(base_info['curl']['data_using_bash'],
                                        stdout=subprocess.PIPE, shell=True)
        data_output, data_error = data_process.communicate()
        if not data_error:
            print(f'{check}', end='')
        else:
            print('\t\tWARNING! An error has occurred during data upload...')
            print('\t\t' + base_info['file_name'])
            input('You can press ctrl+c to stop this program or press any '
                  'other key to continue... ')
    print('')
    return


def try_ingest_2():
    """Tests ingestion of provided files to the Carbon Portal."""
    print(f'- {gear} Trying ingestion of .nc data files '
          f'(This might take a while) '
          f'Expecting {len(glob.glob(nc_files))} checks)... ', end='')
    command_list = list()
    for key, value in archive_out.items():
        command_list.append(value['try_ingest_command'])
    files = nc_files
    files_list = glob.glob(files)
    lists_to_process = list()
    # Break up the list into smaller lists of 5 items per sublist.
    # This way we can produce user output faster, so that the person
    # who executes this script has an idea of what's happening.
    for j in range(int(len(command_list) / 5)):
        lists_to_process.append([])
        for i in range(5):
            index = 5 * j + i
            if index > len(command_list):
                break
            lists_to_process[j].append(command_list[index])
    for item_list in lists_to_process:
        # Create a process for each item in the sublist.
        with Pool(processes=len(item_list)) as pool:
            # Each item in the sublist is passed as an argument to
            # `execute_item()` function.
            results = pool.map(execute_item, item_list)
            # Uncomment this line to print the results of the try-ingest.
            # [print(result) for result in results]
    print('')
    return


def execute_item(item):
    """Used from processes spawned by try_ingest_2()."""
    file = item[8].split('/')[-1]
    try_ingest_command = item
    process = subprocess.Popen(try_ingest_command, stdout=subprocess.PIPE)
    output, error = process.communicate()
    output = output.decode('utf-8')
    if not error and all(value in output for value in ['min', 'max']):
        print(f'{check}', end='', flush=True)
    else:
        print(f'\nWARNING! An error has occurred during try-ingest '
              f'for file {file}')
    return f'\tFor file: {file}\n\t\t-> I got this output: {output}\n' \
           f'\t\t-> and ingestion error = {error}\n'


def handler():
    # archive_files()  # 1. Archive data file names paths, and info.
    # try_ingest_2()  # 2. Try ingesting data files to the carbon portal.
    # archive_json()  # 3. Generate json files and append to archive.
    # archive_json_curl()  # 4. Create and archive data & meta commands.
    # check_permissions()  # 5. Get your cookie using your credentials.
    # upload_metadata()  # 6. Upload meta-data.
    # upload_data()  # 7. Upload data.
    return


if __name__ == '__main__':
    archive_out = get_static_data(cli_flag=True)
    if not archive_out:
        print(f'- {archive} Hey {archive_in} is empty...')
        handler()
    else:
        print(f'- {archive} {archive_in} is not empty...')
        handler()
    store_current_archive()
