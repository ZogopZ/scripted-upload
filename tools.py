import json
import os
import constants
import subprocess


def read_json(path=None):
    """Read json file and load content to dictionary"""
    with open(file=path, mode='r') as json_handle:
        return json.load(json_handle)


def write_json(path=None, content=None):
    """Write dictionary to json file"""
    with open(file=path, mode='w+') as json_handle:
        json.dump(content, json_handle, indent=4)
    return


def regenerate_full_archive(components_dir=None):
    """Combine the archives of each individual component"""
    full_archive = dict()
    for file in os.listdir(path=components_dir):
        json_path = os.path.join(components_dir, file)
        component_key = file.split('_', maxsplit=3)[-1].split('.')[0]
        full_archive[component_key] = read_json(path=json_path)
    write_json(path='full_archive.json', content=full_archive)
    return


def check_permissions():
    valid_cookie = False
    print(f'- {constants.GEAR_ICON} Authenticating user...')
    while not valid_cookie:
        if os.path.exists(constants.COOKIES):
            user_input = input(
                f'\tFile {constants.COOKIES} exists in the current working directory:\n'
                f'\t- Continue with current cookie {constants.ARROW_ICON} y\n'
                f'\t- Regenerate cookie {constants.ARROW_ICON} r\n'
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

