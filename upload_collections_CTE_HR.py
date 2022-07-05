import constants
from pprint import pprint
import tools
import os
import subprocess


monthly_collections = dict()
yearly_collection = dict()


def extract_monthly_collections(archive=None):
    # Collect members from each component's monthly dataset.
    # Each new collection will contain 5 components per month.
    for monthly_key, monthly_component in archive.items():
        component, reconstructed_key = monthly_key.rsplit('.', maxsplit=1)
        if reconstructed_key not in monthly_collections.keys():
            monthly_collections[reconstructed_key] = {
                'members': dict(),
                'sorted_members': list()
            }
        monthly_collections[reconstructed_key]['members'][component] = monthly_component['file_metadata_url']
    # Archive meta-data for each collection.
    for collection_key, collection_info in monthly_collections.items():
        year, month = collection_key[0:4], collection_key[4:6]
        sorted_members = list(collection_info['members'][key] for key in sorted(collection_info['members'].keys()))
        collection_info['sorted_members'] = sorted_members
        collection_info['json'] = {
            "description":
                f"Monthly collection of hourly CO2 fluxes for {year}-{month}, containing hourly "
                f"estimates of biospheric fluxes, anthropogenic emissions (total and per sector), "
                f"GFAS fire emissions and Jena CarboScope ocean fluxes, all re-gridded to match "
                f"the resolution of the biospheric fluxes.\n\nNet ecosystem productivity (gross "
                f"primary production minus respiration). Positive fluxes are emissions, negative "
                f"mean uptake. These fluxes are the result of the SiB4 (Version 4.2-COS, hash "
                f"1e29b25, https://doi.org/10.1029/2018MS001540) biosphere model, driven by ERA5 "
                f"reanalysis data at a 0.5x0.5 degree resolution. The NEP per plant functional "
                f"type are distributed according to the high resolution CORINE land-use map "
                f"(https://land.copernicus.eu/pan-european/corine-land-cover), and aggregated to "
                f"CTE-HR resolution.\n\n"
                f"Anthropogenic emissions include contributions from public power, industry, "
                f"households, ground transport, aviation, shipping, and calcination of cement. "
                f"Our product does not include carbonation of cement and human respiration. Public "
                f"power is based on ENTSO-E data (https://transparency.entsoe.eu/), Industry, "
                f"Ground transport, Aviation, and Shipping is based on Eurostat data "
                f"(https://ec.europa.eu/eurostat/databrowser/). Household emissions are based on a "
                f"degree-day model, driven by ERA5 reanalysis data. Spatial distributions of the "
                f"emissions are based on CAMS data (https://doi.org/10.5194/essd-14-491-2022). "
                f"Cement emissions are taken from GridFED V.2021.3 "
                f"(https://zenodo.org/record/5956612#.YoTmvZNBy9F).\n\n"
                f"GFAS fire emissions (https://doi.org/10.5194/acp-18-5359-2018), re-gridded to "
                f"match the resolution of the biosphere, fossil fuel, and ocean fluxes of the CTE-HR "
                f"product. Please always cite the original GFAS data when using this file, and use "
                f"the original data when only fire emissions are required. For more information, see "
                f"https://doi.org/10.5281/zenodo.6477331 Contains modified Copernicus Atmosphere "
                f"Monitoring Service Information [2020].\n\nOcean fluxes, based on a climatology of "
                f"Jena CarboScope fluxes (https://doi.org/10.17871/CarboScope-oc_v2020, "
                f"https://doi.org/10.5194/os-9-193-2013). An adjustment, based on windspeed and "
                f"temperature, is applied to obtain hourly fluxes at the CTE-HR resolution. Positive "
                f"fluxes are emissions and negative fluxes indicate uptake. Please always cite the "
                f"original Jena CarboScope data when using this file, and use the original data when "
                f"only low resolution ocean fluxes are required.\n\n"
                f"For more information, see https://doi.org/10.5281/zenodo.6477331",
            "members": collection_info['sorted_members'],
            "submitterId": "CP",
            "title": f"High-resolution, near-real-time fluxes over Europe from CTE-HR for {year}-{month}"
        }
        json_file_name = collection_key + '.json'
        json_file_path = os.path.join(constants.JSON_FILES_DIR, json_file_name)
        collection_info['json_file_path'] = json_file_path
        tools.write_json(path=json_file_path, content=collection_info['json'])
        metadata_curl_command_list = \
            ["curl", "-s", "--cookie", "cookies.txt", "-H",
             '"Content-Type: application/json"',
             "-X", "POST",
             "-d", f"@{collection_info['json_file_path']}",
             "https://meta.icos-cp.eu/upload"]
        collection_info['curl'] = dict({
            'metadata_using_bash': ' '.join(metadata_curl_command_list),
            'metadata_using_python': metadata_curl_command_list
        })
    return


def extract_yearly_collections(archive=None):
    year = '2022'
    members = dict()
    # Collect members for a yearly collection.
    for monthly_key, collection_content in archive.items():
        month = monthly_key[4:]
        members[month] = collection_content['file_metadata_url']
    sorted_members = [members[i] for i in sorted(members.keys())]
    # Archive collection's meta-data.
    yearly_collection[year] = dict()
    yearly_collection[year]['json'] = {
        "description": f"Yearly collection of hourly CO2 fluxes for {year}, containing hourly estimates of biospheric fluxes, anthropogenic emissions (total and per sector), GFAS fire emissions and Jena CarboScope ocean fluxes, all re-gridded to match the resolution of the biospheric fluxes.\n\nNet ecosystem productivity (gross primary production minus respiration). Positive fluxes are emissions, negative mean uptake. These fluxes are the result of the SiB4 (Version 4.2-COS, hash 1e29b25, https://doi.org/10.1029/2018MS001540) biosphere model, driven by ERA5 reanalysis data at a 0.5x0.5 degree resolution. The NEP per plant functional type are distributed according to the high resolution CORINE land-use map (https://land.copernicus.eu/pan-european/corine-land-cover), and aggregated to CTE-HR resolution.\n\nAnthropogenic emissions include contributions from public power, industry, households, ground transport, aviation, shipping, and calcination of cement. Our product does not include carbonation of cement and human respiration. Public power is based on ENTSO-E data (https://transparency.entsoe.eu/), Industry, Ground transport, Aviation, and Shipping is based on Eurostat data (https://ec.europa.eu/eurostat/databrowser/). Household emissions are based on a degree-day model, driven by ERA5 reanalysis data. Spatial distributions of the emissions are based on CAMS data (https://doi.org/10.5194/essd-14-491-2022). Cement emissions are taken from GridFED V.2021.3 (https://zenodo.org/record/5956612#.YoTmvZNBy9F).\n\nGFAS fire emissions (https://doi.org/10.5194/acp-18-5359-2018), re-gridded to match the resolution of the biosphere, fossil fuel, and ocean fluxes of the CTE-HR product. Please always cite the original GFAS data when using this file, and use the original data when only fire emissions are required. For more information, see https://doi.org/10.5281/zenodo.6477331 Contains modified Copernicus Atmosphere Monitoring Service Information [2020].\n\nOcean fluxes, based on a climatology of Jena CarboScope fluxes (https://doi.org/10.17871/CarboScope-oc_v2020, https://doi.org/10.5194/os-9-193-2013). An adjustment, based on windspeed and temperature, is applied to obtain hourly fluxes at the CTE-HR resolution. Positive fluxes are emissions and negative fluxes indicate uptake. Please always cite the original Jena CarboScope data when using this file, and use the original data when only low resolution ocean fluxes are required.\n\nFor more information, see https://doi.org/10.5281/zenodo.6477331",
        "members": sorted_members,
        "submitterId": "CP",
        "title": f"High-resolution, near-real-time fluxes over Europe from CTE-HR for {year}"
    }
    # Archive meta-data for each collection.
    json_file_name = f'{year}.json'
    json_file_path = os.path.join(constants.JSON_FILES_DIR, json_file_name)
    yearly_collection[year]['json_file_path'] = json_file_path
    tools.write_json(path=json_file_path, content=yearly_collection[year]['json'])
    metadata_curl_command_list = \
        ["curl", "-s", "--cookie", "cookies.txt", "-H",
         '"Content-Type: application/json"',
         "-X", "POST",
         "-d", f"@{yearly_collection[year]['json_file_path']}",
         "https://meta.icos-cp.eu/upload"]
    yearly_collection[year]['curl'] = dict({
        'metadata_using_bash': ' '.join(metadata_curl_command_list),
        'metadata_using_python': metadata_curl_command_list
    })
    return


def archive_json_curl(archive=None):
    print(f'- {constants.GEAR_ICON} Archiving json curl commands for collections... ', end='')
    for collection_key, collection_info in collection.items():
        metadata_curl_command_list = \
            ["curl", "-s", "--cookie", "cookies.txt", "-H",
             '"Content-Type: application/json"',
             "-X", "POST",
             "-d", f"@{collection_info['json_file_path']}",
             "https://meta.icos-cp.eu/upload"]
        collection_info['curl'] = dict({
            'metadata_using_bash': ' '.join(metadata_curl_command_list),
            'metadata_using_python': metadata_curl_command_list,
        })
    print(constants.CHECK_ICON)
    return


def upload_collections():
    print(f'- {constants.GEAR_ICON} Uploading collections '
          f'(Expecting {len(yearly_collection.items())} checks)... ', end='')
    for collection_key, collection_info in yearly_collection.items():
        process = subprocess.Popen(collection_info['curl']['metadata_using_bash'],
                                   stdout=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        output = output.decode('utf-8')
        if not error and 'https://meta.icos-cp.eu/collections/' in output:
            collection_info['file_metadata_url'] = output
            # print(f'\tSuccessfully uploaded meta-data. Check them out here: '
            #       f'{base_info["file_metadata_url"]}')
            print(f'{constants.CHECK_ICON}', end='')
        else:
            print('\t\tWARNING! An error has occurred during collection upload...')
            print('\t\t' + collection_key)
            input('You can press ctrl+c to stop this program or press any '
                  'other key to continue... ')
    print('')
    return


if __name__ == '__main__':
    # archive_in = tools.read_json(path='archive_in_nc.json')
    # extract_monthly_collections(archive=archive_in)
    # archive_json_curl()
    # tools.check_permissions()
    # upload_collections()
    # tools.write_json(path='monthly_collections.json', content=monthly_collections)

    archive_in = tools.read_json(path='monthly_collections.json')
    extract_yearly_collections(archive=archive_in)
    tools.check_permissions()
    upload_collections()
    tools.write_json(path='yearly_collection.json', content=yearly_collection)
