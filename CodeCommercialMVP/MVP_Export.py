import subprocess
import sys
# implement pip as a subprocess:
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pydrive'])


from MVPDataTool import getSubRegions, downloadFromGDrive, exportMVPTab1, getGDriveFileID, getGFolderTaxonomy, writeToGDrive, exportMVPTab2
import io
import pandas as pd
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import traceback
import getopt
import os
from pathlib import Path
import logging
import datetime as dt


def main(argv):
    csv_file_name = ''
    destination_path = ''
    opts, args = getopt.getopt(argv,"hi:o:",["ifile="])
    for opt, arg in opts:
      if opt == '-h':
         print ('MVP_Export.py -i <inputfile> -d <destinationpath>')
         sys.exit()
      elif opt in ("-i", "--ifile"):
         csv_file_name = arg
      elif opt in ('-d' '--destpath'):
        destination_path = arg


    logging.basicConfig(filename='/tmp/MVP_Export.log', level=logging.DEBUG, 
                        format='%(asctime)s %(levelname)s %(name)s %(message)s')
    logger=logging.getLogger(__name__)
    logfile = open('MVP_Export.log', 'w')

    #GDrive upload authorization
    CLIENT_SECRET = r'credentials.json'
    SCOPES='https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive.metadata'
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET, SCOPES)
        creds = tools.run_flow(flow, store)
    SERVICE = build('drive', 'v3', http=creds.authorize(Http()))
    PARENT_FOLDER = '1Vq8UaffXOsGkppFlnjzBiqRZS4lnopyO'


    datestamp = dt.date.today()
    tab1_csv_df = pd.DataFrame(columns= ['SubRegion','SubSegment','Product','Measure','Value'])
    tab2_csv_df = pd.DataFrame(columns= ['xFN Team', 'xFN Strategic Initiative', 'xFN Geographical Initiative', 'xFN Project', 'Description', 'Expected Pipeline Contribution', 'Expected Closed Booking Contribution', 'Subscription / Services Impact', 'Subsegment Focus', 'Priority', 'Commercial Lead', 'xFN Lead', 'xFN Aligned', 'Why (If No)', 'Milestone 1', 'Milestone 1 Date', 'Milestone 2', 'Milestone 2 date', 'Resourced (Y/N)', 'Explain (If No)', 'Funded (Y/N)', 'Funding Ask', 'GEO', 'Region', 'Subregion'])
    csv_folder_id = '1kPr77CCbLISxS2MIYiFaDDFwS4jKwyaH'
    taxonomy_file_name = 'do_not_use_cy23_taxonomy_beta'
    query = f"'{csv_folder_id}' in parents and trashed = false"
    noregion_folderid = '1fRgIh9LVJtAdrunHvh4IDarQ1rvjTlvY'
    output_tab1_folderid = '1gYSbIy0Gr8n7Il_S_QKaJ-HxatjH5LQA'
    output_tab1_filename = f'MVP_Planner_Output_Tab1.csv'
    output_tab2_folderid = '1Ivf1FyMKu9UqiXXQbz6UNXuh1uJOqbEI'
    output_tab2_filename = f'MVP_Planner_Output_Tab2.csv'    
    mvp_ws_tab1 = 'xFN Planner (Part I) FINAL 111'
    mvp_ws_tab2 = 'xFN Planner (Part II) FINAL 111'
    #Hardcode these values for debugging:
    csv_file_name = f'Commercial_MVP_File_{datestamp}.csv'
    #csv_file_name = 'Commercial_MVP_File_2023-03-06.csv'
    source_path = Path(os.getcwd())     
    tab1_dest_path = Path(f'{source_path}/{output_tab1_filename}')   
    tab2_dest_path = Path(f'{source_path}/{output_tab2_filename}')  
    product_list = ['RHEL SYB',
                'Traditional CCSP',
                'Subscription',
                'OpenShift SYB',
                'Cloud Services Openshift',
                'Ansible SYB',
                'Cloud Services Ansible',
                'Subscription',
                'Planning Total',
                'Other Products SYB',
                'Consulting',
                'Training',
                'Sub. + Services',
                'SUBTOTAL Sub. + Services']

    #1. Download the planner from the taxonomy locations
    #2. Read the file and export to csv using exportMVPData function
    #3. Upload the csv to Google Drive
    #4. Clean up the local drive

    try:
        #Get all the subregions from the data file so we can loop through them and pull the data
        csv_file_id = getGDriveFileID(csv_file_name, SERVICE, query)
        #csv_file_id = '1pULkFcIOxG2ef1lVLPmjGYmbpDkYEN7BYINgDpXKzIM' #LN
        taxonomy_file_id = getGDriveFileID(taxonomy_file_name, SERVICE, query)
        df_subregion = getSubRegions(csv_file_id, csv_file_name, 'ALL', SERVICE)
        #df_subregion_test = df_subregion.loc[df_subregion['reporting_subregion'] == 'JAPAN']
        #print(df_subregion_test)

        for row in df_subregion.itertuples(index=False):
            try:
                (geo, region, subregion) = row
                print(f'Processing geo: {geo}, region: {region}, subregion: {subregion}')                
                folder_id = getGFolderTaxonomy(geo, region, subregion, taxonomy_file_id, taxonomy_file_name, SERVICE, noregion_folderid)            
                filename = f'{subregion}_Planner.xlsx'
                downloadFromGDrive(filename, folder_id, SERVICE)
                source_path = Path(os.getcwd())
                source_path = source_path / filename
                tab1_flat_df = exportMVPTab1(source_path, geo, region, mvp_ws_tab1, product_list)
                tab1_csv_df = pd.concat([tab1_flat_df, tab1_csv_df], axis=0)
                #Run tab 2
                tab2_flat_df = exportMVPTab2(source_path, geo, region, subregion, mvp_ws_tab2)
                tab2_csv_df = pd.concat([tab2_flat_df, tab2_csv_df], axis=0)
                os.remove(source_path)
            except Exception as e:
                    logfile.write(f'{subregion} failed.')
                    traceback.print_exc() 
            finally:
                pass

        tab1_final_df = tab1_csv_df.loc[:,['Geo', 'Region', 'SubRegion', 'SubSegment', 'Product', 'Measure', 'value']]

        if os.path.exists(tab1_dest_path):
            os.remove(tab1_dest_path)
        else:
            print("No file to delete.")

        tab1_final_df.to_csv(tab1_dest_path, index=False)
        writeToGDrive(output_tab1_filename, tab1_dest_path, output_tab1_folderid, 1, SERVICE, PARENT_FOLDER)


        tab2_final_df = tab2_csv_df.loc[:,['Geo', 'Region', 'SubRegion', 'xFN Team', 'xFN Strategic Initiative', 'xFN Geographical Initiative', 'xFN Project', 'Description', 'Expected Pipeline Contribution', 'Expected Closed Booking Contribution', 'Subscription / Services Impact', 'Subsegment Focus', 'Priority', 'Commercial Lead', 'xFN Lead', 'xFN Aligned', 'Why (If No)', 'Milestone 1', 'Milestone 1 Date', 'Milestone 2', 'Milestone 2 date', 'Resourced (Y/N)', 'Explain (If No)', 'Funded (Y/N)', 'Funding Ask']]

        if os.path.exists(tab2_dest_path):
            os.remove(tab2_dest_path)
        else:
            print("No file to delete.")

        tab2_final_df.to_csv(tab2_dest_path, index=False)
        writeToGDrive(output_tab2_filename, tab2_dest_path, output_tab2_folderid, 1, SERVICE, PARENT_FOLDER)

        

    except Exception as e:

        print(f"An exception occurred:")
        traceback.print_exc()   

if __name__ == "__main__":
   main(sys.argv[1:]) 