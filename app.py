from dataclasses import dataclass
import pandas as pd
import datetime
import glob
import warnings
warnings.filterwarnings('ignore')

def Read_csv_file(file_path):
    df = pd.read_csv(file_path)
    return df

def get_csv_files(folder_name):
    files = glob.glob(folder_name+'/*.csv')
    return files

def create_data_frame(columns_name,files):
    df   = pd.DataFrame(columns=columns_name)
    flag = 0 
    for i in files:
        if flag == 0 :
            df = Read_csv_file(i)
            flag = 1
        else:
            new = Read_csv_file(i)
            # print('File \"'+i +'\" Shape Is:')
            # print(new.shape)
            df = pd.concat([df,new])
    return df

def Create_Target_File(Folder_path,data_frame):
    data_frame.to_csv(Folder_path)
    print('Finish Create File')

def Create_Logs(File,Process):
    with open(File,'a') as file:
        file.write(str(datetime.datetime.now()) + ',    ' + Process+'\n')



if __name__ == "__main__":

    columns = ['Date','Rainfall_Terni','Flow_Rate_Lupa',
            'Rainfall_Settefrati',	
            'Temperature_Settefrati'
            ,'Flow_Rate_Madonna_di_Canneto']
    source_folder = 'Source Data'
    Target_Folder = 'Target Data/target.csv'
    log_file      = 'logs.txt' 
    Create_Logs(log_file,'Start Reading Files Name')
    csv_files = get_csv_files(source_folder)
    Create_Logs(log_file,'End Reading Files Name')
    Create_Logs(log_file,'Start Creating Data Frame')
    df = create_data_frame(columns,csv_files)
    Create_Logs(log_file,'End Creating Data Frame')
    print(df.head())
    Create_Logs(log_file,'Start Creating Data Target File')
    Create_Target_File(Target_Folder,df)
    Create_Logs(log_file,'End Creating Data Target File')




