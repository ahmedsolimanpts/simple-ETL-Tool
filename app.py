import pandas as pd
import datetime
import glob
import warnings
warnings.filterwarnings('ignore')

def Read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        raise Exception(e)

def get_csv_files(folder_name):
    files = glob.glob(folder_name+'/*.csv')
    if len(files) > 0:
        return files
    else:
        raise ValueError('Please Enter Valid  Path for Source Data folder No Files Here')



def create_data_frame(columns_name,files):
    df   = pd.DataFrame(columns=columns_name)
    flag = 0 
    for i in files:
        if flag == 0 :
            df = Read_csv_file(i)
            print('File \"'+i +'\" Shape Is:')
            print(df.shape)
            flag = 1
        else:
            new = Read_csv_file(i)
            print('File \"'+i +'\" Shape Is:')
            print(new.shape)
            df = pd.merge(df,new,on='Date')

    return df

def Create_Target_File(Folder_path,data_frame):
    try:
        data_frame.to_csv(Folder_path)
        print('Finish Create File')
    except Exception as e :
        raise Exception(e)

def Create_Logs(File,Process):
    if glob.glob('logs.txt'):
        with open(File,'a') as file:
            file.write(str(datetime.datetime.now()) + ',    ' + Process+'\n')
    else:
        with open(File,'a') as file:
            file.write('Time '+ ',    '+'Message\n')
            file.write(str(datetime.datetime.now()) + ',    ' + Process+'\n')





if __name__ == "__main__":

    columns = ['Date','Rainfall_Terni','Flow_Rate_Lupa',
            'Rainfall_Settefrati',	
            'Temperature_Settefrati'
            ,'Flow_Rate_Madonna_di_Canneto']
    source_folder = 'Source Data'
    Target_Folder = 'Target Data/target.csv'
    log_file      = 'logs.txt' 
    try:
        Create_Logs(log_file,'Start Reading Files Name')
        csv_files = get_csv_files(source_folder)
        Create_Logs(log_file,'End Reading Files Name')
        Create_Logs(log_file,'Start Creating Data Frame')
        df = create_data_frame(columns,csv_files)
        print('Rows :',df.shape[0] ,' and Columns :', df.shape[1])
        Create_Logs(log_file,'End Creating Data Frame')
        Create_Logs(log_file,'Start Creating Data Target File')
        Create_Target_File(Target_Folder,df)
        Create_Logs(log_file,'End Creating Data Target File')
    except Exception as e:
        Create_Logs(log_file,'Session Failed on ' + str( datetime.datetime.now()))
        raise Exception('Session Failed on ' + str(datetime.datetime.now()))




