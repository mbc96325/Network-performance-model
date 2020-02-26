# This file calculates link capacity by reading files of time-table, train car data and passenger per car
# As the timetable is different for different days, the weekdays are selected and the timetalbe name is listed in the
# Line_cars_timetable.xlsx with filename TRS_[line]_timetable.xlsx
# The output is the arc_start, arc_end, arc_time (when line arrives at the arc), link capacity (lines * #of cars *
#  # of passengers per car)

import pandas as pd
import os
import csv


# PATH = "D:/1 - Project/01 Transit Demand Management (TDM)/Transit_Assignment/Passenger assignment" \
#        "/External_Data/Time_Table/"
# os.chdir(PATH)

# ************************** Combine timetable files ********************************#
def format_timedelta(td):
    hours, remainder = divmod(td.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    hours, minutes, seconds = int(hours), int(minutes), int(seconds)
    if hours < 10:
        hours = '0%s' % int(hours)
    if minutes < 10:
        minutes = '0%s' % minutes
    if seconds < 10:
        seconds = '0%s' % seconds
    return '%s:%s:%s' % (hours, minutes, seconds)  
def prepare_timetable(line_car_timetable_path,station_code_path,time_table_path,out_put_name,out_put_path):
    line_car_timetable = pd.read_excel(line_car_timetable_path).dropna()
    station_code = pd.read_excel(station_code_path).dropna()
    out_put_name = out_put_path + out_put_name+'.csv'
    # Combine the timetable files for different lines
    count = 0
    i = 1
    # ------------------------NEW VERSION----------------
    for ix, lt in line_car_timetable.iterrows():
        file = time_table_path + '/' + lt[2]

        if count == 0:
            time_table = pd.read_excel(file, dtype={'Train_No': 'str', 'Trs_No': 'str', 'Trip_No': 'str'})
            columns_list = list(time_table.columns)
            for name in columns_list:
                time_table = time_table.rename(columns = {name: name.replace(" ", "")})
                
            #print (pd.unique(time_table['Car_Num']))
            time_table[['Train_No', 'Trs_No', 'Trip_No']] = \
                    time_table[['Train_No', 'Trs_No', 'Trip_No']].astype('str')
            # print (time_table.columns)
            count += 1
        else:
            new_table = pd.read_excel(file, dtype={'Train_No': 'str', 'Trs_No': 'str', 'Trip_No': 'str',
                                                   'Arr_From': 'str','Dep_From': 'str','Arr_To': 'str','Dep_To': 'str'})
            # print(new_table.columns)
            columns_list = list(new_table.columns)
            for name in columns_list:
                new_table = new_table.rename(columns = {name: name.replace(" ", "")})
            #print (pd.unique(new_table['Car_Num']))
            new_table[['Train_No', 'Trs_No', 'Trip_No']] = \
                new_table[['Train_No', 'Trs_No', 'Trip_No']].astype('str')
            try:
                time_table = pd.concat([time_table, new_table], sort=False)
            except:
                print(new_table.loc[:, ['Train_No', 'Trs_No', 'Trip_No']])
    # -----------------OLD VERSION---------------
    # with open(out_put_name, 'w', newline='') as f:

        # writer = csv.writer(f, delimiter=',')
        # for ix, lt in line_car_timetable.iterrows():

            # df_table = pd.read_excel(file)
            #
            # if i:
            #     writer.writerow(df_table.columns)
            #     i = 0
            # writer.writerows(df_table.values)

    # line code
    # time_table = pd.read_csv(out_put_name)
    time_list = ['Arr_From','Dep_From','Arr_To','Dep_To']
    for time_name in time_list:
        time_table[time_name] = time_table[time_name].astype('str')
        time_table[time_name] = time_table[time_name].apply(lambda x: x.split(' ')[-1])
        time_table[time_name] = pd.to_timedelta(time_table[time_name])
        time_table.loc[time_table[time_name]>pd.Timedelta('1 days'),time_name] -=  pd.Timedelta('1 days')
        time_table[time_name] = time_table[time_name].apply(format_timedelta)
    #time_table.to_csv('test.csv')
  
    time_table.drop(['Train_Trip', 'Train_KM'], axis=1, inplace=True)
    # print (time_table.columns)
    df_merged = time_table.merge(line_car_timetable, left_on='Line', right_on='LINE', how='left')
    
    df_merged['Car_Num'].fillna(df_merged['DEFALT_CARS'], inplace=True)
    
    # station code for from station
    df_merged = df_merged.merge(station_code, left_on=['From', 'Line'], right_on=['STATION', 'LINE'], how='left')
    df_merged['From_ID'] = df_merged['CODE']
    df_merged.drop(['CODE'], axis=1, inplace=True)
    
    # station code for To station
    df_merged = df_merged.merge(station_code, left_on=['To', 'Line'], right_on=['STATION', 'LINE'], how='left')
    df_merged['To_ID'] = df_merged['CODE']
    
    # direction code (down = 2, up =1)
    df_merged['Direction_ID'] = df_merged['Direction'].apply(lambda x: 1 if x == 'UP' else 2)
    
    # prepare the final outputs

    output = df_merged.loc[:,['Line', 'LINE_CODE', 'Train_No', 'Trs_No', 'Trip_No', 'Revenue_Y_N', 'Direction', 'Direction_ID',
                       'From', 'From_ID', 'Arr_From', 'Dep_From', 'To', 'To_ID', 'Arr_To', 'Dep_To', 'Car_Num']]


    # remove revenue is N
    output = output[output.Revenue_Y_N == 'Y']
    output['Trip_No'] = 'T_' + output['Trip_No'] # make the trip no become purely str 
    output = output.drop_duplicates()
    output.to_csv(out_put_name, index=False)
    return output
# # *************************** Calculate link capacity ********************************#
# df = pd.read_csv('Time_table_Weekday.csv')
# time_interval = 15
# crush_load = 313
# to_public = 208
# # Code time, stations into integer values
# df['time_interval'] = (((pd.to_datetime(df['Arr_From'])).astype('int64') // 1e9)
#                            // (time_interval * 60) * (time_interval * 60)).astype('datetime64[s]')
# df['time_interval'] = pd.DatetimeIndex(df['time_interval']).time
# time_code['TIME'] = pd.DatetimeIndex(time_code['TIME']).time
# df = df.merge(time_code, left_on=['time_interval'], right_on=['TIME'], how='left').dropna()
# df = df.merge(station_code, left_on=['From'], right_on=['STATION'], how='left')
# df = df[['From', 'CODE', 'To', 'TIME_INDEX', 'Line']]
# df = df.merge(station_code, left_on=['To'], right_on=['STATION'], how='left')
# df = df[['From', 'CODE_x', 'To', 'CODE_y', 'TIME_INDEX', 'Line']].dropna()
#
# # Then we have From station TO station when line arrives at the arc
# # From|To|Arrive time|Line
# df.rename(columns={'CODE_x': 'ARC_START', 'CODE_y': 'ARC_END', 'TIME_INDEX': 'ARC_TIME', 'Line': 'LINE'
#                                           }, inplace=True)
#
# # Merge with cars data
# arc_line_car = df.merge(line_car_timetable, left_on=['LINE'], right_on=['LINE'], how='left')
# arc_line_car['CAP_CRUSH'] = arc_line_car['CARS'].apply(lambda x: x*crush_load)
# arc_line_car['CAP_COMFO'] = arc_line_car['CARS'].apply(lambda x: x*to_public)
#
# arc_line_car = arc_line_car[['ARC_START', 'ARC_END', 'ARC_TIME', 'CARS', 'CAP_COMFO', 'CAP_CRUSH']]
#
# # Group by arc and time, then we get the link capacity
# df_grouped = arc_line_car.groupby(['ARC_START', 'ARC_END', 'ARC_TIME'])['CARS', 'CAP_COMFO', 'CAP_CRUSH'].sum()
#
# df_grouped.to_csv('arc_time_capacity.csv')


if __name__ == "__main__":
    line_car_timetable_path = 'Editable_files/Line_CarNo_TimetableName.xlsx'
    station_code_path = 'External_data/Line_Station_Code.xlsx'
    time_table_path = 'Time_table_folder'
    out_put_name = 'Timetable_2017-03-16'
    out_put_path = 'External_data_GUI'
    prepare_timetable(line_car_timetable_path, station_code_path, time_table_path, out_put_name, out_put_path)