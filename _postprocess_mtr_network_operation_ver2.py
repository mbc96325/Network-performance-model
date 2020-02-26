# Generate mtr_network_operation information by combining the mtr provided network and operation csv spreadsheet
#  clean the trip leg with walking and blank ones
#  Basically the lines should be in the 9 lines
# Author: Zhenliang Ma, Northeastern University Date: 12/03/2016
import pandas as pd
import csv
import os
import time
from _DefaultValues import *
from itertools import chain
pd.options.mode.chained_assignment = None  # default='warn'

def fast_concate(frames):
    def fast_flatten(input_list):
        return list(chain.from_iterable(input_list))

    COLUMN_NAMES = frames[0].columns
    df_dict = dict.fromkeys(COLUMN_NAMES, [])
    for col in COLUMN_NAMES:
        # Use a generator to save memory
        extracted = (frame[col] for frame in frames)

        # Flatten and save to df_dict
        df_dict[col] = fast_flatten(extracted)
    df = pd.DataFrame.from_dict(df_dict)[COLUMN_NAMES]
    return df

def add_new_transfer(assignment_file):
    # select the lines in the timetable
    lines_list = LINE_CODE_LIST
    mtr_file = pd.read_csv(assignment_file)
    # mtr_file = mtr_file[mtr_file.PASS_LINE_NO.isin(lines_list)]
    mtr_file = mtr_file.sort_values(by=['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO', 'DELAY_TIME'],
                                  ascending=[True, True, True, False])
    # add path choice at ADM
    # passengers transfer from ISL line, Down direction to TWL line, Up direction
    # would have path choice to transfer at ADM or Central
    # [PASS_LINE_NO	PASS_DIRECTION	PASS_STN]
    X_line = [13, 2, 2]
    Y_line = [11, 1, 2]
    new_transfer_station = 1
    fraction_Y = int(INCLUDE_CEN.split('CEN')[1])/100
    # print (fraction_Y) # transfer in CEN
    fraction_X = 1- fraction_Y # transfer in ADM
    mtr_file['index'] = mtr_file.index
    all_path_X = mtr_file.loc[(mtr_file.PASS_LINE_NO == X_line[0])&
                            (mtr_file.PASS_DIRECTION == X_line[1])&
                            (mtr_file.PASS_STN == X_line[2])]
    all_path_Y = mtr_file.loc[(mtr_file.PASS_LINE_NO == Y_line[0])&
                            (mtr_file.PASS_DIRECTION == Y_line[1])&
                            (mtr_file.PASS_STN == Y_line[2])]
    all_path = all_path_X.merge(all_path_Y,left_on = ['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO'],\
                                right_on = ['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO'], how='inner')
    all_path['index_test'] = all_path['index_y'] - all_path['index_x']
    all_path = all_path.loc[all_path['index_test']==1] # two stations not adjacent are filtered
    #----part 1: X_line->new_transfer_station
    all_path_part1 = all_path.loc[:,['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO','DELAY_TIME_x','MP_SHARE_x',\
                                      'OP_SHARE_x', 'EP_SHARE_x', 'OTHER_SHARE_x']]
    all_path_part1['PASS_LINE_NO'] = X_line[0] #
    all_path_part1['PASS_DIRECTION'] = X_line[1]
    all_path_part1['PASS_STN'] = X_line[2]
    all_path_part1['LINK_START'] = X_line[2] #
    all_path_part1['LINK_END'] = new_transfer_station
    all_path_part1['DELAY_TIME'] = all_path_part1['DELAY_TIME_x'] 
    all_path_part1 = all_path_part1.drop(columns = ['DELAY_TIME_x'])
    all_path_part1 = all_path_part1.rename(columns = {'MP_SHARE_x':'MP_SHARE','OP_SHARE_x':'OP_SHARE',
                                                      'EP_SHARE_x':'EP_SHARE', 'OTHER_SHARE_x':'OTHER_SHARE'})
    all_path_part1.loc[:,['MP_SHARE', 'OP_SHARE', 'EP_SHARE', 'OTHER_SHARE']] *= fraction_Y    
    #----part 2: new_transfer_station->new_transfer_station
    all_path_part2 = all_path.loc[:,['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO','DELAY_TIME_x','MP_SHARE_x',\
                                      'OP_SHARE_x', 'EP_SHARE_x', 'OTHER_SHARE_x']]
    all_path_part2['PASS_LINE_NO'] = X_line[0] #
    all_path_part2['PASS_DIRECTION'] = X_line[1]
    all_path_part2['PASS_STN'] = new_transfer_station
    all_path_part2['LINK_START'] = new_transfer_station #
    all_path_part2['LINK_END'] = new_transfer_station
    all_path_part2['DELAY_TIME'] = all_path_part2['DELAY_TIME_x'] - 0.01
    all_path_part2 = all_path_part2.drop(columns = ['DELAY_TIME_x'])
    all_path_part2 = all_path_part2.rename(columns = {'MP_SHARE_x':'MP_SHARE','OP_SHARE_x':'OP_SHARE',
                                                      'EP_SHARE_x':'EP_SHARE', 'OTHER_SHARE_x':'OTHER_SHARE'})
    all_path_part2.loc[:,['MP_SHARE', 'OP_SHARE', 'EP_SHARE', 'OTHER_SHARE']] *= fraction_Y    
    #----part 3: new_transfer_station->Y_line
    all_path_part3 = all_path.loc[:,['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO','DELAY_TIME_y','MP_SHARE_x',\
                                      'OP_SHARE_x', 'EP_SHARE_x', 'OTHER_SHARE_x']]
    all_path_part3['PASS_LINE_NO'] = Y_line[0] #
    all_path_part3['PASS_DIRECTION'] = Y_line[1]
    all_path_part3['PASS_STN'] = new_transfer_station
    all_path_part3['LINK_START'] = new_transfer_station #
    all_path_part3['LINK_END'] = Y_line[2]
    all_path_part3['DELAY_TIME'] = all_path_part3['DELAY_TIME_y'] + 0.01
    all_path_part3 = all_path_part3.drop(columns = ['DELAY_TIME_y'])
    all_path_part3 = all_path_part3.rename(columns = {'MP_SHARE_x':'MP_SHARE','OP_SHARE_x':'OP_SHARE',
                                                      'EP_SHARE_x':'EP_SHARE', 'OTHER_SHARE_x':'OTHER_SHARE'})
    all_path_part3.loc[:,['MP_SHARE', 'OP_SHARE', 'EP_SHARE', 'OTHER_SHARE']] *= fraction_Y        
    #*********************************************
    all_path_ID = all_path.loc[:,['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO']].drop_duplicates()
    mtr_file_index = mtr_file.merge(all_path_ID,left_on=['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO'],\
                                    right_on = ['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO'],how='inner')[['index']]
    
    new_path = mtr_file.loc[mtr_file_index['index'],:]
    new_path = new_path.loc[~((new_path.PASS_LINE_NO == X_line[0])&
                            (new_path.PASS_DIRECTION == X_line[1])&
                            (new_path.PASS_STN == X_line[2]))]
    new_path.loc[:,['MP_SHARE', 'OP_SHARE', 'EP_SHARE', 'OTHER_SHARE']] *= fraction_Y  
    
    new_path = pd.concat([new_path, all_path_part1, all_path_part2, all_path_part3],sort=False)
    new_path = new_path.sort_values(by=['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO', 'DELAY_TIME'],
                                  ascending=[True, True, True, False])  
    #new_path.to_csv('New_path'+para['incl_cen_file']+'.csv', index=False,columns=['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO',\
                                                                                  #'PASS_LINE_NO','PASS_DIRECTION','PASS_STN','LINK_START',\
                                                                                  #'LINK_END','DELAY_TIME','MP_SHARE', 'OP_SHARE', 'EP_SHARE',\
                                                                                  #'OTHER_SHARE'])

    new_path['HARBOUR_CO'] = new_path['HARBOUR_CO'].apply(lambda x: x +'_NEW')
    # add additional delay time
    def add_daley(x):
        delay_time = x.loc[x.TRAN_IND.isna()].iloc[0]['DELAY_TIME']
        x.loc[x['DELAY_TIME']>=delay_time,'DELAY_TIME'] += 6.7 #addtional time
        return x
    new_path = new_path.groupby(['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO'])
    new_path_list = []
    for ind, group in new_path:
        group = add_daley(group)
        new_path_list.append(group)
    new_path = fast_concate(new_path_list)
    mtr_file.loc[mtr_file_index['index'],['MP_SHARE', 'OP_SHARE', 'EP_SHARE', 'OTHER_SHARE']] *= fraction_X
    mtr_file = pd.concat([mtr_file, new_path], sort=False).reset_index(drop=True)
    mtr_file = mtr_file.sort_values(by=['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO', 'DELAY_TIME'],
                                  ascending=[True, True, True, False])
    temp = mtr_file.loc[:,['ORI_STN_NO', 'DES_STN_NO','HARBOUR_CO']].drop_duplicates()
    temp['HARBOUR_CO_ID'] = temp.groupby(['ORI_STN_NO', 'DES_STN_NO']).cumcount()
    temp['HARBOUR_CO_ID'] += 1   
    mtr_file = mtr_file.merge(temp, left_on = ['ORI_STN_NO', 'DES_STN_NO','HARBOUR_CO'],\
                   right_on = ['ORI_STN_NO', 'DES_STN_NO','HARBOUR_CO'],\
                   how = 'inner')
    mtr_file = mtr_file.drop(columns = ['HARBOUR_CO','index'])
    mtr_file = mtr_file.rename(columns={'HARBOUR_CO_ID':'HARBOUR_CO'})
    mtr_file = mtr_file.sort_values(by=['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO', 'DELAY_TIME'],
                                  ascending=[True, True, True, False])    
    # mtr_file.to_csv('External_data/MTR_Network_Operation_Assignment_'+para['incl_cen_file']+'.csv', index=False,
    #                 columns=['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO',
    #                          'PASS_LINE_NO','PASS_DIRECTION','PASS_STN','LINK_START',
    #                          'LINK_END','DELAY_TIME','MP_SHARE', 'OP_SHARE', 'EP_SHARE',
    #                          'OTHER_SHARE'])
    return mtr_file

# def add_dummy_train_to_multiindex_station(mtr_file):
#     boarding = mtr_file.groupby(['ORI_STN_NO', 'DES_STN_NO', 'HARBOUR_CO'], sort = False).first().reset_index()
#     nocons = boarding.loc[boarding['ORI_STN_NO']!=boarding['LINK_START']]
#
#     return mtr_file

def post_process(assignment_file):
    # print (user_file_path)
    mtr_file = add_new_transfer(assignment_file)

    return mtr_file
if __name__ == "__main__":
    print ('Directly run _prepare_input_files_for_assignment')
#     # para_list = pd.read_excel('parameter.xls')
#     user_file_path = '0_user_configuration_parameter.csv'
#     assignment_file = 'External_data/mtr_network_operation_assignment.csv'
#     mtr_file = post_process(user_file_path, assignment_file)

