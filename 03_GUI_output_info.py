from tkinter import *
from tkinter import filedialog
from tkinter import messagebox
from _passenger_assignment_capcity_priority_ver6 import assignment_main
from _network_performance_analysis_for_GUI import train_load, link_flow, station_metrix, MetroViz
from tkinter.ttk import Progressbar
import time
import pandas as pd


window = Tk()
window.title("Generate Output Information")

window.geometry('600x300')

global input_file_path
global input_file_path2
global empty_train
global user_configuration
global output_file
global station_code

input_file_path = ''
empty_train = 'Editable_files\Empty_Train_Arrangement.csv'
user_configuration = ''
transfer_walking_time = 'Editable_files\Transfer_Walking_Time.csv'
output_file = ''
input_file_path2 = ''
external = 'External_data'
station_code = external + '/' + 'Line_Station_Code.xlsx'


def Choose_input_file_path2():
    global input_file_path2
    file = filedialog.askdirectory()
    input_file_path2 = file
    lbl_7_file.configure(text=input_file_path2.split('/')[-1])


initial_row = 23
lbl_5_seg = Label(window, text="==================Generate Output Information====================").grid(row=initial_row,sticky=W)

lbl_7 = Label(window, text="Choose input file folder (i.e. NPM_...)").grid(row=initial_row+1,sticky=W)
btn_7 = Button(window, text="Open", command=Choose_input_file_path2).grid(row=initial_row+1,column=1)
lbl_7_file = Label(window, text="")
lbl_7_file.grid(row=initial_row+2, sticky=W)
lbl_7_seg = Label(window, text="-----------------------------------------").grid(row=initial_row+3,sticky=W)
initial_row = 26
CheckVar1 = IntVar()
CheckVar2 = IntVar()
CheckVar3 = IntVar()
CheckVar4 = IntVar()

C1 = Checkbutton(window, text = "Train load (each headway)", variable = CheckVar1)
C2 = Checkbutton(window, text = "Link flow (every 15 min)", variable = CheckVar2)
C3 = Checkbutton(window, text = "Platform Metrics (every 15 min)", variable = CheckVar3)
C4 = Checkbutton(window, text = "MetroViz files", variable = CheckVar4)

C1.grid(row=initial_row+2, sticky=W)
lbl_C1_file = Label(window, text="")
lbl_C1_file.grid(row=initial_row+2, sticky=E)
C2.grid(row=initial_row+3, sticky=W)
lbl_C2_file = Label(window, text="")
lbl_C2_file.grid(row=initial_row+3, sticky=E)
C3.grid(row=initial_row+4, sticky=W)
lbl_C3_file = Label(window, text="")
lbl_C3_file.grid(row=initial_row+4, sticky=E)
C4.grid(row=initial_row+5, sticky=W)
lbl_C4_file = Label(window, text="")
lbl_C4_file.grid(row=initial_row+5, sticky=E)
def out_put_data():
    global input_file_path2, station_code
    file_list = [input_file_path2]
    len_list = [len(i) for i in file_list]
    if 0 in len_list:
        messagebox.showinfo('Uncomplete input', 'Please specify the input files folder')
    else:
        station_code_data = pd.read_excel(station_code).dropna()
        sim_time = input_file_path2.split('/')[-1].split('_')[2]
        date = input_file_path2.split('/')[-1].split('_')[1]
        # print (sim_time)
        time_period =[int(sim_time.split('-')[0]),
                      int(sim_time.split('-')[1])]
        # print (time_period)
        file_carrier_state = {}
        if CheckVar1.get() == 1:
            file_carrier = input_file_path2 + '/' + 'carrier_state.csv'
            file_carrier_state = pd.read_csv(file_carrier)
            # print (file_carrier_state)
            train_load_file = train_load(file_carrier_state, time_period)
            # change stn ID to name
            train_load_file = train_load_file.merge(station_code_data, left_on = ['LnID','LnkStart'],right_on = ['LINE','CODE'],how = 'left')
            train_load_file['LnkStart'] = train_load_file['STATION']
            train_load_file = train_load_file.drop(columns=['LINE','STATION','CODE'])
            train_load_file = train_load_file.merge(station_code_data, left_on = ['LnID','LnkEnd'],right_on = ['LINE','CODE'],how = 'left')
            train_load_file['LnkEnd'] = train_load_file['STATION']
            train_load_file = train_load_file.drop(columns=['LINE','STATION','CODE'])
            na_rows = train_load_file.loc[(train_load_file['LnkStart'].isna()) | (train_load_file['LnkEnd'].isna())]
            if len(na_rows)>0:
                messagebox.showinfo('Error in Station ID', 'The following stations do not have the matched name,\
                 please add to External_data\\Line_Station_Code.xlsx')
                print(na_rows)
            else:
                train_load_file.to_csv(input_file_path2 + '/' + 'SO_Train_Load_'+ date + '.csv', index=False,
                                       columns = ['LnID','LnCode','Dir','TrpID','LnkID','LnkStart','LnkEnd','EntTime','Load'])
                lbl_C1_file.configure(text='' + 'SO_Train_Load_'+ date + '.csv finish')

        if CheckVar2.get() == 1:
            if len(file_carrier_state)==0:
                file_carrier = input_file_path2 + '/' + 'carrier_state.csv'
                file_carrier_state = pd.read_csv(file_carrier)
            # print (file_carrier_state)
            time_interval = 15*60 # 15min
            link_flow_file = link_flow(file_carrier_state, time_period, time_interval)
            # change stn ID to name
            link_flow_file = link_flow_file.merge(station_code_data, left_on = ['LnID','LnkStart'],right_on = ['LINE','CODE'],how = 'left')
            link_flow_file['LnkStart'] = link_flow_file['STATION']
            link_flow_file = link_flow_file.drop(columns=['LINE','STATION','CODE'])
            link_flow_file = link_flow_file.merge(station_code_data, left_on = ['LnID','LnkEnd'],right_on = ['LINE','CODE'],how = 'left')
            link_flow_file['LnkEnd'] = link_flow_file['STATION']
            link_flow_file = link_flow_file.drop(columns=['LINE','STATION','CODE'])
            na_rows = link_flow_file.loc[(link_flow_file['LnkStart'].isna()) | (link_flow_file['LnkEnd'].isna())]
            if len(na_rows)>0:
                messagebox.showinfo('Error in Station ID', 'The following stations do not have the matched name,\
                 please add to External_data\\Line_Station_Code.xlsx')
                print(na_rows)

            else:
                link_flow_file.to_csv(input_file_path2 + '/' + 'SO_Link_Flow_'+ date + '.csv', index=False)
                lbl_C2_file.configure(text='' + 'SO_Link_Flow_'+ date + '.csv finish')
        if CheckVar3.get() == 1:
            file_passenger = input_file_path2 + '/' + 'passenger_state.csv'
            file_passenger_state = pd.read_csv(file_passenger)
            # print (file_carrier_state)
            time_interval = 15 * 60  # 15min
            station_metrix_file = station_metrix(file_passenger_state, time_period, time_interval)
            # change stn ID to name
            station_metrix_file = station_metrix_file.merge(station_code_data, left_on = ['LnID','StnID'],right_on = ['LINE','CODE'],how = 'left')
            station_metrix_file['StnName'] = station_metrix_file['STATION']
            na_rows = station_metrix_file.loc[(station_metrix_file['StnName'].isna())]
            if len(na_rows)>0:
                messagebox.showinfo('Error in Station ID', 'The following stations do not have the matched name,\
                 please add to External_data\\Line_Station_Code.xlsx')
                print(na_rows)
            else:
                station_metrix_file.to_csv(input_file_path2 + '/' + 'SO_Platform_Metrics_'+ date + '.csv', index=False,
                                       columns = ['LnID','StnID','StnName','Dir','EntTime','Arrivals','Board_1st','Board_2nd','Board_3rd','Board_>=4th','Avg_WT','LB_rate'])
                lbl_C3_file.configure(text='' + 'SO_Platform_Metrics_'+ date + '.csv finish')

        if CheckVar4.get() == 1:

            if len(file_carrier_state)==0:
                file_carrier = input_file_path2 + '/' + 'carrier_state.csv'
                file_carrier_state = pd.read_csv(file_carrier)
            tb_txn = input_file_path2 + '/' + 'tb_txn.csv'
            file_tb_txn = pd.read_csv(tb_txn)
            time_interval = 15 * 60  # 15min
            MetroViz_data = MetroViz(file_carrier_state, file_tb_txn, time_period, time_interval)
            MetroViz_link, MetroViz_Enter_OD = MetroViz_data.MetroViz_output()

            # change stn ID to name
            # MetroViz_link = MetroViz_link.merge(station_code_data, left_on = ['LnID','LnkStart'],right_on = ['LINE','CODE'],how = 'left')
            # MetroViz_link['LnkStart'] = MetroViz_link['STATION']
            # MetroViz_link = MetroViz_link.drop(columns=['LINE','STATION','CODE'])
            # MetroViz_link = MetroViz_link.merge(station_code_data, left_on = ['LnID','LnkEnd'],right_on = ['LINE','CODE'],how = 'left')
            # MetroViz_link['LnkEnd'] = MetroViz_link['STATION']
            # MetroViz_link = MetroViz_link.drop(columns=['LINE','STATION','CODE'])
            # na_rows = MetroViz_link.loc[(MetroViz_link['LnkStart'].isna()) | (MetroViz_link['LnkEnd'].isna())]
            # if len(na_rows)>0:
            #     messagebox.showinfo('Error in Station ID', 'The following stations do not have the matched name,\
            #      please add to External_data\\Line_Station_Code.xlsx')
            #     print(na_rows)
            # else:
            MetroViz_link.to_csv(input_file_path2 + '/' + 'MetroViz_Link_Flow_'+ date + '.csv', index=False,
                                       columns = ['LnkID','LnID','Dir','EntTime','Flow'])

            # change stn ID to name
            # length1 = len(MetroViz_Enter_OD)
            # station_code_data_only_code = station_code_data.copy().drop(columns = ['LINE']).drop_duplicates()
            #
            #
            # MetroViz_Enter_OD = MetroViz_Enter_OD.merge(station_code_data_only_code, left_on = ['OriStnID'],right_on = ['CODE'],how = 'left')
            # length2 = len(MetroViz_Enter_OD)
            # if length1 != length2:
            #     print('Duplicate Index')
            # MetroViz_Enter_OD['OriStnName'] = MetroViz_Enter_OD['STATION']
            # MetroViz_Enter_OD = MetroViz_Enter_OD.drop(columns=['STATION','CODE'])
            # MetroViz_Enter_OD = MetroViz_Enter_OD.merge(station_code_data_only_code, left_on=['DesStnID'], right_on=['CODE'],
            #                                             how='left')
            # MetroViz_Enter_OD['DesStnName'] = MetroViz_Enter_OD['STATION']
            # MetroViz_Enter_OD = MetroViz_Enter_OD.drop(columns=['STATION','CODE'])
            # na_rows = MetroViz_Enter_OD.loc[(MetroViz_Enter_OD['OriStnName'].isna()) | (MetroViz_Enter_OD['DesStnName'].isna())]
            # if len(na_rows)>0:
            #     messagebox.showinfo('Error in Station ID', 'The following stations do not have the matched name,\
            #      please add to External_data\\Line_Station_Code.xlsx')
            #     print(na_rows)
            # else:
            MetroViz_Enter_OD.to_csv(input_file_path2 + '/' + 'MetroViz_OD_Demand_'+ date + '.csv', index=False,
                                       columns = ['OriStnID','DesStnID','EntTime','Demand'])
            lbl_C4_file.configure(text='MetroViz input data finish')

btn_output = Button(window, text="Get output data", command=out_put_data)
btn_output.grid(row=initial_row+6)


window.mainloop()