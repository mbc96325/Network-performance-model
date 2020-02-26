from tkinter import *
from tkinter import filedialog
from tkinter import messagebox
from _passenger_assignment_capcity_priority_ver6 import assignment_main
from _network_performance_analysis_for_GUI import train_load, link_flow, station_metrix, MetroViz
from tkinter.ttk import Progressbar
import time
import pandas as pd

window = Tk()
window.title("NPM Model")

window.geometry('600x450')

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
access_egress_time = 'Editable_files\Access_egress_time.csv'
output_file = ''
input_file_path2 = ''
external = 'External_data'
station_code = external + '/' + 'Line_Station_Code.xlsx'

def Choose_user_configuration():
    global user_configuration
    file = filedialog.askopenfilename()
    user_configuration = file
    lbl_user_file.configure(text=user_configuration.split('/')[-1])

def Choose_input_file_path():
    global input_file_path
    file = filedialog.askdirectory()
    input_file_path = file
    lbl_1_file.configure(text=input_file_path.split('/')[-1])


def Choose_empty_train():
    global empty_train
    file = filedialog.askopenfilename()
    empty_train = file
    lbl_2_file.configure(text=empty_train.split('/')[-1])

def Choose_transfer_walking_time():
    global transfer_walking_time
    file = filedialog.askopenfilename()
    transfer_walking_time = file
    lbl_3_file.configure(text=transfer_walking_time.split('/')[-1])

def Choose_access_egress_time():
    global access_egress_time
    file = filedialog.askopenfilename()
    access_egress_time = file
    lbl_4_file.configure(text=access_egress_time.split('/')[-1])

def run_assignment():
    global input_file_path, empty_train,transfer_walking_time

    file_list = [input_file_path, empty_train,transfer_walking_time]
    len_list = [len(i) for i in file_list]
    if 0 in len_list:
        messagebox.showinfo('Uncomplete input', 'Please specify the input files')
    else:
        tic = time.time()
        Test_name = input_file_path.split('/')[-1].split('_')[1]
        time_period = input_file_path.split('/')[-1].split('_')[-1]
        para = [Test_name, time_period]
        assignment_main(para, input_file_path, empty_train,transfer_walking_time,access_egress_time, bar, window)
        running_time = round((time.time() - tic) / 60, 2)
        lbl_6_finish.configure(text='NPM running finish!, total time: ' + str(running_time) + ' min, ' + 'please check in \n'\
                                    + input_file_path.split('/')[-1])

initial_row=0
lbl_5_seg = Label(window, text="=======================NPM Assignment=======================").grid(row=initial_row,sticky=W)



initial_row = 4
lbl_1 = Label(window, text="Choose input file folder (i.e. NPM_...)").grid(row=initial_row+1,sticky=W)
btn_1 = Button(window, text="Open", command=Choose_input_file_path).grid(row=initial_row+1,column=1)
lbl_1_file = Label(window, text="")
lbl_1_file.grid(row=initial_row+2, sticky=W)
lbl_1_seg = Label(window, text="-----------------------------------------").grid(row=initial_row+3,sticky=W)

initial_row = 8
lbl_2 = Label(window, text="Choose Empty_Train_Arrangement file").grid(row=initial_row+1,sticky=W)
btn_2 = Button(window, text="Open", command=Choose_empty_train).grid(row=initial_row+1,column=1)
lbl_2_file = Label(window, text="Default: \'Editable_files\\Empty_Train_Arrangement.csv\'")
lbl_2_file.grid(row=initial_row+2, sticky=W)
lbl_2_seg = Label(window, text="-----------------------------------------").grid(row=initial_row+3,sticky=W)

initial_row = 12
lbl_3 = Label(window, text="Choose Transfer_Walking_Time file").grid(row=initial_row+1,sticky=W)
btn_3 = Button(window, text="Open", command=Choose_transfer_walking_time).grid(row=initial_row+1,column=1)
lbl_3_file = Label(window, text="Default: \'Editable_files\\Transfer_Walking_Time.csv\'")
lbl_3_file.grid(row=initial_row+2, sticky=W)
lbl_3_seg = Label(window, text="-----------------------------------------").grid(row=initial_row+3,sticky=W)

initial_row = 16
lbl_4 = Label(window, text="Choose Access_Egress_Time file").grid(row=initial_row+1,sticky=W)
btn_4 = Button(window, text="Open", command=Choose_transfer_walking_time).grid(row=initial_row+1,column=1)
lbl_4_file = Label(window, text="Default: \'Editable_files\\Access_egress_time.csv\'")
lbl_4_file.grid(row=initial_row+2, sticky=W)
lbl_4_seg = Label(window, text="-----------------------------------------").grid(row=initial_row+3,sticky=W)

# lbl_5 = Label(window, text="Choose output folder (recommend in '\External_data')").grid(row=11,sticky=W)
# btn_5 = Button(window, text="Open", command=Choose_out_put_path).grid(row=11,column=1)
# lbl_5_file = Label(window, text="Unspecified output folder")
# lbl_5_file.grid(row=12, sticky=W)
# lbl_5_seg = Label(window, text="-----------------------------------------").grid(row=13,sticky=W)

initial_row = 20


temp = Label(window, text="").grid(row=initial_row+2, column=0, sticky=W)
lbl_bar = Label(window, text="Running progress").grid(row=initial_row+3, column=0)
bar = Progressbar(window, length=180, style='black.Horizontal.TProgressbar')
bar['value'] = 0
bar.grid(row=initial_row+3, column=0, sticky=W)
btn_run = Button(window, text="Run NPM", command=run_assignment).grid(row=initial_row+1,column=0)

lbl_6_finish = Label(window, text="")
lbl_6_finish.grid(row=initial_row+4, column=0, sticky=W)
temp = Label(window, text="").grid(row=initial_row+5, column=0, sticky=W)





window.mainloop()