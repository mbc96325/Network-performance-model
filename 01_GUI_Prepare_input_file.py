from tkinter import *
from tkinter import filedialog
from tkinter import messagebox
from _select_and_combine_timetable import prepare_timetable
from _postprocess_mtr_network_operation_ver2 import post_process
from _prepare_input_files_for_assignment_ver5 import prepare_input
import os
import pandas as pd

window = Tk()
window.title("Prepare NPM Input Data")

window.geometry('600x600')

global station_code
global Line_CarNo_TimetableName
global time_table_path
global out_put_path
global next_GUI, afc_file, sjsc_file
external = 'External_data'
Line_CarNo_TimetableName = 'Editable_files\Line_CarNo_TimetableName.xlsx'
time_table_path = 'Time_table_folder'
out_put_path = ''
afc_file = 'Editable_files\AFC_TXN_2017-03-16.csv'
sjsc_file = 'Editable_files\SJSC_TXN_2017-03-16.csv'
next_GUI = ''

def Choose_external_file():
    global external
    dir = filedialog.askdirectory()
    external = dir
    lbl_1_file.configure(text=external.split('/')[-1])


def Choose_Line_CarNo_TimetableName():
    global Line_CarNo_TimetableName
    file = filedialog.askopenfilename()
    Line_CarNo_TimetableName = file
    lbl_2_file.configure(text=Line_CarNo_TimetableName.split('/')[-1])

def Choose_time_table_path():
    global time_table_path
    dir = filedialog.askdirectory()
    time_table_path = dir
    # print (dir)
    lbl_3_file.configure(text=time_table_path.split('/')[-1])

def Choose_out_put_path():
    global out_put_path
    dir = filedialog.askdirectory()
    out_put_path = dir
    # print (dir)
    lbl_5_file.configure(text=out_put_path.split('/')[-1])

def Choose_afc_file():
    global afc_file
    file = filedialog.askopenfilename()
    afc_file = file
    lbl_afc_file.configure(text=afc_file.split('/')[-1])

def Choose_sjsc_file():
    global sjsc_file
    file = filedialog.askopenfilename()
    sjsc_file = file
    #print (sjsc_file)
    lbl_sjsc_file.configure(text=sjsc_file.split('/')[-1])


def prepare_input_file(time_table_file):
    global afc_file, sjsc_file,external
    file_list = [afc_file, sjsc_file]

    len_list = [len(i) for i in file_list]
    if 0 in len_list:
        messagebox.showinfo('Uncomplete input', 'Please specify the input files')
    else:
        mtr_raw_assignment_file_path = external + '/' + 'mtr_network_operation_assignment.csv'
        #mtr_raw_assignment_file_path = external + '/' + 'mtr_network_operation_assignment.csv' # new line
        network_file = post_process(mtr_raw_assignment_file_path)
        Test_name = str(txt1.get())
        if '_' in Test_name:
            messagebox.showinfo('Test name not available', '"_" is not allowed in the name')
        else:
            time_period = str(txt2.get())
            #print (sjsc_file)
            para = [Test_name, time_period]
            out_put_path = prepare_input(para, network_file, time_table_file, afc_file, sjsc_file)
            lbl_6_finish.configure(text='Preparing input files finish!, please check in \n'\
                                        + out_put_path)

def generate_time_table():
    global Line_CarNo_TimetableName, external, time_table_path
    Test_name = str(txt1.get())
    if '_' in Test_name:
        messagebox.showinfo('Test name not available', '"_" is not allowed in the name')
    else:
        timetable_name = "Timetable_" + Test_name
        file_list = [external, time_table_path ,Line_CarNo_TimetableName, afc_file, sjsc_file]

        time_period = str(txt2.get())
        sim_time_period = time_period.split('-')
        time_start = str(int(pd.to_timedelta(sim_time_period[0]).total_seconds()))
        time_end = str(int(pd.to_timedelta(sim_time_period[1]).total_seconds()))
        file_path = ''
        output_file_path = file_path + 'NPM_' + Test_name + '_' + time_start + '-' + \
                           time_end + '/'
        if not os.path.exists(output_file_path):
            os.makedirs(output_file_path)
        len_list = [len(i) for i in file_list]
        # print(len_list)
        if 0 in len_list:
            messagebox.showinfo('Uncomplete input', 'Please specify the input files')
        else:
            out_put_path = external
            #station_code = external + '/' + 'Line_Station_Code.xlsx'
            station_code = external + '/' + 'Line_Station_Code.xlsx'
            lbl_6_finish.configure(text='Processing time table...')
            window.update()
            time_table = prepare_timetable(Line_CarNo_TimetableName, station_code, time_table_path, timetable_name, output_file_path)
            return time_table

def prepare_all():
    global row_jump, next_GUI
    time_table = []
    time_table = generate_time_table()
    lbl_6_finish.configure(text='Process time table finish. Now prepare input files...')
    window.update()
    if len(time_table) > 0:
        prepare_input_file(time_table)
    else:
        print('Error in generate time table')
        messagebox.showinfo('Error', 'Error in generate time table')
    # row_jump = 30 + 5
    # next_GUI = '02_GUI_NPM_MAIN.py'
    # btn_next = Button(window, text="NEXT", command=jump_NPM).grid(row=row_jump, column=2)
    window.update()

global row_jump
lbl_1 = Label(window, text="Choose External data folder").grid(row=0,sticky=W)
btn_1 = Button(window, text="Open", command=Choose_external_file).grid(row=0,column=1)
lbl_1_file = Label(window, text="Default: \'\\External_data\'")
lbl_1_file.grid(row=1, sticky=W)
lbl_1_seg = Label(window, text="-----------------------------------------").grid(row=2,sticky=W)

lbl_2 = Label(window, text="Choose Line_CarNo_TimetableName file").grid(row=3,sticky=W)
btn_2 = Button(window, text="Open", command=Choose_Line_CarNo_TimetableName).grid(row=3,column=1)
lbl_2_file = Label(window, text="Default: \'Editable_files\\Line_CarNo_TimetableName.xlsx\'")
lbl_2_file.grid(row=4, sticky=W)
lbl_2_seg = Label(window, text="-----------------------------------------").grid(row=5,sticky=W)

lbl_4 = Label(window, text="Please name this test (e.g. Test1)").grid(row=9,sticky=W)
txt1 = Entry(window, width=20)
txt1.grid(row=9+1,column=0,sticky=W)
txt1.insert(END, 'Test1')
lbl_4_seg = Label(window, text="-----------------------------------------").grid(row=11,sticky=W)

row_jump = 12
lbl_4 = Label(window, text="Please input simulation period \n (Format: XX:XX:XX-XX:XX:XX, 24-hour system)",justify=LEFT).grid(row=row_jump,sticky=W)
txt2 = Entry(window, width=20)
txt2.grid(row=row_jump+1,column=0,sticky=W)
txt2.insert(END, '18:00:00-19:00:00')
lbl_4_seg = Label(window, text="-----------------------------------------").grid(row=row_jump+2,sticky=W)

row_jump = 11+5
lbl_afc = Label(window, text="Choose AFC data file").grid(row=row_jump,sticky=W)
btn_afc  = Button(window, text="Open", command=Choose_afc_file).grid(row=row_jump,column=1)
lbl_afc_file = Label(window, text="Default: \'Editable_files\\AFC_TXN_2017-03-16.csv\'")
lbl_afc_file.grid(row=row_jump+1, sticky=W)
lbl_seg = Label(window, text="-----------------------------------------").grid(row=row_jump+2,sticky=W)

row_jump = 14+5
lbl_sjsc = Label(window, text="Choose SJSC data file").grid(row=row_jump,sticky=W)
btn_sjsc = Button(window, text="Open", command=Choose_sjsc_file).grid(row=row_jump,column=1)
lbl_sjsc_file = Label(window, text="Default: \'Editable_files\\SJSC_TXN_2017-03-16.csv\'")
lbl_sjsc_file.grid(row=row_jump+1, sticky=W)
lbl_seg = Label(window, text="-----------------------------------------").grid(row=row_jump+2,sticky=W)

row_jump = 25
lbl_3 = Label(window, text="Choose Time table folder").grid(row=row_jump,sticky=W)
btn_3 = Button(window, text="Open", command=Choose_time_table_path).grid(row=row_jump,column=1)
lbl_3_file = Label(window, text="Default: \'\\Time_table_folder\'")
lbl_3_file.grid(row=row_jump+1, sticky=W)
lbl_3_seg = Label(window, text="-----------------------------------------").grid(row=row_jump+2,sticky=W)

# lbl_5 = Label(window, text="Choose output folder (recommend in '\External_data')").grid(row=11,sticky=W)
# btn_5 = Button(window, text="Open", command=Choose_out_put_path).grid(row=11,column=1)
# lbl_5_file = Label(window, text="Unspecified output folder")
# lbl_5_file.grid(row=12, sticky=W)
# lbl_5_seg = Label(window, text="-----------------------------------------").grid(row=13,sticky=W)
row_jump = 25+5
btn_run = Button(window, text="Run", command=prepare_all).grid(row=row_jump,column=0)

lbl_5_seg = Label(window, text="").grid(row=row_jump+1,sticky=W)
lbl_6_finish = Label(window, text="")
lbl_6_finish.grid(row=row_jump+2, column=0, sticky=W)

def jump_NPM():
    global next_GUI
    os.system(next_GUI)


window.mainloop()