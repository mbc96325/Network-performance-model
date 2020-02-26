# Analyse the system performance using the passenger assignment results
# Inputs: carrier_state, station_state and passenger_state
#  station_state: station_id,queue_id,queue_line,queue_line_direction,queue_time_start,queue_time_end,queue_length
#  carrier_state: carrier_id,carrier_line,carrier_direction,carrier_capacity,link_start,link_end,link_entry_time,
#                   carrier_load
#  passenger_state: pax_id,pax_origin,pax_destination,pax_path,pax_entry_time,itinerary_id,arrival_station,arrival_time,
#                   departure_station,departure_time,departure_line,departure_direction,pax_number,denied_boarding_times
# Outputs: station crowding, passenger left behind rate, link congestion

from _DefaultValues import *
import os
import pandas as pd
import numpy as np
import time
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
pd.options.mode.chained_assignment = None  # default='warn'
max_denied_times = 10

def transfer_linecode_to_name(linecode):
    return LINE_CODE_NAME[linecode]

def format_timedelta(td):
    hours, remainder = divmod(td, 3600)
    minutes, seconds = divmod(remainder, 60)
    hours, minutes, seconds = int(hours), int(minutes), int(seconds)
    if hours < 10:
        hours = '0%s' % int(hours)
    if minutes < 10:
        minutes = '0%s' % minutes
    if seconds < 10:
        seconds = '0%s' % seconds
    return '%s:%s:%s' % (hours, minutes, seconds)

def train_load(carrier_state, time_period):
    # print (carrier_state)
    carrier_state = carrier_state.loc[(carrier_state['link_entry_time']>=time_period[0]) &
                                      (carrier_state['link_entry_time'] < time_period[1]) &
                                      (carrier_state['carrier_line'] != 10)] # filter out AEL
    # print (carrier_state)
    train_load = carrier_state.loc[:,['link_start','link_end',\
                                      'carrier_line','carrier_load']]
    train_load['LnID'] = train_load['carrier_line'].apply(transfer_linecode_to_name)
    train_load['Dir'] = carrier_state['carrier_direction'].apply(lambda x: 'UP' if x == 1  else 'DOWN')
    train_load['TrpID'] = carrier_state['carrier_id'].apply(lambda x: x.split('_')[-1])
    train_load['EntTime'] = carrier_state['link_entry_time'].apply(format_timedelta)
    train_load = train_load.rename(columns = {'link_start':'LnkStart','link_end':'LnkEnd',
                                              'carrier_line':'LnCode','carrier_load':'Load'})

    train_load['LnkID'] = train_load['LnkStart'].apply(int).apply(str) + '_' + train_load['LnkEnd'].apply(int).apply(str)
    train_load = train_load.sort_values(by = ['LnCode','Dir','LnkID','EntTime'])
    # print(train_load)
    return train_load



def station_crowding(station_state, time_period, time_interval, col_values):
    # calculate the station crowding level
    time_list = np.arange(time_period[0], time_period[1], time_interval)
    crowding = pd.DataFrame(index=range(len(station_state)),
                            columns=['station', 'line', 'direction', 'time_period_l', 'time_period_u', ] + col_values)
    col_time_start = 'last_train_departure'
    col_time_end = 'current_train_departure'

    irecord = 0
    state_grouped = station_state.groupby(['station_id', 'queue_line', 'queue_line_direction'])
    for platform, info in state_grouped:
        for t in time_list:
            t_lb = t
            t_ub = t + time_interval

            # Get all the records with line_arrival_time are between the start_time and end_time
            # Differentiate four different cases
            waiting_g1 = 0
            waiting_g2 = 0
            waiting_g3 = 0
            waiting_g4 = 0

            # Generate groups
            queue_g1 = info[((info[col_time_start] < t_lb) & (t_lb < info[col_time_end]) & (info[col_time_end] < t_ub))]
            queue_g2 = info[((info[col_time_start] >= t_lb) & (info[col_time_end] <= t_ub))]
            queue_g3 = info[((t_lb < info[col_time_start]) & (info[col_time_start] < t_ub) & (info[col_time_end] > t_ub))]
            queue_g4 = info[((info[col_time_start] < t_lb) & (info[col_time_end] >= t_ub)) |
                            ((info[col_time_start] <= t_lb) & (info[col_time_end] > t_ub)) |
                            ((info[col_time_start] < t_lb) & (info[col_time_end] > t_ub))]

            crowding.loc[irecord, ['station', 'line', 'direction', 'time_period_l', 'time_period_u']] = list(platform) + [t_lb, t_ub]
            # Calculate waiting passengers for each group
            for col_value in col_values:
                if len(queue_g1) > 0:
                    waiting_g1 = sum((queue_g1[col_time_end] - t_lb) / (queue_g1[col_time_end] - queue_g1[col_time_start]) * queue_g1[col_value])
                if len(queue_g2) > 0:
                    waiting_g2 = sum(queue_g2[col_value])
                if len(queue_g3) > 0:
                    waiting_g3 = sum((t_ub - queue_g3[col_time_start]) / (queue_g3[col_time_end] - queue_g3[col_time_start]) * queue_g3[col_value])
                if len(queue_g4) > 0:
                    waiting_g4 = sum((t_ub - t_lb) / (queue_g4[col_time_end] - queue_g4[col_time_start]) * queue_g4[col_value])

                # Calculate total waiting passengers
                waiting_pax = waiting_g1 + waiting_g2 + waiting_g3 + waiting_g4

                # Store the results in pandas
                crowding.loc[irecord, col_value] = waiting_pax

            irecord += 1

    # keep only the record with value
    crowding = crowding.iloc[:irecord, :]

    return crowding

def link_flow(carrier_state, time_period, time_interval):
    # calculate the link load level
    carrier_state = carrier_state.loc[(carrier_state['link_entry_time']>=time_period[0]) &
                                      (carrier_state['link_entry_time'] < time_period[1]) &
                                      (carrier_state['carrier_line'] != 10)] # filter out AEL
    carrier_state['link_entry_time_interval'] = carrier_state['link_entry_time'] // time_interval * time_interval
    link_flow_file = carrier_state.groupby(['link_start','link_end',\
                                           'carrier_line','carrier_direction',\
                                           'link_entry_time_interval']).sum().reset_index(drop=False)\
        [['link_start','link_end','carrier_line','carrier_direction','link_entry_time_interval','carrier_load']]

    link_flow_file['LnID'] = link_flow_file['carrier_line'].apply(transfer_linecode_to_name)
    link_flow_file['Dir'] = link_flow_file['carrier_direction'].apply(lambda x: 'UP' if x == 1  else 'DOWN')
    link_flow_file['EntTime'] = link_flow_file['link_entry_time_interval'].apply(format_timedelta)
    link_flow_file = link_flow_file.rename(columns = {'link_start':'LnkStart','link_end':'LnkEnd',
                                              'carrier_line':'LnCode','carrier_load':'Load'})

    link_flow_file['LnkID'] = link_flow_file['LnkStart'].apply(int).apply(str) + '_' + link_flow_file['LnkEnd'].apply(int).apply(str)
    link_flow_file = link_flow_file.pivot_table(index=['LnID','LnCode','Dir','LnkID','LnkStart','LnkEnd'], columns='EntTime', values=['Load'])
    link_flow_file.columns = [str(s2) for (s1, s2) in link_flow_file.columns.tolist()]
    link_flow_file = link_flow_file.reset_index(drop=False)
    link_flow_file = link_flow_file.sort_values(by = ['LnCode','Dir','LnkID'])

    # print(link_flow_file)

    return link_flow_file

def station_metrix(passenger_state, time_period, time_interval):
    # calculate the link load level
    passenger_state = passenger_state.loc[(passenger_state['arrival_time']>=time_period[0]) &
                                      (passenger_state['arrival_time'] < time_period[1]) &
                                      (passenger_state['departure_line'] != 10)] # filter out AEL

    passenger_state['arrival_time_interval'] = passenger_state['arrival_time'] // time_interval * time_interval
    passenger_state['waiting_time'] = (passenger_state['departure_time'] - passenger_state['arrival_time']) / 60 # min
    passenger_state['waiting_time'] = passenger_state['waiting_time'].apply(lambda x: round(x, 2)) # reserve two digits
    passenger_state['Board_1st'] = 0
    passenger_state['Board_2nd'] = 0
    passenger_state['Board_3rd'] = 0
    passenger_state['Board_>=4th'] = 0
    passenger_state.loc[passenger_state['denied_boarding_times'] == 0, 'Board_1st'] = 1
    passenger_state.loc[passenger_state['denied_boarding_times'] == 1, 'Board_2nd'] = 1
    passenger_state.loc[passenger_state['denied_boarding_times'] == 2, 'Board_3rd'] = 1
    passenger_state.loc[passenger_state['denied_boarding_times']>=3,'Board_>=4th'] = 1
    station_metrix = passenger_state.groupby(['departure_station','departure_line','departure_direction',
                                           'arrival_time_interval']).\
        agg({'Board_1st':'sum','Board_2nd':'sum', 'Board_3rd':'sum','Board_>=4th':'sum','waiting_time': 'mean','pax_number':'sum'}).reset_index(drop=False)

    station_metrix['EntTime'] = station_metrix['arrival_time_interval'].apply(format_timedelta)
    station_metrix = station_metrix.rename(columns = {'waiting_time':'Avg_WT','pax_number':'Arrivals','departure_station':'StnID'})
    station_metrix['Dir'] = station_metrix['departure_direction'].apply(lambda x: 'UP' if x == 1  else 'DOWN')
    station_metrix['LnID'] = station_metrix['departure_line'].apply(transfer_linecode_to_name)
    station_metrix['LB_rate'] = (station_metrix['Arrivals'] - station_metrix['Board_1st']) / station_metrix['Arrivals']
    station_metrix['LB_rate'] = station_metrix['LB_rate'].apply(lambda x: round(x, 2))
    station_metrix = station_metrix.sort_values(by=['departure_line','Dir','StnID','EntTime'])
    # print(link_flow_file)

    return station_metrix


class MetroViz(object):
    def __init__(self, carrier_state, file_tb_txn, time_period, time_interval):
        self.carrier_state = carrier_state
        self.file_tb_txn = file_tb_txn
        self.time_period = time_period
        self.time_interval = time_interval

    def MetroViz_link(self):
        # calculate the link load level
        carrier_state = self.carrier_state.loc[(self.carrier_state['link_entry_time'] >= self.time_period[0]) &
                                          (self.carrier_state['link_entry_time'] < self.time_period[1]) &
                                          (self.carrier_state['carrier_line'] != 10)]  # filter out AEL
        carrier_state['link_entry_time_interval'] = carrier_state['link_entry_time'] // self.time_interval * self.time_interval
        link_flow_file = carrier_state.groupby(['link_start', 'link_end', \
                                                'carrier_line', 'carrier_direction', \
                                                'link_entry_time_interval']).sum().reset_index(drop=False) \
            [['link_start', 'link_end', 'carrier_line', 'carrier_direction', 'link_entry_time_interval', 'carrier_load']]

        link_flow_file['LnID'] = link_flow_file['carrier_line']  #.apply(transfer_linecode_to_name)
        link_flow_file['Dir'] = link_flow_file['carrier_direction']  #.apply(lambda x: 'UP' if x == 1 else 'DOWN')
        link_flow_file['EntTime'] = link_flow_file['link_entry_time_interval'].apply(format_timedelta)
        link_flow_file = link_flow_file.rename(columns={'link_start': 'LnkStart', 'link_end': 'LnkEnd',
                                                        'carrier_line': 'LnCode', 'carrier_load': 'Flow'})

        link_flow_file['LnkID'] = link_flow_file['LnkStart'].apply(int).apply(str) + '_' + link_flow_file['LnkEnd'].apply(int).apply(str)

        link_flow_file = link_flow_file.reset_index(drop=False)
        link_flow_file = link_flow_file.sort_values(by=['LnkStart', 'LnkEnd', 'LnkID'])

        return link_flow_file

    def MetroViz_Enter_OD(self):
        # calculate the link load level
        Enter_OD = self.file_tb_txn.loc[:,['pax_origin','pax_destination','pax_tapin_time']]
        Enter_OD = Enter_OD.loc[Enter_OD['pax_origin']!=Enter_OD['pax_destination']]
        # Enter_OD = Enter_OD.loc[Enter_OD['pax_tapin_time'] <= (20 * 3600 + 30 * 60)]
        Enter_OD['time_interval'] = Enter_OD['pax_tapin_time'] // self.time_interval * self.time_interval

        Enter_OD['Demand'] = 1
        Enter_OD = Enter_OD.groupby(['pax_origin', 'pax_destination', 'time_interval']).sum().reset_index(drop=False)
        Enter_OD = Enter_OD.rename(columns = {'pax_origin':'OriStnID','pax_destination':'DesStnID'})
        Enter_OD['EntTime'] = Enter_OD['time_interval'].apply(format_timedelta)

        Enter_OD = Enter_OD.sort_values(by=['OriStnID', 'DesStnID', 'EntTime', 'EntTime'])

        return Enter_OD
    def MetroViz_output(self):
        link_flow = self.MetroViz_link()
        Enter_OD = self.MetroViz_Enter_OD()
        return link_flow,Enter_OD




# --------------------------------------------------- Main function -------------------------------------------------

# parameter setting
# if __name__ == '__main__':
#     PATH = ''
#     file_path = 'Assignment_18-19_2017-03-16_InclCEN50_Y_Empty_var_0_0/'
#
#
#
#     time_period = [18*3600, 19*3600]
#     time_interval_station = 15*60
#     time_interval_link = 15*60
#     time_interval_passenger = 60*60
#     time_interval_od = 15*60
#
#     output_file_station =PATH + file_path + 'station_crowding.csv'
#     output_file_link = PATH + file_path + 'link_load.csv'
#     output_file_passenger = PATH + file_path + 'passenger_left_behind_' + str(time_interval_passenger) + '.csv'
#     output_file_od = PATH + file_path + 'exit_od.csv'
#
#     # read files
#     file_station_state = pd.read_csv(PATH + file_path + 'station_state.csv')
#     file_carrier_state = pd.read_csv(PATH + file_path + 'carrier_state.csv')
#     file_passenger_state = pd.read_csv(PATH + file_path + 'passenger_state.csv')
#     file_od_state = pd.read_csv(PATH + file_path + 'od_state.csv')
#
#     tic = time.time()
#     #calculate station crowding
#     #print('calculate station crowding ...')
#     #col_values = ['arrivals', 'boarded', 'denied']
#     #crowding = station_crowding(file_station_state, time_period, time_interval_station, col_values)
#     #crowding.to_csv(output_file_station, index=False)
#     #
#     # calculate link load
#     print('calculate link load ...')
#     load = link_load(file_carrier_state, time_period, time_interval_link)
#     load.to_csv(output_file_link, index=False)
#
#     #-----calculate passenger left behind-----
#     print('calculate passenger left behind ...')
#     passenger = left_behind(file_passenger_state, time_period, time_interval_passenger)
#     passenger.to_csv(output_file_passenger, index=False)
#
#     #calculate exit od demand
#     print('calculate exit od demand ....')
#     od = exit_od(file_od_state, time_period, time_interval_od)
#     od.to_csv(output_file_od, index=False)
#
#     print('The elapsed time is %s seconds' % (time.time()-tic))