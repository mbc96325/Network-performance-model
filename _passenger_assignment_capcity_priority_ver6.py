
##############################################################################################
# This program is used to assign OD demand on network with link capacity constraints and flow priority
# Inputs: Passenger graph and set of passenger groups
# Output: Flow function
# Note: 1. the time is recorded as the minutes difference compared to a reference time (e.g. 7:00 am).
#       2. OD demand is time, demand in pandas.
#       3. Dictionaries in Python are implemented as hash tables, there is no ordering. To track the order of joined
#          queue with arrival times, it needs a list of order of the arrival times.
#       4. tap_in demand is in time period, for example 7:15 = 7:00-7:15

from _DefaultValues import *
import pandas as pd
import math
import copy
import csv
import time
from itertools import chain
import os
import numpy as np
pd.options.mode.chained_assignment = None  # default='warn'


# Define generic functions

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

def generate_queue_id(queue_station, queue_line, queue_direction):
    return str(int(queue_station)) + '_' + str(int(queue_line)) + '_' + str(int(queue_direction))


def generate_passenger_id(pax_origin, pax_destination, path, pax_departure):
    return str(int(pax_origin)) + '_' + str(int(pax_destination)) + '_' + str(int(path)) + '_' + str(int(pax_departure))


def create_carrier_list():
    # create carrier object list as a dictionary with key to be carrier ID
    # the format is {'line_direction_depFromFirstStation': Carrier object}
    global CARRIERS

    list_carriers = {}
    for carrier_id, info_carrier in CARRIERS.iterrows():
        # create an carrier object
        obj_carrier = Carrier(carrier_id, info_carrier.carrier_car_no, info_carrier.carrier_line,
                              info_carrier.carrier_direction, info_carrier.carrier_last_station,
                              info_carrier.carrier_serve_stations)
        list_carriers[carrier_id] = obj_carrier

    return list_carriers


def create_queue_pool():
    # create queue pool object list as a dictionary with the key to be the queue ID (station_line_direction)
    # the format is {'station_line_direction': Queue object}
    global QUEUES

    queue_pool = {}
    for queue_id, info_queue in QUEUES.iterrows():
        # create an carrier object
        obj_queue = Queue(queue_id, info_queue.initial_time_sim)
        queue_pool[queue_id] = obj_queue

    return queue_pool



# Define object classes
class ChoiceModel(object):
    def __init__(self, tb_path_attribute, beta):
        self.tb_path_attribute = tb_path_attribute
        self.beta = beta
        self.path_utility = self.calculate_path_utility(self.beta)
        self.path_fraction = self.calculate_path_fraction()

        # origin, destination, path_id, time_interval, in_vehicle_time, no_of_transfer, waiting_time
        # transfer_time, commonality_factor,

    def calculate_path_utility(self, beta):
        # Inputs: origin|destination|path_id|time_interval|att_1|att_2|att_3|
        self.tb_path_attribute['path_utility'] = np.exp(beta[0] +
                                                        beta[1] * self.tb_path_attribute['in_vehicle_time'] +
                                                        beta[2] * self.tb_path_attribute['no_of_transfer'] +
                                                        beta[3] * self.tb_path_attribute['waiting_time'] +
                                                        beta[4] * self.tb_path_attribute['transfer_time'] +
                                                        beta[5] * self.tb_path_attribute['commonality_factor'])
        return self.tb_path_attribute[['origin', 'destination', 'path_id', 'time_interval', 'path_utility']]

    def calculate_path_fraction(self):
        # inputs: 'origin', 'destination', 'path_id', 'time_interval', 'path_utility'
        self.path_utility['path_share'] = \
            self.path_utility.groupby(['origin', 'destination', 'time_interval'])['path_utility'].transform(lambda x: x / sum(x))

        return self.path_utility[['origin', 'destination', 'path_id', 'time_interval', 'path_share']]



class DemandModel(object):
    def __init__(self, tb_txn, choice_file, beta=DEFAULT_BETA, static=True):
        self.tb_txn = tb_txn   # ['user_id', 'pax_origin', 'pax_destination', 'pax_tapin_time', 'pax_tapout_time']
        self.tb_txn['tapin_ti'] = self.tb_txn['pax_tapin_time'] // TIME_INTERVAL_CHOICE * TIME_INTERVAL_CHOICE
        self.beta = beta
        self.static = static
        if self.static:
            self.choice_file = choice_file
        else:
            CM = ChoiceModel(choice_file, self.beta)
            self.choice_file = CM.path_fraction

        self.access_time = DEFAULT_ACCESS_TIME   # in seconds
        self.initialize_txn_table()

    def assign_random_path_based_on_choice_fraction(self):
        # rearrange the path fraction to the format
        # Inputs:  origin, destination, path_id, time_interval, path_share
        # Outputs: self.txn with pax_path

        # # Generate an empty od_t_path table  (deemed as not needed!)
        # od_t = self.choice_file[['origin', 'destination', 'time_interval']].drop_duplicates()
        # od_path_time = copy.deepcopy(od_t)
        # od_path_time['path_id'] = 1
        #
        # for k in np.arange(1, MAXIMUM_PATH_NUM):
        #     od_t['path_id'] = k
        #     od_path_time = od_path_time.append(od_t)
        #
        # # Merge the template with the actual path fraction data
        # path_fraction = od_path_time.merge(self.choice_file, on=['origin', 'destination', 'time_interval', 'path_id'],
        #                                    how='left').fillna(0)
        # table = pd.pivot_table(path_fraction, index=['origin', 'destination', 'time_interval'], columns='path_id',
        # values='path_share').reset_index()
        # Rearrange the choce table: origin, destination,  time_interval, path_id, path1_share, path2_share...
        choice_table = pd.pivot_table(self.choice_file, index=['origin', 'destination', 'time_interval'],
                                      columns='path_id', values='path_share').reset_index()
        choice_table.fillna(0, inplace=True)

        # Merge with txn file
        """fix me here, why operation table misses many paths (o|d|time|path fraction) for different time periods"""
        '''Because passengers' tap in time is beyond max time interval in choice file'''
        txn_choice = self.tb_txn.merge(choice_table, left_on=['pax_origin', 'pax_destination', 'tapin_ti'],
                                       right_on=['origin', 'destination', 'time_interval'], how='inner')
        txn_choice.drop(['origin', 'destination', 'time_interval'], axis=1, inplace=True)
        
        #print (len(self.tb_txn))
        #print (len(txn_choice))
        #print(len(self.tb_txn) - len(txn_choice))

        _, col_num_txn = self.tb_txn.shape
        _, col_num_txn_choice = txn_choice.shape
        path_num = col_num_txn_choice - col_num_txn
        path_list = np.arange(1, path_num + 1)

        # ver 1: Groupby and transform to randomly assign a path to each passenger (too slow)
        # np.random.seed(RND_SEED)
        # txn_choice['pax_path'] = txn_choice.groupby(['pax_origin', 'pax_destination', 'tapin_ti']).transform(
        #     lambda x: np.random.choice(path_list, len(x), p=txn_choice.head(1)[path_list].values[0] / sum(
        #         txn_choice.head(1)[path_list].values[0])))

        # ver 2: vectorized algorithm
        # Generate a random number between 1 and 100 (path fractions are in percentage)
        np.random.seed(RND_SEED)
        txn_choice['rand_num'] = np.random.randint(0, 100, len(txn_choice)) + 0.00000001   # to avoid boundary

        # scale up to make sure the choice fractions sum up to 1
        txn_choice['choice_sum'] = txn_choice[path_list].sum(axis=1)
        for path in path_list:
            txn_choice[path] = txn_choice[path]/txn_choice['choice_sum'] * 100

        # Generate cumulative choice fractions and assign passenger a path
        and_columns = ['and_' + str(x) for x in path_list]
        txn_choice[and_columns[0]] = (txn_choice[path_list[0]] > txn_choice['rand_num']) * path_list[0]
        for k in np.arange(1, path_num):
            # cumulative choice probability
            txn_choice[path_list[k]] = txn_choice[path_list[k]] + txn_choice[path_list[k-1]]

            # Operation to determine where the random numbers dwell in (path k and path k-1, then path k is true)
            txn_choice[and_columns[k]] = \
                ((txn_choice['rand_num'] >= txn_choice[path_list[k-1]]) &
                 (txn_choice['rand_num'] < txn_choice[path_list[k]])) * path_list[k]

        # the random chosen path is the sum of the and_columns
        txn_choice['pax_path'] = txn_choice[and_columns].sum(axis=1)

        # update the self.tb_txn table since some transactions may not be able to match with path choice table
        self.tb_txn = txn_choice[self.tb_txn.columns]
        self.tb_txn.loc[:,'pax_path'] = txn_choice['pax_path']

        return txn_choice['pax_path']


    def initialize_txn_table(self):
        # inputs: transaction table
        #  Outputs: tb_txn
        # ['user_id', 'pax_origin', 'pax_destination', 'pax_tapin_time' 'tapin_ti',
        # 'pax_path','pax_itinerary_ind', 'iti_source_station', 'iti_source_line', 'iti_source_direction',
        # 'iti_source_arrival_time', 'iti_source_departure_time',
        # 'iti_end_station', 'iti_end_arrival_time'
        # 'pax_number','denied_boarding_times']

        # Assign a randomized path for each transaction based on the path fractions for od_t
        # self.tb_txn.loc[:, 'pax_path'] = -1   # initialized value
        # self.tb_txn.groupby(['pax_origin', 'pax_destination', 'tapin_ti']).apply(self.assign_random_path)
        # for od_t, grp in self.tb_txn.groupby(['pax_origin', 'pax_destination', 'tapin_ti']):
        #     choice_fraction = self.CM.calculate_path_fraction(od_t)   # [[fractions], [path_ids]]
        #     user_path = np.random.choice(choice_fraction[1], len(grp), p=choice_fraction[0])  # a list of paths
        #     self.tb_txn.loc[grp.index, 'pax_path'] = user_path
        self.assign_random_path_based_on_choice_fraction()

        # Initialize other attributes used for event-based assignment
        self.tb_txn.loc[:, 'pax_itinerary_ind'] = 1
        txn_itinerary = pd.merge(self.tb_txn, ITINERARY,
                                 left_on=['pax_origin', 'pax_destination', 'pax_path', 'pax_itinerary_ind'],
                                 right_on=['origin', 'destination', 'path', 'itinerary'], how='left')

        self.tb_txn['iti_source_station'] = txn_itinerary['boarding_station']
        self.tb_txn['iti_source_line'] = txn_itinerary['line']
        self.tb_txn['iti_source_direction'] = txn_itinerary['direction']
        #----Assume access time is normally distribution-----
        # add acess time to passengers
        # print(ACC_EGR_TIME.columns)
        self.tb_txn = self.tb_txn.merge(ACC_EGR_TIME,left_on = ['pax_origin','iti_source_line','iti_source_direction'],
                                              right_on = ['station','line','direction'],how='left')
        self.tb_txn.loc[self.tb_txn['access_time'].isna(),'access_time' ] = self.access_time
        self.tb_txn = self.tb_txn.drop(columns= ['station','line','direction','egress_time'])
        if RND_FLAG !=0:
            b_a = np.minimum(np.array([60]*len(self.tb_txn)), np.array(self.tb_txn['access_time'])) # 1min
            random_access_time = (np.random.random_sample((len(self.tb_txn),)) - 0.5) * 2 * b_a   # U [acc_time-b_a, *+b_a]
            self.tb_txn['access_time'] += random_access_time
        self.tb_txn['access_time'] *= DEFAULT_ACCESS_TIME_DECREASE_FACTOR
        self.tb_txn['iti_source_arrival_time'] = self.tb_txn['pax_tapin_time'] + self.tb_txn['access_time']
        self.tb_txn.loc[:, 'iti_source_departure_time'] = -1
        self.tb_txn['iti_end_station'] = txn_itinerary['alighting_station']
        self.tb_txn.loc[:, 'iti_end_arrival_time'] = -1
        self.tb_txn.loc[:, 'pax_number'] = 1
        self.tb_txn.loc[:, 'denied_boarding_times'] = 0

        self.tb_txn['index'] = self.tb_txn['user_id']
        # add disrupt information
        if len(link_disrupt)>0:
            self.tb_txn = self.tb_txn.merge(DISRUPT_PATH, left_on = ['pax_origin','pax_destination','pax_path'],
                              right_on = ['pax_origin','pax_destination','pax_path'],how = 'left').fillna(-1)
            self.tb_txn = self.tb_txn.drop(columns = ['disrupt'])
        self.tb_txn.set_index('index', inplace=True)

        return self.tb_txn


class Passenger(object):
    def __init__(self):
        self.columns = ['user_id', 'pax_origin', 'pax_destination', 'pax_tapin_time' 'tapin_ti',
                        'pax_path', 'pax_itinerary_ind', 'iti_source_station', 'iti_source_line', 'iti_source_direction',
                        'iti_source_arrival_time', 'iti_source_departure_time',
                        'iti_end_station', 'iti_end_arrival_time',
                        'pax_number', 'denied_boarding_times']
        self.record = RECORDINFO
        self.info_boarding = []

    def empty_passenger_data_frame(self):
        pax = pd.DataFrame(columns=self.columns)
        # pax.ix[:, :len(pax.columns) - 2] = pax.ix[:, :len(pax.columns) - 2].astype(int)  # integer for information
        # pax.ix[:, len(pax.columns) - 1] = pax.ix[:, len(pax.columns) - 1].astype(float)  # float for pax number

        return pax

    def create_passenger_data_frame(self, origin, destination, path, departure_time, itinerary_ind, arrival_station,
                                    arrival_time, next_boarding_station, next_alighting_station, next_boarding_line,
                                    next_boarding_direction, pax_number, denied_boarding_times):
        pax = self.empty_passenger_data_frame()
        pax_id = generate_passenger_id(origin, destination, path, departure_time)
        pax.loc[pax_id] = [origin, destination, path, departure_time, itinerary_ind, arrival_station, arrival_time,
                           next_boarding_station, next_alighting_station, next_boarding_line, next_boarding_direction,
                           pax_number, denied_boarding_times]

        return pax

    def pax_func(self, tb_demand_itinerary):
        #  pax is the passenger data frame of demand
        pax = tb_demand_itinerary
        return self.create_passenger_data_frame(pax.origin, pax.destination, pax.path, pax.time, pax.itinerary,
                                                pax.boarding_station, pax.time, pax.boarding_station,
                                                pax.alighting_station, pax.line, pax.direction, pax.demand, 0)

    def save_state_boarding(self, pax_data_frame, departure_time):
        global passenger_state
        # create passenger itinerary and write it to csv file when it finished the journey
        # format:

        if self.record == 1:

            info = pax_data_frame[['user_id','pax_origin','pax_destination','iti_source_arrival_time', 'iti_source_station', 'iti_source_line',
                                   'iti_source_direction', 'pax_number', 'denied_boarding_times']].rename(
                columns={'iti_source_station': 'departure_station', 'iti_source_line': 'departure_line',
                         'iti_source_direction': 'departure_direction', 'iti_source_arrival_time':'arrival_time'})

            info['departure_time'] = departure_time
            passenger_state.append(info)

    def save_state_tapout(self, pax_data_frame, arrival_time):
        global OD_state
        # create passenger itinerary and write it to csv file when it finished the journey
        # format:
        # 'user_id', 'pax_origin', 'pax_destination', 'pax_tapin_time' 'tapin_ti',
        # 'pax_path', 'pax_itinerary_ind', 'iti_source_station', 'iti_source_line', 'iti_source_direction',
        # 'iti_source_arrival_time', 'iti_source_departure_time',
        # 'iti_end_station', 'iti_end_arrival_time',
        # 'pax_number', 'denied_boarding_times'
        # if self.record == 1:
        # for pax_id, pax_info in pax_data_frame.iterrows():
        #     csv_writer.writerow(
        #         (pax_id, pax_info.pax_origin, pax_info.pax_destination, pax_info.pax_path,
        #          pax_info.pax_departure_time, pax_info.pax_itinerary_ind,
        #          pax_info.arrival_station, pax_info.arrival_time, pax_info.next_boarding_station,
        #          departure_time, pax_info.next_boarding_line, pax_info.next_boarding_direction,
        #          pax_info.pax_number, pax_info.denied_boarding_times))
        info = pax_data_frame[['user_id','pax_origin', 'pax_destination','pax_path', 'pax_tapin_time', 'pax_number']].rename(
            columns={'pax_origin': 'origin', 'pax_destination': 'destination', 'pax_tapin_time': 'entry_time',
                     'pax_number': 'flow'})
        info['exit_time'] = arrival_time

        OD_state.append(info)


class Queue(object):
    # the queue is differentiated by line and direction, pax join different queues based on their boarding line and dir
    # the queue is in the DataFrame format: tb_txn
    #         # ['user_id', 'pax_origin', 'pax_destination', 'pax_tapin_time' 'tapin_ti',
    #         # 'pax_path','pax_itinerary_ind', 'iti_source_station', 'iti_source_line', 'iti_source_direction',
    #         # 'iti_source_arrival_time', 'iti_source_departure_time',
    #         # 'iti_end_station', 'iti_end_arrival_time'
    #         # 'pax_number','denied_boarding_times']

    def __init__(self, queue_id, initial_time_sim):
        self.passenger_queue = []  # the same format with tb_txn in DemandModel object
        self.last_tapin_update = initial_time_sim   # keep track of new tap-in passengers update time
        self.last_train_departure = initial_time_sim
        self.queue_id = queue_id
        self.record = RECORDINFO

    def accumulate_tap_in_txn(self, t_lower, t_upper):
        global TB_TXN

        # accumulate new tap-in transactions at platform and put them into queues
        # TB_TXN
        queue_station = int(self.queue_id.split('_')[0])
        queue_line = int(self.queue_id.split('_')[1])
        queue_direction = int(self.queue_id.split('_')[2])

        new_tap_ins = TB_TXN.loc[(TB_TXN.iti_source_station == queue_station) &
                      (TB_TXN.iti_source_line == queue_line) &
                      (TB_TXN.iti_source_direction == queue_direction) &
                      (TB_TXN.iti_source_arrival_time >= t_lower) &
                      (TB_TXN.iti_source_arrival_time < t_upper)]

        pax_transfer = new_tap_ins.loc[new_tap_ins['iti_source_station'] == new_tap_ins['iti_end_station']]

        new_tap_ins = new_tap_ins.loc[new_tap_ins['iti_source_station'] != new_tap_ins['iti_end_station']]
        new_tap_ins['first_train_arrival_time'] = -1 # used to record the first train for the passenger
        return new_tap_ins, pax_transfer


    def update_waiting_queue_on_arrival(self, transfer_passengers):
        # With consideration of transfer walking time, when train arrives, transfer passengers join the waiting
        # queue at a future time period (transfer walking time is processed in off-loading pax in Carrier object)
        # Be aware that it may happen that the current event at the platform is earlier than transfer arrival
        if len(self.passenger_queue) > 0:
            self.passenger_queue = self.passenger_queue.append(transfer_passengers,sort =False)
        else:
            self.passenger_queue = transfer_passengers

    def update_waiting_queue_on_departure(self, event_time):
        # Update the new tap ins between last update and event time (train departure time)
        # tic = time.time()
        # extract the new tap in transactions
        new_tap_in, pax_transfer = self.accumulate_tap_in_txn(self.last_tapin_update, event_time)

        # update the waiting queue
        if len(self.passenger_queue) > 0:
            self.passenger_queue = self.passenger_queue.append(new_tap_in,sort = False)
        else:
            self.passenger_queue = new_tap_in

        # update the last update time
        self.last_tapin_update = event_time
        return pax_transfer

    def save_state(self, station, queue_line, queue_line_direction, queue_time_start, queue_time_end,
                   new_arrival_num, new_boarding_num, total_in_platform, denied_num, branch_num):
        global station_state
        if self.record == 1:
            # write attributes to csv file for statistical analysis and visualization
            if queue_time_start >= queue_time_end:
                queue_time_start = queue_time_end - DEFAULT_HEADWAY
            station_state.append(pd.DataFrame({'queue_id': self.queue_id, 'station_id': station,
                                                     'queue_line': queue_line, 'queue_line_direction': queue_line_direction,
                                                     'last_train_departure': queue_time_start,
                                                     'current_train_departure': queue_time_end, 'arrivals': new_arrival_num,
                                                     'boarded': new_boarding_num, 'denied': denied_num,
                                               'branch': branch_num}, index=[0]))

        # print (station_state)


class Carrier(object):
    def __init__(self, carrier_id, carrier_car_no, carrier_line, carrier_direction, carrier_last_station,
                 carrier_serve_stations):
        # Initialize the Carrier object
        # 1. Passengers is a list of Passenger object, passengers = [pax1, pax2, ...]
        # 2. Transfer passengers are those who transfer to other lines after alighting the current line
        #    Format: {(next_line, next_direction): pax_object}
        self.carrier_id = carrier_id
        self.carrier_line = carrier_line
        self.carrier_direction = carrier_direction
        self.carrier_capacity = carrier_car_no * DEFAULT_CAR_CAP_1
        self.carrier_car_no = carrier_car_no
        self.carrier_last_station = carrier_last_station
        self.carrier_serve_stations = [int(y) for y in carrier_serve_stations.split('_')]
        self.load = 0
        self.egress_time = DEFAULT_EGRESS_TIME
        self.onboard_passengers = pd.DataFrame()
        self.record = RECORDINFO

    def offload_passenger(self, station, event_time, carrier_id):
        global ITINERARY

        # Offload passengers
        # 1. Update the carrier load
        # 2. Update the onboard passengers
        # 3. Update the transfer passengers
        # Passengers are stored in pandas DataFrame
        # Format:
        # ['user_id', 'pax_origin', 'pax_destination', 'pax_tapin_time' 'tapin_ti',
        # 'pax_path','pax_itinerary_ind', 'iti_source_station', 'iti_source_line', 'iti_source_direction',
        # 'iti_source_arrival_time', 'iti_source_departure_time',
        # 'iti_end_station', 'iti_end_arrival_time'
        # 'pax_number','denied_boarding_times']

        if self.load > 0:   # there are on-board passengers when train arrives at the station
            # process alighted passengers
            # **************************************************************************************************
            disrupt_flag = 0
            pax_alighted_disrupt = pd.DataFrame()
            # Deal with link disrupt
            if len(link_disrupt) > 0:
                direction = int(carrier_id.split('_')[1])
                if (station,direction) in DISRUPT_STATION:
                    if event_time >= DISRUPT_STATION[(station,direction)][0] and event_time <= DISRUPT_STATION[(station,direction)][1]:
                        disrupt_flag = 1
            if disrupt_flag == 1:
                # Off load all
                pax_alighted = self.onboard_passengers.loc[self.onboard_passengers['iti_end_station'] == station]

                # update carrier load and update onboard passengers
                pax_alighted_disrupt = \
                    self.onboard_passengers.loc[(self.onboard_passengers['iti_end_station'] != station) &
                                                (self.onboard_passengers['iti_end_station'] > 0)]
                self.onboard_passengers.drop(self.onboard_passengers.index, inplace = True)
                # update arrival information and denied boarding times (0 when just arrive at a station)
            else:
                pax_alighted = self.onboard_passengers.loc[self.onboard_passengers['iti_end_station'] == station]

                # update carrier load and update onboard passengers
                self.onboard_passengers = \
                    self.onboard_passengers.loc[(self.onboard_passengers['iti_end_station'] != station) &
                                                (self.onboard_passengers['iti_end_station'] > 0)]
            self.load = self.onboard_passengers['pax_number'].sum()

            # update arrival information and denied boarding times (0 when just arrive at a station)
            pax_alighted.loc[:, 'iti_end_arrival_time'] = event_time

            # **************************************************************************************************
            # passengers arrival at final destination, then save the state
            pax_destination = pax_alighted.loc[pax_alighted.pax_destination == station]

            egress_time = ACC_EGR_TIME.loc[(ACC_EGR_TIME['station'] == station)&
                                           (ACC_EGR_TIME['line'] == self.carrier_line)&
                                           (ACC_EGR_TIME['direction'] == self.carrier_direction),'egress_time']
            if len(egress_time) == 0:
                egress_time = self.egress_time
            else:
                egress_time = egress_time.values[0]

            if RND_FLAG != 0:
                b_a = min(60, egress_time)  # 1min
                random_egress_time = (np.random.random_sample(
                    (len(pax_destination),)) - 0.5) * 2 * b_a  # U [acc_time-b_a, *+b_a]
                egress_time += random_egress_time
            egress_time *= DEFAULT_EGRESS_TIME_DECREASE_FACTOR
            Passenger().save_state_tapout(pax_destination, event_time + egress_time)
            # **************************************************************************************************

            # possible transfers
            # Merge operation exclude index, remember to reset and keep it
            # update the itinerary index
            pax_alighted.pax_itinerary_ind = pd.to_numeric(pax_alighted.pax_itinerary_ind) + 1
            pax_transfer = pax_alighted.reset_index().merge(ITINERARY,
                                  left_on=['pax_origin', 'pax_destination', 'pax_path', 'pax_itinerary_ind'],
                                  right_on=['origin', 'destination', 'path', 'itinerary'],
                                  how='inner').set_index('index').dropna(how='any')

            if len(pax_transfer) > 0:   # if pax_merged is not empty
                # if the merged itinerary is NAN, then the pax has reached the destination, otherwise update itinerary
                pax_transfer['iti_source_station'] = pax_transfer['boarding_station']
                pax_transfer['iti_source_line'] = pax_transfer['line']
                pax_transfer['iti_source_direction'] = pax_transfer['direction']
                pax_transfer.loc[:, 'iti_source_arrival_time'] = -1   # depending on which transfer platform is
                pax_transfer.loc[:, 'iti_source_departure_time'] = -1
                pax_transfer['iti_end_station'] = pax_transfer['alighting_station']
                pax_transfer.loc[:, 'iti_end_arrival_time'] = -1
                pax_transfer.loc[:, 'denied_boarding_times'] = 0

                # return transfer passengers
                transfer_passengers = pax_transfer.iloc[:, :len(pax_alighted.columns)].dropna(how='any')

                # print('carrier %s with load %s after alight at station %s' % (self.carrier_id, self.load, station))
                # print('transfer pax at station %s from carrier %s' % (station, self.carrier_id))
                # print(transfer_passengers)
            else:
                transfer_passengers = {}
            if len(pax_alighted_disrupt) > 0: #put them into the queue of this station
                pax_alighted_disrupt['iti_source_station'] = station
                line = int(carrier_id.split('_')[0])
                pax_alighted_disrupt['iti_source_line'] = line
                pax_alighted_disrupt['iti_source_direction'] = int(carrier_id.split('_')[1])
                pax_alighted_disrupt['iti_source_arrival_time'] = event_time
                pax_alighted_disrupt['iti_source_departure_time'] = -1
                queue_id = generate_queue_id(station, line, direction)
                _queue = QUEUES_POOL.get(queue_id)
                _queue.update_waiting_queue_on_arrival(pax_alighted_disrupt)

        else:
            transfer_passengers = {}

        return transfer_passengers

    def load_passenger(self, station, event_time):
        # carrier load passengers and update the queue information at station at the same time
        # 1. update on-board passengers
        # 2. update waiting to board passengers at the station
        global QUEUES_POOL, EMPTY_TRAIN_TIME_LIST

        queue_id = generate_queue_id(station, self.carrier_line, self.carrier_direction)
        _queue = QUEUES_POOL.get(queue_id)    # queue reference to QUEUES_POOL, changes of it will change the pool
        new_boardings = 0
        denied_num = 0
        branch_num = 0

        #******Update carrier capacity based on station*******
        # if ((station, self.carrier_line, self.carrier_direction) in DEFAULT_CAP_2_STATION):
        #     self.carrier_capacity = self.carrier_car_no * DEFAULT_CAR_CAP_2
        #     # print ('capacity 2 is used')
        # else:
        #     self.carrier_capacity = self.carrier_car_no * DEFAULT_CAR_CAP_1
        #*****************************************************
        #******Update carrier capacity of effective capacity*******
        # Cap = DEFAULT_CAR_CAP_1 + EFFECTIVE_CAP_PARA_LOAD*train load + EFFECTIVE_CAP_PARA_WAIT * waiting pax
        if (station, self.carrier_line, self.carrier_direction) in DEFAULT_CAP_2_STATION:
            if EFFECTIVE_CAP_TRUE:
                if len(link_disrupt):
                    wait_pax = \
                        _queue.passenger_queue.loc[(_queue.passenger_queue['iti_source_arrival_time'] < event_time) &
                                               (_queue.passenger_queue['iti_end_station'].apply(int).isin(self.carrier_serve_stations))&
                                                    ((_queue.passenger_queue['d_time_start'] > event_time)
                                                     |(_queue.passenger_queue['d_time_end'] < event_time))]
                else:
                    wait_pax = _queue.passenger_queue.loc[(_queue.passenger_queue['iti_source_arrival_time'] < event_time) &
                                                          (_queue.passenger_queue['iti_end_station'].apply(int).isin(self.carrier_serve_stations))]
                self.carrier_capacity = int(math.ceil(self.carrier_car_no * DEFAULT_CAR_CAP_1 + EFFECTIVE_CAP_PARA_LOAD * self.load
                                                      + EFFECTIVE_CAP_PARA_WAIT * len(wait_pax)))
            else:
                self.carrier_capacity = self.carrier_car_no * DEFAULT_CAR_CAP_1 # new version: always use the DEFAULT_CAR_CAP_1
        else:
            self.carrier_capacity = self.carrier_car_no * DEFAULT_CAR_CAP_1

        #*****************************************************
        residual_current = self.carrier_capacity - self.load
        if residual_current < 0: # in case the change of capacity casued load>capacity
            self.carrier_capacity = self.load
            residual_current = 0
        # for empty train arrangement, set the residual_current to be 0, and pop up the control strategy
        #---------------A new judgement for empty train------------
        if (self.carrier_id, event_time) in EMPTY_TRAIN_TIME_LIST:
            residual_current = 0  # empty train arrangement, no passenger can board
        # --------------
        # if self.carrier_id == '11_1_508': #11502 65968
        #     a=1
        if _queue is not None:   # if there is waiting passengers
            # generated the to be assigned pax in the queue (platform arrival time is less than event time)
            # and also the train goes to their destination (branch line problems)
            if len(link_disrupt)>0:
                to_be_assigned_pax = \
                    _queue.passenger_queue.loc[(_queue.passenger_queue['iti_source_arrival_time'] < event_time) &
                                           (_queue.passenger_queue['iti_end_station'].apply(int).isin(self.carrier_serve_stations))&
                                                ((_queue.passenger_queue['d_time_start'] > event_time)
                                                 |(_queue.passenger_queue['d_time_end'] < event_time))] # disrupt_time
            else:
                # a= _queue.passenger_queue
                # print(len(_queue.passenger_queue))
                to_be_assigned_pax = \
                    _queue.passenger_queue.loc[(_queue.passenger_queue['iti_source_arrival_time'] < event_time) &
                                           (_queue.passenger_queue['iti_end_station'].apply(int).isin(self.carrier_serve_stations))] # disrupt_time
            if len(to_be_assigned_pax) > 0:  # if there is waiting to be assigned passengers
                if residual_current > 0:  # if there is capacity to load passengers
                    # sort the pax based on platform arrival time
                    to_be_assigned_pax.sort_values(by='iti_source_arrival_time', ascending=True, inplace=True)
                    # deny = to_be_assigned_pax.loc[to_be_assigned_pax.denied_boarding_times>=1]
                    # print(denied_pax)
                    new_boardings = min(residual_current, len(to_be_assigned_pax))   # either the capacity or all board
                    boarded_pax = to_be_assigned_pax.head(new_boardings)  # the first k passengers will board
                    boarded_pax.loc[:, 'iti_source_departure_time'] = event_time  # update departure time from platform

                    # update on-board passengers and carrier load
                    self.onboard_passengers = self.onboard_passengers.append(boarded_pax,sort=False)
                    self.load += new_boardings

                    # update waiting queue on platform (drop the passengers from the queue already boarded)
                    if len(boarded_pax) > 0:
                        boarded_user_ids = boarded_pax['user_id'].values
                        _queue.passenger_queue = \
                            _queue.passenger_queue[~_queue.passenger_queue['user_id'].isin(boarded_user_ids)]

                    if residual_current < len(to_be_assigned_pax):  # denied boardings
                        branch_pax = to_be_assigned_pax[to_be_assigned_pax['denied_boarding_times'] == 0]
                        branch_num = to_be_assigned_pax['pax_number'].sum() if len(branch_pax) > 0 else 0
                        denied_pax = to_be_assigned_pax.tail(len(to_be_assigned_pax) - residual_current)
                        denied_num = len(denied_pax)
                        denied_pax_ids = denied_pax['user_id'].values
                        _queue.passenger_queue.loc[
                            _queue.passenger_queue['user_id'].isin(denied_pax_ids), 'denied_boarding_times'] += 1

                    # ********************************* PASSENGER STATE ***************************************
                    # save passenger state when they successfully board the carrier
                    # print('_________________________________________________________')
                    # print(_queue.passenger_queue[block_counter_time])
                    Passenger().save_state_boarding(boarded_pax, event_time)
                    # **********************************************************************************************

                else:   # all to be assigned passengers denied boarding times increase by 1
                    denied_pax_ids = to_be_assigned_pax['user_id'].values
                    _queue.passenger_queue.loc[
                        _queue.passenger_queue['user_id'].isin(denied_pax_ids), 'denied_boarding_times'] += 1

        return new_boardings, denied_num, branch_num

    def save_state(self, link_start, link_end, link_entry_time):
        global carrier_state
        if self.record == 1:
            # write attributes to csv file for statistical analysis and visualization
            carrier_state.append(pd.DataFrame({'carrier_id': self.carrier_id, 'carrier_line': self.carrier_line,
                                                     'carrier_direction': self.carrier_direction,
                                                     'carrier_car_no': self.carrier_car_no,
                                                     'carrier_capacity': self.carrier_capacity, 'link_start': link_start,
                                                     'link_end': link_end, 'link_entry_time': link_entry_time,
                                                     'carrier_load': self.load}, index=[0]))


class Event(object):
    global NETWORK
    global LIST_CARRIERS
    global QUEUES_POOL
    global TRANSFER
    global OUT_PUT

    def __init__(self, event_id, event_time, event_station, event_type, carrier_id):
        self.event_id = event_id
        self.event_time = event_time
        self.event_station = event_station
        self.event_type = event_type
        self.carrier_id = carrier_id
        self.transfer_time = DEFAULT_TRANSFER_TIME

    def process_event(self):
        _carrier = LIST_CARRIERS[self.carrier_id]   # carrier list is created beforehand

        if self.event_type == 0:   # arrival event (carrier offload pax, pax join queue)
            # tic = time.time()
            # update carrier information and return transfer passengers
            transfer_passengers = _carrier.offload_passenger(self.event_station, self.event_time, self.carrier_id)
            # print('carrier %s with load %s after offloading at station %s at time %s' %
            #       (_carrier.carrier_id, _carrier.load, self.event_station, self.event_time))

            # if transfer_passengers is not empty, join them into the corresponding queue
            if len(transfer_passengers) > 0:
                transfer_grouped = \
                    transfer_passengers.groupby(['iti_source_station', 'iti_source_line', 'iti_source_direction'])

                for queue_info, pax in transfer_grouped:
                    queue_id = generate_queue_id(queue_info[0], queue_info[1], queue_info[2])

                    # find the corresponding queue
                    _queue = QUEUES_POOL.get(queue_id)
                    if _queue is None:    # due to the inconsistency between the raw file of network and time table
                        print('Missing platform in input file:', queue_id)
                        continue

                    # for debug 
                    #if queue_info[0]== 1: # CEN
                        #print([self.event_time, len(queue_transfer_passengers)])
                        #print([self.event_time, len(transfer_passengers.loc[pax_list, :])])
                        #if len(queue_transfer_passengers) != len(transfer_passengers.loc[pax_list, :]):
                            #transfer_passengers.loc[pax_list, :].to_csv('1.csv')

                    # update the waiting queue on arrival, considering transfer walking time
                    platform_x = generate_queue_id(self.event_station, _carrier.carrier_line, _carrier.carrier_direction)
                    platform_y = queue_id
                    transfer_time = sum(
                        TRANSFER_WT.loc[(TRANSFER_WT.platform_x == platform_x) &
                                 (TRANSFER_WT.platform_y == platform_y)]['walking_time'])   # in seconds
                    #---- assume transfer time is uniformly distributed---
                    if transfer_time == 0:
                        transfer_time = self.transfer_time
                    if RND_FLAG !=0:
                        b_a = min(60, transfer_time)
                        random_transfer_time = (np.random.random_sample((len(pax),))-0.5)*2*b_a + transfer_time # U [0,2*transfer_time]
                    else:
                        random_transfer_time = transfer_time
                    random_transfer_time *= DEFAULT_TRANSFER_TIME_DECREASE_FACTOR
                    pax.loc[:, 'iti_source_arrival_time'] = self.event_time + np.round(random_transfer_time)

                    _queue.update_waiting_queue_on_arrival(pax)

                # print('Arrival event:', time.time() - tic)

        elif self.event_type == 1:  # departure event (waiting queue, carrier load pax)
            # tic = time.time()
            # update the waiting pax in the corresponding queue
            queue_id = generate_queue_id(self.event_station, _carrier.carrier_line, _carrier.carrier_direction)
            _queue = QUEUES_POOL.get(queue_id)

            if _queue is not None:  # due to the inconsistency between the raw file of network and time table
                # Update queue before loading passengers
                transfer_passengers = _queue.update_waiting_queue_on_departure(self.event_time)


                last_train_departure = _queue.last_train_departure
                current_train_departure = self.event_time

                # Calculate the new arrivals between last and current train departures (tap-ins and transfers)
                total_in_platform = len(_queue.passenger_queue)
                # b=_queue.passenger_queue
                # b = b.merge(new_arrivals,left_on =['user_id'],right_on = ['user_id'],how = 'left')
                new_arrivals = \
                    _queue.passenger_queue[(_queue.passenger_queue['iti_source_arrival_time'] >= last_train_departure) &
                                           (_queue.passenger_queue['iti_source_arrival_time'] < current_train_departure)]

                new_arrival_num = new_arrivals['pax_number'].sum() if len(new_arrivals) > 0 else 0

                # carrier loading pax and update the queue information at stations at the same time
                # new boarding num is the total boarded passengers when train departure from the station

                new_boarding_num, denied_num, branch_num = _carrier.load_passenger(self.event_station, self.event_time)

                # calculate the denied passengers between last and current train departures
                # the not willing to boarding passengers are those whose destination is not in the train serving list,
                # due to branch lines and short-turning operations
                # waiting_pax = \
                #     _queue.passenger_queue[(_queue.passenger_queue['iti_source_arrival_time'] >= last_train_departure) &
                #                            (_queue.passenger_queue['iti_source_arrival_time'] < current_train_departure)]
                # denied = waiting_pax[waiting_pax['denied_boarding_times'] > 0]
                # denied_num = denied['pax_number'].sum() if len(denied) > 0 else 0

                # calculate not willing boarding passengers


                # ***************************************STATION STATE ********************************************
                # save queue state after carrier loading passengers
                _queue.save_state(self.event_station, _carrier.carrier_line,
                                  _carrier.carrier_direction, _queue.last_train_departure, self.event_time,
                                  new_arrival_num, new_boarding_num, total_in_platform, denied_num, branch_num)
                # ***************************************************************************************************

                # update the last train departure time as the current event time
                _queue.last_train_departure = self.event_time   # update the last train departure time from the platform

                # ***************************************CARRIER STATE**********************************************
                #  save carrier state when it departures (link load)
                if _carrier.carrier_last_station != self.event_station:
                    link_start = self.event_station
                    link_end = NETWORK.loc[(NETWORK.carrier_line == _carrier.carrier_line) &
                                       (NETWORK.carrier_direction == _carrier.carrier_direction) &
                                       (NETWORK.link_start == link_start)].link_end.values[0]

                    _carrier.save_state(link_start, link_end, self.event_time)
                # ***************************************************************************************************
            # print('Departure event:', time.time() - tic)
            # ************ put people with first transfers to next station:e.g. station 1 and 39
            transfer_passengers['pax_itinerary_ind'] += 1
            pax_transfer = transfer_passengers.merge(ITINERARY, left_on=['pax_origin', 'pax_destination', 'pax_path',
                                                                       'pax_itinerary_ind'],
                                              right_on=['origin', 'destination', 'path', 'itinerary'],
                                              how='inner')
            if len(pax_transfer) > 0:  # if pax_merged is not empty

                # if the merged itinerary is NAN, then the pax has reached the destination, otherwise update itinerary
                pax_transfer['iti_source_station'] = pax_transfer['boarding_station']
                pax_transfer['iti_source_line'] = pax_transfer['line']
                pax_transfer['iti_source_direction'] = pax_transfer['direction']
                pax_transfer.loc[:, 'iti_source_arrival_time'] = -1  # depending on which transfer platform is
                pax_transfer.loc[:, 'iti_source_departure_time'] = -1
                pax_transfer['iti_end_station'] = pax_transfer['alighting_station']
                pax_transfer.loc[:, 'iti_end_arrival_time'] = -1
                pax_transfer.loc[:, 'denied_boarding_times'] = 0
                pax_transfer = pax_transfer.iloc[:, :len(new_arrivals.columns)].dropna(how='any')

                transfer_grouped = pax_transfer.groupby(['iti_source_station', 'iti_source_line', 'iti_source_direction'])

                for queue_info, pax in transfer_grouped:
                    queue_id = generate_queue_id(queue_info[0], queue_info[1], queue_info[2])

                    # find the corresponding queue
                    _queue = QUEUES_POOL.get(queue_id)
                    if _queue is None:  # due to the inconsistency between the raw file of network and time table
                        print('Missing platform in input file:', queue_id)
                        continue

                    # update the waiting queue on arrival, considering transfer walking time
                    platform_x = generate_queue_id(self.event_station, _carrier.carrier_line, _carrier.carrier_direction)
                    platform_y = queue_id
                    transfer_time = sum(
                        TRANSFER_WT.loc[(TRANSFER_WT.platform_x == platform_x) &
                                             (TRANSFER_WT.platform_y == platform_y)]['walking_time'])  # in seconds
                    # ---- assume transfer time is uniformly distributed---
                    if transfer_time == 0:
                        transfer_time = self.transfer_time

                    if  RND_FLAG !=0:
                        b_a = min(60, transfer_time)
                        random_transfer_time = (np.random.random_sample((len(pax),))-0.5)*2*b_a + transfer_time # U [0,2*transfer_time]
                    else:
                        random_transfer_time = transfer_time
                    random_transfer_time *= DEFAULT_TRANSFER_TIME_DECREASE_FACTOR
                    pax.loc[:, 'iti_source_arrival_time'] = self.event_time + random_transfer_time

                    _queue.update_waiting_queue_on_arrival(pax)

class NPMModel(object):
    def __init__(self, para_npm, input_file_path,empty_train,transfer_walking_time, access_egress_time):
        self.para = para_npm
        # window and bar are used for update the GUI progress
        self.input_file_path = input_file_path
        self.output_file_path = input_file_path
        self.empty_train = empty_train
        self.transfer_walking_time = transfer_walking_time
        self.access_egress_time = access_egress_time
        self.info_print_num = 100
        self.npm_initialization()

        # the optimization objective calculation time period
        reference_time = pd.to_timedelta(REFERENCE_TIME).total_seconds()
        self.opt_time_period = [pd.to_timedelta(SIM_TIME_PERIOD[0]).total_seconds() - reference_time,
                                pd.to_timedelta(SIM_TIME_PERIOD[1]).total_seconds() - reference_time]
        # self.opt_time_period = [0, 10000000]

    def npm_initialization(self):
        # Variable settings
        para = self.para
        tic_ini = time.time()
        global SIM_TIME_PERIOD

        SIM_TIME_PERIOD = para[1].split('-')

        # File path
        file_path = ''
        # path_external_data = file_path + 'External_Data/'  # input data are in External_data folder
        input_file_path = self.input_file_path
        self.output_file_path = input_file_path

        # Read input files and make them global files
        global TB_EXIT_DEMAND, TB_TXN, PATH_CHOICE, PATH_ATTRIBUTE, TRANSFER_WT, NETWORK, ITINERARY, \
            OPERATION_ARRANGEMENT, EVENTS, CARRIERS, QUEUES, LIST_CARRIERS, QUEUES_POOL, EMPTY_TRAIN_TIME_LIST,\
            DISRUPT_PATH, DISRUPT_STATION, ACC_EGR_TIME
        TB_EXIT_DEMAND = pd.read_csv(input_file_path + 'tb_exit_demand.csv')
        TB_TXN = pd.read_csv(input_file_path + 'tb_txn.csv')
        PATH_CHOICE = pd.read_csv(input_file_path + 'tb_path_share.csv')
        # PATH_CHOICE = pd.read_csv(input_file_path + 'tb_path_share_Estimated_tran_over_dist_ADM_50.csv')
        # PATH_ATTRIBUTE = pd.read_csv(input_file_path + 'tb_path_attribute.csv')
        ACC_EGR_TIME = pd.read_csv(self.access_egress_time)
        TRANSFER_WT = pd.read_csv(self.transfer_walking_time)
        NETWORK = pd.read_csv(input_file_path + 'tb_network.csv')  # Service network information
        ITINERARY = pd.read_csv(input_file_path + 'tb_itinerary.csv')  # Itinerary table

        if len(link_disrupt)>0:
            DISRUPT_STATION = {}
            DISRUPT_PATH = pd.read_csv(input_file_path + 'tb_path_disrupt.csv')
            for link in link_disrupt:
                link_start = int(link[0].split('-')[0])
                link_end = int(link[0].split('-')[1])
                direction = NETWORK.loc[(NETWORK['link_start'] == link_start)&
                                        (NETWORK['link_end'] == link_end),'carrier_direction'].values[0]
                disrupt_time = [pd.to_timedelta(link[1].split('-')[0]).total_seconds(),
                                pd.to_timedelta(link[1].split('-')[1]).total_seconds()]
                DISRUPT_STATION[(link_start,direction)] = disrupt_time

        OPERATION_ARRANGEMENT = pd.read_csv(self.empty_train)
        EVENTS = pd.read_csv(input_file_path + 'tb_event.csv')  # Event list
        CARRIERS = pd.read_csv(input_file_path + 'tb_carrier.csv', index_col=0)  # Carrier table
        QUEUES = pd.read_csv(input_file_path + 'tb_queue.csv', index_col=0)

        LIST_CARRIERS = create_carrier_list()
        QUEUES_POOL = create_queue_pool()
        #****************process empty train*****************
        EMPTY_TRAIN_TIME_LIST = []
        operation_control = OPERATION_ARRANGEMENT[OPERATION_ARRANGEMENT.test_name == para[0]]
        if len(operation_control) > 0:
            operation_control.loc[:,'dispatch_time'] = pd.to_timedelta(operation_control.time).dt.total_seconds() - \
                                                      pd.to_timedelta(REFERENCE_TIME).total_seconds()
            for index, operation_info in operation_control.iterrows():
                event_temp = EVENTS.copy()
                event_temp['carrier'] = event_temp['carrier_id'].apply(lambda x: x.split('_'))
                event_temp['line'] = event_temp['carrier'].apply(lambda x: int(x[0]))
                event_temp['dir'] = event_temp['carrier'].apply(lambda x: int(x[1]))
                event_temp = event_temp.loc[(event_temp['event_type']==1)&
                                            (event_temp['line'] == operation_info.line)&
                                            (event_temp['dir'] == operation_info.direction) &
                                            (event_temp['event_station'] == operation_info.station)] #departure
                event_temp['time_diff'] = event_temp['event_time'] - operation_info.dispatch_time
                event_temp['time_diff'] = event_temp['time_diff'].abs()
                min_index = event_temp['time_diff'].idxmin()
                EMPTY_TRAIN_TIME_LIST.append((EVENTS.loc[min_index,'carrier_id'], EVENTS.loc[min_index,'event_time']))
        #****************************************************
        # print('Assignment initialization time:', time.time() - tic_ini, 'seconds')
    def run_assignment(self):
        global EVENTS

        tic_o = time.time()
        tic = time.time()
        total_event = len(EVENTS)
        for index, event_info in EVENTS.iterrows():
            event = Event(event_info.event_id, event_info.event_time, event_info.event_station, event_info.event_type,
                          event_info.carrier_id)
            if (index % self.info_print_num) == 0:
                print('event %s is processing' % event_info.event_id)
                print('Running Times:', time.time() - tic)
                progress = int(index)//total_event * 100
                tic = time.time()

            event.process_event()

        print('Running Assignment Time:', round((time.time() - tic_o) / 60, 2), 'min')



    def npm_with_static_choice(self):
        # Static version with path fraction pre-calculated and record detailed information
        output_path = self.output_file_path
        global TB_TXN
        TB_TXN = DemandModel(TB_TXN, choice_file=PATH_CHOICE, static=True).tb_txn    # static demand choice model

        # Object state recorder
        global carrier_state, passenger_state, OD_state, station_state
        #************Try the fast concate version************
        # carrier_state = pd.DataFrame(columns=['carrier_id', 'carrier_line', 'carrier_direction', 'carrier_car_no',
        #                                       'link_start', 'link_end', 'link_entry_time', 'carrier_load'])
        #
        # passenger_state = pd.DataFrame(columns=['arrival_time', 'departure_station', 'departure_time', 'departure_line',
        #                                         'departure_direction', 'pax_number', 'denied_boarding_times'])
        #
        # OD_state = pd.DataFrame(columns=['origin', 'destination', 'entry_time', 'exit_time', 'flow'])
        #
        # station_state = pd.DataFrame(
        #     columns=['queue_id', 'station_id', 'queue_line', 'queue_line_direction', 'last_train_departure',
        #              'current_train_departure', 'arrivals', 'boarded', 'denied', 'branch'])
        # ************Try the fast concate version************
        carrier_state = []
        passenger_state = []
        OD_state = []
        station_state = []
        #*******************************************************
        # Run the assignment
        # print('Run assignment on ' + DATE + ' with capacity ' + str(DEFAULT_CAR_CAP_1) + '_' + str(DEFAULT_CAR_CAP_2) + ' per car')
        # print('assignment time period is between ' + SIM_TIME_PERIOD[0] + ' and ' + SIM_TIME_PERIOD[1] +
        #       ' with %s min warm up and %s min cool down time' % (DEFAULT_WARM_UP_TIME, DEFAULT_COOL_DOWN_TIME))
        self.run_assignment()

        # Record the state
        carrier_csv_file = 'carrier_state.csv'
        carrier_state = fast_concate(carrier_state)
        carrier_state.to_csv(output_path + carrier_csv_file, index=False)

        passenger_csv_file = 'passenger_state.csv'
        passenger_state = fast_concate(passenger_state)
        passenger_state.to_csv(output_path + passenger_csv_file, index=False)

        OD_csv_file = 'od_state.csv'
        OD_state = fast_concate(OD_state)
        OD_state.to_csv(output_path + OD_csv_file, index=False)

        station_csv_file = 'station_state.csv'
        station_state = fast_concate(station_state)
        station_state.to_csv(output_path + station_csv_file, index=False)

    def npm_with_self_adjusted_choice(self, beta):
        # Dynamic version with path attribute and only record information for optimization
        global TB_TXN
        TB_TXN = DemandModel(TB_TXN, choice_file=PATH_ATTRIBUTE, beta=beta, static=False).tb_txn

        # state recorder
        global OD_state
        OD_state = pd.DataFrame(columns=['origin', 'destination', 'entry_time', 'exit_time', 'flow'])
        self.run_assignment()

        return OD_state

def assignment_main(para_npm, input_file_path, empty_train,transfer_walking_time,access_egress_time, bar, window):

    global RECORDINFO
    RECORDINFO = 1
    input_file_path = input_file_path+ '/'
    tic = time.time()
    # run static npm model with mtr choice file
    m = NPMModel(para_npm, input_file_path, empty_train,transfer_walking_time,access_egress_time)
    # Static version with path fraction pre-calculated and record detailed information
    output_path = m.output_file_path
    global TB_TXN
    TB_TXN = DemandModel(TB_TXN, choice_file=PATH_CHOICE, static=True).tb_txn    # static demand choice model

    # Object state recorder
    global carrier_state, passenger_state, OD_state, station_state

    carrier_state = []
    passenger_state = []
    OD_state = []
    station_state = []

    global EVENTS
    tic_o = time.time()
    tic = time.time()
    total_event = len(EVENTS)
    for index, event_info in EVENTS.iterrows():
        event = Event(event_info.event_id, event_info.event_time, event_info.event_station,
                      event_info.event_type,
                      event_info.carrier_id)
        if (index % m.info_print_num) == 0:
            print('event %s is processing' % event_info.event_id)
            print('Running Times:', time.time() - tic)
            progress = int(event_info.event_id) / total_event * 100
            print('Progress:', round(progress,2), '%')
            bar['value'] = progress
            window.update()
            tic = time.time()
        event.process_event()

    print('Running Assignment Time:', round((time.time() - tic_o) / 60, 2), 'min')

    # Record the state
    carrier_csv_file = 'carrier_state.csv'
    carrier_state = fast_concate(carrier_state)
    carrier_state.to_csv(output_path + carrier_csv_file, index=False)

    passenger_csv_file = 'passenger_state.csv'
    passenger_state = fast_concate(passenger_state)
    passenger_state.to_csv(output_path + passenger_csv_file, index=False)

    OD_csv_file = 'od_state.csv'
    OD_state = fast_concate(OD_state)
    OD_state.to_csv(output_path + OD_csv_file, index=False)

    station_csv_file = 'station_state.csv'
    station_state = fast_concate(station_state)
    station_state.to_csv(output_path + station_csv_file, index=False)
    print('Total_time:', round((time.time() - tic_o) / 60, 2), 'min')

# -------------------------------------------- Main ---------------------------------------------------------
if __name__ == "__main__":


    def assignment_main_test(para_npm, input_file_path, empty_train, transfer_walking_time, access_egress_time):

        global RECORDINFO
        RECORDINFO = 1
        input_file_path = input_file_path + '/'
        tic = time.time()
        # run static npm model with mtr choice file
        m = NPMModel(para_npm, input_file_path, empty_train, transfer_walking_time, access_egress_time)
        # Static version with path fraction pre-calculated and record detailed information
        output_path = m.output_file_path
        global TB_TXN
        TB_TXN = DemandModel(TB_TXN, choice_file=PATH_CHOICE, static=True).tb_txn  # static demand choice model

        # Object state recorder
        global carrier_state, passenger_state, OD_state, station_state

        carrier_state = []
        passenger_state = []
        OD_state = []
        station_state = []

        global EVENTS
        tic_o = time.time()
        tic = time.time()
        total_event = len(EVENTS)
        info_print_num = 100
        for index, event_info in EVENTS.iterrows():
            event = Event(event_info.event_id, event_info.event_time, event_info.event_station,
                          event_info.event_type,
                          event_info.carrier_id)
            event.process_event()
            if (index % info_print_num) == 0:
                print('event %s is processing' % event_info.event_id,'event time is',event_info.event_time)
                print('Running Times:', time.time() - tic)
                progress = int(event_info.event_id) / total_event * 100
                print('Progress:', round(progress,2), '%')
                tic = time.time()
        print('Running Assignment Time:', round((time.time() - tic_o) / 60, 2), 'min')

        # Record the state
        carrier_csv_file = 'carrier_state.csv'
        carrier_state = fast_concate(carrier_state)
        carrier_state.to_csv(output_path + carrier_csv_file, index=False)

        passenger_csv_file = 'passenger_state.csv'
        passenger_state = fast_concate(passenger_state)
        passenger_state.to_csv(output_path + passenger_csv_file, index=False)

        OD_csv_file = 'od_state.csv'
        OD_state = fast_concate(OD_state)
        OD_state.to_csv(output_path + OD_csv_file, index=False)

        station_csv_file = 'station_state.csv'
        station_state = fast_concate(station_state)
        station_state.to_csv(output_path + station_csv_file, index=False)
        print('Total_time:', round((time.time() - tic) / 60, 2), 'min')


    input_file_path = 'NPM_Test1_64800-68400'
    empty_train = 'Editable_files\Empty_Train_Arrangement.csv'
    transfer_walking_time = 'Editable_files\Transfer_Walking_Time.csv'
    access_egress_time = 'Editable_files\Access_egress_time.csv'



    file_list = [input_file_path, empty_train,transfer_walking_time]
    tic = time.time()
    Test_name = input_file_path.split('/')[-1].split('_')[1]
    time_period = input_file_path.split('/')[-1].split('_')[-1]
    para = [Test_name, time_period]
    assignment_main_test(para, input_file_path, empty_train,transfer_walking_time,access_egress_time)
    running_time = round((time.time() - tic) / 60, 2)



