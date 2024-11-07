from processor_core.Dataset import DataSet
from processor_core.CDF_Func import CDFfunc
from os import path, listdir
import pandas as pd


def command_processor(process_config: dict) -> str:
    # phase 0 - setup Dataset instance, parameters and options using the configuration dict, check configuration ======
    script_name = "CommandPE_processor"
    script_version = "1.4.5"

    command_data = DataSet(dataset_config=process_config)

    # read in the variables from the process_config dictionary
    run_serial = command_data.serial
    input_location = command_data.input_location
    output_location = command_data.output_location

    # get input file names from the configuration
    unit_pos_file = str(process_config['unit_pos_file'])
    weapon_fired_file = str(process_config['weapon_fired_file'])
    weapon_endgame_file = str(process_config['weapon_endgame_file'])
    unit_destroyed_file = str(process_config['unit_destroyed_file'])
    sensor_detection_file = str(process_config['sensor_detection_file'])

    input_file_ls = [unit_pos_file, weapon_fired_file, weapon_endgame_file, unit_destroyed_file, sensor_detection_file]
    sensor_file_present = True
    wpn_fired_file_present = True

    try:
        zero_hour = float(process_config['zero_hour'])
    except ValueError:
        zero_hour = 0.0
    if pd.isna(zero_hour):
        zero_hour = 0.0

    # command specific - whether to treat weapons as entities
    weapon_entities = CDFfunc.parse_config_bool(process_config['weapon_entities'])
    command_data.add_metadata('weapons_as_entities', weapon_entities)
    # command specific - ignore location updates for an entity that have the same location
    ignore_same_location_updates = CDFfunc.parse_config_bool(process_config['ignore_same_location_updates'])
    command_data.add_metadata('ignore_same_location_updates', ignore_same_location_updates)

    # command specific - minimum interval between location updates
    try:
        min_loc_update_interval = int(process_config['min_location_update_interval'])
    except ValueError:
        min_loc_update_interval = 0

    if min_loc_update_interval < 0:
        min_loc_update_interval = 0
    command_data.add_metadata('min_location_update_interval', min_loc_update_interval)

    # check that the specified configuration can be processed
    issues_list = []

    if not path.isdir(input_location):
        issues_list.append(f"input location: {input_location} not found")
    else:
        file_ls = listdir(input_location)
        for input_file in input_file_ls:
            if input_file not in file_ls:
                if input_file == sensor_detection_file:
                    sensor_file_present = False
                elif input_file == weapon_fired_file:
                    wpn_fired_file_present = False
                else:
                    issues_list.append(f"{input_file} missing")

    if len(issues_list) > 0:
        return_val = f"failed - no files generated - {len(issues_list)} issues:"
        for issue in issues_list:
            return_val = return_val + f" {issue},"
        return return_val

    # set up the script log
    logger = CDFfunc.setup_logger(f"{script_name}_log_S{run_serial}", output_folder=output_location)
    logger.info(f"Script logger started, saving log file to {output_location}")
    logger.info(f"{script_name} version {script_version}")
    logger.info(f"Using Dataset version {DataSet.version} and CDF functions version {CDFfunc.version}")

    # write input location and files plus model specific parameters and options and output location to the script log
    logger.info(f"Input location - {input_location}")
    logger.info(f"Input file names: {input_file_ls}")
    logger.info(f"zero hour parameter - {zero_hour}")
    logger.info(f"Treat weapons as entities - {weapon_entities}")
    logger.info(f"Output location - {output_location}")

    # command specific - if sensor file or weapon fired file not present record in log and add to metadata
    if not sensor_file_present:
        logger.warning(f"Sensor detection attempt file {sensor_detection_file} not present - "
                       f"no CDF spot / seen events will be generated")
        command_data.add_metadata('sensor_file_present', sensor_file_present)

    if not wpn_fired_file_present:
        logger.warning(f"Weapon fired file {weapon_fired_file} not present - "
                       f"no CDF shot events will be generated and weapon entities may not be identified correctly")
        command_data.add_metadata('wpn_fired_file_present', wpn_fired_file_present)

    # phase 1 - (no longer used - Dataset initialised in phase 0) =====================================================

    # phase 2 - read input files and generate source data frames ======================================================
    logger.info("Generating source dataframes for unit data")

    source_file = path.join(input_location, unit_pos_file)

    col_maps = {'UnitID': 'id', 'UnitName': 'name', 'UnitClass': 'type', 'UnitType': 'commander', 'UnitSide': 'side'}

    logger.debug(f"Extracting data from {source_file} for unit data dataframe")
    for mapping in col_maps.items():
        logger.debug(f"{mapping[0]} column mapped to {mapping[1]}")

    unit_data_df = pd.read_csv(source_file, skiprows=[1], usecols=list(col_maps.keys()))
    unit_data_df = unit_data_df[list(col_maps.keys())]
    unit_data_df.columns = col_maps.values()

    logger.info("Generating source dataframes for event data")

    # df_dict structure:
    '''
    df_dict structure:
        df_name: name of the df for logging
        source_file: command output file to get data from
        source_file_avail: read if True, if not make an empty df (True for mandatory files)
        col_maps: {col name in input file : col name in df}
        col_types: {col name in input file: data type to read column as} - explicitly define data type where needed
        
    add all df_dicts to the df_dict_ls variable
    '''

    move_df_dict = {'df_name': 'move_df',
                    'source_file': path.join(input_location, unit_pos_file),
                    'source_file_avail': True,
                    'col_maps': {'Time': 'time_str',
                                 'UnitID': 'id',
                                 'UnitLongitude': 'x',
                                 'UnitLatitude': 'y',
                                 'UnitSpeed_kts': 'spd_detail',
                                 'UnitCourse': 'crs_detail',
                                 'UnitAltitude_m': 'alt_detail',
                                 'Status': 'status_detail',
                                 'DamagePercent': 'dmg_detail',
                                 'Fire': 'fire_detail',
                                 'Flood': 'flood_detail'},
                    'col_types': {'Fire': str,
                                  'Flood': str}}

    spot_df_dict = {'df_name': 'spot_df',
                    'source_file': path.join(input_location, sensor_detection_file),
                    'source_file_avail': sensor_file_present,
                    'col_maps': {'Time': 'time_str',
                                 'SensorParentID': 'spotter_id',
                                 'TargetID': 'spotted_id',
                                 'DetectionResult': 'result',
                                 'SensorName': 'sensor_name_detail',
                                 'TargetRangeHoriz_nm': 'range_detail'},
                    'col_types': {}}

    shot_df_dict = {'df_name': 'shots_df',
                    'source_file': path.join(input_location, weapon_fired_file),
                    'source_file_avail': wpn_fired_file_present,
                    'col_maps': {'Time': 'time_str',
                                 'FiringUnitID': 'id',
                                 'WeaponID': 'wpn_id',
                                 'WeaponName': 'wpn_instance_detail',
                                 'WeaponType': 'wpn_type_detail',
                                 'WeaponClass': 'wpn_name_detail'},
                    'col_types': {}}

    unit_kills_df_dict = {'df_name': 'unit_kills_df',
                          'source_file': path.join(input_location, weapon_endgame_file),
                          'source_file_avail': True,
                          'col_maps': {'Time': 'time_str',
                                       'ParentFiringUnitID': 'killer_id',
                                       'WeaponID': 'wpn_id',
                                       'TargetID': 'victim_id',
                                       'WeaponName': 'wpn_instance_detail',
                                       'DistanceFromFiringUnit_Horiz': 'range_detail',
                                       'Result': 'result'},
                          'col_types': {}}

    unit_destroyed_df_dict = {'df_name': 'unit_destroyed_df',
                              'source_file': path.join(input_location, unit_destroyed_file),
                              'source_file_avail': True,
                              'col_maps': {'Time': 'time_str',
                                           'UnitID': 'victim_id',
                                           'Reason': 'loss_reason_detail',
                                           'Cause': 'loss_cause_detail'},
                              'col_types': {}}

    df_dict_ls = [move_df_dict, spot_df_dict, shot_df_dict, unit_kills_df_dict, unit_destroyed_df_dict]

    event_df_ls = []
    for df_dict in df_dict_ls:
        df_name = df_dict['df_name']
        source_file = df_dict['source_file']
        source_file_avail = df_dict['source_file_avail']
        col_maps = df_dict['col_maps']
        col_types = df_dict['col_types']
        if source_file_avail:
            logger.info(f"Extracting data from {source_file} for {df_name}")
            for mapping in col_maps.items():
                logger.debug(f"{mapping[0]} column mapped to {mapping[1]}")
            event_df_ls.append(pd.read_csv(source_file, skiprows=[1], usecols=list(col_maps.keys()), dtype=col_types))
            event_df_ls[-1] = event_df_ls[-1][list(col_maps.keys())]
            event_df_ls[-1].columns = col_maps.values()
        else:
            logger.warning(f"{source_file} not available - generating empty dataframe for {df_name}")
            event_df_ls.append(pd.DataFrame(columns=col_maps.values()))
        # command specific - remove trailing tenths from all event time strings and add time values column
        time_val_ls = []
        for time_str in event_df_ls[-1]['time_str'].to_list():
            time_str_split = time_str.split(":")
            if "." in time_str_split[-1]:
                time_str_split[-1] = time_str_split[-1].split(".")[0]
            time_val = CDFfunc.get_time_val(unit='secs', input_time_str=":".join(time_str_split), zero_hr=zero_hour)
            time_val_ls.append(time_val)
        event_df_ls[-1]['time'] = time_val_ls

    move_df = event_df_ls.pop(0)
    spots_df = event_df_ls.pop(0)
    shots_df = event_df_ls.pop(0)
    unit_kills_df = event_df_ls.pop(0)
    unit_destroyed_df = event_df_ls.pop(0)

    # process move_df
    # drop any rows with same time and id (from rounding event times to the nearest second)
    move_df.drop_duplicates(subset=['id', 'time'], inplace=True, keep='first')
    # if ignoring same location updates - drop any rows where the location for a particular id has not changed
    if ignore_same_location_updates:
        move_df.drop_duplicates(subset=['id', 'x', 'y'], inplace=True, keep='first')
    # if a minimum interval between location updates has been specified then reduce move_df accordingly:
    if min_loc_update_interval > 0:
        # get times for all lines that are rounded to the nearest specified interval
        move_df['rounded_time'] = round(move_df['time'] / min_loc_update_interval, 0) * min_loc_update_interval
        # drop duplicates of id and rounded time
        move_df.drop_duplicates(subset=['id', 'rounded_time'], inplace=True, keep='first')
    # fill null values in Fire and Flood columns in move_df with 'None' for consistent detail values for those keys
    fire_col = move_df_dict['col_maps']['Fire']
    flood_col = move_df_dict['col_maps']['Flood']
    move_df[fire_col].fillna('None', inplace=True)
    move_df[flood_col].fillna('None', inplace=True)

    # filter spots_df to only include successful spots
    spots_df = spots_df.loc[spots_df['result'] == "SUCCESS"]

    # filter unit_kills_df to only include KILL results
    unit_kills_df = unit_kills_df.loc[unit_kills_df['result'] == "KILL"]
    unit_kills_df['loss_cause_detail'] = 'engaged by weapon'
    unit_kills_df['loss_reason_detail'] = unit_kills_df['wpn_instance_detail']

    # reduce unit_destroyed_df to only include units not in unit_kills_df
    units_killed_ls = unit_kills_df['victim_id'].to_list()
    id_mask = [victim_id not in units_killed_ls for victim_id in unit_destroyed_df['victim_id'].to_list()]
    unit_destroyed_df = unit_destroyed_df.loc[id_mask]
    unit_destroyed_df.drop_duplicates(subset='victim_id', keep='last', inplace=True)
    unit_destroyed_df['killer_id'] = "no secondary entity"

    # combine unit_destroyed_df and unit_kills_df into kills_df
    kills_df = pd.concat(objs=[unit_kills_df, unit_destroyed_df])

    # phase 3 - generate the entities within the dataset instance and set their properties using unit_data_df =========

    if not command_data.entity_data_from_table or command_data.get_num_entities() == 0:
        unit_data_map = {'uid': 'id',
                         'unit_name': 'name',
                         'unit_type': 'type',
                         'affiliation': 'side',
                         'commander': 'commander'}

        logger.info(f"Getting entity UIDs from {unit_data_map['uid']} column of unit_data_df ")
        uid_list = CDFfunc.get_unique_list(unit_data_df[unit_data_map['uid']].tolist())
        logger.info(f"{len(uid_list)} unique ids found in unit_data_df")

        for mapping in unit_data_map.items():
            logger.debug(f"entity {mapping[0]} from {mapping[1]} column of unit_data_df")

        for uid in uid_list:
            logger.debug(f"Adding entity {uid} and populating data from unit_data_df")
            name_ls = CDFfunc.get_col_slice(unit_data_df, uid, unit_data_map['uid'], unit_data_map['unit_name'])
            type_ls = CDFfunc.get_col_slice(unit_data_df, uid, unit_data_map['uid'], unit_data_map['unit_type'])
            affil_ls = CDFfunc.get_col_slice(unit_data_df, uid, unit_data_map['uid'], unit_data_map['affiliation'])
            commander_ls = CDFfunc.get_col_slice(unit_data_df, uid, unit_data_map['uid'], unit_data_map['commander'])

            unit_data_ls = [name_ls, type_ls, affil_ls, commander_ls]
            for idx, data_list in enumerate(unit_data_ls):
                if len(CDFfunc.get_unique_list(data_list)) > 1:
                    logger.warning(f"multiple values of {list(unit_data_map.keys())[idx+1]} for entity {uid}, "
                                   f"{data_list}, {data_list[0]} used")

            command_data.add_entity(uid)
            command_data.set_entity_data(uid=uid, unit_name=name_ls[0], unit_type=type_ls[0], commander=commander_ls[0],
                                         affiliation=affil_ls[0], init_comps=1, cbt_per_comp=1)

    # commandPE specific - use the weapon endgame file and weapon fired file to get a list of weapon entity uids
    logger.info("Identifying and processing weapon entity uids")
    wpn_uid_ls = unit_kills_df['wpn_id'].to_list()
    if wpn_fired_file_present:
        wpn_uid_ls.extend(shots_df['wpn_id'].to_list())
    else:
        logger.warning("Weapon fired file not present - identification of weapon entities may not be complete")
    wpn_uid_ls = CDFfunc.get_unique_list(wpn_uid_ls)

    known_uid_ls = []
    for entity in command_data.entities:
        known_uid_ls.append(entity.uid)

    for wpn_uid in wpn_uid_ls:
        if weapon_entities and wpn_uid in known_uid_ls:
            wpn_add_str = "-WPN"
            unit_type_str = command_data.entities[command_data.get_entity_index(wpn_uid)].unit_type
            if wpn_add_str not in unit_type_str[-len(wpn_add_str):]:
                unit_type_str += wpn_add_str
            command_data.set_entity_data(uid=wpn_uid, init_comps=0, cbt_per_comp=0, unit_type=unit_type_str)
            logger.debug(f"Entity with uid {wpn_uid} identified as weapon - "
                         f"init_comps and cbt_per_comp set to 0, -WPN appended to unit_type")
        elif wpn_uid in known_uid_ls:
            command_data.remove_entity(wpn_uid)
            logger.debug(f"Entity with uid {wpn_uid} identified as weapon and removed")
        else:
            logger.debug(f"uid {wpn_uid} identified as weapon "
                         f"but does not correspond to an entity in Dataset entity array ")

    # phase 4 - read the event data into the entities =================================================================

    # event_map structure:
    '''
    event_map structure:
        df: the dataframe to pull the data from
        df_name: name of the df for logging
        mask_col: column to mask on using the uid
        data_maps: [[data column in df, target list for append to list]]
        detail_keys: [keys for the detail key-value pairs]
        detail_cols: [columns in the df with the values for the detail key-value pairs]
        detail_list: detail target list for append to list
    
    add all event maps to the event_map_ls
    '''

    location_event_map = {'df': move_df,
                          'df_name': 'unit_pos_df',
                          'mask_col': 'id',
                          'data_maps': [['time', 'location_time'],
                                        ['x', 'location_x'],
                                        ['y', 'location_y']],
                          'detail_keys': ['status', 'course', 'speed', 'altitude',
                                          'damage', 'fire', 'flood'],
                          'detail_cols': ['status_detail', 'crs_detail', 'spd_detail', 'alt_detail',
                                          'dmg_detail', 'fire_detail', 'flood_detail'],
                          'detail_list': 'location_detail'}

    spot_event_map = {'df': spots_df,
                      'df_name': 'unit_spots_df',
                      'mask_col': 'spotter_id',
                      'data_maps': [['time', 'spot_time'],
                                    ['spotted_id', 'spot_entity']],
                      'detail_keys': ['sensor name', 'range'],
                      'detail_cols': ['sensor_name_detail', 'range_detail'],
                      'detail_list': 'spot_detail'}

    seen_event_map = {'df': spots_df,
                      'df_name': 'unit_spots_df',
                      'mask_col': 'spotted_id',
                      'data_maps': [['time', 'seen_time'],
                                    ['spotter_id', 'seen_entity']],
                      'detail_keys': ['sensor name', 'range'],
                      'detail_cols': ['sensor_name_detail', 'range_detail'],
                      'detail_list': 'seen_detail'}

    shot_event_map = {'df': shots_df,
                      'df_name': 'unit_shots_df',
                      'mask_col': 'id',
                      'data_maps': [['time', 'shots_time']],
                      'detail_keys': ['weapon type', 'weapon name', 'weapon instance'],
                      'detail_cols': ['wpn_type_detail', 'wpn_name_detail', 'wpn_instance_detail'],
                      'detail_list': 'shots_detail'}

    kill_event_map = {'df': kills_df,
                      'df_name': 'kills_df',
                      'mask_col': 'killer_id',
                      'data_maps': [['time', 'kills_time'],
                                    ['victim_id', 'kills_victim']],
                      'detail_keys': ['weapon instance', 'range'],
                      'detail_cols': ['wpn_instance_detail', 'range_detail'],
                      'detail_list': 'kills_detail'}

    loss_event_map = {'df': kills_df,
                      'df_name': 'kills_df',
                      'mask_col': 'victim_id',
                      'data_maps': [['time', 'losses_time'],
                                    ['killer_id', 'losses_killer']],
                      'detail_keys': ['loss cause', 'loss reason'],
                      'detail_cols': ['loss_cause_detail', 'loss_reason_detail'],
                      'detail_list': 'losses_detail'}

    event_map_ls = [location_event_map, spot_event_map, seen_event_map, shot_event_map, kill_event_map, loss_event_map]

    for event_map in event_map_ls:
        event_df = event_map['df']
        df_name = event_map['df_name']
        mask_col = event_map['mask_col']
        data_maps = event_map['data_maps']
        detail_keys = event_map['detail_keys']
        detail_cols = event_map['detail_cols']
        detail_list = event_map['detail_list']

        logger.info(f'loading event data from {df_name} into entities, masking on {mask_col}, data maps: {data_maps},'
                    f' detail keys: {detail_keys}, detail columns: {detail_cols}')

        for entity in command_data.entities:
            uid = entity.uid

            logger.debug(f'reading event data from {df_name} for entity {uid}')
            for mapping in event_map['data_maps']:
                data_col = mapping[0]
                tgt_list = mapping[1]

                data_ls = CDFfunc.get_col_slice(df=event_df, uid=uid, mask_col=mask_col, tgt_col=data_col)
                if len(data_ls) > 0:
                    command_data.append_to_list(uid=uid, target_list=tgt_list, data_list=data_ls)
                else:
                    logger.debug(f"no data for {tgt_list} from {df_name} for entity {uid}")

            logger.debug(f'adding encoded event detail for entity {uid}')
            detail_val_ls = []
            for detail_col in detail_cols:
                detail_val_ls.append(CDFfunc.get_col_slice(df=event_df, uid=uid, mask_col=mask_col, tgt_col=detail_col))

            detail_data_encoded = CDFfunc.encode_event_detail_list(*detail_val_ls, detail_keys=detail_keys)
            if len(detail_data_encoded) > 0:
                command_data.append_to_list(uid=uid, target_list=detail_list, data_list=detail_data_encoded)
            else:
                logger.debug(f"no data for {detail_list} from {df_name} for entity {uid}")

    # phase 5 - finalise the data in the dataset instance and export the files ========================================
    logger.info("Finalising data and saving output files (see dataset instance log for details)")
    command_data.finalise_data()
    command_data.export_data()
    return_val = "complete"
    return return_val


# Script to call the model processor function and run each configuration in the config file ===========================

batch_logger = CDFfunc.setup_logger(f"Batch_log")
batch_logger.info(f"Batch run started")
configuration_file = "CommandPE_config.csv"
batch_logger.info(f"Loading configuration file - {configuration_file}")

try:
    configuration_dict = pd.read_csv(configuration_file, skiprows=1).to_dict(orient='records')
except FileNotFoundError:
    batch_logger.error(f"Batch run aborted - configuration file not found")
else:
    num_configs = len(configuration_dict)
    batch_logger.info(f"{num_configs} configurations in file")
    run_count = 0
    for configuration in configuration_dict:
        run_count += 1
        serial = configuration['serial']
        case = configuration['case']
        rep = configuration['replication']
        batch_logger.info(f"Configuration {run_count} of {num_configs}, Serial {serial}")
        result_str = f"Serial {serial} - case {case}, replication {rep} - "
        if CDFfunc.parse_config_bool(configuration['process']):
            result_str = result_str + command_processor(process_config=configuration)
        else:
            result_str = result_str + "not set to process"
        batch_logger.info(result_str)

    batch_logger.info("Batch run complete")
