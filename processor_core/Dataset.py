import yaml
import pandas as pd
from datetime import datetime
from .CDF_Func import CDFfunc
from .Entity import Entity
from os import path, makedirs, listdir


class DataSet:
    """ Dataset class.

    Model processing scripts will use an instance of the dataset class to read the model output data and convert it to
    CDF format output. The core of the Dataset class is an array of instances of the Entity class. These represent
    the units within the model output. Events that involve a particular Entity (as primary or secondary) are identified
    using the Entity unique id and the relevant data read into that Entity instance. Once this process is complete
    the functions and methods in the Dataset class are used to combine the entity data and process it into the CDF
    outputs.

    Attributes:
        instance count: Count of Dataset class instances created.
    """
    version: str = "1.6.2"

    def __init__(self, dataset_config: dict, log_file: bool = True, log_stream: bool = True) -> None:
        """ Dataset class init method.

        Increment instance count, set up loggers, define Dataset parameters and set up output column labels.

        Args:
            dataset_config: dictionary object to define dataset parameters and set options. Most commonly this
            will be a row from a _config.csv file for the model processor. Dictionary keys as follows:
                - serial: (parameter) serial of this run in the batch
                - case: (parameter) case name for inclusion in CDF outputs
                - replication:(parameter) replication number for inclusion in CDF outputs
                - input_location: (parameter) location that processor will read files from
                - output_location: (parameter) location to save CDF output files and log files in
                - model_name: (parameter) Name of the model that generated the output data
                - data_name: (parameter) Name of the data set
                - data_date: (parameter) Date the data set was generated
                - data_details: (parameter) Relevant data details
                - time_unit: (parameter) Output unit for CDF time values
                - distance_unit: (parameter) Output unit for CDF distance values
                - cbt_pwr_unit: (parameter) Output unit for CDF combat power values
                - force_unique_unit_names: (option) force unit names for entities to be unique
                - entity_data_from_table: (option) read entity data from an existing CDF entity table
                - entity_table_file: (parameter) name of the CDF entity table file to read data from (see option above)
                - split_files_by_type: (option) separate CDF output files into subfolders by type in output_location
                - drop_location_events: (option)  drop location update events from CDF events output
                - drop_seen_events: (option) drop seen by secondary events from CDF events output
                - drop_spot_events: (option) drop spotted by secondary events from CDF events output
                - drop_shot_events: (option) drop shot events from CDF events output
            log_file: generate a dataset log file (default True)
            log_stream: print dataset log entries (default True)
        """

        # set all parameters to default values
        self.serial = '0'
        self.case = 'case_name'
        self.replication = 'rep_num'
        self.input_location = 'Input'
        self.output_location = 'Output'
        self.output_csv = True
        self.output_parquet = False
        self.model_name = 'not defined'
        self.data_name = 'not defined'
        self.data_date = 'not defined'
        self.data_details = 'not defined'
        self.time_unit = 'not defined'
        self.distance_unit = 'not defined'
        self.cbt_pwr_unit = 'not defined'
        self.force_unique_unit_names = True
        self.entity_data_from_table = False
        self.entity_table_file = "entity_data_table.csv"
        self.split_files_by_type = False
        self.drop_location_events = False
        self.drop_spot_events = False
        self.drop_seen_events = False
        self.drop_shot_events = False

        location_param_ls = ['input_location', 'output_location']

        # go through parameters, check if there is a value in dataset_config and set accordingly, warn if not
        default_param_ls = []
        for parameter in vars(self).keys():
            if parameter in dataset_config.keys() and not pd.isna(dataset_config[parameter]):
                if parameter in location_param_ls:
                    setattr(self, parameter, CDFfunc.parse_config_location(str(dataset_config[parameter])))
                elif type(vars(self)[parameter]) is bool:
                    setattr(self, parameter, CDFfunc.parse_config_bool(str(dataset_config[parameter])))
                else:
                    setattr(self, parameter, str(dataset_config[parameter]))
            else:
                default_param_ls.append(parameter)
        # set the date time string for init of this instance
        self.init_date_time_str = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
        # set up log output parameters
        self.log_stream = log_stream
        self.log_file = log_file
        # set up the metadata dict
        self.metadata_dict = vars(self).copy()

        # check if output location exists and create it if it does not
        if not path.isdir(self.output_location):
            makedirs(self.output_location)

        self.dataset_log_folder = "Dataset_log"
        log_location = self.output_location
        if self.split_files_by_type:
            log_location = path.join(log_location, self.dataset_log_folder)

        # set up the logger
        self.logger = CDFfunc.setup_logger(name=f'{self.dataset_log_folder}_S{self.serial}',
                                           date_time_str=self.init_date_time_str,
                                           output_folder=log_location,
                                           log_file=self.log_file,
                                           log_stream=self.log_stream)
        self.logger.info("Dataset logger started")
        if log_file:
            self.logger.info(f"saving log file to {self.output_location}")
        self.logger.info(f"Dataset version {self.__class__.version}")
        self.logger.info(f"Using CDF functions version {CDFfunc.version}")

        self.logger.info("Setting Dataset parameters")
        # confirm all settings in the log and warn about any default values
        num_default_vals = len(default_param_ls)
        if num_default_vals > 0:
            self.logger.warning(f"Values for {num_default_vals} Parameters not in config, "
                                f"parameters {default_param_ls} all set to Dataset default values")
        for setting in vars(self).items():
            if setting[0] in default_param_ls:
                self.logger.warning(f"{setting[0]} set as {setting[1]} (Dataset default value)")
            else:
                self.logger.debug(f"{setting[0]} set as {setting[1]}")

        # warn if output_csv and output_parquet set to false
        if not self.output_csv and not self.output_parquet:
            self.logger.warning("Config is not set to output csv or parquet - no CDF output files will be generated!")

        # set up the split folder names (inc. one for log files) first so that the CDF file names will always match
        self.meta_folder_name = "CDF_Metadata"
        self.entity_folder_name = "CDF_EntityTable"
        self.events_folder_name = "CDF_Events"
        self.cbt_folder_name = "CDF_Cbt_Pwr"

        # set up placeholders for the cdf file names and paths and then generate them by calling generate function
        self.cdf_file_date_time_str = None
        self.output_name_str = None
        self.metadata_filename = None
        self.entity_filename = None
        self.events_filename = None
        self.cbt_filename = None
        self.metadata_file_path = None
        self.entity_file_path = None
        self.events_file_path = None
        self.cbt_pwr_file_path = None

        self.generate_cdf_filenames_and_paths()

        # set a default name for dataset save files
        self.save_file_name = "dataset_save.yaml"

        # labels for case and rep columns
        self.case_col_lbl = "case"
        self.rep_col_lbl = "rep"

        # labels for the CDF events file columns
        self.evn_tbl_time_col_lbl = "time"
        self.evn_tbl_event_id_col_lbl = "event_id"
        self.evn_tbl_event_type_col_lbl = "event_type"
        self.evn_tbl_event_detail_col_lbl = "event_detail"

        self.evn_tbl_prim_id_col_lbl = "primary_entity_id"
        self.evn_tbl_prim_name_col_lbl = "primary_entity_name"
        self.evn_tbl_prim_type_col_lbl = "primary_entity_type"
        self.evn_tbl_prim_comd_col_lbl = "primary_entity_commander"
        self.evn_tbl_prim_lvl_col_lbl = "primary_entity_level"
        self.evn_tbl_prim_affil_col_lbl = "primary_entity_affiliation"
        self.evn_tbl_prim_force_col_lbl = "primary_entity_force"
        self.evn_tbl_prim_x_col_lbl = "primary_x"
        self.evn_tbl_prim_y_col_lbl = "primary_y"

        self.evn_tbl_sec_id_col_lbl = "secondary_entity_id"
        self.evn_tbl_sec_name_col_lbl = "secondary_entity_name"
        self.evn_tbl_sec_type_col_lbl = "secondary_entity_type"
        self.evn_tbl_sec_comd_col_lbl = "secondary_entity commander"
        self.evn_tbl_sec_lvl_col_lbl = "secondary_entity_level"
        self.evn_tbl_sec_affil_col_lbl = "secondary_entity_affiliation"
        self.evn_tbl_sec_force_col_lbl = "secondary_entity_force"
        self.evn_tbl_sec_x_col_lbl = "secondary_x"
        self.evn_tbl_sec_y_col_lbl = "secondary_y"

        self.evn_tbl_col_types_dict = {self.evn_tbl_time_col_lbl: float,
                                       self.evn_tbl_event_id_col_lbl: str,
                                       self.evn_tbl_event_type_col_lbl: str,
                                       self.evn_tbl_event_detail_col_lbl: str,

                                       self.evn_tbl_prim_id_col_lbl: str,
                                       self.evn_tbl_prim_name_col_lbl: str,
                                       self.evn_tbl_prim_type_col_lbl: str,
                                       self.evn_tbl_prim_comd_col_lbl: str,
                                       self.evn_tbl_prim_lvl_col_lbl: float,
                                       self.evn_tbl_prim_affil_col_lbl: str,
                                       self.evn_tbl_prim_force_col_lbl: str,
                                       self.evn_tbl_prim_x_col_lbl: float,
                                       self.evn_tbl_prim_y_col_lbl: float,

                                       self.evn_tbl_sec_id_col_lbl: str,
                                       self.evn_tbl_sec_name_col_lbl: str,
                                       self.evn_tbl_sec_type_col_lbl: str,
                                       self.evn_tbl_sec_comd_col_lbl: str,
                                       self.evn_tbl_sec_lvl_col_lbl: float,
                                       self.evn_tbl_sec_affil_col_lbl: str,
                                       self.evn_tbl_sec_force_col_lbl: str,
                                       self.evn_tbl_sec_x_col_lbl: float,
                                       self.evn_tbl_sec_y_col_lbl: float}

        # labels for CDF event types
        self.loc_event_lbl = "location update"
        self.shot_event_lbl = "shot"
        self.kill_event_lbl = "kill"
        self.loss_event_lbl = "loss"
        self.spot_event_lbl = "spotted secondary"
        self.seen_event_lbl = "seen by secondary"
        self.stop_event_lbl = "stopped seeing secondary"
        self.status_event_lbl = "status update"

        self.loc_event_short_lbl = "loc"
        self.shot_event_short_lbl = "shot"
        self.kill_event_short_lbl = "kill"
        self.loss_event_short_lbl = "loss"
        self.spot_event_short_lbl = "spot"
        self.seen_event_short_lbl = "seen"
        self.stop_event_short_lbl = "stop"
        self.status_event_short_lbl = "status"

        # map labels to their short label equivalents using a dict
        self.event_lbl_map = {self.loc_event_lbl: self.loc_event_short_lbl,
                              self.shot_event_lbl: self.shot_event_short_lbl,
                              self.kill_event_lbl: self.kill_event_short_lbl,
                              self.loss_event_lbl: self.loss_event_short_lbl,
                              self.spot_event_lbl: self.spot_event_short_lbl,
                              self.seen_event_lbl: self.seen_event_short_lbl,
                              self.stop_event_lbl: self.stop_event_short_lbl,
                              self.status_event_lbl: self.status_event_short_lbl}

        # set up variables with the last event number for each event type
        self.loc_event_last_ser = 0
        self.shot_event_last_ser = 0
        self.kill_event_last_ser = 0
        self.loss_event_last_ser = 0
        self.spot_event_last_ser = 0
        self.seen_event_last_ser = 0
        self.stop_event_last_ser = 0
        self.status_event_last_ser = 0

        # labels for CDF combat power columns
        self.cbt_tbl_time_col_lbl = "time"
        self.cbt_tbl_item_col_lbl = "item"
        self.cbt_tbl_comp_col_lbl = "components"
        self.cbt_tbl_pwr_col_lbl = "combat_power"
        self.cbt_tbl_event_col_lbl = "event_id"

        self.cbt_tbl_col_types_dict = {self.cbt_tbl_time_col_lbl: float,
                                       self.cbt_tbl_item_col_lbl: str,
                                       self.cbt_tbl_comp_col_lbl: 'int64',
                                       self.cbt_tbl_pwr_col_lbl: float,
                                       self.cbt_tbl_event_col_lbl: str}

        # labels for CDF entity table columns
        self.ent_tbl_id_col_lbl = "id"
        self.ent_tbl_name_col_lbl = "name"
        self.ent_tbl_type_col_lbl = "type"
        self.ent_tbl_commander_id_col_lbl = "commander_id"
        self.ent_tbl_commander_name_col_lbl = "commander_name"
        self.ent_tbl_level_col_lbl = "level"
        self.ent_tbl_affil_col_lbl = "affiliation"
        self.ent_tbl_force_col_lbl = "force"
        self.ent_tbl_init_comp_col_lbl = "init_comps"
        self.ent_tbl_cbt_per_comp_col_lbl = "cbt_per_comp"
        self.ent_tbl_init_cbt_pwr_col_lbl = "init_cbt_pwr"
        self.ent_tbl_sys_entity_col_lbl = "system_entity"
        self.ent_tbl_start_entity_col_lbl = "start_entity"
        self.ent_tbl_add_time_col_lbl = "time_added"
        self.ent_tbl_total_events_lbl = "total_events"
        self.ent_tbl_loc_events_lbl = "location_events"
        self.ent_tbl_spot_events_lbl = "spot_events"
        self.ent_tbl_seen_events_lbl = "seen_events"
        self.ent_tbl_shot_events_lbl = "shot_events"
        self.ent_tbl_kill_events_lbl = "kill_events"
        self.ent_tbl_loss_events_lbl = "loss_events"
        self.ent_tbl_stop_events_lbl = "stop_events"
        self.ent_tbl_status_events_lbl = "status_events"

        self.ent_tbl_col_types_dict = {self.ent_tbl_id_col_lbl: str,
                                       self.ent_tbl_name_col_lbl: str,
                                       self.ent_tbl_type_col_lbl: str,
                                       self.ent_tbl_commander_id_col_lbl: str,
                                       self.ent_tbl_commander_name_col_lbl: str,
                                       self.ent_tbl_level_col_lbl: 'int64',
                                       self.ent_tbl_affil_col_lbl: str,
                                       self.ent_tbl_force_col_lbl: str,
                                       self.ent_tbl_init_comp_col_lbl: 'int64',
                                       self.ent_tbl_cbt_per_comp_col_lbl: float,
                                       self.ent_tbl_init_cbt_pwr_col_lbl: float,
                                       self.ent_tbl_sys_entity_col_lbl: bool,
                                       self.ent_tbl_start_entity_col_lbl: bool,
                                       self.ent_tbl_add_time_col_lbl: float,
                                       self.ent_tbl_total_events_lbl: 'int64',
                                       self.ent_tbl_loc_events_lbl: 'int64',
                                       self.ent_tbl_spot_events_lbl: 'int64',
                                       self.ent_tbl_seen_events_lbl: 'int64',
                                       self.ent_tbl_shot_events_lbl: 'int64',
                                       self.ent_tbl_kill_events_lbl: 'int64',
                                       self.ent_tbl_loss_events_lbl: 'int64',
                                       self.ent_tbl_stop_events_lbl: 'int64',
                                       self.ent_tbl_status_events_lbl: 'int64'}

        # empty dataframes for each of the CDF output files
        self.CDF_entity_table_df = pd.DataFrame()
        self.CDF_events_df = pd.DataFrame()
        self.CDF_combat_power_DF = pd.DataFrame()

        # array of instances of the Entity class
        self.entities = []

        # if reading entity data from table use generate_entities_from_table to populate entities list
        if self.entity_data_from_table:
            entity_data_file_path = path.join(self.input_location, self.entity_table_file)
            if path.isfile(entity_data_file_path):
                self.logger.info(f"Reading data from {entity_data_file_path} and generating entities")
                self.generate_entities_from_table(pd.read_csv(entity_data_file_path))
            else:
                self.logger.error(f"{entity_data_file_path} not found, entities will be generated from input data")

    def __del__(self):
        self.logger.info("dataset instance deleted")

    def generate_cdf_filenames_and_paths(self):
        """
        generate filenames and paths for output cdf files and record in metadata dict via the update_config function
        this is used in init function and can also be used to refresh when dataset config is updated
        """
        # set up file names for the metadata file and CDF output files including date-time, model name and data name
        file_date_time_str = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
        self.update_config('cdf_file_date_time_str', file_date_time_str)

        output_name_str = f"{self.case}-{self.replication}_S{self.serial}_{self.cdf_file_date_time_str}"
        output_name_str = output_name_str.replace(" ", "_")
        self.update_config('output_name_str', output_name_str)

        metadata_filename = f"{self.meta_folder_name}_{self.output_name_str}.yaml"
        entity_filename = f"{self.entity_folder_name}_{self.output_name_str}.csv"
        events_filename = f"{self.events_folder_name}_{self.output_name_str}.csv"
        cbt_filename = f"{self.cbt_folder_name}_{self.output_name_str}.csv"
        self.update_config('metadata_filename', metadata_filename)
        self.update_config('entity_filename', entity_filename)
        self.update_config('events_filename', events_filename)
        self.update_config('cbt_filename', cbt_filename)

        if self.split_files_by_type:
            metadata_file_path = path.join(self.output_location, self.meta_folder_name, self.metadata_filename)
            entity_file_path = path.join(self.output_location, self.entity_folder_name, self.entity_filename)
            events_file_path = path.join(self.output_location, self.events_folder_name, self.events_filename)
            cbt_pwr_file_path = path.join(self.output_location, self.cbt_folder_name, self.cbt_filename)
        else:
            metadata_file_path = path.join(self.output_location, self.metadata_filename)
            entity_file_path = path.join(self.output_location, self.entity_filename)
            events_file_path = path.join(self.output_location, self.events_filename)
            cbt_pwr_file_path = path.join(self.output_location, self.cbt_filename)

        self.update_config('metadata_file_path', metadata_file_path)
        self.update_config('entity_file_path', entity_file_path)
        self.update_config('events_file_path', events_file_path)
        self.update_config('cbt_pwr_file_path', cbt_pwr_file_path)

    def add_entity(self, uid: str) -> None:
        """ Check if an entity with uid is already in the entity array and if not add a new entity with uid

        Args:
            uid: Sets the uid of the new Entity instance
        """
        if uid not in self.get_uid_ls():
            self.entities.append(Entity(uid))
            self.logger.debug(f"Entity added - entity uid {uid}")
        else:
            self.logger.error(f"entity with uid {uid} already in entities array")

    def get_entity_index(self, search_id: str) -> int:
        """ Get the index of an Entity instance within the entities array.

        Args:
            search_id: The uid of the Entity instance to return the index of.

        Returns:
            int: The index number of the Entity instance if it is within the entities array, Otherwise None.
        """
        ent_idx = None
        for index, entity in enumerate(self.entities):
            if entity.uid == search_id:
                ent_idx = index

        if ent_idx is None:
            self.logger.error(f"Get entity index failed - uid: {search_id}")

        return ent_idx

    def get_num_entities(self) -> int:
        """
        Return the number of entity instances in the entities array
        """
        return len(self.entities)

    def get_uid_ls(self) -> list:
        """
        Return a list of uids for the entity instances in the entities array
        """
        uid_ls = []
        for entity in self.entities:
            uid_ls.append(entity.uid)

        return uid_ls

    def remove_entity(self, uid: str) -> None:
        """ Remove an Entity instance from the entities array.

        Args:
            uid: The uid of the Entity instance to remove.
        """
        ent_idx = self.get_entity_index(uid)
        if ent_idx is not None:
            del self.entities[ent_idx]
            self.logger.debug(f"Entity removed - entity uid {uid}")
        else:
            self.logger.error(f"Removal of entity uid: {uid} failed - unknown uid")
        pass

    def set_entity_data(self, uid: str, **input_data) -> None:
        """ Set the value of one or more parameters of an Entity instance.

        Args:
             uid: The uid of the Entity instance to set parameters of.
             **input_data: Keyword arguments defining values for the Entity parameters:
                unit_name: Name of the unit that this Entity instance is representing (optional).
                unit_type: The unit type for this Entity instance (optional).
                commander: The uid of the Entity instance which is the commander of thi Entity instance (optional).
                level: The level of this Entity instance in the organisation hierarchy (optional).
                affiliation: The affiliation of this Entity instance (optional).
                force: The force to which the affiliation of this Entity instance belongs (optional).
                init_comps: The number of components that this Entity instance starts with (optional).
                cbt_per_comp: The combat power that each component of this Entity instance has (optional).
        """
        # unit_name, unit_type, commander, level, affiliation, affiliation, force, init_comps, cbt_per_comp

        ent_idx = self.get_entity_index(uid)
        if ent_idx is None:
            self.logger.error(f"Set entity data called with unrecognised uid - {uid} ")
        else:
            settings = input_data.items()

            for setting in settings:
                if setting[0] in vars(self.entities[ent_idx]).keys():
                    setattr(self.entities[ent_idx], setting[0], setting[1])
                    self.logger.debug(f"Entity {uid} - {setting[0]} set as  {setting[1]}")
                else:
                    self.logger.error(f"Set entity data called for entity {uid} "
                                      f"with unrecognised parameter {setting[0]}")

    def generate_entities_from_table(self, input_entity_table_df: pd.DataFrame) -> None:
        """ Add Entity instances and set parameters from an input dataframe.

        This function adds instances of the Entity class and sets their parameters using data from an input Dataframe.
        The input Dataframe column names must match the labels defined in the Dataset init method.

        Args:
             input_entity_table_df: Dataframe to read the entity data from.
        """
        self.logger.info(f"Generating entities from source entity table")

        id_ls = input_entity_table_df[self.ent_tbl_id_col_lbl].tolist()
        name_ls = input_entity_table_df[self.ent_tbl_name_col_lbl].tolist()
        type_ls = input_entity_table_df[self.ent_tbl_type_col_lbl].tolist()
        cmd_ls = input_entity_table_df[self.ent_tbl_commander_id_col_lbl].tolist()
        lvl_ls = input_entity_table_df[self.ent_tbl_level_col_lbl].tolist()
        afl_ls = input_entity_table_df[self.ent_tbl_affil_col_lbl].tolist()
        force_ls = input_entity_table_df[self.ent_tbl_force_col_lbl].tolist()
        comps_ls = input_entity_table_df[self.ent_tbl_init_comp_col_lbl].tolist()
        cbt_per_comp_ls = input_entity_table_df[self.ent_tbl_cbt_per_comp_col_lbl].tolist()
        system_entity_ls = input_entity_table_df[self.ent_tbl_sys_entity_col_lbl].to_list()
        start_entity_ls = input_entity_table_df[self.ent_tbl_start_entity_col_lbl].to_list()

        if len(CDFfunc.get_unique_list(id_ls)) < len(id_ls):
            self.logger.error("Entities with same uid present in source entity table")

        for idx, uid in enumerate(id_ls):
            self.add_entity(uid)
            self.set_entity_data(uid, unit_name=name_ls[idx], unit_type=type_ls[idx],
                                 commander=cmd_ls[idx], affiliation=afl_ls[idx], force=force_ls[idx],
                                 init_comps=comps_ls[idx], cbt_per_comp=cbt_per_comp_ls[idx],
                                 system_entity=system_entity_ls[idx], start_entity=start_entity_ls[idx])

            if not pd.isnull(lvl_ls[idx]):
                self.set_entity_data(uid, level=lvl_ls[idx])

    def append_to_list(self, uid: str, target_list: str, data_list: list) -> None:
        """ Add event data to an Entity instance

        This function adds event data to an Entity instance. Each Entity instance has sets of data lists (i.e.
        location_x, location_y, location_detail etc.) that hold event data for the different events types (see init
        method of the Entity class for details of all lists). This function appends a set of values to a target list.
        Note that the lists for an Entity instance must be of the same length when they are processed and consequently
        the same number of data items must be sent to all the lists in an event set.

        Args:
            uid: The uid of the Entity instance to add data to.
            target_list: The list of the Entity instance to append values to.
            data_list: List of values to append.
        """
        target_list = target_list.lower()
        unrecognised_target_list = False
        ent_idx = self.get_entity_index(uid)

        if ent_idx is None:
            self.logger.error(f"Append to list called with unrecognised uid - {uid}")

        if data_list is None:
            self.logger.error(f"No data passed to append to list - entity uid {uid}, target list {target_list}")

        if ent_idx is not None and data_list is not None:
            if target_list == "location_time":
                self.entities[ent_idx].location_time.extend(data_list)
                for i in range(len(data_list)):
                    self.add_event_id(prim_uid=uid, add_event_type=self.loc_event_lbl)
            elif target_list == "location_x":
                self.entities[ent_idx].location_x.extend(data_list)
            elif target_list == "location_y":
                self.entities[ent_idx].location_y.extend(data_list)
            elif target_list == "location_detail":
                self.entities[ent_idx].location_detail.extend(data_list)

            elif target_list == "shots_time":
                self.entities[ent_idx].shots_time.extend(data_list)
                for i in range(len(data_list)):
                    self.add_event_id(prim_uid=uid, add_event_type=self.shot_event_lbl)
            elif target_list == "shots_detail":
                self.entities[ent_idx].shots_detail.extend(data_list)

            elif target_list == "kills_time":
                self.entities[ent_idx].kills_time.extend(data_list)
            elif target_list == "kills_victim":
                self.entities[ent_idx].kills_victim.extend(data_list)
                for sec_uid in data_list:
                    self.add_event_id(prim_uid=uid, sec_uid=sec_uid, add_event_type=self.kill_event_lbl)
            elif target_list == "kills_detail":
                self.entities[ent_idx].kills_detail.extend(data_list)

            elif target_list == "losses_time":
                self.entities[ent_idx].losses_time.extend(data_list)
            elif target_list == "losses_killer":
                self.entities[ent_idx].losses_killer.extend(data_list)
                for sec_uid in data_list:
                    self.add_event_id(prim_uid=uid, sec_uid=sec_uid, add_event_type=self.loss_event_lbl)
            elif target_list == "losses_detail":
                self.entities[ent_idx].losses_detail.extend(data_list)

            elif target_list == "spot_time":
                self.entities[ent_idx].spot_time.extend(data_list)
            elif target_list == "spot_entity":
                self.entities[ent_idx].spot_entity.extend(data_list)
                for sec_uid in data_list:
                    self.add_event_id(prim_uid=uid, sec_uid=sec_uid, add_event_type=self.spot_event_lbl)
            elif target_list == "spot_detail":
                self.entities[ent_idx].spot_detail.extend(data_list)

            elif target_list == "seen_time":
                self.entities[ent_idx].seen_time.extend(data_list)
            elif target_list == "seen_entity":
                self.entities[ent_idx].seen_entity.extend(data_list)
                for sec_uid in data_list:
                    self.add_event_id(prim_uid=uid, sec_uid=sec_uid, add_event_type=self.seen_event_lbl)
            elif target_list == "seen_detail":
                self.entities[ent_idx].seen_detail.extend(data_list)

            elif target_list == "stop_time":
                self.entities[ent_idx].stop_time.extend(data_list)
            elif target_list == "stop_entity":
                self.entities[ent_idx].stop_entity.extend(data_list)
                for sec_uid in data_list:
                    self.add_event_id(prim_uid=uid, sec_uid=sec_uid, add_event_type=self.stop_event_lbl)
            elif target_list == "stop_detail":
                self.entities[ent_idx].stop_detail.extend(data_list)

            elif target_list == "state_time":
                self.entities[ent_idx].state_time.extend(data_list)
                for i in range(len(data_list)):
                    self.add_event_id(prim_uid=uid, add_event_type=self.status_event_lbl)
            elif target_list == "state_detail":
                self.entities[ent_idx].state_detail.extend(data_list)

            else:
                unrecognised_target_list = True

        if unrecognised_target_list is True:
            self.logger.error(f"Unrecognised target list passed to append to list "
                              f"- entity uid {uid}, target list {target_list}")
        else:
            self.logger.debug(f"Entity uid {uid} - data appended to {target_list}")

    def add_location(self, uid: str, time: float, x: float, y: float, detail_keys: list, detail_vals: list) -> None:
        """
        Add a single location update event to an Entity instance.

        Args:
            uid: The uid of the Entity instance to add the location update event to.
            time: The time of the location update event.
            x: The x value for the location update event.
            y: The y value for the location update event.
            detail_keys: The keys for the key value pairs that form detail for the event
            detail_vals: The values for the key value pairs that form the detail for the event
        """
        ent_idx = self.get_entity_index(uid)
        if ent_idx is not None:
            self.entities[ent_idx].location_time.append(time)
            self.entities[ent_idx].location_x.append(x)
            self.entities[ent_idx].location_y.append(y)
            detail = CDFfunc.encode_event_detail(detail_key_ls=detail_keys, detail_val_ls=detail_vals)
            self.entities[ent_idx].location_detail.append(detail)
            self.logger.debug(f"Entity uid {uid} - {self.loc_event_lbl} event added, time {str(time)}, "
                              f"x {str(x)}, y {str(y)}")

            self.add_event_id(prim_uid=uid, add_event_type=self.loc_event_lbl)
        else:
            self.logger.error(f"Add location called with unrecognised uid - {uid}")

    def add_shot(self, uid: str, time: float, detail_keys: list, detail_vals: list) -> None:
        """
        Add a single shot event to an Entity instance.

        Args:
            uid: The uid of the Entity instance to add the shot event to.
            time: The time value for the shot event.
            detail_keys: The keys for the key value pairs that form detail for the event
            detail_vals: The values for the key value pairs that form the detail for the event
        """
        ent_idx = self.get_entity_index(uid)
        if ent_idx is not None:
            self.entities[ent_idx].shots_time.append(time)
            detail = CDFfunc.encode_event_detail(detail_key_ls=detail_keys, detail_val_ls=detail_vals)
            self.entities[ent_idx].shots_detail.append(detail)
            self.logger.debug(f"Entity uid {uid} - {self.shot_event_lbl} event added, time {time}, detail {detail}")

            self.add_event_id(prim_uid=uid, add_event_type=self.shot_event_lbl)
        else:
            self.logger.error(f"Add shot called with unrecognised uid - {uid}")

    def add_kill(self, uid: str, time: float, victim: str, detail_keys: list, detail_vals: list) -> None:
        """
        Add a single kill event to an Entity instance.

        Args:
            uid: The uid of the Entity instance to add the kill event to.
            time: The time value for the kill event.
            victim: The uid of the Entity instance that was killed in the kill event.
            detail_keys: The keys for the key value pairs that form detail for the event
            detail_vals: The values for the key value pairs that form the detail for the event
        """
        ent_idx = self.get_entity_index(uid)
        if ent_idx is not None:
            self.entities[ent_idx].kills_time.append(time)
            self.entities[ent_idx].kills_victim.append(victim)
            detail = CDFfunc.encode_event_detail(detail_key_ls=detail_keys, detail_val_ls=detail_vals)
            self.entities[ent_idx].kills_detail.append(detail)
            self.logger.debug(f"Entity uid {uid} - {self.kill_event_lbl} event added, "
                              f"time {time}, victim {victim}, detail {detail}")

            self.add_event_id(prim_uid=uid, sec_uid=victim, add_event_type=self.kill_event_lbl)
        else:
            self.logger.error(f"Add kill called with unrecognised uid - {uid}")

    def add_loss(self, uid: str, time: float, killer: str, detail_keys: list, detail_vals: list) -> None:
        """
        Add a single loss event to an Entity instance.

        Args:
            uid: The uid of the Entity instance to add the loss event to.
            time: The time value for the loss event.
            killer: The uid of the Entity instance that caused the loss
            detail_keys: The keys for the key value pairs that form detail for the event
            detail_vals: The values for the key value pairs that form the detail for the event
        """
        ent_idx = self.get_entity_index(uid)
        if ent_idx is not None:
            self.entities[ent_idx].losses_time.append(time)
            self.entities[ent_idx].losses_killer.append(killer)
            detail = CDFfunc.encode_event_detail(detail_key_ls=detail_keys, detail_val_ls=detail_vals)
            self.entities[ent_idx].losses_detail.append(detail)

            self.logger.debug(f"Entity uid {uid} - {self.loss_event_lbl} event added, "
                              f"time {time}, killer {killer}, detail {detail}")

            self.add_event_id(prim_uid=uid, sec_uid=killer, add_event_type=self.loss_event_lbl)
        else:
            self.logger.error(f"Add loss called with unrecognised uid - {uid}")

    def add_spot(self, uid: str, time: float, entity: str, detail_keys: list, detail_vals: list) -> None:
        """
        Add a single spot event to an Entity instance.

        Args:
            uid: The uid of the Entity instance to add the spot event to.
            time: The time value for the spot event.
            entity: The uid of the Entity instance that was spotted by this Entity instance.
            detail_keys: The keys for the key value pairs that form detail for the event
            detail_vals: The values for the key value pairs that form the detail for the event
        """
        ent_idx = self.get_entity_index(uid)
        if ent_idx is not None:
            self.entities[ent_idx].spot_time.append(time)
            self.entities[ent_idx].spot_entity.append(entity)
            detail = CDFfunc.encode_event_detail(detail_key_ls=detail_keys, detail_val_ls=detail_vals)
            self.entities[ent_idx].spot_detail.append(detail)
            self.logger.debug(f"Entity uid {uid} - {self.spot_event_lbl} event added, "
                              f"time {time}, entity {entity}, detail {detail}")

            self.add_event_id(prim_uid=uid, sec_uid=entity, add_event_type=self.spot_event_lbl)
        else:
            self.logger.error(f"Add spot called with unrecognised uid - {uid}")

    def add_seen(self, uid: str, time: float, entity: str, detail_keys: list, detail_vals: list) -> None:
        """
        Add a single seen event to an Entity instance.

        Args:
            uid: The uid of the Entity instance to add the seen event to.
            time: The time value for the seen event.
            entity: The uid of the Entity instance that saw this Entity instance.
            detail_keys: The keys for the key value pairs that form detail for the event
            detail_vals: The values for the key value pairs that form the detail for the event
        """
        ent_idx = self.get_entity_index(uid)
        if ent_idx is not None:
            self.entities[ent_idx].seen_time.append(time)
            self.entities[ent_idx].seen_entity.append(entity)
            detail = CDFfunc.encode_event_detail(detail_key_ls=detail_keys, detail_val_ls=detail_vals)
            self.entities[ent_idx].seen_detail.append(detail)
            self.logger.debug(f"Entity uid {uid} - {self.seen_event_lbl} event added, "
                              f"time {time}, entity {entity}, detail {detail}")

            self.add_event_id(prim_uid=uid, sec_uid=entity, add_event_type=self.seen_event_lbl)
        else:
            self.logger.error(f"Add seen called with unrecognised uid - {uid}")

    def add_stop(self, uid: str, time: float, entity: str, detail_keys: list, detail_vals: list):
        """
        Add single stopped seeing event to an Entity instance.

        Args:
            uid: The uid of the Entity instance to add the event to.
            time: The time value for the stopped seeing event.
            entity: The uid of the Entity instance that this Entity instance lost sight of.
            detail_keys: The keys for the key value pairs that form detail for the event
            detail_vals: The values for the key value pairs that form the detail for the event
        """
        ent_idx = self.get_entity_index(uid)
        if ent_idx is not None:
            self.entities[ent_idx].stop_time.append(time)
            self.entities[ent_idx].stop_entity.append(entity)
            detail = CDFfunc.encode_event_detail(detail_key_ls=detail_keys, detail_val_ls=detail_vals)
            self.entities[ent_idx].stop_detail.append(detail)
            self.logger.debug(f"Entity uid {uid} - {self.stop_event_lbl} event added, "
                              f"time {time}, entity {entity}, detail {detail}")

            self.add_event_id(prim_uid=uid, sec_uid=entity, add_event_type=self.stop_event_lbl)
        else:
            self.logger.error(f"Add stop called with unrecognised uid - {uid}")

    def add_status(self, uid: str, time: float, detail_keys: list, detail_vals: list):
        """
                Add a single status update event to an Entity instance.

                Args:
                    uid: The uid of the Entity instance to add the status update event to.
                    time: The time value for the status update event.
                    detail_keys: The keys for the key value pairs that form detail for the event
                    detail_vals: The values for the key value pairs that form the detail for the event
                """
        ent_idx = self.get_entity_index(uid)
        if ent_idx is not None:
            self.entities[ent_idx].state_time.append(time)
            detail = CDFfunc.encode_event_detail(detail_key_ls=detail_keys, detail_val_ls=detail_vals)
            self.entities[ent_idx].state_detail.append(detail)
            self.logger.debug(f"Entity uid {uid} - {self.status_event_lbl} event added, time {time}, detail {detail}")

            self.add_event_id(prim_uid=uid, add_event_type=self.status_event_lbl)
        else:
            self.logger.error(f"Add state called with unrecognised uid - {uid}")

    def remove_event(self, remove_id: str) -> None:
        """
        Remove an event from the Dataset instance
        Args:
            remove_id: event id of the event to remove
        """
        event_id_dict = self.get_event_id_dict()

        ent_uid = self.search_event_id_dict(search_id=remove_id, data_key='prim_uid', event_id_dict=event_id_dict)
        ent_idx = self.get_entity_index(ent_uid)
        data_idx = self.search_event_id_dict(search_id=remove_id, data_key='data_idx', event_id_dict=event_id_dict)
        event_type = self.search_event_id_dict(search_id=remove_id, data_key='type', event_id_dict=event_id_dict)
        removed = False

        if ent_idx is None or data_idx is None or event_type is None:
            self.logger.error(f"remove_event - bad remove_id {remove_id} (no event removed)")
        else:
            if event_type == self.loc_event_lbl:
                del self.entities[ent_idx].location_time[data_idx]
                del self.entities[ent_idx].location_x[data_idx]
                del self.entities[ent_idx].location_y[data_idx]
                del self.entities[ent_idx].location_detail[data_idx]
                removed = True
            elif event_type == self.shot_event_lbl:
                del self.entities[ent_idx].shots_time[data_idx]
                del self.entities[ent_idx].shots_detail[data_idx]
                removed = True
            elif event_type == self.spot_event_lbl:
                del self.entities[ent_idx].spot_time[data_idx]
                del self.entities[ent_idx].spot_entity[data_idx]
                del self.entities[ent_idx].spot_detail[data_idx]
                removed = True
            elif event_type == self.seen_event_lbl:
                del self.entities[ent_idx].seen_time[data_idx]
                del self.entities[ent_idx].seen_entity[data_idx]
                del self.entities[ent_idx].seen_detail[data_idx]
                removed = True
            elif event_type == self.kill_event_lbl:
                del self.entities[ent_idx].kills_time[data_idx]
                del self.entities[ent_idx].kills_victim[data_idx]
                del self.entities[ent_idx].kills_detail[data_idx]
                removed = True
            elif event_type == self.loss_event_lbl:
                del self.entities[ent_idx].losses_time[data_idx]
                del self.entities[ent_idx].losses_killer[data_idx]
                del self.entities[ent_idx].losses_detail[data_idx]
                removed = True
            elif event_type == self.stop_event_lbl:
                del self.entities[ent_idx].stop_time[data_idx]
                del self.entities[ent_idx].stop_entity[data_idx]
                del self.entities[ent_idx].stop_detail[data_idx]
                removed = True
            elif event_type == self.status_event_lbl:
                del self.entities[ent_idx].state_time[data_idx]
                del self.entities[ent_idx].state_detail[data_idx]
                removed = True
            else:
                self.logger.error(f"remove_event called with remove_id {remove_id} that has an unrecognised "
                                  f"event type {event_type} (event not removed)")

        if removed:
            event_id_dict_idx = None
            # remove the entry for the event from the entity's event_id_dict
            for idx, event_id in enumerate(self.entities[ent_idx].entity_event_id_dict['evn_id']):
                if event_id == remove_id:
                    event_id_dict_idx = idx
            for key in self.entities[ent_idx].entity_event_id_dict.keys():
                del self.entities[ent_idx].entity_event_id_dict[key][event_id_dict_idx]
            # adjust data_idx for all events of the same type with a later event_id
            for idx in range(event_id_dict_idx, len(self.entities[ent_idx].entity_event_id_dict['evn_id'])):
                if self.entities[ent_idx].entity_event_id_dict['type'][idx] == event_type:
                    self.entities[ent_idx].entity_event_id_dict['data_idx'][idx] -= 1
            # add a debug event to the log
            self.logger.debug(f"event {remove_id} removed from entity {ent_uid}")

    def get_event_id_dict(self) -> dict:
        """
        Return a combined event_id_dict for all entities in entities array
        """
        event_dict = {'evn_ser': [],
                      'evn_id': [],
                      'type': [],
                      'prim_uid': [],
                      'sec_uid': [],
                      'data_idx': []}

        for entity in self.entities:
            for key in event_dict:
                event_dict[key].extend(entity.entity_event_id_dict[key])

        return event_dict

    def get_event_id_ls(self, sort_list=True) -> list:
        """
        Return a list of all event Ids for all entities in the entities array
        Check for duplicates and generate an error in the dataset log if any are found
        Args:
            sort_list: return a list sorted by values (default True)
        """
        event_ls = []

        for entity in self.entities:
            event_ls.extend(entity.entity_event_id_dict['evn_id'])

        if len(CDFfunc.get_unique_list(event_ls)) != len(event_ls):
            self.logger.error("duplicate event ids present")

        if sort_list:
            event_ls.sort()

        return event_ls

    def search_event_id_dict(self, search_id: str, data_key: str,
                             event_id_dict: dict, evn_id_key='evn_id') -> int or str:
        """
        Search an event_id_dict for a specific event and return a data item for that event
        Args:
            search_id: the event id to search for
            data_key: the key of the data item to return
            event_id_dict: the event_id_dict to search in
            evn_id_key: the key for the event_id list in the event_id_dict (default 'evn_id')
        """
        return_val = None
        id_found = False

        if data_key not in event_id_dict.keys():
            self.logger.error(f"search_event_id_dict called with data_key {data_key} not present in the event_id_dict")
        else:
            for idx, event_id in enumerate(event_id_dict[evn_id_key]):
                if event_id == search_id:
                    return_val = event_id_dict[data_key][idx]
                    id_found = True

            if return_val is None and not id_found:
                self.logger.error(f"search_event_id_dict called with search_id {search_id} that does not match any "
                                  f"event ids in the event_id_dict")

        return return_val

    def get_event_data(self, search_id: str) -> dict:
        """
        Retrieve the data items for a specific event and return as a dict object
        Args:
            search_id: the event_id to return the data items for
        """
        return_dict = {}
        event_id_dict = self.get_event_id_dict()

        ent_uid = self.search_event_id_dict(search_id=search_id, data_key='prim_uid', event_id_dict=event_id_dict)
        ent_idx = self.get_entity_index(ent_uid)
        data_idx = self.search_event_id_dict(search_id=search_id, data_key='data_idx', event_id_dict=event_id_dict)
        event_type = self.search_event_id_dict(search_id=search_id, data_key='type', event_id_dict=event_id_dict)

        if ent_idx is None or data_idx is None or event_type is None:
            self.logger.error(f"get_event_data_dict - bad search_id {search_id}")
        else:
            time = None
            detail = None
            sec_uid = None
            x = None
            y = None
            retrieved = True

            if event_type == self.loc_event_lbl:
                time = self.entities[ent_idx].location_time[data_idx]
                detail = self.entities[ent_idx].location_detail[data_idx]
                x = self.entities[ent_idx].location_x[data_idx]
                y = self.entities[ent_idx].location_y[data_idx]
            elif event_type == self.shot_event_lbl:
                time = self.entities[ent_idx].shots_time[data_idx]
                detail = self.entities[ent_idx].shots_detail[data_idx]
            elif event_type == self.spot_event_lbl:
                time = self.entities[ent_idx].spot_time[data_idx]
                sec_uid = self.entities[ent_idx].spot_entity[data_idx]
                detail = self.entities[ent_idx].spot_detail[data_idx]
            elif event_type == self.seen_event_lbl:
                time = self.entities[ent_idx].seen_time[data_idx]
                sec_uid = self.entities[ent_idx].seen_entity[data_idx]
                detail = self.entities[ent_idx].seen_detail[data_idx]
            elif event_type == self.kill_event_lbl:
                time = self.entities[ent_idx].kills_time[data_idx]
                sec_uid = self.entities[ent_idx].kills_victim[data_idx]
                detail = self.entities[ent_idx].kills_detail[data_idx]
            elif event_type == self.loss_event_lbl:
                time = self.entities[ent_idx].losses_time[data_idx]
                sec_uid = self.entities[ent_idx].losses_killer[data_idx]
                detail = self.entities[ent_idx].losses_detail[data_idx]
            elif event_type == self.stop_event_lbl:
                time = self.entities[ent_idx].stop_time[data_idx]
                sec_uid = self.entities[ent_idx].stop_entity[data_idx]
                detail = self.entities[ent_idx].stop_detail[data_idx]
            elif event_type == self.status_event_lbl:
                time = self.entities[ent_idx].state_time[data_idx]
                detail = self.entities[ent_idx].state_detail[data_idx]
            else:
                self.logger.error(f"get_event_data called with a search_id {search_id} "
                                  f" that has an unrecognised event type {event_type}")
                retrieved = False

            return_dict = dict(event_id=search_id, event_type=event_type, prim_uid=ent_uid,
                               time=time, sec_uid=sec_uid, detail=detail)
            if retrieved:
                self.logger.debug(f"event data retrieved for event {search_id} (primary entity {ent_uid})")

            if event_type == self.loc_event_lbl:
                return_dict['x'] = x
                return_dict['y'] = y

        return return_dict

    def add_event_id(self, add_event_type: str, prim_uid: str, sec_uid: str = None) -> None:
        """
        Determine the next available serial for the event type and add an entry to the primary entity's
        entity_event_id_dict with the event type, serial, id, primary and secondary entity uids and the
        index position of the data items in the entity's data lists.
        Args:
            add_event_type: the type of event to add an event id for
            prim_uid: the uid of the primary entity for the event
            sec_uid: the uid of the secondary entity for the event (optional, default None)
        """

        evn_ser = 0
        if add_event_type == self.loc_event_lbl:
            evn_ser = self.loc_event_last_ser + 1
            self.update_config('loc_event_last_ser', evn_ser)
        elif add_event_type == self.shot_event_lbl:
            evn_ser = self.shot_event_last_ser + 1
            self.update_config('shot_event_last_ser', evn_ser)
        elif add_event_type == self.kill_event_lbl:
            evn_ser = self.kill_event_last_ser + 1
            self.update_config('kill_event_last_ser', evn_ser)
        elif add_event_type == self.loss_event_lbl:
            evn_ser = self.loss_event_last_ser + 1
            self.update_config('loss_event_last_ser', evn_ser)
        elif add_event_type == self.spot_event_lbl:
            evn_ser = self.spot_event_last_ser + 1
            self.update_config('spot_event_last_ser', evn_ser)
        elif add_event_type == self.seen_event_lbl:
            evn_ser = self.seen_event_last_ser + 1
            self.update_config('seen_event_last_ser', evn_ser)
        elif add_event_type == self.stop_event_lbl:
            evn_ser = self.stop_event_last_ser + 1
            self.update_config('stop_event_last_ser', evn_ser)
        elif add_event_type == self.status_event_lbl:
            evn_ser = self.status_event_last_ser + 1
            self.update_config('status_event_last_ser', evn_ser)
        else:
            self.logger.error(f'add_event_id called with unrecognised event type {add_event_type}')

        if add_event_type in self.event_lbl_map.keys():
            evn_id = self.event_lbl_map[add_event_type] + '-' + str(evn_ser)
        else:
            evn_id = add_event_type + '-' + str(evn_ser)
            self.logger.error(f"no short label mapped for {add_event_type}")

        ent_idx = self.get_entity_index(prim_uid)
        if ent_idx is not None:
            # to get the data_idx count the number of that event type already in that entity's entity_event_id_dict
            data_idx = self.entities[ent_idx].entity_event_id_dict['type'].count(add_event_type)

            # add to the entity event dict
            self.entities[ent_idx].entity_event_id_dict['evn_ser'].append(evn_ser)
            self.entities[ent_idx].entity_event_id_dict['evn_id'].append(evn_id)
            self.entities[ent_idx].entity_event_id_dict['type'].append(add_event_type)
            self.entities[ent_idx].entity_event_id_dict['prim_uid'].append(prim_uid)
            self.entities[ent_idx].entity_event_id_dict['sec_uid'].append(sec_uid)
            self.entities[ent_idx].entity_event_id_dict['data_idx'].append(data_idx)

        else:
            self.logger.error(f"add_event_id called with unrecognised primary uid {prim_uid}")

    def finalise_data(self) -> None:
        """
        Execute all the data production and checking functions in sequence.
        """
        self.check_dataset_details()
        self.assign_entity_levels()
        self.check_entity_data()

        self.generate_cdf_entity_table_df()
        self.check_cdf_entity_table_df()

        self.generate_cdf_events_df()
        self.check_cdf_events_df()

        self.generate_cdf_cbt_pwr_df()
        self.check_cdf_cbt_pwr_df()

        self.add_case_and_rep_to_cdf_df()

        self.add_summary_metadata()

        if self.drop_location_events:
            self.drop_event_type(self.loc_event_lbl)
        if self.drop_seen_events:
            self.drop_event_type(self.seen_event_lbl)
        if self.drop_spot_events:
            self.drop_event_type(self.spot_event_lbl)
        if self.drop_shot_events:
            self.drop_event_type(self.shot_event_lbl)

    def export_data(self) -> None:
        """
        Output CDF entity table, events and combat power files
        """
        # create output location if it does not already exist
        if not path.isdir(self.output_location):
            makedirs(self.output_location)
        # if splitting by type check for the required subfolders and create them if they do not already exist
        if self.split_files_by_type:
            output_subfolder_ls = [self.entity_folder_name, self.events_folder_name,
                                   self.cbt_folder_name, self.meta_folder_name]
            for subfolder in output_subfolder_ls:
                subfolder_path = path.join(self.output_location, subfolder)
                if not path.isdir(subfolder_path):
                    makedirs(subfolder_path)
        # refresh cdf filenames and paths
        self.generate_cdf_filenames_and_paths()

        # write the metadata file
        with open(self.metadata_file_path, "w") as metadata_file:
            yaml.safe_dump(self.metadata_dict, metadata_file)
        self.logger.info(f"{self.metadata_file_path} exported")

        if self.output_csv:
            self.logger.info("Exporting CDF files in .csv format:")

            self.CDF_entity_table_df.to_csv(self.entity_file_path, index=False)
            self.CDF_events_df.to_csv(self.events_file_path, index=False)
            self.CDF_combat_power_DF.to_csv(self.cbt_pwr_file_path, index=False)

            self.logger.info(f"{self.entity_file_path} exported")
            self.logger.info(f"{self.events_file_path} exported")
            self.logger.info(f"{self.cbt_pwr_file_path} exported")

        if self.output_parquet:
            self.logger.info("Exporting CDF files in .parquet format:")
            try:
                pq_entity_file_path = self.entity_file_path.replace(".csv", ".parquet")
                pq_events_file_path = self.events_file_path.replace(".csv", ".parquet")
                pq_cbt_pwr_file_path = self.cbt_pwr_file_path.replace(".csv", ".parquet")

                self.CDF_entity_table_df.to_parquet(pq_entity_file_path, index=False)
                self.CDF_events_df.to_parquet(pq_events_file_path, index=False)
                self.CDF_combat_power_DF.to_parquet(pq_cbt_pwr_file_path, index=False)

                self.logger.info(f"{pq_entity_file_path} exported")
                self.logger.info(f"{pq_events_file_path} exported")
                self.logger.info(f"{pq_cbt_pwr_file_path} exported")
            except ImportError:
                self.logger.error("Parquet export failed - no parquet engine installed")

    def check_dataset_details(self) -> None:
        """
        Check detail of the Dataset instance.

        Check the parameters of the Dataset instance and add warning events in the Dataset instance log if  any
        are blank or null.
        """
        self.logger.info("Checking dataset details")
        detail_lbl_ls = ['model_name', 'data_name', 'data_date', 'data_details',
                         'time_unit', 'distance_unit', 'cbt_pwr_unit']
        detail_item_ls = [self.model_name, self.data_name, self.data_date, self.data_details,
                          self.time_unit, self.distance_unit, self.cbt_pwr_unit]

        for idx, detail in enumerate(detail_item_ls):
            if pd.isnull(detail) or detail == "":
                self.logger.warning(f"{detail_lbl_ls[idx]} not set")

    def assign_entity_levels(self) -> None:
        """
        Attempt to discover and assign levels to Entity instances in the entities array.

        Attempt to discover and assign levels to Entity instances in the entities array. This function will identify
        any level 1 entities (level already set as 1 or where an Entity instance is its own commander) and then iterate
        through the entities array multiple times assigning Entity instance levels as commander Entity instance level
        plus 1.
        """
        self.logger.info("Attempting to discover and assign entity levels")

        lvl1_ent_ls = []
        for entity in self.entities:
            if entity.commander == entity.uid or entity.level == 1:
                self.logger.debug(f"Entity {entity.uid} identified as level 1 entity")
                entity.level = 1
                lvl1_ent_ls.append(entity.uid)

        assigned_count = 0
        lvl_complete = False
        max_iter = 1000
        iter_count = 0

        known_uid_ls = []
        for entity in self.entities:
            known_uid_ls.append(entity.uid)

        while not lvl_complete and iter_count < max_iter:
            lvl_complete = True
            iter_count += 1
            self.logger.debug(f"Pass {iter_count} of assigning levels (limit of {max_iter})")
            for entity in self.entities:
                if entity.level is None:
                    self.logger.debug(f"{entity.uid} attempting level assign")
                    if entity.commander in known_uid_ls:
                        if self.entities[self.get_entity_index(entity.commander)].level is not None:
                            lvl_complete = False
                            ent_lvl = self.entities[self.get_entity_index(entity.commander)].level + 1
                            entity.level = ent_lvl
                            self.logger.debug(f"Entity {entity.uid} determined to be level {ent_lvl}")
                            assigned_count += 1
                        else:
                            self.logger.debug(f"{entity.uid} level assign failed - commander level not determined")
                    else:
                        self.logger.debug(f"{entity.uid} level assign failed - unknown commander")

        self.logger.info(f"Levels assigned to {assigned_count} out of {len(self.entities)} entities ")

    def check_entity_data(self) -> None:
        """
        Check data for Entity instances.

        Iterate through entities array and:
        Check for any parameters not set (add Dataset log debug event and set to defined default value).
        Check for repeat values for the Entity unit_name parameter (add Dataset log warning event and optionally
        add a suffix to make value unique).
        Check if the same value has been used for force and affiliation parameters (add Dataset log warning event
        and add suffix to force parameter value).
        """
        force_suffix_str = " - Force"
        not_set_str = "not set"
        not_set_lvl = 1
        not_set_comps = 1
        not_set_cbt_comp = 1

        for entity in self.entities:
            # check str entity data for values that have not been set
            if entity.unit_name is None:
                entity.unit_name = not_set_str
                self.logger.debug(f"Unit_name not set for uid {entity.uid}, set to {not_set_str}")
            if entity.unit_type is None:
                entity.unit_type = not_set_str
                self.logger.debug(f"Unit_type not set for uid {entity.uid}, set to {not_set_str}")
            if entity.commander is None:
                entity.commander = not_set_str
                self.logger.debug(f"Commander not set for uid {entity.uid}, set to {not_set_str}")
            if entity.affiliation is None:
                entity.affiliation = not_set_str
                self.logger.debug(f"Affiliation not set for uid {entity.uid}, set to {not_set_str}")
            if entity.force is None:
                entity.force = entity.affiliation + force_suffix_str
                self.logger.debug(f"Force not set for uid {entity.uid}, set to {entity.force}")

            # check for numerical data entries that are not set, set to default and generate warning:
            if entity.level is None:
                entity.level = not_set_lvl
                self.logger.debug(f"Level not set for uid {entity.uid} - set to {str(not_set_lvl)}")
            if entity.init_comps is None:
                entity.init_comps = not_set_comps
                self.logger.debug(f"Init comps not set for uid {entity.uid} - set to {str(not_set_comps)}")
            if entity.cbt_per_comp is None:
                entity.cbt_per_comp = not_set_cbt_comp
                self.logger.debug(f"Cbt pwr per comp not set for uid {entity.uid} - set to {str(not_set_cbt_comp)}")

        # check if entity names are unique and add suffix if forcing unique entity names
        repeat_dict = {}
        repeat_found = False

        for entity in self.entities:
            ent_name = entity.unit_name.lower()
            if ent_name in list(repeat_dict.keys()):
                repeat_dict[ent_name] += 1
                if not self.force_unique_unit_names:
                    self.logger.warning(f"Name for entity {entity.uid} ({entity.unit_name}) is not unique")
                if self.force_unique_unit_names:
                    if not repeat_found:
                        self.logger.info("Repeat entity names automatically updated, details in dataset instance log")
                        repeat_found = True
                    entity.unit_name = entity.unit_name + "-" + str(repeat_dict[ent_name])
                    self.logger.debug(f"Name for entity {entity.uid} updated to {entity.unit_name} (repeat unit name)")
            else:
                repeat_dict[ent_name] = 1

        self.logger.debug(f"Summary of entity name repeats:")
        for item in repeat_dict.items():
            if item[1] > 1:
                self.logger.debug(f"{item[1]} x repeats of {item[0]}")

        # check if force name matches any affiliation name
        afil_ls = []
        for entity in self.entities:
            afil_ls.append(entity.affiliation)
        afil_ls = CDFfunc.get_unique_list(afil_ls)

        for entity in self.entities:
            if entity.force in afil_ls:
                self.logger.warning(f"Force ({entity.force}) for entity {entity.uid} is also a value for affiliation, "
                                    f"appending '{force_suffix_str}' to force for entity")
                entity.force = entity.force + force_suffix_str

    def generate_cdf_entity_table_df(self) -> None:
        """
        Generate the CDF entity table as a Dataframe.
        """
        self.logger.info("Generating CDF entity table file")
        # reset the dataframe
        self.CDF_entity_table_df = pd.DataFrame()

        unit_id_ls = []
        for entity in self.entities:
            unit_id_ls.append(entity.uid)

        unit_name_ls = []
        unit_type_ls = []
        unit_commander_id_ls = []
        unit_commander_name_ls = []
        unit_level_ls = []
        affiliation_ls = []
        force_ls = []
        init_comps_ls = []
        cbt_per_comp_ls = []
        init_cbt_pwr_ls = []

        system_entity_ls = []
        start_entity_ls = []
        add_time_ls = []
        total_events_ls = []
        loc_events_ls = []
        seen_events_ls = []
        spot_events_ls = []
        shot_events_ls = []
        kill_events_ls = []
        loss_events_ls = []
        stop_events_ls = []
        status_events_ls = []

        for entity in self.entities:
            unit_name_ls.append(entity.unit_name)
            unit_type_ls.append(entity.unit_type)
            unit_commander_id_ls.append(entity.commander)

            # in some cases commander may not be a recognised entity
            if unit_commander_id_ls[-1] in unit_id_ls:
                commander_name = self.entities[self.get_entity_index(unit_commander_id_ls[-1])].unit_name
                unit_commander_name_ls.append(commander_name)
            else:
                self.logger.debug(f"Commander uid {unit_commander_id_ls[-1]} for {entity.uid} not recognised, "
                                  f"commander_name set as blank")
                unit_commander_name_ls.append("")

            unit_level_ls.append(entity.level)
            affiliation_ls.append(entity.affiliation)
            force_ls.append(entity.force)
            init_comps_ls.append(entity.init_comps)
            cbt_per_comp_ls.append(entity.cbt_per_comp)
            init_cbt_pwr_ls.append(entity.init_comps * entity.cbt_per_comp)

            system_entity_ls.append(entity.system_entity)
            start_entity_ls.append(entity.start_entity)
            add_time_ls.append(entity.add_time)
            loc_events_ls.append(len(entity.location_time))
            seen_events_ls.append(len(entity.seen_time))
            spot_events_ls.append(len(entity.spot_time))
            shot_events_ls.append(len(entity.shots_time))
            kill_events_ls.append(len(entity.kills_time))
            loss_events_ls.append(len(entity.losses_time))
            stop_events_ls.append(len(entity.stop_time))
            status_events_ls.append(len(entity.state_time))

            total_events_ls.append(len(entity.entity_event_id_dict['evn_id']))

        if not CDFfunc.compare_list_lengths(unit_id_ls, unit_name_ls, unit_type_ls,
                                            unit_commander_id_ls, unit_level_ls, affiliation_ls, force_ls,
                                            init_comps_ls, cbt_per_comp_ls, init_cbt_pwr_ls,
                                            system_entity_ls, start_entity_ls, add_time_ls,
                                            loc_events_ls, seen_events_ls, spot_events_ls,
                                            shot_events_ls, kill_events_ls, loss_events_ls,
                                            stop_events_ls, status_events_ls, total_events_ls):
            self.logger.error("Generate_cdf_entity_table function - mismatched list lengths")

        self.CDF_entity_table_df = pd.DataFrame(data=zip(unit_id_ls, unit_name_ls, unit_type_ls,
                                                         unit_commander_id_ls, unit_commander_name_ls,
                                                         unit_level_ls, affiliation_ls, force_ls,
                                                         init_comps_ls, cbt_per_comp_ls, init_cbt_pwr_ls,
                                                         system_entity_ls, start_entity_ls, add_time_ls,
                                                         total_events_ls, status_events_ls, loc_events_ls,
                                                         seen_events_ls, spot_events_ls, stop_events_ls,
                                                         shot_events_ls, kill_events_ls, loss_events_ls),
                                                columns=[self.ent_tbl_id_col_lbl,
                                                         self.ent_tbl_name_col_lbl,
                                                         self.ent_tbl_type_col_lbl,
                                                         self.ent_tbl_commander_id_col_lbl,
                                                         self.ent_tbl_commander_name_col_lbl,
                                                         self.ent_tbl_level_col_lbl,
                                                         self.ent_tbl_affil_col_lbl,
                                                         self.ent_tbl_force_col_lbl,
                                                         self.ent_tbl_init_comp_col_lbl,
                                                         self.ent_tbl_cbt_per_comp_col_lbl,
                                                         self.ent_tbl_init_cbt_pwr_col_lbl,
                                                         self.ent_tbl_sys_entity_col_lbl,
                                                         self.ent_tbl_start_entity_col_lbl,
                                                         self.ent_tbl_add_time_col_lbl,
                                                         self.ent_tbl_total_events_lbl,
                                                         self.ent_tbl_status_events_lbl,
                                                         self.ent_tbl_loc_events_lbl,
                                                         self.ent_tbl_seen_events_lbl,
                                                         self.ent_tbl_spot_events_lbl,
                                                         self.ent_tbl_stop_events_lbl,
                                                         self.ent_tbl_shot_events_lbl,
                                                         self.ent_tbl_kill_events_lbl,
                                                         self.ent_tbl_loss_events_lbl])

        # try to apply column types to the CDF entity table df
        try:
            self.CDF_entity_table_df = self.CDF_entity_table_df.astype(dtype=self.ent_tbl_col_types_dict)
        except ValueError as error:
            self.logger.error(f"Unable to type cast for one or more columns in CDF entity table df: {str(error)}, "
                              f"may cause issues with parquet export")

    def check_cdf_entity_table_df(self) -> None:
        """
        Check CDF entity table.

        Entity table checks:
        Check that entity uid values are unique (add Dataset error).
        Check that affiliation to force is a many-to-one mapping (add Dataset error).
        """
        self.logger.info("Checking CDF entity table file")

        entity_table_issue_count = 0
        entity_uid_list = self.CDF_entity_table_df[self.ent_tbl_id_col_lbl].tolist()

        # check that entity uids are unique
        unique_uid_list = CDFfunc.get_unique_list(entity_uid_list)
        if len(unique_uid_list) < len(entity_uid_list):
            for uid in unique_uid_list:
                if entity_uid_list.count(uid) > 1:
                    self.logger.error(f"CDF entity table check - {entity_uid_list.count(uid)} instances "
                                      f"of uid {uid} in CDF entity table")
                    entity_table_issue_count += 1

        # check that affiliation to force is a many-to-one mapping
        unique_affil_list = CDFfunc.get_unique_list(self.CDF_entity_table_df[self.ent_tbl_affil_col_lbl].tolist())
        for affil in unique_affil_list:
            test_mask = (self.CDF_entity_table_df[self.ent_tbl_affil_col_lbl] == affil)
            force_list = CDFfunc.get_unique_list(
                self.CDF_entity_table_df.loc[test_mask, self.ent_tbl_force_col_lbl].tolist())
            if len(force_list) > 1:
                self.logger.error(f"CDF entity table check - {affil} maps to multiple forces: {force_list}")
                entity_table_issue_count += 1

        # add code for additional checks

        if entity_table_issue_count == 0:
            self.logger.info("No issues found in CDF entity table file")
        else:
            self.logger.warning(f"{entity_table_issue_count} potential issues found in CDF entity table file, "
                                f"may cause issues with parquet export")

    def generate_cdf_events_df(self) -> None:
        """
        Generate CDF event output as a Dataframe
        """
        self.logger.info("Generating CDF events file")
        self.CDF_events_df = pd.DataFrame()

        # set up empty lists to hold the key data
        event_time_ls = []
        event_primary_entity_ls = []
        event_primary_entity_x_ls = []
        event_primary_entity_y_ls = []
        event_type_ls = []
        event_detail_ls = []
        event_secondary_entity_ls = []
        event_id_ls = []

        def extend_event_lists(ent_event_id_df, event_type,
                               time_data_ls, detail_data_ls,
                               primary_x_data_ls=None, primary_y_data_ls=None):
            ent_event_type_id_df = ent_event_id_df.loc[ent_event_id_df['type'] == event_type].copy()

            event_time_ls.extend(time_data_ls)
            event_detail_ls.extend(detail_data_ls)
            if primary_x_data_ls:
                event_primary_entity_x_ls.extend(primary_x_data_ls)
            if primary_y_data_ls:
                event_primary_entity_y_ls.extend(primary_y_data_ls)

            event_type_ls.extend(ent_event_type_id_df['type'].to_list())
            event_id_ls.extend(ent_event_type_id_df['evn_id'].to_list())
            event_primary_entity_ls.extend(ent_event_type_id_df['prim_uid'].to_list())
            event_secondary_entity_ls.extend(ent_event_type_id_df['sec_uid'].to_list())

            while len(event_primary_entity_x_ls) < len(event_time_ls):
                event_primary_entity_x_ls.append(None)
                event_primary_entity_y_ls.append(None)

        # cycle through entities and extend event lists with data from that entity
        for entity in self.entities:
            # add location events
            extend_event_lists(ent_event_id_df=pd.DataFrame(data=entity.entity_event_id_dict),
                               time_data_ls=entity.location_time, detail_data_ls=entity.location_detail,
                               primary_x_data_ls=entity.location_x, primary_y_data_ls=entity.location_y,
                               event_type=self.loc_event_lbl)
            # add shot events
            extend_event_lists(ent_event_id_df=pd.DataFrame(data=entity.entity_event_id_dict),
                               time_data_ls=entity.shots_time, detail_data_ls=entity.shots_detail,
                               event_type=self.shot_event_lbl)
            # add kill events
            extend_event_lists(ent_event_id_df=pd.DataFrame(data=entity.entity_event_id_dict),
                               time_data_ls=entity.kills_time, detail_data_ls=entity.kills_detail,
                               event_type=self.kill_event_lbl)
            # add loss events
            extend_event_lists(ent_event_id_df=pd.DataFrame(data=entity.entity_event_id_dict),
                               time_data_ls=entity.losses_time, detail_data_ls=entity.losses_detail,
                               event_type=self.loss_event_lbl)
            # add spot events
            extend_event_lists(ent_event_id_df=pd.DataFrame(data=entity.entity_event_id_dict),
                               time_data_ls=entity.spot_time, detail_data_ls=entity.spot_detail,
                               event_type=self.spot_event_lbl)
            # add seen events
            extend_event_lists(ent_event_id_df=pd.DataFrame(data=entity.entity_event_id_dict),
                               time_data_ls=entity.seen_time, detail_data_ls=entity.seen_detail,
                               event_type=self.seen_event_lbl)
            # add stop events
            extend_event_lists(ent_event_id_df=pd.DataFrame(data=entity.entity_event_id_dict),
                               time_data_ls=entity.stop_time, detail_data_ls=entity.stop_detail,
                               event_type=self.stop_event_lbl)
            # add status events
            extend_event_lists(ent_event_id_df=pd.DataFrame(data=entity.entity_event_id_dict),
                               time_data_ls=entity.state_time, detail_data_ls=entity.state_detail,
                               event_type=self.status_event_lbl)

        if not CDFfunc.compare_list_lengths(event_time_ls,
                                            event_primary_entity_ls,
                                            event_primary_entity_x_ls, event_primary_entity_y_ls,
                                            event_id_ls,
                                            event_type_ls,
                                            event_detail_ls,
                                            event_secondary_entity_ls):
            self.logger.error("Generate_cdf_event_file function - mismatched list lengths")
            self.logger.debug(f"list lengths: "
                              f"\n\ttime - {len(event_time_ls)} "
                              f"\n\tprimary entity - {len(event_primary_entity_ls)}"
                              f"\n\tprimary x - {len(event_primary_entity_x_ls)}"
                              f"\n\tprimary y - {len(event_primary_entity_y_ls)}"
                              f"\n\tsecondary entity - {len(event_secondary_entity_ls)}"
                              f"\n\tevent id - {len(event_id_ls)}"
                              f"\n\tevent type - {len(event_type_ls)}"
                              f"\n\tevent detail - {len(event_detail_ls)}")

        self.CDF_events_df = pd.DataFrame(data=zip(event_time_ls,
                                                   event_primary_entity_ls,
                                                   event_primary_entity_x_ls, event_primary_entity_y_ls,
                                                   event_id_ls,
                                                   event_type_ls,
                                                   event_detail_ls,
                                                   event_secondary_entity_ls),
                                          columns=[self.evn_tbl_time_col_lbl,
                                                   self.evn_tbl_prim_id_col_lbl,
                                                   self.evn_tbl_prim_x_col_lbl, self.evn_tbl_prim_y_col_lbl,
                                                   self.evn_tbl_event_id_col_lbl,
                                                   self.evn_tbl_event_type_col_lbl,
                                                   self.evn_tbl_event_detail_col_lbl,
                                                   self.evn_tbl_sec_id_col_lbl])

        # make the event type column categorical and set a sort order putting location updates as the first type
        self.CDF_events_df[self.evn_tbl_event_type_col_lbl] = \
            pd.Categorical(self.CDF_events_df[self.evn_tbl_event_type_col_lbl],
                           [self.loc_event_lbl, self.status_event_lbl,
                            self.spot_event_lbl, self.seen_event_lbl, self.stop_event_lbl,
                            self.shot_event_lbl, self.kill_event_lbl, self.loss_event_lbl])
        # order the cdf events df by time and then by event type
        self.CDF_events_df.sort_values(by=[self.evn_tbl_time_col_lbl, self.evn_tbl_event_type_col_lbl],
                                       inplace=True, ignore_index=True)

        # fill in blanks in the primary location x / y cols by filling with the last location update values
        # assuming that the entity remains at its last reported location for each event until the next location update
        self.CDF_events_df[self.evn_tbl_prim_x_col_lbl] = \
            self.CDF_events_df.groupby(by=self.evn_tbl_prim_id_col_lbl)[self.evn_tbl_prim_x_col_lbl].ffill()
        self.CDF_events_df[self.evn_tbl_prim_y_col_lbl] = \
            self.CDF_events_df.groupby(by=self.evn_tbl_prim_id_col_lbl)[self.evn_tbl_prim_y_col_lbl].ffill()

        # attach secondary entity locations - get a df of locations by time
        unit_locations_df = self.CDF_events_df[[self.evn_tbl_time_col_lbl, self.evn_tbl_prim_id_col_lbl,
                                                self.evn_tbl_prim_x_col_lbl, self.evn_tbl_prim_y_col_lbl,
                                                self.evn_tbl_event_type_col_lbl, self.evn_tbl_sec_id_col_lbl]].copy()

        # mask event type is 'location update'
        loc_event_mask = unit_locations_df[self.evn_tbl_event_type_col_lbl] == self.loc_event_lbl
        # mask secondary entity id is blank
        no_sec_id_mask = pd.isnull(unit_locations_df[self.evn_tbl_sec_id_col_lbl])

        # where event type is location update and there is no secondary id copy the primary id to the sec id column
        unit_locations_df.loc[loc_event_mask & no_sec_id_mask, self.evn_tbl_sec_id_col_lbl] = \
            unit_locations_df[self.evn_tbl_prim_id_col_lbl]

        # where event type is location update copy primary x / y to secondary x / y columns
        unit_locations_df.loc[loc_event_mask, self.evn_tbl_sec_x_col_lbl] = \
            unit_locations_df[self.evn_tbl_prim_x_col_lbl]
        unit_locations_df.loc[loc_event_mask, self.evn_tbl_sec_y_col_lbl] = \
            unit_locations_df[self.evn_tbl_prim_y_col_lbl]

        # fill the secondary x / y columns down for each entity id
        # i.e. assumption as for primary that the entity is still at the same position until the next location update
        unit_locations_df[self.evn_tbl_sec_x_col_lbl] = \
            unit_locations_df.groupby(by=self.evn_tbl_sec_id_col_lbl)[self.evn_tbl_sec_x_col_lbl].ffill()
        unit_locations_df[self.evn_tbl_sec_y_col_lbl] = \
            unit_locations_df.groupby(by=self.evn_tbl_sec_id_col_lbl)[self.evn_tbl_sec_y_col_lbl].ffill()

        # remove the secondary x / y values for the location update events
        unit_locations_df.loc[loc_event_mask, self.evn_tbl_sec_x_col_lbl] = None
        unit_locations_df.loc[loc_event_mask, self.evn_tbl_sec_y_col_lbl] = None

        # add the secondary x / y locations from unit_locations_df to the CDF events file
        self.CDF_events_df[self.evn_tbl_sec_x_col_lbl] = unit_locations_df[self.evn_tbl_sec_x_col_lbl]
        self.CDF_events_df[self.evn_tbl_sec_y_col_lbl] = unit_locations_df[self.evn_tbl_sec_y_col_lbl]

        # get a dictionary with entity details keyed to unit id using the entity table
        entity_dict = self.CDF_entity_table_df.set_index(self.ent_tbl_id_col_lbl).to_dict()

        # dict for primary entity details (CDF column title - field in the entity dict to get the data from)
        cdf_primary_entity_cols_dict = dict({self.evn_tbl_prim_name_col_lbl: self.ent_tbl_name_col_lbl,
                                             self.evn_tbl_prim_type_col_lbl: self.ent_tbl_type_col_lbl,
                                             self.evn_tbl_prim_comd_col_lbl: self.ent_tbl_commander_id_col_lbl,
                                             self.evn_tbl_prim_lvl_col_lbl: self.ent_tbl_level_col_lbl,
                                             self.evn_tbl_prim_affil_col_lbl: self.ent_tbl_affil_col_lbl,
                                             self.evn_tbl_prim_force_col_lbl: self.ent_tbl_force_col_lbl})
        # iterate the dictionary to add the columns to the CDF events dataframe
        for col in cdf_primary_entity_cols_dict.items():
            self.CDF_events_df[col[0]] = self.CDF_events_df[self.evn_tbl_prim_id_col_lbl].map(entity_dict[col[1]])

        # dict for secondary entity details (CDF column title - field in entity dict to get data from)
        cdf_secondary_entity_cols_dict = dict({self.evn_tbl_sec_name_col_lbl: self.ent_tbl_name_col_lbl,
                                               self.evn_tbl_sec_type_col_lbl: self.ent_tbl_type_col_lbl,
                                               self.evn_tbl_sec_comd_col_lbl: self.ent_tbl_commander_id_col_lbl,
                                               self.evn_tbl_sec_lvl_col_lbl: self.ent_tbl_level_col_lbl,
                                               self.evn_tbl_sec_affil_col_lbl: self.ent_tbl_affil_col_lbl,
                                               self.evn_tbl_sec_force_col_lbl: self.ent_tbl_force_col_lbl})
        # iterate the dictionary to add the columns to the CDF events dataframe
        for col in cdf_secondary_entity_cols_dict.items():
            self.CDF_events_df[col[0]] = self.CDF_events_df[self.evn_tbl_sec_id_col_lbl].map(entity_dict[col[1]])

        # replace any None values in secondary entity ID column and mapped columns with blank strings
        replace_none_vals_col_ls = [self.evn_tbl_sec_id_col_lbl,
                                    self.evn_tbl_sec_name_col_lbl, self.evn_tbl_sec_type_col_lbl,
                                    self.evn_tbl_sec_comd_col_lbl,
                                    self.evn_tbl_sec_affil_col_lbl, self.evn_tbl_sec_force_col_lbl]
        for column in replace_none_vals_col_ls:
            self.CDF_events_df[column].fillna(value='', inplace=True)

        # rearrange columns of the CDF events file
        self.CDF_events_df = self.CDF_events_df[[self.evn_tbl_time_col_lbl,
                                                 self.evn_tbl_prim_id_col_lbl,
                                                 self.evn_tbl_prim_name_col_lbl,
                                                 self.evn_tbl_prim_type_col_lbl,
                                                 self.evn_tbl_prim_comd_col_lbl,
                                                 self.evn_tbl_prim_lvl_col_lbl,
                                                 self.evn_tbl_prim_affil_col_lbl,
                                                 self.evn_tbl_prim_force_col_lbl,
                                                 self.evn_tbl_prim_x_col_lbl, self.evn_tbl_prim_y_col_lbl,
                                                 self.evn_tbl_event_id_col_lbl,
                                                 self.evn_tbl_event_type_col_lbl,
                                                 self.evn_tbl_event_detail_col_lbl,
                                                 self.evn_tbl_sec_id_col_lbl,
                                                 self.evn_tbl_sec_name_col_lbl,
                                                 self.evn_tbl_sec_type_col_lbl,
                                                 self.evn_tbl_sec_comd_col_lbl,
                                                 self.evn_tbl_sec_lvl_col_lbl,
                                                 self.evn_tbl_sec_affil_col_lbl,
                                                 self.evn_tbl_sec_force_col_lbl,
                                                 self.evn_tbl_sec_x_col_lbl, self.evn_tbl_sec_y_col_lbl]]

        # try to apply column types to the CDF events df
        try:
            self.CDF_events_df = self.CDF_events_df.astype(dtype=self.evn_tbl_col_types_dict)
        except ValueError as error:
            self.logger.error(f"Unable to type cast for one or more columns in CDF events df: {str(error)}")

    def check_cdf_events_df(self) -> None:
        """
        Check CDF event Dataframe.

        CDF event Dataframe checks:
        Check for secondary entity uids that are not present in the entities array (add Dataset warning).
        Check for negative event times (add Dataset error).
        Check for entities suffering more loss events than they have components (add Dataset error)
        Check for entities not involved in any events (i.e. as primary or secondary) (add Dataset warning).
        """
        self.logger.info("Checking CDF events file")
        cdf_events_file_issue_count = 0
        event_id_ls = self.CDF_events_df[self.evn_tbl_event_id_col_lbl].to_list()
        event_time_ls = self.CDF_events_df[self.evn_tbl_time_col_lbl].to_list()
        primary_entity_id_ls = self.CDF_events_df[self.evn_tbl_prim_id_col_lbl].to_list()
        secondary_ent_id_ls = self.CDF_events_df[self.evn_tbl_sec_id_col_lbl].to_list()
        known_ent_id_ls = self.CDF_entity_table_df[self.ent_tbl_id_col_lbl].to_list()

        # check for unknown secondary entity ids
        for idx, ent_id in enumerate(secondary_ent_id_ls):
            if ent_id != "" and ent_id != "no secondary entity" and ent_id is not None:
                if ent_id not in known_ent_id_ls:
                    self.logger.warning(f"CDF events check - unrecognised secondary entity id {ent_id} "
                                        f"for event {event_id_ls[idx]}")
                    cdf_events_file_issue_count += 1

        # check for any negative event times and check for any non-numeric event time values
        for idx, event_time in enumerate(event_time_ls):
            try:
                if event_time < 0:
                    self.logger.error(f"CDF events check - Negative time value of {event_time} "
                                      f"for event {event_id_ls[idx]}")
                    cdf_events_file_issue_count += 1
            except TypeError:
                self.logger.error(f"CDF events check - Non-numeric time value {event_time} "
                                  f"for event {event_id_ls[idx]}")
                cdf_events_file_issue_count += 1

        # check for any entities that have suffered more loss events than they have components
        for entity in self.entities:
            loss_evnts_mask = (self.CDF_events_df[self.evn_tbl_prim_id_col_lbl] == entity.uid) & \
                              (self.CDF_events_df[self.evn_tbl_event_type_col_lbl] == self.loss_event_lbl)
            loss_evnts_ls = list(self.CDF_events_df.loc[loss_evnts_mask, self.evn_tbl_event_id_col_lbl])
            num_loss_evnts = len(loss_evnts_ls)
            num_comps = entity.init_comps

            if num_comps > 0:
                if num_loss_evnts > num_comps:
                    self.logger.error(f"CDF events check - Entity {entity.uid} suffered {num_loss_evnts} loss events"
                                      f" but only had {num_comps} components")
                    self.logger.debug(f"loss events for entity {entity.uid} - {loss_evnts_ls}")
                    cdf_events_file_issue_count += 1
            else:
                self.logger.debug(f"CDF events check - "
                                  f"Init comps vs. loss events check skipped for entity {entity.uid} (0 initial comps)")

        # check for entities not involved in any events
        for entity in self.entities:
            if entity.uid not in primary_entity_id_ls and entity.uid not in secondary_ent_id_ls:
                self.logger.warning(f"CDF events check - Entity {entity.uid} not involved in any events")
                cdf_events_file_issue_count += 1

        # check for no_key or no_val in event detail fields
        for idx, event_detail_str in enumerate(self.CDF_events_df[self.evn_tbl_event_detail_col_lbl].to_list()):
            if 'no_key' in event_detail_str:
                self.logger.warning(f"event {event_id_ls[idx]} at time {event_time_ls[idx]} "
                                    f"had a detail value with no key")
                cdf_events_file_issue_count += 1
            if 'no_val' in event_detail_str:
                self.logger.warning(f"event {event_id_ls[idx]} at time {event_time_ls[idx]} "
                                    f"had a detail key with no value")
                cdf_events_file_issue_count += 1

        # add code for additional checks

        if cdf_events_file_issue_count == 0:
            self.logger.info("No issues found in CDF events file")
        else:
            self.logger.warning(f"{cdf_events_file_issue_count} potential issues found in CDF events file")

    def generate_cdf_cbt_pwr_df(self) -> None:
        """
        Generate CDF combat power output as a Dataframe.
        """
        self.logger.info("Generating CDF combat power file")
        # reset the dataframe
        self.CDF_combat_power_DF = pd.DataFrame()

        # initialise lists to hold the data - need to get the time, affiliation / force affected and the cbt pwr loss
        loss_time = []
        loss_item = []
        loss_comps = []
        loss_pwr = []

        # identify the affiliations and forces from the entity table and make lists to hold starting comps / pwr
        affiliations = CDFfunc.get_unique_list(self.CDF_entity_table_df[self.ent_tbl_affil_col_lbl].tolist())
        affiliations_start_comps = [0] * len(affiliations)
        affiliations_start_pwr = [0] * len(affiliations)
        forces = CDFfunc.get_unique_list(self.CDF_entity_table_df[self.ent_tbl_force_col_lbl].tolist())
        forces_start_comps = [0] * len(forces)
        forces_start_pwr = [0] * len(forces)

        # go through each entity and add its starting power and comps to the appropriate list
        for entity in self.entities:
            for idx, affiliation in enumerate(affiliations):
                if entity.affiliation == affiliation:
                    affiliations_start_comps[idx] = affiliations_start_comps[idx] + entity.init_comps
                    affiliations_start_pwr[idx] = affiliations_start_pwr[idx] + (
                            entity.init_comps * entity.cbt_per_comp)

            for idx, force in enumerate(forces):
                if entity.force == force:
                    forces_start_comps[idx] = forces_start_comps[idx] + entity.init_comps
                    forces_start_pwr[idx] = forces_start_pwr[idx] + (entity.init_comps * entity.cbt_per_comp)

        # add the starting comps and power to the lists
        for idx, affiliation in enumerate(affiliations):
            loss_time.append(0)
            loss_item.append(affiliation)
            loss_comps.append(affiliations_start_comps[idx])
            loss_pwr.append(affiliations_start_pwr[idx])

        for idx, force in enumerate(forces):
            loss_time.append(0)
            loss_item.append(force)
            loss_comps.append(forces_start_comps[idx])
            loss_pwr.append(forces_start_pwr[idx])

        # go through each entity and get the loss events and the affiliation / force affected
        for entity in self.entities:
            for time in entity.losses_time:
                if entity.init_comps > 0:
                    loss_time.append(time)
                    loss_item.append(entity.affiliation)
                    loss_comps.append(-1)
                    loss_pwr.append(-entity.cbt_per_comp)

                    loss_time.append(time)
                    loss_item.append(entity.force)
                    loss_comps.append(-1)
                    loss_pwr.append(-entity.cbt_per_comp)
                else:
                    self.logger.debug(f"Loss event for entity {entity.uid} at time {time} ignored in generation"
                                      f" of cbt_pwr file as entity had {entity.init_comps} initial components")

        # construct into a dataframe and sort by time
        if not CDFfunc.compare_list_lengths(loss_time, loss_item, loss_comps, loss_pwr):
            self.logger.error("Generate_cdf_cbt_pwr_file function - mismatched list lengths")

        self.CDF_combat_power_DF = pd.DataFrame(data=zip(loss_time, loss_item, loss_comps, loss_pwr),
                                                columns=[self.cbt_tbl_time_col_lbl, self.cbt_tbl_item_col_lbl,
                                                         'comp loss', 'pwr loss'])
        self.CDF_combat_power_DF[self.cbt_tbl_time_col_lbl] = \
            pd.to_numeric(self.CDF_combat_power_DF[self.cbt_tbl_time_col_lbl])
        self.CDF_combat_power_DF.sort_values(by=[self.cbt_tbl_time_col_lbl, self.cbt_tbl_item_col_lbl],
                                             inplace=True, ignore_index=True)
        # add cumulative total columns for comps and pwr
        if len(self.CDF_combat_power_DF[self.cbt_tbl_time_col_lbl].to_list()) > 0:
            self.CDF_combat_power_DF[self.cbt_tbl_comp_col_lbl] = \
                self.CDF_combat_power_DF.groupby([self.cbt_tbl_item_col_lbl])['comp loss'].cumsum()
            self.CDF_combat_power_DF[self.cbt_tbl_pwr_col_lbl] = \
                self.CDF_combat_power_DF.groupby([self.cbt_tbl_item_col_lbl])['pwr loss'].cumsum()
        else:
            self.CDF_combat_power_DF[self.cbt_tbl_comp_col_lbl] = pd.Series(dtype=int)
            self.CDF_combat_power_DF[self.cbt_tbl_pwr_col_lbl] = pd.Series(dtype=int)
        # drop the comp loss and pwr loss columns
        self.CDF_combat_power_DF.drop(labels=['comp loss', 'pwr loss'], axis=1, inplace=True)

        self.attach_loss_events_to_cdf_cbt_pwr_df()

        # try to apply column types to the CDF combat power df
        try:
            self.CDF_combat_power_DF = self.CDF_combat_power_DF.astype(dtype=self.cbt_tbl_col_types_dict)
        except ValueError as error:
            self.logger.error(f"Unable to type cast for one or more columns in CDF combat power df: {str(error)},"
                              f"may cause issues with parquet export")

    def attach_loss_events_to_cdf_cbt_pwr_df(self) -> None:
        """
        Identify the CDF loss events that caused drops in force / affiliation components / combat power and add
        event IDs to the CDF combat power Dataframe.
        """
        self.logger.info("Attaching loss event ids to CDF combat power file")

        # extract a list of times and items from the CDF combat power file
        time_ls = self.CDF_combat_power_DF[self.cbt_tbl_time_col_lbl].to_list()
        item_ls = self.CDF_combat_power_DF[self.cbt_tbl_item_col_lbl].to_list()

        num_items = len(CDFfunc.get_unique_list(item_ls))
        # add 'none' at the start of the list for the initial level for each unique item
        eventid_ls = ['none'] * num_items
        # remove times for initial levels from lists
        del time_ls[:num_items]
        del item_ls[:num_items]

        # cut a dataframe from CDF events with just the losses and extract lists
        losses_df = self.CDF_events_df.loc[self.CDF_events_df[self.evn_tbl_event_type_col_lbl] == self.loss_event_lbl]
        loss_time_ls = losses_df[self.evn_tbl_time_col_lbl].to_list()
        loss_affil_ls = losses_df[self.evn_tbl_prim_affil_col_lbl].to_list()
        loss_force_ls = losses_df[self.evn_tbl_prim_force_col_lbl].to_list()
        loss_eventid_ls = losses_df[self.evn_tbl_event_id_col_lbl].to_list()
        loss_entity_ls = losses_df[self.evn_tbl_prim_id_col_lbl].to_list()

        # go through the lists from the combat power file and try to find the associated loss event from CDF event lists
        for time_idx, time in enumerate(time_ls):
            event_identified = False
            for loss_idx, loss_time in enumerate(loss_time_ls):
                if not event_identified:
                    if self.entities[self.get_entity_index(loss_entity_ls[loss_idx])].init_comps > 0:
                        if time == loss_time and loss_affil_ls[loss_idx] == item_ls[time_idx]:
                            eventid_ls.append(loss_eventid_ls[loss_idx])
                            loss_affil_ls[loss_idx] = None
                            event_identified = True
                        if time == loss_time and loss_force_ls[loss_idx] == item_ls[time_idx]:
                            eventid_ls.append(loss_eventid_ls[loss_idx])
                            loss_force_ls[loss_idx] = None
                            event_identified = True

            # if the event has not been identified after going through all CDF loss events warn and append placeholder
            if not event_identified:
                self.logger.warning(f"No loss event identified for component drop at time {time}")
                eventid_ls.append("event not found")

        # final list length check and add the column to the cbt pwr file
        if CDFfunc.compare_list_lengths(eventid_ls, self.CDF_combat_power_DF[self.cbt_tbl_time_col_lbl].to_list()):
            self.CDF_combat_power_DF[self.cbt_tbl_event_col_lbl] = eventid_ls
        else:
            self.logger.error("Mismatched list lengths - attaching loss event ids to CDF combat power file aborted")
            self.CDF_combat_power_DF[self.cbt_tbl_event_col_lbl] = 'event id attachment aborted'

    def check_cdf_cbt_pwr_df(self) -> None:
        """
        Check CDF combat power Dataframe.

        CDF combat power Dataframe checks:
        Check for negative time values (add Dataset error).
        Check for negative total components (add Dataset error).
        """
        self.logger.info("Checking CDF combat power file")
        cdf_cbt_pwr_file_issue_count = 0

        # check for negative times
        time_ls = self.CDF_combat_power_DF[self.cbt_tbl_time_col_lbl].to_list()
        for time in time_ls:
            if time < 0:
                self.logger.error(f"CDF cbt pwr check - Negative time value {time} in CDF combat power file")
                cdf_cbt_pwr_file_issue_count += 1

        # check for negative component values (combat power is a multiplication of comps so wil also be negative)
        cbt_items_ls = CDFfunc.get_unique_list(self.CDF_combat_power_DF[self.cbt_tbl_item_col_lbl].to_list())

        for cbt_item in cbt_items_ls:
            neg_comps_mask = (self.CDF_combat_power_DF[self.cbt_tbl_item_col_lbl] == cbt_item) & \
                             (self.CDF_combat_power_DF[self.cbt_tbl_comp_col_lbl] < 0)

            if sum(neg_comps_mask) > 0:
                cdf_cbt_pwr_file_issue_count += 1
                neg_comps_times_ls = self.CDF_combat_power_DF.loc[neg_comps_mask,
                                                                  self.cbt_tbl_time_col_lbl].to_list()
                neg_comps_loss_events_ls = self.CDF_combat_power_DF.loc[neg_comps_mask,
                                                                        self.cbt_tbl_event_col_lbl].to_list()

                self.logger.error(f"CDF cbt pwr check - "
                                  f"{cbt_item} had negative components at times {neg_comps_times_ls}")
                self.logger.debug(f"associated loss events were {neg_comps_loss_events_ls}")
                cdf_cbt_pwr_file_issue_count += 1

        # add code for additional CDF combat power file checks

        if cdf_cbt_pwr_file_issue_count == 0:
            self.logger.info("No issues found in CDF combat power file")
        else:
            self.logger.warning(f"{cdf_cbt_pwr_file_issue_count} potential issues found in CDF combat power file")

    def add_case_and_rep_to_cdf_df(self) -> None:
        """
        Add case and replication columns to CDF outputs
        values from configuration
        """
        self.logger.info("adding case and replication columns to CDF outputs")
        self.CDF_entity_table_df.insert(0, self.case_col_lbl, self.case)
        self.CDF_entity_table_df.insert(1, self.rep_col_lbl, self.replication)
        self.CDF_entity_table_df = self.CDF_entity_table_df.astype(dtype={self.case_col_lbl: str,
                                                                          self.rep_col_lbl: str})

        self.CDF_events_df.insert(0, self.case_col_lbl, self.case)
        self.CDF_events_df.insert(1, self.rep_col_lbl, self.replication)
        self.CDF_events_df = self.CDF_events_df.astype(dtype={self.case_col_lbl: str,
                                                              self.rep_col_lbl: str})

        self.CDF_combat_power_DF.insert(0, self.case_col_lbl, self.case)
        self.CDF_combat_power_DF.insert(1, self.rep_col_lbl, self.replication)
        self.CDF_combat_power_DF = self.CDF_combat_power_DF.astype(dtype={self.case_col_lbl: str,
                                                                          self.rep_col_lbl: str})

    def drop_event_type(self, event_type: str) -> None:
        """
        Drop all events of a defined type from the CDF events Dataframe
        Function called during finalise data process to selectively drop any of location, seen, spot or shot events
        depending on configuration.
        This function will only drop events from the CDF events dataframe and CDF events output. It will not remove
        event data from entities and should only be called after generate_cdf_event_file() .

        Args:
            event_type: type of event to drop from CDF events (must match value in event type column)
        """
        self.logger.info(f"Dropping events of type {event_type} from CDF events")

        event_keep_mask = self.CDF_events_df[self.evn_tbl_event_type_col_lbl] != event_type
        events_dropped = len(self.CDF_events_df) - sum(event_keep_mask)
        self.CDF_events_df = self.CDF_events_df.loc[event_keep_mask]

        self.logger.info(f"{events_dropped} events of type {event_type} dropped")

    def update_config(self, setting: str, value):
        """
        Update a dataset config element and record in metadata dict
        note - updating the serial will
        """
        if setting not in vars(self):
            self.logger.error(f"update_config called with unrecognised setting: {setting}")
        elif setting == 'init_date_time_str':
            self.logger.error("init_date_time_str parameter cannot be updated by update_config")
        else:
            self.logger.debug(f"{setting} updated to {value}")
            setattr(self, setting, value)
            self.metadata_dict[setting] = value

    def add_metadata(self, meta_key, meta_value, replace=False) -> None:
        """
        Add item meta_key: meta_value to the metadata dictionary.
        Items already in the metadata dictionary can be updated with the meta_value by setting replace True.
        The metadata dictionary is also used to import and export dataset setting. Consequently, items with
        meta_key that matches a variable of the dataset instance cannot be added or updated using this function
        in this case the update_config function should be used.
        """
        if meta_key in vars(self):
            self.logger.warning(f"Metadata not added as key {meta_key} is a variable of dataset instance "
                                f"(update_config function should be used)")
        elif meta_key in self.metadata_dict.keys() and not replace:
            self.logger.warning(f"Metadata not added as key {meta_key} already exists in metadata dict with value "
                                f"{self.metadata_dict[meta_key]} and add_metadata function called with replace False")
        elif meta_key in self.metadata_dict.keys():
            old_value = self.metadata_dict[meta_key]
            self.metadata_dict[meta_key] = meta_value
            self.logger.debug(f"metadata updated - key: {meta_key}, old value: {old_value}, new value: {meta_value}")
        else:
            self.metadata_dict[meta_key] = meta_value
            self.logger.debug(f"Metadata added - key: {meta_key}, value: {meta_value}")

    def add_summary_metadata(self) -> None:
        """
        Add summary statistics to the metadata file
        """
        total_entities = self.get_num_entities()
        total_events = len(self.CDF_events_df[self.evn_tbl_time_col_lbl].to_list())
        total_items = len(CDFfunc.get_unique_list(self.CDF_combat_power_DF[self.cbt_tbl_item_col_lbl].to_list()))

        if len(self.CDF_events_df[self.evn_tbl_time_col_lbl].to_list()) > 0:
            first_event_str = f"{self.CDF_events_df[self.evn_tbl_time_col_lbl].to_list()[0]} {self.time_unit}"
            last_event_str = f"{self.CDF_events_df[self.evn_tbl_time_col_lbl].to_list()[-1]} {self.time_unit}"
        else:
            first_event_str = 'no events'
            last_event_str = 'no events'

        summary_meta_dict = {'total_entities': total_entities,
                             'total_forces_and_affiliations': total_items,
                             'total_events': total_events,
                             'first_event': first_event_str,
                             'last_event': last_event_str}

        for key in summary_meta_dict.keys():
            self.add_metadata(meta_key=key, meta_value=summary_meta_dict[key], replace=True)

    def export_dataset_dict(self) -> dict:
        """
        Export the current state of the Dataset instance as a dictionary object
        """
        self.logger.info("exporting Dataset instance state")

        dataset_dict = {'ent_dict_ls': []}

        for entity in self.entities:
            dataset_dict['ent_dict_ls'].append(entity.export_entity_dict())

        dataset_dict['metadata_dict'] = self.metadata_dict.copy()

        return dataset_dict

    def save_dataset(self, save_location=None, save_file=None):
        """
        Save dataset state dictionary to a yaml file
        """
        if not save_location:
            save_location = self.output_location
        if not save_file:
            save_file = self.save_file_name

        if not path.isdir(save_location):
            makedirs(save_location)

        export_path = path.join(save_location, save_file)
        export_dict = self.export_dataset_dict()
        self.logger.info(f"saving dataset state to file: {export_path}")
        with open(export_path, "w") as save_file:
            yaml.safe_dump(export_dict, save_file)

    def import_dataset_dict(self, dataset_dict: dict) -> None:
        """
        Import a Dataset state dictionary object and use it to set the state of the Dataset instance
        """
        self.logger.info("importing Dataset instance state")
        self.logger.debug(f"imported dataset_dict: \n{dataset_dict}")

        # reset the cdf dataframes
        self.CDF_entity_table_df = pd.DataFrame()
        self.CDF_events_df = pd.DataFrame()
        self.CDF_combat_power_DF = pd.DataFrame()

        # empty the entities array
        self.entities = []
        # create entities and load data from the dataset_dict
        for ent_dict in dataset_dict['ent_dict_ls']:
            self.add_entity(uid=ent_dict['uid'])
            ent_idx = self.get_entity_index(ent_dict['uid'])
            self.entities[ent_idx].import_entity_dict(ent_dict)

        # reset the metadata_dict preserving the init_date_time_str of this instance
        self.metadata_dict = dict(init_date_time_str=self.init_date_time_str)
        # copy the metadata_dict from the dataset_dict
        import_metadata_dict = dataset_dict['metadata_dict'].copy()
        # go through metadata_dict and update settings or add as metadata item (ignore init_date-time_str)
        for key in dataset_dict['metadata_dict']:
            if key != 'init_date_time_str':
                if key in vars(self):
                    self.update_config(key, import_metadata_dict[key])
                else:
                    self.add_metadata(key, import_metadata_dict[key])

    def load_dataset(self, load_location=None, load_file=None):
        """
        Load dataset state from a yaml file
        """
        self.logger.info("attempting to load dataset state from file")
        if not load_location:
            load_location = self.output_location
        if not load_file:
            load_file = self.save_file_name

        if not path.isdir(load_location):
            self.logger.error(f"specified location {load_location} not found")
        else:
            file_ls = listdir(load_location)
            if load_file not in file_ls:
                self.logger.error(f"specified file {load_file} not found in {load_location}")
            else:
                load_path = path.join(load_location, load_file)
                self.logger.info(f"loading dataset state from file: {load_path}")
                with open(load_path, "r") as load_file:
                    load_dict = yaml.safe_load(load_file)

                self.import_dataset_dict(dataset_dict=load_dict)
