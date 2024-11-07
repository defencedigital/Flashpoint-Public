# Model processor description

Model processor scripts contain a processor function that takes a dict object (the configuration for that 
processing run) as an argument, [generates CDF outputs](CDFOutputs.md) and returns a string value (the outcome of 
the processing run). Each model processor script has this function at the top (typically a function called 
ModelName_processor) followed by standard code that loads the configuration file and passes each line of the 
configuration file to the processor function, recording each configuration and outcome in the batch log. 

The processor function generates an instance of the Dataset class (Dataset.py) which in turn generates an array of 
instances of the Entity (Entity.py) class. The Dataset class contains all the functions and methods needed to interact 
with the Entity instances and to generate the output CDF files. Further detail on the structure of model processor 
scripts can be found in the sections below.

The CDFfunc class (CDF_Func.py) has static methods that provide helper / utility functions. These assist with 
extraction and processing of data from model output files. This class also contains a static method for setting up 
loggers for model processing scripts and the Dataset class. Details of the methods available can be 
found here: [CDF Functions](CDF_Functions.md).

## Test suite

A full pytest test suite for the Dataset class and associated CDF functions is included in the test folder. 
Whilst this is primarily intended to support testing and development of the common elements, it can also be 
used to understand how individual building blocks function and to produce some simple demo outputs by running the 
Dataset_end_to_end_test.py tests. *Note that unless pyarrow is installed all the Dataset_end_to_end tests and
Dataset_export_test tests that produce parquet format outputs will fail*.

# Processor script structure

Processing scripts (ModelName_Processor.py) consist of a processor function and standard code (batch code) that loads 
a configuration file and passes each line of the configuration to the processing function. This is supported by 
various imports. These elements are described in the following sections.

## Imports

The supporting CDF elements are imported using the code snippet below:

    from Datset import Dataset  
    from CDF_Func import CDFfunc

The processing functions also utilise pandas and elements of os. Imports to support the code described here are below.
However, additional imports may be required to support the output formats of particular models etc.

    import pandas as pd
    from os import listdir and path

## Batch code

_(note that this code is after the processor function code in the script file in order to call it)_

The batch code is common to all processing scripts. The only part that will change is the file name that the 
[configuration_file](ConfigFields.md) variable is set to (typically ModelName_config.csv). The first part of this 
code sets up the batch logger, sets this variable and adds some initial info events to the batch log:

    batch_logger = CDFfunc.setup_logger(f"Batch_log")
    batch_logger.info(f"Batch run started")
    configuration_file = "ModelName_config.csv"
    batch_logger.info(f"Loading configuration file - {configuration_file}")

The next part of the code is contained within a try except block. The code will attempt to use pd.read_csv to read 
the [config file](ConfigFields.md) and then turn it into a dict object using .to_dict by record (see code snippet 
below). If the file cannot be found then the exception is caught and an error event generated in the batch log.

    configuration_dict = pd.read_csv(configuration_file, skiprows=1).to_dict(orient='records')

The remainder of the code is contained within an else block and thus will only be executed if the try except is passed.

The initial lines get the number of configurations from the dict object, add an info event to the logger and set the
run_count variable to 0.

The following for loop code goes through each record of the config_dict object and, if that line is set to process,
sends it to the processor function and records the outcome as the return value from the processor function. If the
line is not set to process it is skipped and the outcome set appropriately. Finally, the outcome for that record is 
added to the batch log.

    for configuration in configuration_dict:
        run_count += 1
        serial = configuration['serial']
        case = configuration['case']
        rep = configuration['replication']
        batch_logger.info(f"Configuration {run_count} of {num_configs}, Serial {serial}")
        result_str = f"Serial {serial} - case {case}, replication {rep} - "

        if CDFfunc.parse_config_bool(configuration['process']):
            result_str = result_str + flashpoint_processor(process_config=configuration)
        else:
            result_str = result_str + "not set to process"

        batch_logger.info(result_str)

The final line of the batch code just adds a final info event to the batch log to indicate the batch run is complete.

# Processor function

The processor function follows a standard sequence of 6 phases. These are described in the following sections. The
majority of phases will only have minor (if any) variations between different models however phase 2, which reads the
model output files and generates source dataframes, varies significantly due to variation in output files, formats
and structures between models.

_The processor function is defined ahead of the batch code (so the batch code can call it)_ with a dict objet 
as an argument and a string value as a return type: 

    def ModeName_processor(process_config: dict) -> str:

## Phase 0
**Extract parameters from the dict object, check parameters are valid and set up script logger**

The first two lines of this phase set the script name and version variables so that they can be surfaced in logging.

The next blocks of code unpack values from the process_config dict object that are needed either for checking that the
configuration is valid or to set model processor specific parameters and options. 
Note that the parse_config_bool function from CDFfunc is used to convert any 1 / 0 values from the config files into 
boolean values to set options and the parse_config_location function from CDFfunc is used to convert any file paths
to ensure consistency.

    input_location = CDFfunc.parse_config_location(str(process_config['input_location'])

The values in the input files group are added to a list enabling them to be checked for at the input location
(see below): 

    input_file_ls = [unit_data_file, .... ]

The setting of the zero_hour parameter is contained within a try except block and set to default if the value read is
nan. This accounts for non-numerical or blank values being supplied in the configuration. Note that not all model
processors will utilise the zero_hour parameter.

    try:
        zero_hour = float(process_config['zero_hour'])
    except ValueError:
        zero_hour = 0.0

    if pd.isna(zero_hour):
        zero_hour = 0.0

Once the parameters have been unpacked these are checked to make sure this is a valid
configuration. The following code checks that the specified input location exists and all the input files are present
at that location. Note that this may vary depending on the structure of the model output files. 
Any issues found are added to an issues list.

    if not path.isdir(input_location):
        issues_list.append(f"input location /{input_location} not found")
    else:
        file_ls = listdir(input_location)
        for input_file in input_file_ls:
            if input_file not in file_ls:
                issues_list.append(f"{input_file} missing")

if the length of the issues list is greater than 0 then it is appended to a failure message and returned, ending the 
processing. The batch code (see above) then writes this message into the batch log.

    if len(issues_list) > 0:
        return_val = f"failed - no files generated - {issues_list}"
        return return_val

If there are no issues with the configuration then the remaining code sets up the script log and records in it the 
configuration along with the versions of the model processing script, [Dataset](Dataset_version_log.md) and 
[CDF functions](CDF_Func_version_log.md).

    logger = CDFfunc.setup_logger(f"{script_name}_log_S{run_serial}", output_folder=output_location)
    logger.info(f"Script logger started, saving log file to /{output_location}")
    logger.info(f"{script_name} version {script_version}")
    logger.info(f"Using Dataset version {DataSet.version} and CDF functions version {CDFfunc.version}")

    logger.info(f"Input location - /{input_location}")
    ....

## Phase 1  
**Set up the dataset instance**

This phase sets up an instance of the Dataset class and passes the configuration dict object to it. This is then used
to set all the parameters and options within Dataset (i.e. those that are common to all processors).
A logger info event is also added.
    
    logger.info("Setting up dataset instance")
    command_data = DataSet(dataset_config=process_config)

Once the dataset instance has been set up metadata items can be added by using the add_metadata(meta_key, meta_val) 
function. The passed key and value arguments will be added to the instances metadata and exported in the CDF metadata
file. This can be used for adding notes, system messages and additional data specific to that model.

## Phase 2
**Read input files and generate source data frames**

The general approach to the processing functions is to generate source dataframes and then use common techniques with 
the CDF functions in subsequent phases to slice data out of them to read into the entity instances. Methods with 
the Dataset class then operate with the entity instances to produce the CDF outputs.

Processing functions will require two types of source data frames - entity data (unit_data_df), which is 
processed in phase 3, and event data dataframes for each event type which are processed in phase 4. 
This phase reads processor function input files (model output files) and generates the source dataframes from them.

This phase will have the most variability from model to model as they will all capture events in 
different ways and have different output formats and file structures. An in depth explanation of all the techniques 
used is beyond the scope of this document however some general principles are described below followed by some 
practical examples.

* Separate dataframes should be produced for the unit data, overall CDF event types and any special
case events that need to be added (most commonly loss events with no reciprocal kill events)
* Column names for event data frames should be the same as the CDF event parameters to make subsequent 
phases easier to follow in the code
* Joining of extra data (i.e. to provide detail for events, get x / y coordinates from locations etc.)
 should be done in this phase
* Multiple detail columns can be added at this stage to encode as the CDF event detail key-value pairs
* Mapping variables (see existing processing functions for examples) should be used where possible so that the
mapping of model data to CDF event data can be seen in the code itself and easily captured in the script logging

### Phase 2 practical examples

#### CommandPE

Generation of the unit data dataframe (unit_data_df) is straight forward for this model. It is generated from the unit 
position output file (UnitPositions.csv) via a column mapping dictionary (col_maps). Essentially the whole unit 
position file is read into a dataframe. This is then reduced to the selected columns using the keys of the dictionary 
and the columns renamed using the values of the dictionary as in the code snippet below.
    
    col_maps = {'unit pos file col name': 'unit_data_df col name', ... }
    unit_data_df = pd.read_csv(source_file, skiprows=[1], usecols=col_maps.keys())
    unit_data_df = unit_data_df[list(col_maps.keys())]
    unit_data_df.columns = col_maps.values()

The unit position file is used by default as all the units involved in the scenario will appear within it. 
Some studies may not generate the unit position file, either due to run time or output size constraints, and in these 
cases the same approach can still be used by selecting an output file that is likely to include all units of interest
for the CDF outputs and updating the source_file variable and the keys of the col_maps dictionary as appropriate.

The CommandPE model produces a set of output files and a subset of these are read in as dataframes using dictionaries 
aligned to the CDF event types. Part of the dictionary for move events is shown below. The dictionary contains col_maps 
that have the same format as those used to generate the unit data dataframe. The dictionary also defines the source file 
(i.e. the commandPE output file) to read from and the df_name which is used 
for logging purposes. 

    move_df_dict = {'df_name': 'move_df',
                    'source_file': input_location + unit_pos_file,
                    'col_maps': {'col name in source file': 'col name in event data frame', 
                     ..}}

The df dictionaries are added to a list (df_dict_ls) which is processed to add dataframes to an event dataframe list 
(event_data_frames). Finally individual dataframes are popped from this list. This can be seen in the code snippet
below. After the dataframe is appended to the event_data_frames list the next line of code reorders columns
to match the order of the col_map keys so that they can then be renamed easily using the col_map values in the 
following line. Since CommandPE outputs times as a text string the raw time column (time_str) is then converted into 
a time value column in the dataframe using the get_time_val function from CDFfunc.

    df_dict_ls = [move_df_dict, ...]

    for df_dict in df_dict_ls:
        df_name = df_dict['df_name']
        source_file = df_dict['source_file']
        col_maps = df_dict['col_maps']
        event_data_frames.append(pd.read_csv(source_file, skiprows=[1], usecols=col_maps.keys()))
        event_data_frames[-1] = event_data_frames[-1][list(col_maps.keys())]
        event_data_frames[-1].columns = col_maps.values()
        event_data_frames[-1]['time'] = [CDFfunc.get_time_val(unit='secs', input_time_str=t, zero_hr=zero_hour)
                                         for t in event_data_frames[-1]['time_str'].to_list()]

The individual dataframes produced can then be popped from event_data_frames list and, after some further processing 
of individual dataframes, used in phase 4 using the event_map approach.

## Phase 3: 
**Generate the entities within the dataset instance and set their properties using source dataframes**

This phase needs to include the code that will read entity data from an existing CDF entity table if the 
entity_data_from_table variable is set true. This is implemented a simple if / else block that calls the 
entity_data_from_table function if required:

    if entity_data_from_table:
        file_path = path.join(input_location, entity_table_file)
        dataset_instance.entity_data_from_table(pd.read_csv(file_path))
    else:
        ...

The approach to generating entity data from model output data is to set up a mapping of column names in the unit data 
dataframe to the entity parameters. The code snippet below shows an example. Setting up and working through mappings 
in this way reduces code repetition and enables the source for each CDF item to easily be captured in logging. 

    unit_data_map = {'uid': 'df column name to get the unique id (uid) from',
                     'entity parameter': 'df column name to extract data from',
                     'entiy parameter': 'df column name to extract data from..}

The list of uids can then be extracted from the identified column using the get_unique_list function from CDFfunc.

    uid_list = CDFfunc.get_unique_list(unit_data_df[unit_data_map['uid']].tolist())

The mappings can then be worked through for each uid to generate a data list for each parameter using the get_col_slice 
function from CDFfunc. The code snippet below shows an example for the unit_name parameter. Extracting a list enables 
the uniqueness of entries to be checked, with warnings generated where there are multiple parameter values.
    
    name_ls = CDFfunc.get_col_slice(unit_data_df, uid, unit_data_map['uid'], unit_data_map['unit_name'])

Finally, the add_entity and set_entity_data functions within the dataset are used to add an entity instance to the 
dataset instance with that uid and set its parameters using the first element of each data list: 
    
    dataset_instance.add_entity(uid)
    dataset_instance.set_entity_data(uid, unit_name=name_ls[0], unit_type=type_ls[0], 
                               commander=commander_ls[0], affiliation=affil_ls[0], 
                               force=force_ls[0], init_comps=num_sub_units, cbt_per_comp=1)

## Phase 4
**Read the event data into the entities from source dataframes**

This phase populates event data into the array of entity instances within the dataset instance. The approach is to set 
up an event map object (dict) for each CDF event type and use those to read the data for that event type from the 
source dataframes into the individual entities within the dataset instance. An example event map from the combat 
mission processor function is shown in the code snippet below.

    location_event_map = {'df': location_df, # the source dataframe
                          'df_name': 'location_df', # the dataframe name for logging
                          'mask_col': 'id', # the column to use to identify the entity to read data into 
                          'data_maps': [['time', 'location_time'],
                                        ['x', 'location_x'],
                                        ['y', 'location_y']],
                          'detail_keys': ['terrain', 'elevation', 'mounted in vehicle'],
                          'detail_cols': ['terrain_detail', 'elevation_detail', 'riding_detail'],
                          'detail_list': 'location_detail'}

The data_maps handle the regular CDF columns and are a list of two value lists of the form: 

    [['source df column', 'target list for append to list function]..] 

Since the detail for CDF events are encoded as key value pairs these are handled slightly differently: The detail_keys 
are input as a list with the detail_cols list defining the corresponding source df columns to pull the values from. 
The detail_list value contains the name of the target list in the append_to_list function for the encoded detail list.

These maps are then combined into a list (event_map_ls) and processed using the code snippet below. The code breaks out 
the elements of the event_map and then iterates through the entity instances within the Dataset instance, slicing the 
source df by the entities uid and sending data to the appropriate lists within the entity instance via the 
append_to_list function. The detail for the event is processed slightly differently as the individual value lists need 
to be pulled out first and then encoded with the detail_keys using the encode_event_detail_list function from CDFfunc 
before being read into the entity instance via the append_to_list function.  

    for event_map in event_map_ls:
        event_df = event_map['df']
        df_name = event_map['df_name']
        mask_col = event_map['mask_col']
        data_maps = event_map['data_maps']
        detail_keys = event_map['detail_keys']
        detail_cols = event_map['detail_cols']
        detail_list = event_map['detail_list']
    
        for entity in combat_data.entities:
            uid = entity.uid
   
            for mapping in event_map['data_maps']:
                data_col = mapping[0]
                tgt_list = mapping[1]
    
                data_ls = CDFfunc.get_col_slice(df=event_df, uid=uid, mask_col=mask_col, tgt_col=data_col)
                combat_data.append_to_list(uid=uid, target_list=tgt_list, data_list=data_ls)
    
            detail_val_ls = []
            for detail_col in detail_cols:
                detail_val_ls.append(CDFfunc.get_col_slice(df=event_df, uid=uid, 
                mask_col=mask_col, tgt_col=detail_col))
    
            detail_data_encoded = CDFfunc.encode_event_detail_list(*detail_val_ls, detail_keys=detail_keys)
            combat_data.append_to_list(uid=uid, target_list=detail_list, data_list=detail_data_encoded)

Additional events can also be added to entities as needed using the add single event functions (add_location, add_shot, 
add_seen, add_spot etc.) from the DataSet class. This can be used to add events that require more complex filtering 
of the source dataframes (for example, entities losses due to transport vehicles being destroyed) or from dataframes
that capture special cases of events. However, the event_map approach described above is used as much as possible to 
maximise consistency of phases and structure between the processor functions.

## Phase 5
**Finalise the data in the dataset instance and export the files**

The functions to do this are contained within the Dataset class and are simply called and a log Info event added:
    
    logger.info(f"Finalising data and saving output files (see dataset instance log for details")
    dataset_instance.finalise_data()
    dataset_instance.export_data()

Finally, the return value (return_val) is set to "complete" and returned. The batch code will then write this into the 
batch log as the outcome for the configuration (line of the config file).
    
    return_val = "complete"
    return return_val
