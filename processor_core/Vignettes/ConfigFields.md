# Configuration file set up

The sections below provide a guide to the fields in the model processor configuration file (ModelName_config.csv) 
that are common across model processors. The purpose of the configuration file is to eliminate the need for users 
to set variables directly in the processing scripts and to allow batches of model outputs to be processed efficiently. 

Default values are indicated where a field has them. These are used where the value is not picked up from the 
configuration for any reason and a warning is generated in the log. These values will likely allow processing to 
proceed but may produce unexpected results. Any default value warnings in the Dataset log should be checked
carefully.

Each line in the configuration file sets the parameters to process the results for an individual game or run of the 
model. All parameters are set per line. Whilst it is recognised that some parameters will be the same across the batch 
in the majority of cases, the configuration file has been set up this way to provide maximum flexibility.

The fields are divided into logical groupings. The first two rows of the configuration file contain the group and 
field names and thus the **configurations to process should be entered from the third row of the configuration file 
onwards**. Unless otherwise indicated every row should have all fields populated.

A batch log file (batch_log_Date_Time.log) will be generated in the working directory when the processor script is run. 
This will contain an entry for each line in the configuration file that indicates if that line was processed 
successfully or if it failed to process. Where a line fails to process the reasons will be indicated so that the line 
can be updated accordingly and the configuration rerun. 
Note that subsets of lines can be run using the 'process' field (see below).

# Batch settings

## serial - default: 0
Identifier for the game or run of the model within the batch. This is generally a number from 1 to the number of games 
or runs within the batch but can be any unique set of values. This will be surfaced in the batch log along with the
outcome and incorporated into the output file names.

## case - default: 'case_name'
The name of the case for this game or run. This will be incorporated into the output file names and populated into the 
'case' column of the CDF output files. This is intended to represent variations to the settings, simulation scenario 
or game.

## replication - default: 'rep_num'
The replication of this game or run. This will be incorporated into the output file names and populated into the 'rep'
column of the CDF output files. This is intended to represent multiple runs of the same scenario with all settings the 
same (i.e. for a stochastic simulation).

## process
Set whether to process this line (1) or not (0). This column has been included to allow selected subsets of 
configurations to be processed. This may be useful, for example, to update configuration for any lines that failed and 
reprocess, to process results for additional or rerun games or runs or to process results in batches as they become
available.

# io settings
Paths to locations can be specified as absolute or relative paths. Forward and backslash characters can be used 
interchangeably and trailing slash characters will be ignored.

**Any path beginning with a slash character** (i.e. /Input/Game1) or drive letter notation **will be taken as an 
absolute path**. Relative paths should begin with the name of a folder in the working directory (i.e. Input/Game1). 
In addition ./ , ../ etc can be used to specify folders within the working directory, a level above the working
directory etc.

## input_location - default: 'Input'
The location where the files specified in the input files section are located.
If this location does not exist the line will fail to process.

## output_location - default: 'Output'
The location where the CDF output files and log files will be generated. If the location does not exist it will be 
created by the Dataset instance. This could be set the same for all lines (i.e. place all output files in the same 
location) or could be used to sort output files by case, set of cases or any other parameter.  
Combined with the split_files_by_type option (see below) practically any output structure required can be achieved.

# data settings - default: 'not defined'
These fields enable metadata for the model and the game or run being processed to be provided. 
The field titles are self-explanatory and should provide additional information for the visualisation stage. i.e.
time and distance units etc. This data is recorded in the CDF metadata file and also surfaced in the dataset log.

# general options
The fields in this section set options that are common to all processing scripts.

## force_unique_unit_names - default: 1 (True)
Set whether to force entity names in the CDF output to be unique (1) or not (0). If set true each repeat of an entity
name will have an incrementing number appended. i.e. unit_name, unit_name-2, unit_name-3 etc. 

## zero_hour
Set a start time value (decimal hours) that will be subtracted from time values in the model outputs during processing.
This is used with models that output absolute time to get an elapsed time from a starting point. This setting has no
effect for models that output elapsed time values. If not required this can be set to 0 or safely left blank.

## entity_data_from_table - default: 0 (False)
Set whether to read entity data from an existing CDF entity table file (1) or not (0). This enables detail that the 
script cannot pick up from the model output files to be defined manually as needed. The description of the entity 
table fields in [CDF structure](CDFStructure.md) indicates which fields are read from the source CDF entity table file.

## entity_table_file - default: 'entity_data_table.csv'
If the config line is set to read entity data from an existing entity table file then this field must specify the name
of the entity table file (including file extension) to read from. This file must be at the input location specified 
for the line and must be in .csv format. Otherwise, this field can be safely left blank.

## output_csv - default 1 (True)
Generate output files in .csv format (1) or not (0)

## output_parquet - default 0 (False)
Generate output files in .parquet format (1) or not (0) **(requires pyarrow package to be installed)**

_Note_ - setting both of these to 0 (False) will result in a configuration that generates no output other than
log files and the CDF metadata file. In this case a warning will be generated in the Dataset log but processing
will otherwise proceed normally.

## split_files_by_type - default: 0 (False)
Set whether to split the CDF output files into subfolders by type (1) or not (0) at the output location. If enabled
this puts the generated CDF entity, events, combat power, metadata and Dataset log files into separate subfolders at 
the specified output location. If the same output_location is specified for multiple lines then the files for all of 
those lines will be sorted into the same set of  subfolders. This option can be combined with definition of 
output_location to achieve a wide range of output file structures. Note that the location of processor script log files 
is not affected by this option.

## drop_event options
Set whether to drop events of the specified type from the CDF events file (1) or not (0). These options can be used
to reduce the CDF events file size by removing events that are not relevant to the analysis. These options only affect 
the content of CDF events file, other outputs (i.e. entity table event counts, metadata total_events etc.) will reflect 
the event numbers including any dropped from the CDF events file. 

### drop_location_events - default: 0 (False)
Set whether to drop location update events from the CDF events file (1) or not (0). Note that locations will still be 
attached to other event types.

### drop_seen_events - default: 0 (False)
As drop_location_events but for seen by secondary entity events. 

### drop_spot_events - default: 0 (False)
As drop_location_events but for spotted secondary entity events.

### drop_shot_events - default: 0 (False)
As drop_location_events but for shot events.

# input files
The exact input structure and file names required will vary from model to model, see the model processor readme for 
details of the set-up. If any of the files specified are not present at the input location then the line will fail.

# model specific parameters and options
Fields for selecting any model specific options and defining any model specific parameters (see model processor readme
for details). Note that this section will not be included for model processors that have no model specific parameters 
or options.
