# Common Data Format Processor outputs

## CDF Files

Each line of the [configuration file](ConfigFields.md) will generate the following CDF files:
* CDF entity table - table of entities identified by the CDF
* CDF events file - events that each entity was involved in 
* CDF combat power file - components and combat power over time by affiliation and by force
* CDF metadata file - parameters and settings for the processing run and any other data items passed from the 
processor function

The metadata file is always in yaml format. The remaining CDF files can be generated in csv and/or parquet format
as required by setting the output_csv and output_parquet [configuration options](ConfigFields.md) but note that 
pyarrow must be installed to generate parquet format outputs. Parquet files are strictly typed and the data type 
for each output field can be found in the CDF Data Fields section below.

The output fields of these files are described in the following sections. In addition, all CDF output files include 
'case' and 'rep' as the first two columns (both string type) with values corresponding to those in the [configuration 
file](ConfigFields.md) for that line. These fields facilitate joining data from multiple CDF files.
* 'rep' is short for replication and is intended to represent multiple runs of a scenario with all settings the same 
(i.e. for a stochastic simulation)
* Any variations to the simulation scenario or game would then be a different 'case'

A config file could be set up for all the cases and replications of a particular study and then run selectively 
using the 'process' column (see [configuration options](ConfigFields.md)) as the model output files are produced. 

## CDF Data fields

### CDF Entity table file

CDF_EntityTable_case_rep_serial_date_time.csv/.parquet

Fields marked with * are set from the input entity table when using the entity_data_from_table option with any caveats 
noted in brackets (see entity_data_from_table option [here](ConfigFields.md) for details). 
All other fields are either deduced (i.e. commander_name is always the name of the entity with the id in the 
commander_id column) or calculated (i.e. init_cbt_pwr is always init_comps multiplied by cbt_per_comp) from the entity 
data or come from the CDF event data (i.e. total events and breakdown) or dataset configuration (i.e. case and rep).

* id - unique id (uid) of the entity (used to identify events involving the entity) * (string)
* name - name of the entity *_(will be updated if duplicates and force_unique_unit_names option is true)_ (string)
* type - type of entity * (string)
* commander_id - uid of the entity which is the entity's commander in the force structure * (string)
* commander_name - name of the entity which is the entity's commander in the force structure (string)
* level - level of the entity in the force structure *_(will be deduced if left blank)_ (integer)
* affiliation - affiliation to which the entity belongs * (string)
* force - force to which the entity's affiliation belongs *_(will be updated if same as any affiliation)_ (string)
* init_comps - initial number of components that form the entity * (integer)
* cbt_per_comp - combat power of each component belonging to the entity _(unit in CDF metadata file)_ * (float)
* init_cbt_pwr - initial combat power of the entity _(unit in CDF metadata file)_ (float)
* system_entity - whether the entity exists within the model (False) or is purely used to track system events via
the 'status update' CDF event type *
* start_entity - whether the entity was present at the start of the game / simulation * (boolean)
* time_added - simulation / game time that the entity first appeared in the data _(unit in CDF metadata file)_ (float)
* total_events - total number of CDF events in which the entity was the primary entity (integer)
* further _events columns break this total down by CDF event type (integer)

### CDF Combat power file

CDF_Cbt_pwr_case_rep_serial_date_time.csv/.parquet

* time - model time _(unit in CDF metadata file)_ (float)
* item - affiliation or force (string)
* components - total components of the Item at Time (integer)
* combat_power - total combat power of the Item at Time _(unit in CDF metadata file)_ (float)
* event_id - event id of the loss event which caused the drop in combat power of the item (string)

### CDF Events file
 
CDF_Events_case_rep_serial_date_time.csv/.parquet

* time - model time when the event occurred _(unit in CDF metadata file)_ (float)
* primary_entity_id - uid of the primary entity involved in the event (string)
* primary_entity_(name / type / commander / level / affiliation / force) - as entity table 
(all string except level which is float)
* primary_x - x location of the primary entity when the event occurred _(unit in CDF metadata file)_ (float)  
* primary_y - y location of the primary entity when the event occurred _(unit in CDF metadata file)_ (float)
* event_id - unique id of the event (event type followed by a number i.e. loc-0, kill-3, seen-7 etc). Note: these 
follow the order in which events were added to the Dataset instance rather than event time order. (string)
* event_type - type of event (see below) (string)
* event_detail - key value pairs in json string format - {"key1":"value1", "key2":"value2" .. } 
giving additional detail for the event. Detail keys will vary by model and by CDF event type. (string)
* secondary_entity_id - uid of the secondary entity involved in the event (string)
* secondary_entity_(name / type / commander / level / affiliation / force) - as entity table
(all string except level which is float)

### CDF metadata file

CDF_Metadata_case_rep_serial_date_time.yaml

This file records the parameters and settings for Dataset instance used for the processing run. These are listed
below and can be used to add context and units to visualisations. The file will also contain any additional model 
specific metadata passed from the model processor.

**Parameters:** serial, case, replication, input_location, output_location, model_name, distance_unit, time_unit,
cbt_pwr_unit, data_name, data_details and data_date.

**Settings:** force_unique_unit_names, entity_data_from_table, split_files_by_type, drop_location_events, 
drop_seen_events, drop_shot_events and drop_spot_events

**Summary stats:** total_events, total_entities, total_forces_and_affiliations

## CDF Event types

All CDF events involve a primary entity and some may involve a secondary entity. These events are
described by data fields as defined in the previous section. **Note** that not all event types will populate 
all the data fields, this will depend on the event type and CDF implementation for the model. Options within 
the configuration file can be used to selectively drop events by type, i.e. to reduce CDF events file size.

The CDF event types are:
* **status update** - Status update for primary entity **Note** this event type can be used in combination
with dummy entities (game state, sim state, system etc.) to record changes to the overall state or system events
* **location Update** - Primary entity location
* **spotted secondary** - Primary entity spotted (is seeing) the Secondary entity
* **seen by secondary** - Primary entity was seen (spotted) by the Secondary entity
* **stopped seeing secondary** - Primary entity is no longer seeing the Secondary entity
* **shot** - Shot fired by the Primary entity
* **kill** - Primary entity killed (caused a component loss to) the Secondary entity
* **loss** - Primary entity suffered a (component) loss caused by the Secondary entity

# Logs and log files

The model processor will generate a batch log with basic details about the configuration 
file. overall progress, the details of each configuration line and the outcome for that line i.e. complete, 
not set to process or failed to process. Where a line fails to process the list of issues will be recorded in the 
batch log. Since the batch log events are all either Info or Error events (see below) they are surfaced to the 
console during the processing run and are also saved to file as Batch_log_date_time.log in the working directory.

The model processor will generate two logs for each line of the configuration file at the configured output location.
One for the dataset instance utilised and one for the processor script itself. These reference the serial number of 
the corresponding line of the configuration file in the log file name as well as the serial, case and replication in 
the logs themselves.

The script log (ModelName_log_Serial_date_time.log) will show what data was read in from the model output files and 
how that mapped to the CDF elements. 

The dataset instance log (Dataset_log_Serial_date-time.log) contains details of the interactions between the 
processing script and the dataset instance. 

Log event categorisation is common across all logs and is as follows:
* **Debug** events provide a complete record of the data read in by the processing script and how 
that was passed into the dataset instance. These will also give details of any default values set, 
entity levels discovered, entity parameters updated etc. 
* **Info** events provide updates on progress, overall stats (such as number of entities found, number of events by 
type etc.) and confirm any options set by the configuration line. 
* **Warning** events alert the user to situations where CDF outputs should be treated with some 
caution. For example parameters not set, CDF events with unknown entities, repeated entity names, 
unknown event types etc. In these cases the CDF outputs can be used however the implications of
the warning events need to be understood.
* **Error** events indicate that something has gone wrong with either the processing itself or its interaction with 
the Dataset class. In these cases CDF outputs should not be used until the error events have been investigated 
and resolved.

All event categories will be captured in the respective saved log files. Info, Warning and Error events from all logs 
will also be output to the console during the processing run.
