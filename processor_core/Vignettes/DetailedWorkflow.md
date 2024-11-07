# Common Data Format workflow

The overall workflow for all model processors is the same and is described in detail further on, but broadly:
- Download model processor repository
- Set-up configuration file
- Run model processor script
- Manually verify [CDF outputs](CDFOutputs.md) against raw model output

In all cases the readme file **must** be reviewed to understand model specific parameters and options and any workflow 
variations.

1. ***Either:*** download the repository and unzip the folder to a suitable location ***or if comfortable with basic
git operations*** clone the repository to a suitable location. Cloning has the advantage of being able to update to the 
latest version of the model processor easily via git pull. _(In both cases the folder containing the 
ModelName_processor.py file etc. is referred to as the **working directory** throughout the rest of this guide)_
2. Update the configuration file found in the working directory (ModelName_config.csv) and save it. A guide to the 
common fields in the configuration file can be found [here](ConfigFields.md). Care should be taken with the 
specification of input and output locations, paying particular attention to relative vs. absolute paths. 
*Note that fields in the 'input files' and 'model specific parameters and options' groups will vary from model to 
model and will be described in the repository readme file*.
3. Run the ModelName_Processor.py file in the working directory. Dependencies are listed in the repository readme
and are all that is required for the majority of model processors.
4. [CDF outputs](CDFOutputs.md) will be generated for each line in the config file in the output location defined 
for that line. A batch log file (Batch_log_date_time.log) will be generated in the working directory.
5. Check the batch log file (Batch_log_date_time.log in the working directory) for any configuration file lines that 
failed to process.
6. Update any failed lines in the configuration file as needed and utilise the 'process' field to set just those 
lines to run. Save the updated configuration file.
7. Repeat steps 3 to 6 as required.
8. Manually verify that the [Common Data Format outputs](CDFOutputs.md) are an accurate representation of the raw
model outputs.
