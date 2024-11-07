import logging
import pandas as pd
from datetime import datetime
from os import path, makedirs


class CDFfunc:

    version: str = "1.1.4"

    @staticmethod
    def get_unique_list(*input_lists: list) -> list:
        """ Get unique items from multiple lists.

        Args:
            *input_lists: Input list objects.

        Returns:
            List object. List of unique items from all the input lists.
        """
        # pass in lists and return a list of the unique items across all of them
        output_list = []
        for input_list in input_lists:
            for item in input_list:
                if item not in output_list:
                    output_list.append(item)

        return output_list

    @staticmethod
    def compare_list_lengths(*input_lists: list) -> bool:
        """ Compare lengths of lists.

        Args:
            *input_lists: Input list objects.

        Returns:
            Bool. True if lists are all the same length, Otherwise False.
        """
        test_len = len(input_lists[0])
        list_lengths_consistent = True
        for input_list in input_lists:
            if len(input_list) != test_len:
                list_lengths_consistent = False

        return list_lengths_consistent

    @staticmethod
    def remove_nan(input_list: list) -> list:
        """ Remove null values from a list

        Args:
            input_list: Input list object.

        Returns:
             List object. Input lists with null values removed.
        """
        output_list = []
        for item in input_list:
            if not pd.isnull(item):
                output_list.append(item)

        return output_list

    @staticmethod
    def get_col_slice(df: pd.DataFrame, uid, mask_col: str, tgt_col: str) -> list:
        """ Get a slice of a Dataframe column.

        Args:
            df: The Dataframe to get the column slice from.
            uid: The value to slice on.
            mask_col: The column to slice by.
            tgt_col: The column to return data from.

        Returns:
            List object. The slice from the column.
        """
        mask = df[mask_col] == uid
        output_list = df.loc[mask, tgt_col].tolist()

        return output_list

    @staticmethod
    def get_df_slice(df: pd.DataFrame, slice_col: str, slice_val, rtn_cols: list) -> pd.DataFrame:
        """ Get a slice of a Dataframe.

        Args:
            df: The Dataframe to take a slice from.
            slice_col: The column to slice by.
            slice_val: The value to slice on.
            rtn_cols: The columns to include in the returned Dataframe.

        Returns:
             Dataframe. The sliced dataframe.
        """
        output_df = df.loc[df[slice_col] == slice_val]
        output_df = output_df.loc[:, rtn_cols]

        return output_df

    @staticmethod
    def get_time_val(input_time_str: str, zero_hr: float = 0, unit: str = "hrs") -> float:
        """ Get elapsed time value from a time string.

        Args:
            input_time_str: Time string in format 'mm:ss', 'hh:mm:ss' or 'days:hh:mm:ss'.
                Delimiter may be : or . or a mix.
            zero_hr: hours from midnight on day 0 to calculate elapsed time from (optional, default 0).
            unit: Unit for return elapsed time value: 'secs', 'mins' or 'hrs' (optional, default 'hrs').

        Returns:
            Float. Elapsed time value.
        """
        components = (input_time_str.replace(":", ".")).split(".")
        if len(components) == 2:
            output_time = float(components[0])/60 + float(components[1])/3600
        elif len(components) == 3:
            output_time = float(components[0]) + float(components[1]) / 60 + float(components[2]) / 3600
        elif len(components) == 4:
            output_time = float(components[0]) * 24
            output_time += float(components[1]) + float(components[2]) / 60 + float(components[3]) / 3600
        else:
            output_time = 0.0

        output_time = output_time - zero_hr

        if unit == "mins":
            output_time = output_time * 60
        elif unit == "secs":
            output_time = output_time * 3600

        return output_time

    @staticmethod
    def row_per_event(input_df: pd.DataFrame, event_count_col: str, drop_zero: bool = True) -> pd.DataFrame:
        """ Convert a Dataframe to a row per event format.

        Add duplicate rows to a Dataframe based on the value in a specified event count column.
        The values in the event count column must be of the int type.

        For example, if a row has a value of 2 in the event count column there will be two copies of that row
        in the output Dataframe. If a row has a value of 3 in the event count column there will be three copies
        of that row in the returned Dataframe and so on.

        Rows with a value of 0 in the event count column will be dropped by default but can optionally be retained.

        Args:
            input_df: The input Dataframe.
            event_count_col: The column of the dataframe that contains the event count values.
            drop_zero: Drop rows with event count of 0 (optional, default True)

        Returns:
            Dataframe. Dataframe with a row per event.
        """
        df_ls = [input_df]
        max_val = input_df[event_count_col].max()

        for idx in range(1, max_val):
            df_ls.append(input_df.loc[input_df[event_count_col] > idx])

        output_df = pd.concat(df_ls)

        if drop_zero:
            output_df = output_df.loc[output_df[event_count_col] != 0]

        return output_df

    @staticmethod
    def flashpoint_str_extract(input_str: str, output_by_group: bool = True) -> list:
        """ Extract items from an input string

        Input string with groups of items separated by , and items within groups separated by |.
        Example: a|b, c|d, e|f
        Returns the items as a list object. By default, items are returned as a list of lists with a list per item
        group i.e. [[a,b],[c,d],[e,f]] optionally items can be returned with a list per position i.e. [[a,c,e],[b,d,f]].

        Args:
            input_str: Input string to extract items from.
            output_by_group: Output with list per group (True) or list per position (False) (optional, default = True)

        Returns:
            List. List of list objects containing the items in the input string.
        """
        output_ls = []
        data_ls = []

        input_groups_ls = input_str.split(",")
        for group in input_groups_ls:
            data_ls.append(group.split("|"))

        if output_by_group:
            output_ls = data_ls
        else:
            group_len = len(data_ls[0])
            # check all groups have the same length
            for group in data_ls:
                if group_len != len(group):
                    output_ls = None

            if output_ls is not None:
                for i in range(group_len):
                    values_ls = []
                    for group in data_ls:
                        values_ls.append(group[i])
                    output_ls.append(values_ls)

        return output_ls

    @staticmethod
    def flashpoint_event_pivot(all_event_df: pd.DataFrame, event_type: str, key_ls: list) -> pd.DataFrame:
        """ Pivot an 'all event' dataframe

        Extract the data associated with events of a specified type from an 'all event' dataframe that has 'Key',
        'Value' and 'EventID' columns. Return the data as a pivoted Dataframe with a column for each of the specified
        Keys.

        Args:
            all_event_df: The input Dataframe.
            event_type: The event type to extract a pivoted Dataframe for.
            key_ls: The list of keys to return as columns in the pivoted Dataframe.

        Returns:
            Dataframe. Pivoted Dataframe with a column for each of the specified Keys.
        """
        mask = all_event_df['Value'] == event_type
        event_ls = all_event_df.loc[mask, 'EventID']
        output_df = all_event_df.loc[(all_event_df['EventID'].isin(event_ls)) & (all_event_df['Key'].isin(key_ls))]
        output_df = pd.pivot(data=output_df, index='EventID', columns='Key', values='Value')
        output_df.reset_index(inplace=True)

        return output_df

    @staticmethod
    def flashpoint_add_coord_cols(event_df: pd.DataFrame, hex_col: str, map_df: pd.DataFrame,
                                  x_lbl: str, y_lbl: str) -> pd.DataFrame:
        """ Add co-ordinate columns to a Dataframe.

        Add X and Y co-ordinate columns to an input Dataframe from a map Dataframe.
        The input Dataframe must have a hex index number in a specified column.
        The map Dataframe must have columns with the hex number ('HexNumber')
        and the corresponding x ('XCoord') and y ('YCoord') co-ordinates.

        Args:
            event_df: The input Dataframe to add co-ordinate columns to.
            hex_col: The column of the input Dataframe that contains the hex index number.
            map_df: The map Dataframe with the X and Y locations of each hex.
                Column labels must be 'HexNumber', 'XCoord' and 'YCoord'.
            x_lbl: The label for the X co-ordinate column of the output Dataframe.
            y_lbl: The label for the y co-ordinate column of the output Dataframe.

        Returns:
              Dataframe. Input Dataframe With X (x_lbl) and Y (y_lbl) co-ordinate columns added.
        """
        # add x and y cols to an event df using a hex number col and a map df
        event_df[hex_col] = pd.to_numeric(event_df[hex_col])
        output_df = pd.merge(left=event_df, right=map_df, how='left', left_on=hex_col, right_on='HexNumber')
        output_df.rename(columns={'XCoord': x_lbl, 'YCoord': y_lbl}, inplace=True)
        output_df.drop(columns=['HexNumber', hex_col], inplace=True)

        return output_df

    @staticmethod
    def flashpoint_kills_from_lists(input_df: pd.DataFrame, kia_col_lbl: str, fall_col_lbl: str) -> pd.DataFrame:
        """ Add numerical Kills column to Dataframe.

        Add a 'Kills' column to a dataframe containing the count of items from KIA list and Fall out list
        columns as a numerical value. Entries in the list columns should be str type with single items (i.e. 'R12')
        or multiple items separated by commas (i.e. 'R12' or 'R12, G24' etc).

        Args:
            input_df: The input Dataframe to add the numerical kills column to.
            kia_col_lbl: Label of the KIA list column of the input Dataframe.
            fall_col_lbl: Label of the Fall-out list column of the input Dataframe.

        Return:
            Dataframe. Dataframe with 'Kills' column added.
        """
        col_lbl_ls = [kia_col_lbl, fall_col_lbl]
        cas_val_ls = []

        for lbl in col_lbl_ls:
            cas_str_ls = input_df[lbl].tolist()

            for idx, cas_str in enumerate(cas_str_ls):
                if pd.notnull(cas_str):
                    cas_str = str(cas_str).split(",")
                else:
                    cas_str = []

                if len(cas_val_ls) < len(cas_str_ls):
                    cas_val_ls.append(len(cas_str))
                else:
                    cas_val_ls[idx] = cas_val_ls[idx] + len(cas_str)

        output_df = input_df.copy()
        output_df['Kills'] = cas_val_ls

        return output_df

    @staticmethod
    def setup_logger(name: str, date_time_str: str = None,
                     output_folder=None, log_file: bool = True, log_stream: bool = True) -> logging.Logger:
        """
        Set up and return a new logger object.

        Args:
            name: Name of the logger object.
            date_time_str: provide a set date time string for filename (will be generated if not provided).
            log_file: Generate a log file
            log_stream: Stream log events
            output_folder: Folder to create log files in (default of None will save file in working directory)

        Returns:
            Logger object.
        """

        logger_ser = 0
        logger = logging.getLogger(name=name)

        # level will be 0 if level not set - increment ser to get a new logger object
        while logger.level != 0:
            logger_ser += 1
            logger_name = name + f"_{str(logger_ser)}"
            logger = logging.getLogger(name=logger_name)

        logger.setLevel(logging.DEBUG)

        if date_time_str is None:
            date_time = datetime.now()
            date_time_str = date_time.strftime("%d-%m-%Y_%H-%M-%S")

        # if a log file is being produced and an output folder is specified
        if log_file and output_folder is not None:
            # check if output folder exists and create it if it does not
            if not path.isdir(output_folder):
                makedirs(output_folder)

        if output_folder is not None:
            output_file_path = path.join(output_folder, f'{name}_{date_time_str}.log')
        else:
            output_file_path = f'{name}_{date_time_str}.log'

        if log_file:
            log_file_handler = logging.FileHandler(output_file_path)
            log_file_formatter = logging.Formatter('%(asctime)s: %(levelname)s - %(message)s', '%d/%m/%Y %H:%M:%S')

            log_file_handler.setFormatter(log_file_formatter)
            log_file_handler.setLevel(logging.DEBUG)

            logger.addHandler(log_file_handler)

        if log_stream:
            log_stream_handler = logging.StreamHandler()
            log_stream_formatter = logging.Formatter('%(asctime)s - %(name)s: %(levelname)s - %(message)s', '%H:%M:%S')

            log_stream_handler.setFormatter(log_stream_formatter)
            log_stream_handler.setLevel(logging.INFO)

            logger.addHandler(log_stream_handler)

        return logger

    @staticmethod
    def encode_event_detail_list(*detail_val_cols: list, detail_keys: list) -> list:
        """ Encode a list of detail entries for a CDF event as key-value pairs in json string format

        This function should be used to generate a list of detail key-value pairs before sending that list to an
        entity within the Dataset instance via the append_to_list function.

        Args:
            detail_keys: list of detail keys
            detail_val_cols: lists, each list contains the values for the corresponding detail key

        Returns:
            list of detail entries each encoded as a json string
        """

        return_ls = []

        # copy list args to prevent modification of input lists
        detail_keys = detail_keys.copy()

        detail_val_cols_ls = []
        for input_ls in detail_val_cols:
            detail_val_cols_ls.append(input_ls.copy())

        for detail_val_ls in zip(*detail_val_cols_ls):
            detail_val_ls = list(detail_val_ls)
            return_ls.append(CDFfunc.encode_event_detail(detail_key_ls=detail_keys, detail_val_ls=detail_val_ls))

        return return_ls

    @staticmethod
    def encode_event_detail(detail_key_ls: list, detail_val_ls: list) -> str:
        """ encode detail for CDF event as json string

        Args:
            detail_key_ls
            detail_val_ls

        Returns:
            encoded event detail in json string format
        """

        # copy list args to prevent modification of input lists
        detail_key_ls = detail_key_ls.copy()
        detail_val_ls = detail_val_ls.copy()

        key_val_separator = ':'
        pair_separator = ', '

        chars_ls = [':', ',', '{', '}', '"', " "]
        replacement_char = "_"

        return_str = '{'

        if detail_key_ls is not None and detail_val_ls is not None:
            if len(detail_key_ls) > len(detail_val_ls):
                while len(detail_key_ls) != len(detail_val_ls):
                    detail_val_ls.append('no_val')
            elif len(detail_val_ls) > len(detail_key_ls):
                while len(detail_key_ls) != len(detail_val_ls):
                    detail_key_ls.append('no_key')

        for idx, detail_key in enumerate(detail_key_ls):
            detail_val = detail_val_ls[idx]
            for char in chars_ls:
                detail_key = str(detail_key).replace(char, replacement_char)
                detail_val = str(detail_val).replace(char, replacement_char)

            return_str = return_str + f'"{detail_key}"{key_val_separator}"{detail_val}"'

            if idx+1 < len(detail_key_ls):
                return_str = return_str + pair_separator

        return_str = return_str + '}'

        return return_str

    @staticmethod
    def parse_config_bool(input_val) -> bool:
        """ return True for input value of 1 and False for any other input value

        Args:
            input_val

        Returns:
            boolean equivalent
        """
        if str(input_val) == "1":
            return_val = True
        else:
            return_val = False
        return return_val

    @staticmethod
    def parse_config_location(input_loc: str) -> str:
        """ return consistent path string with single backslashes and any additional slashes removed.

        Args:
            input_loc

        Returns:
            formatted path string
        """

        path_str = path.normpath(input_loc)
        return_val = path_str

        return return_val
