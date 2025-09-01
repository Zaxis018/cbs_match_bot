import os
import re
import shutil
import subprocess
import datetime as dt
from pathlib import Path
from datetime import datetime, timedelta

import nepali_datetime
import fitz

import Levenshtein
import numpy as np
import pandas as pd
import pytz
from abydos import phonetic
from Errors import WeightageParamsError
from qrlib.QRUtils import display
from Variables import BotVariable

from app.component.FTPComponent import FTPComponent
from app.Constants import CODE_1, CODE_2, CODE_3, CODE_4, CODE_5
from qrlib.QREnv import QREnv
from qrlib.QRLogger import QRLogger


def delete_file(path):
    if os.path.exists(path):
        os.remove(path)
        return True
    else:
        return False


def task_kill(process_name: str):  # type: ignore
    call = "TASKLIST", "/FI", "imagename eq %s" % process_name
    # use buildin check_output right away
    output = subprocess.check_output(call).decode()
    # check in last line for process name
    last_line = output.strip().split("\r\n")[-1]
    if last_line:
        subprocess.run(["taskkill", "/F", "/IM", f"{process_name}"])
    else:
        pass


def get_hour() -> int:
    utc_time = dt.datetime.now(pytz.utc)
    nepal_time = utc_time.astimezone(pytz.timezone("Asia/Kathmandu"))
    nepal_hour = nepal_time.hour
    return nepal_hour


def parse_value(value: str, type: str) -> str:
    value = value.strip()
    if type == "name":
        result = re.findall(r"[A-Za-z ]*", value)
        return " ".join(result)
    elif type == "client_code":
        result = re.search(r"\d{8}", value)
    elif type == "main_code":
        result = re.search(r"[0-9a-zA-Z]{10}", value)
    elif type == "citizenship_no":
        result = re.search(r"[0-9/-]*", value)
    elif type == "fuzzy_match":
        result = re.search(r"\d+", value)
    else:
        return ""
    if not result:
        return ""
    return result.group()


def task_kill(process_name: str):
    call = "TASKLIST", "/FI", "imagename eq %s" % process_name
    # use buildin check_output right away
    output = subprocess.check_output(call).decode()
    # check in last line for process name
    last_line = output.strip().split("\r\n")[-1]
    if last_line:
        subprocess.run(["taskkill", "/F", "/IM", f"{process_name}"])
    else:
        pass


def get_remarks(
    source: str,
    extracted_code: str,
    agency: str,
    accnt_status: str,
    cases: str,
    chalani_no: str,
):
    # source_keys = CODE_1.keys()
    # ecode_keys = CODE_2.keys()
    display(
        f"Ger remarks using {source}, {extracted_code}, {agency}, {accnt_status}, {cases}"
    )
    # source = source.lower().replace(' ', '').strip()
    extracted_code = extracted_code.lower().replace(" ", "").strip()
    agency = agency.lower().replace(" ", "").strip()
    accnt_status = accnt_status.lower().replace(" ", "").strip()
    cases = cases.lower().replace(" ", "").strip()
    # source = re.sub(r'\s+', ' ', source)
    # extracted_code = re.sub(r'\s+', ' ', extracted_code)
    # agency = re.sub(r'\s+', ' ', agency)
    # accnt_status = re.sub(r'\s+', ' ', accnt_status)
    # cases = re.sub(r'\s+', ' ', cases)

    agency_keys = list(CODE_3.keys())
    status_keys = list(CODE_4.keys())
    cases_keys = list(CODE_5.keys())

    # if (source.find('rastra bank')) >= 0:
    #     source = "Nepal Rastra Bank"
    # elif (source.find('analysis') >= 0):
    #     source = 'FIU Analysis'
    # else:
    #     source = 'Direct Letter'

    if (extracted_code.find("NRB") >= 0) or (extracted_code.find("nrb") >= 0):
        code_list = extracted_code.split("-")
        code2 = code_list[1]
    else:
        # code2 = CODE_2['Direct Letter']
        code2 = chalani_no

    display(f"Agency in get remarks is {agency}")
    # agency_low_key = [x.lower().replace(' ', '').strip() for x in agency_keys]
    for index, item in enumerate(agency_keys):

        # display(f'Item for agency is {item.lower().replace(" ", "").strip().find(agency)}')
        if item.lower().replace(" ", "").strip().find(agency) >= 0:
            agency = agency_keys[index]
            break
        # else:
        #   agency = 'Others'
    no_status_count = 0
    for index, item in enumerate(status_keys):
        if accnt_status.lower().strip().find("information") >= 0:
            return ""

        if accnt_status.find(item.lower().replace(" ", "").strip()) >= 0:
            block_status = status_keys[index]
            break
        else:
            no_status_count += 1

    if no_status_count == len(status_keys):
        raise Exception(f"No status found {accnt_status}")

    for index, item in enumerate(cases_keys):
        if cases.find(item.lower().replace(" ", "").strip()) >= 0:
            cases = cases_keys[index]
            break
        # else:
        #   cases = 'Others'

    display(f"{source}/{agency}/{code2}/{block_status}/{cases}")
    return f"{CODE_1[source]}/{CODE_3[agency]}/{code2}/{CODE_4[block_status]}/{CODE_5[cases]}"


def compare_two_stirngs_in_df(string1: str, *args, **kwargs):
    """
    Required kwargs key value:
        string2: str ==> value that required to compare with
        soundex: bool ==> if soundex is need to be applie
    """
    if not all(x in kwargs.keys() for x in ["string2", "soundex"]):
        return np.nan
    string2: str = str(kwargs["string2"]).strip()
    string1 = re.sub(r"\s+", " ", str(string1)).upper()
    string2 = re.sub(r"\s+", " ", str(string2)).upper()
    similarity = float(Levenshtein.ratio(str(string1).strip(), string2.strip()))
    return float(round(similarity * 100.0, 2))


def get_current_date() -> str:
    now = dt.datetime.now()
    return str(now.strftime("%Y-%m-%d %H:%M:%S"))


def get_yesterday_date() -> str:
    current_date = dt.datetime.now()
    yesterday = current_date - timedelta(days=1)
    # Extract the date component
    date_of_yesterday = yesterday.date()
    return str(date_of_yesterday.strftime("%Y-%m-%d")).strip()


def get_weightage(elements: list[str], type: str) -> dict:
    """
    Required value of elements as per neccessity:
        - name
        - father_name
        - gfather_name
        - citizenship_no
        - pan_no
        - registration_no
        - account_no
    Note : any other value will raise exception Errors.WeightageParamsError
    Type value:
        - account information
        - legal
        - natural
    """
    logger = QRLogger().logger
    path = BotVariable.CONFIG_PATH + "/Weightage Distribution.xlsx"

    weights = {}
    elements = [i.strip() for i in elements]
    logger.info(f"Elements are {elements}")

    if type == "account information":
        # * weightage for account information
        logger.info("If account information available")
        df = pd.read_excel(
            path, sheet_name="Account information provided", skiprows=[0, 1, 2, 3]
        )
        df = df.fillna("")
        df.columns = [str(x).strip().lower() for x in df.columns.to_list()]
        logger.info("Filtering dataframe completed.")
        if len(elements) == 1:
            logger.info("Calculation weight for account no")
            df = df[df["condition"].str.contains("Only Account", na=False)]
            logger.info(f"1:Shape of dataframe is {df.shape}")
            index = int(df.index.values.tolist()[0])
            weights["account_no"] = df.at[index, "account number"]
        else:
            logger.info("Calculation weight for account no and name")
            df = df[df["condition"].str.contains("Name and Account", na=False)]
            logger.info(f"2:Shape of dataframe is {df.shape}")
            index = int(df.index.values.tolist()[0])
            weights["name"] = df.at[index, "name"]
            weights["account_no"] = df.at[index, "account number"]
        return weights
    elif type == "natural":
        # * Weightage condition for natural account
        logger.info("If account is natural.")
        df = pd.read_excel(
            path, sheet_name="Natural", skiprows=[0, 1, 2, 3, 4, 5, 6, 7]
        )
        df = df.drop(columns=df.columns[df.columns.str.contains("Assumption")])
        df = df.dropna(axis=0, how="all")
        df = df.dropna(axis=1, how="all").fillna("")
        df.columns = [str(x).strip().lower() for x in df.columns.to_list()]
        logger.info("Filtering dataframe completed")
        logger.info(f"element length : {len(elements)}")

        if len(elements) == 4:
            df = df[df["condition"].str.contains("All information", na=False)]
            logger.info(f"3:Shape of dataframe is {df.shape}")
            index = int(df.index.values.tolist()[0])
            weights["name"] = df.at[index, "name"]
            weights["citizenship_no"] = df.at[index, "citizenship number"]
            weights["father_name"] = df.at[index, "father name"]
            weights["gfather_name"] = df.at[index, "grandfather name"]
        elif len(elements) == 3:
            # df = df.tail(8)
            if all(x in elements for x in ["name", "father_name", "gfather_name"]):
                condition = "Name, Father Name, Grandfather name"
                first, second, third = "name", "father_name", "gfather_name"
                df_first, df_second, df_third = (
                    "name",
                    "father name",
                    "grandfather name",
                )
            elif all(x in elements for x in ["name", "father_name", "citizenship_no"]):
                condition = "Name, Citizenship number, Father Name"
                first, second, third = "name", "citizenship_no", "father_name"
                df_first, df_second, df_third = (
                    "name",
                    "citizenship number",
                    "father name",
                )
            elif all(x in elements for x in ["name", "gfather_name", "citizenship_no"]):
                condition = "Name, Citizenship number, Grandfather name"
                first, second, third = "name", "citizenship_no", "gfather_name"
                df_first, df_second, df_third = (
                    "name",
                    "citizenship number",
                    "grandfather name",
                )
            elif all(
                x in elements
                for x in ["citizenship_no", "father_name", "citizenship_no"]
            ):
                condition = "Citizenship number, Father Name, Grandfather name"
                first, second, third = "citizenship_no", "father_name", "gfather_name"
                df_first, df_second, df_third = (
                    "citizenship number",
                    "father name",
                    "grandfather name",
                )
            else:
                raise WeightageParamsError(elements)
            df = df[df["condition"].str.contains(condition, na=False)]
            logger.info(f"4:Shape of dataframe is {df.shape}")
            index = int(df.index.values.tolist()[0])
            weights[first] = df.at[index, df_first]
            weights[second] = df.at[index, df_second]
            weights[third] = df.at[index, df_third]
        elif len(elements) == 2:
            df = df.tail(20)
            if all(x in elements for x in ["name", "father_name"]):
                condition = "Name,Father Name"
                first, second = "name", "father_name"
                df_first, df_second = "name", "father name"
            elif all(x in elements for x in ["name", "citizenship_no"]):
                condition = "Name,Citizenship number"
                first, second = "name", "citizenship_no"
                df_first, df_second = "name", "citizenship number"
            elif all(x in elements for x in ["name", "gfather_name"]):
                condition = "Name,Grandfather name"
                first, second = "name", "gfather_name"
                df_first, df_second = "name", "grandfather name"
            elif all(x in elements for x in ["citizenship_no", "father_name"]):
                condition = "Citizenship number,Father Name"
                df = df[df["condition"].str.contains(condition, na=False)]
                logger.info(f"5:Shape of dataframe is {df.shape}")
                index = int(df.index.values.tolist()[0])
                first, second = "citizenship_no", "father_name"
                df_first, df_second = "citizenship_no", "father name"
            elif all(x in elements for x in ["citizenship_no", "father_name"]):
                condition = "Citizenship number,Grandfather name"
                first, second = "citizenship_no", "gfather_name"
                df_first, df_second = "citizenship_no", "grandfather name"
            elif all(x in elements for x in ["citizenship_no", "father_name"]):
                condition = "Father Name,Grandfather name"
                first, second = "gfather_name", "father_name"
                df_first, df_second = "grandfather name", "father name"
            else:
                raise WeightageParamsError(elements)
            df = df[df["condition"].str.contains(condition, na=False)]
            logger.info(f"6:Shape of dataframe is {df.shape}")
            index = int(df.index.values.tolist()[0])
            weights[first] = df.at[index, df_first]
            weights[second] = df.at[index, df_second]
        else:
            if len(elements) != 1:
                raise WeightageParamsError(elements)
            element = elements[0]
            df = df.tail(5)
            df.reset_index()

            if element == "name":
                condition = "Name"
                df = df[df["condition"].str.contains(condition, na=False)]
                logger.info(f"7:Shape of dataframe is {df.shape}")
                logger.info(f"8:Shape of dataframe is {df.shape}")
                index = int(df.index.values.tolist()[0])
                weights["name"] = df.at[index, "name"]
            elif element == "citizenship_no":
                condition = "Citizenship Number"
                df = df[df["condition"].str.contains(condition, na=False)]
                logger.info(f"9:Shape of dataframe is {df.shape}")
                index = int(df.index.values.tolist()[0])
                weights["citizenship_no"] = df.at[index, "citizenship number"]
            elif element == "Father Name":
                condition = "Father Name"
                df = df[df["condition"].str.contains(condition, na=False)]
                logger.info(f"10:Shape of dataframe is {df.shape}")
                index = int(df.index.values.tolist()[0])
                weights["father_name"] = df.at[index, "father name"]
            elif element == "gfather_name":
                condition = "Grand Father"
                df = df[df["condition"].str.contains(condition, na=False)]
                logger.info(f"11:Shape of dataframe is {df.shape}")
                logger.info(f"12:Shape of dataframe is {df.shape}")
                index = int(df.index.values.tolist()[0])
                weights["gfather_name"] = df.at[index, "grandfather name"]
            else:
                raise WeightageParamsError(elements)
        return weights
    elif type == "legal":
        # * Weightage condition for legal account
        df = pd.read_excel(path, sheet_name="Legal", skiprows=[0, 1, 2, 3])
        df = df.drop(columns=df.columns[df.columns.str.contains("Unnamed")])
        df = df.dropna(axis=0, how="all")
        df = df.dropna(axis=1, how="all").fillna("")
        df.columns = [str(x).strip().lower() for x in df.columns.to_list()]
        if len(elements) == 3:
            df = df[df["condition"].str.contains("All information", na=False)]
            logger.info(f"13:Shape of dataframe is {df.shape}")
            index = int(df.index.values[0])
            weights["name"] = df.at[index, "name"]
            weights["pan_no"] = df.at[index, "pan number"]
            weights["registration_no"] = df.at[index, "registration number"]
        elif len(elements) == 2:
            if "name" in elements and "pan_no" in elements:
                search = "Name, Pan"
                first, second = "name", "pan_no"
                df_first, df_second = "name", "pan number"
            elif "name" in elements and "registration_no" in elements:
                search = "Name, Registration"
                first, second = "name", "registration_no"
                df_first, df_second = "name", "registration number"
            elif "pan_no" in elements and "registration_no" in elements:
                search = "Pan number, Registration"
                first, second = "pan_no", "registration_no"
                df_first, df_second = "pan number", "registration number"
            else:
                raise WeightageParamsError(elements)
            logger.info(f"Registration search is {search}")
            df = df[df["condition"].str.contains(search, na=False)]
            logger.info(f"14:Shape of dataframe is {df.shape}")
            logger.info(
                f"Datafarame index for two elements in legal is {df.index.values}"
            )
            index = int(df.index.values.tolist()[0])
            weights[first] = df.at[index, df_first]
            weights[second] = df.at[index, df_second]
        else:
            df = df.tail(4)
            df.reset_index(inplace=True)
            if elements[0] == "name":
                search = "Name"
                first, df_first = "name", "name"
            elif elements[0] == "pan_no":
                search = "Pan"
                first, df_first = "pan_no", "pan number"
            elif elements[0] == "registration_no":
                search = "Registration"
                first, df_first = "registration number", "registration number"
            else:
                raise WeightageParamsError(elements)
            df = df[df["condition"].str.contains(search, na=False)]
            logger.info(f"15:Shape of dataframe is {df.shape}")
            logger.info(f"Datafarame index for only one in legal is {df.index.values}")
            index = int(df.index.values.tolist()[0])
            weights[first] = df.at[index, df_first]
        return weights
    else:
        raise WeightageParamsError(elements)


def download_file(ftp: FTPComponent, file, folder: str = ""):
    with ftp as ftp:
        ftp.set_cwd(folder)
        ftp.download_file(file)
        ftp.reset_wd()


def remove_file(ftp: FTPComponent, filename: str, folder: str = ""):
    with ftp as ftp:
        ftp.set_cwd(folder)
        ftp.delete_file(filename)
        ftp.reset_wd()


def upload_file(ftp: FTPComponent, localpath, file):
    with ftp as ftp:
        ftp.set_cwd(BotVariable.READ)
        ftp.upload_file(localpath, file)
        ftp.reset_wd()


def upload_file_report(ftp: FTPComponent, localpath, file):
    with ftp as ftp:
        ftp.set_cwd(BotVariable.REPORT)
        ftp.upload_file(localpath, file)
        ftp.reset_wd()


def move_file_in_condition(ftp: FTPComponent, condition: str, filename: str):
    file: str = filename
    if condition.find("local_read_failed") >= 0:
        path_from = BotVariable.DOWNLOAD_PATH_SOURCE + "/" + file
        if not os.path.exists(path_from):
            return
        path_to = BotVariable.DOWNLOAD_PATH_READ + "/" + file
        shutil.move(path_from, path_to)
    elif condition.find("local_error_failed") >= 0:
        path_from = BotVariable.DOWNLOAD_PATH_SOURCE + "/" + file
        if not os.path.exists(path_from):
            return
        path_to = BotVariable.DOWNLOAD_PATH_ERROR + "/" + file
        shutil.move(path_from, path_to)
    elif condition.find("ftp_read_failed") >= 0:
        path_from = BotVariable.DOWNLOAD_PATH_READ
        if not os.path.exists(path_from):
            return
        upload_file(ftp, path_from, file)
    elif condition.find("ftp_error_failed") >= 0:
        path_from = BotVariable.DOWNLOAD_PATH_ERROR
        if not os.path.exists(path_from):
            return
        upload_file(ftp, path_from, file)


def remove_unmatched_files():
    path = BotVariable.UNMATCHED_SUCESS_PATH
    files = os.listdir(path)
    for file in files:
        os.remove(f"{path}/{file}")


def get_local_working_directory() -> str:
    vault = QREnv.VAULTS["work_directory"]
    path = str(vault["folderpath"]).replace("\\", "/")
    if path.endswith("/"):
        path = path[:-1]
    return path


def convert_bs_to_ad(nepali_date):
    """Convert Bikram Sambat date to Gregorian (AD) date and return in YYYY-MM-DD format."""
    if not nepali_date:
        return None
    
    nepali_date_str = str(nepali_date)
    
    try:
        year, month, day = map(int, nepali_date_str.split('-'))        
        nepali_date_obj = nepali_datetime.date(year, month, day)        
        gregorian_datetime_obj = nepali_date_obj.to_datetime_date()        
        return gregorian_datetime_obj.strftime("%Y-%m-%d")
    
    except (ValueError, AttributeError) as e:
        return None
    
def standardize_date_format(date_obj, format="%Y-%m-%d"):
    """
    Convert any date object to a standard string format for easy comparison.
    
    Args:
        date_obj: Either a datetime.date object, string date, or BS date string
        format: The output format string (default: YYYY-MM-DD)
        
    Returns:
        A formatted date string or None if conversion fails
    """
    if not date_obj:
        return None
    
    
    date_formats = [
        "%Y-%m-%d %H:%M:%S",  # Format: 1949-06-16 00:00:00
        "%Y-%m-%d",           # Format: 1949-06-16
        "%d/%m/%Y",           # Format: 16/06/1949
        "%m/%d/%Y",           # Format: 06/16/1949
        "%Y/%m/%d",           # Format: 1949/06/16
        "%Y%m%d",             # Format: 19490616
        "%d-%m-%Y",           # Format: 16-06-1949
        "%m-%d-%Y",           # Format: 06-16-1949
        "%b %d, %Y",          # Format: Jun 16, 1949
        "%d %b %Y",           # Format: 16 Jun 1949
        "%B %d, %Y",          # Format: June 16, 1949
        "%d %B %Y",           # Format: 16 June 1949
        "%Y.%m.%d",           # Format: 1949.06.16
        "%d.%m.%Y",           # Format: 16.06.1949
        "%m.%d.%Y",           # Format: 06.16.1949
        "%Y/%m/%d %H:%M:%S",  # Format: 1949/06/16 00:00:00
        "%d-%b-%Y",           # Format: 16-Jun-1949
        "%d-%B-%Y",           # Format: 16-June-1949
        "%Y-%b-%d",           # Format: 1949-Jun-16
        "%Y-%B-%d"            # Format: 1949-June-16
    ]
    
    
    for date_format in date_formats:
        try:
            date_obj = datetime.strptime(str(date_obj), date_format)
            return date_obj.strftime("%Y%m%d")
        except (ValueError, TypeError):
            continue
    
    return None


def delete_files_in_folder(path):
    files = os.listdir(path=path)
    for file in files:
        file_path = os.path.join(path, file)
        os.remove(file_path)
        
def check_pdf_file(file_path):
    try:
        with open(file_path, "rb"):
            doc = fitz.open(file_path)
            if len(doc) == 0:
                print("No pages found in the document.")
                return None, None
            return doc, doc.page_count
    except Exception as e:
        print(f"Error opening file: {e}")
        return None, None
    
def rename_file_stem(filepath):
    try:
        file_path = Path(filepath)
        if not file_path.exists():
            print(f"Error: File not found at {filepath}")
            return None
        stem = file_path.stem
        new_stem = stem.replace('.', '_').replace(',','_')
        new_filepath = file_path.with_stem(new_stem)
        os.rename(file_path, new_filepath)  
        return new_filepath
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
