"""Runtime Variable"""


class BotVariable:
    LOCAL_PATH: str = ""
    CONFIG_PATH: str = ""
    DOWNLOAD_PATH_SOURCE: str = ""
    DOWNLOAD_PATH_READ: str = ""
    DOWNLOAD_PATH_ERROR: str = ""
    REPORT_GEN_PATH: str = ""
    REPORT_UNMATCHED_PATH: str = ""
    UNMATCHED_SUCESS_PATH: str = ""
    XML_DOWNLOAD_PATH: str = ""

    DATABASE_PATH: str = ""

    FTP_DEFAULT_FOLDER = "/letteraction"
    FTP_SCANNED_FOLDER = "/letteraction/scanned"
    FTP_EXCEL_FOLDER = "/letteraction/excel"
    FTP_MATCHED_FOLDER = "letteraction/matched"
    FTP_UNMATCHED_FOLDER = "letteraction/unmatched"
    FTP_ACTION_FOLDER = "letteraction/action"
    FTP_WORKING_FOLDER = "letteraction"

    ERROR = "Error"
    SUCCESS = "Success"
    READ = "Read"
    SOURCE = "Source"
    REPORT = "Report"
    MATCHED = "Matched"
    UNMATCHED = "Unmatched"
