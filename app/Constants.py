TIMEOUT = 60
MAX_ERROR_RETRY_COUNT = 5
MAX_FOLLOW_UP_COUNT = 5

# DB Related Constant
BOT_DB_TICKETS_TABLE_NAME = "letter_action_tickets_table"
BOT_DB_SOURCE_FILE_TABLE_NAME = "source_file_table"
BOT_DB_UNMATCHED_MATCHED_FILE_TABLE_NAME = "unmatched_matched_file_table"
BOT_DB_ACCOUNT_MATCHED_DATA_TABLE_NAME = "account_matched_data_table"
BOT_DB_RESTRICTION_MATCHEDDATA_TABLE_NAME = "restriction_matched_data_table"
BOT_DB_LOCKER_MATCHEDDATA_TABLE_NAME = "locker_matched_data_table"
BOT_DB_EMAIL_TABLE_NAME = "emails_table"
BOT_DB_UNIQUE_TABLE_NAME = "unique_id_table"

# QUICKXTRACT APIs
QUICKXTRACT_BASE_URL = "192.168.1.124:8000/"
API_PREFIX = "api/v1/"

TICKET_FETCH_API = "letteraction/tickets/"
TICKET_DETAIL_API="letteraction/tickets/{ticket_uuid}/"

MATCH_FETCH_API = "letteraction/tickets/{ticket_uuid}/matches/"
MATCH_POST_API = "letteraction/matches/{match_uuid}/actions/"

# FOR Letter Action Process
CLIENTCODE_LENGTH = 8
BRANCH_CODE_LENGTH = 3
MAINCODE_LENGTH = 20
HIGH_RISK_LIST = [
    # 'Nepal Rastra Bank',
    # 'Commission for the Investigation of Abuse of Authority',
    "Department of Money Laundering Investigation",
    # 'Nepal Police Headquarter',
    # 'Central Investigation Bureau',
    # 'Area Police Office',
    # 'Police Office',
    # 'Metropolitan Pollice Office',
    # 'District Police Office',
    # 'Supreme Court of Nepal',
    # 'District Court',
    # 'Financial Intelligence Unit',
]
CODE_1 = {"Nepal Rastra Bank": "NRB", "Direct Letter": "DIR", "FIU Analysis": "FIU"}

CODE_2 = {"Nepal Rastra Bank": "0065", "Direct Letter": "5802"}

CODE_3 = {
    "Commission for the Investigation of Abuse of Authority": "CIA",
    "Department of Money Laundering Investigation": "DML",
    "Nepal Police Headquarter": "PHQ",
    "Central Investigation Bureau": "CIB",
    "Police Office": "NPO",
    "Area Police Office": "APO",
    "Metropolitan Police Office": "MPO",
    "District Police Office": "DPO",
    "Inland Revenue Department": "IRD",
    "Department of Revenue Investigation": "DRI",
    "Large Tax-Payer Office": "LTP",
    "Medium Tax-Payer Office": "MTP",
    "Taxpayer Service Office": "TSO",
    "Nepal Rastra Bank": "NRB",
    "Financial Intelligence Unit": "FIU",
    "Province Police Office": "PPO",
    "Supreme Court of Nepal": "SCO",
    "District Court": "DCO",
    "Kathmandu Upatyaka Aparadh Anusandhan Karyalaya": "MPCD",
    "Others": "O",
}

CODE_4 = {
    "Debit Restriction": "D",
    "Credit Restriction": "C",
    "Account Block": "B",
    "Account Release": "R",
}

CODE_5 = {
    "Vat Evasion": "VE",
    "Tax Evasion ": "TE",
    "Cheque Fraud": "CF",
    "Bribery/Corruption": "BR",
    "Banking Offence": "BO",
    "Fraudulence": "FR",
    "Criminal activity": "CA",
    "Others": "O",
}

RESTRICTION = {
    "normal": "f",
    "block": "b",
    "debit restrict": "+",
    "credit restrict": "-",
    "link block": "l",
    "disputed block": "d",
    "waiting": "o",
    "true block": "t",
    "closed": "c",
}

REVRSE_RESTRICTION = {
    "f": "normal",
    "b": "block",
    "+": "debit restrict",
    "-": "credit restrict",
    "l": "link block",
    "d": "disputed block",
    "o": "waiting",
    "t": "true block",
    "c": "closed",
}

# key = standard field ,  value = (<CBS_FIELD_NAME>, <QUICKXTRACT_FIELD_NAME>)
FIELD_MAPPING = {
    'name': ('ACCT_NAME', 'name'),
    'fathers_name': ('CUST_FATHERS_NAME', 'fathers_name'),
    'grandfathers_name': ('CUST_GRANDFATHERS_NAME', 'grandfathers_name'),
    'citizenship_no': ('CTZ_NUMBER', 'citizenship_no'),
    'spouse_name': ('CUST_SPOUSE_NAME', 'spouse_name'),
    'citizenship_issue_date': ('CTZ_ISSUE_DATE', 'issue_date'),
    'pan_number': ('PAN', 'pan_no'),
    'registration_no': ('REGISTRATION', 'registration_no'),
    'account_no': ('ACCT_NUMBER', 'account_no'),
    'nid': ('NID_NUMBER', 'nid'),
    'dob': ('CUST_DOB', 'dob'),
}


PREFILTER_THRESHOLD = 0.70                               # 70% threshold for pre-filtering
MAX_CANDIDATES_AFTER_PREFILTER = 100

COLUMN_ORDER = [
    'CIF_ID', 'patra_sankya', 'chalani_no', 'ticket_name',  'letter_date', 'institution', 
    'entity_type', 'criteria', 'enforcement_request', 'subject', 'letter_head', 
    'ACCT_NAME', 'name', 'name_score',
    'CUST_FATHERS_NAME', 'fathers_name', 'fathers_name_score', 
    'CUST_SPOUSE_NAME', 'spouse_name', 'spouse_name_score', 
    'CUST_GRANDFATHERS_NAME', 'grandfathers_name', 'grandfathers_name_score',
    'CUST_DOB', 'dob', 'dob_score', 'dob_number',
    'CTZ_NUMBER', 'citizenship_no', 'citizenship_no_score', 
    'CTZ_ISSUE_DATE', 'issue_date', 'CTZ_ISSUED_DISTRICT', 'NID_NUMBER', 
    'PAN', 'pan_no', 'pan_no_score',
    'REGISTRATION', 'registration_no', 'registration_no_score',
    'nid','ACCT_NUMBER', 'account_no', 'account_no_score', 
    'ACCT_STATUS', 'FREZ_CODE', 'KYC_STATUS', 'MOBILE_NO', 'PP_NUMBER', 'PP_EXPDATE',
    'SCHM_TYPE', 'SCHM_CODE', 'PRODUCT_NAME', 'DEBITCARD', 'CREDITCARD', 'ECOMECARD',
    'INSTANTCARD', 'CIPS', 'TERMDEPOSIT', 'LOAN', 'MOBILEBANKING', 'PHONELOAN', 
    'AVAILABLE_AMT', 'FREZ_CODE', 'FREZ_REASON_CODE', 'FREZ_REASON_CODE_2',
    'FREZ_REASON_CODE_3', 'FREZ_REASON_CODE_4', 'FREZ_REASON_CODE_5', 
    'FREEZE_RMKS', 'FREEZE_RMKS2', 'FREEZE_RMKS3', 'FREEZE_RMKS4', 'FREEZE_RMKS5',
    'total_score', 'action', 'remarks', 'reason_code'
]