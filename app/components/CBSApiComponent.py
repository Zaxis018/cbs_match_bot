import os
import json
import base64
import requests
from datetime import datetime
from typing import Dict, Optional, Tuple

from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5

from qrlib.QRUtils import get_secret
from qrlib.QRComponent import QRComponent
from tenacity import retry, stop_after_attempt, wait_exponential

class CbsApiComponent(QRComponent):
    def __init__(self):
        super().__init__()
        self.pem_file = None
        self._load_configuration()
        self.private_key = self._read_private_key()

    def _load_configuration(self):
        try:
            secrets = get_secret("apims_cred")            
            self.api_url = secrets.get("api_url")
            self.username = secrets.get("username")
            self.password = secrets.get("password")
            self.pem_file = secrets.get("pem_file")
            
            if not all([self.api_url, self.username, self.password, self.pem_file]):
                raise ValueError("Missing required configuration values")
        except Exception as e:
            raise

    def _read_private_key(self):
        try:
            with open(self.pem_file, 'rb') as file:
                private_key = file.read()
            return private_key
        except FileNotFoundError:
            error_message = f"Private key file not found at: {self.pem_file}"
            raise FileNotFoundError(error_message)
        except Exception as e:
            raise

    def _create_signature(self, req_model, timestamp=None):
        """Creates a digital signature for the request using Python's cryptography."""
        logger = self.run_item.logger
        try:
            if timestamp is None:
                timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:23]

            signature_model = {
                "Model": req_model,
                "TimeStamp": timestamp
            }
            
            signature_json = json.dumps(signature_model, separators=(",", ":"))
            key = RSA.import_key(self.private_key)
            h = SHA256.new(signature_json.encode('utf-8'))
            signer = PKCS1_v1_5.new(key)
            signature = signer.sign(h)
            encoded_signature = base64.b64encode(signature).decode('utf-8')
            logger.info("Successfully created and encoded signature.")

            return encoded_signature, timestamp
        except Exception as e:
            error_message = f"Error creating signature using Python crypto: {e}"
            logger.exception(error_message)
            raise Exception(error_message)

    def _create_request_data(self, request_model, signature, timestamp, function):
        """Creates the request data payload for the API call."""
        logger = self.run_item.logger
        try:
            request_bytes = json.dumps(request_model, separators=(",", ":")).encode("utf-8")
            request_data = {
                "FunctionName": function,
                "Data": base64.b64encode(request_bytes).decode('utf-8'),
                "Signature": signature,
                "TimeStamp": timestamp
            }
            logger.info(f"Request data created for function: {function}")
            return request_data
        except Exception as e:
            error_message = f"Error creating request data: {e}"
            logger.exception(error_message)
            raise Exception(error_message)

    def _create_request_data_inquiry(self, request_model, signature, timestamp):
        """Creates request data for inquiry function specifically."""
        logger = self.run_item.logger
        try:
            request_bytes = json.dumps(request_model, separators=(",", ":")).encode("utf-8")
            request_data = {
                "FunctionName": "FreezeAcctsInq",
                "Data": base64.b64encode(request_bytes).decode('utf-8'),
                "Signature": signature,
                "TimeStamp": timestamp
            }
            logger.info("Request data created for inquiry function")
            return request_data
        except Exception as e:
            error_message = f"Error creating inquiry request data: {e}"
            logger.exception(error_message)
            raise Exception(error_message)

    def _send_request(self, request_data):
        """Sends the request to the API endpoint."""

        logger = self.run_item.logger
        request_data_json = json.dumps(request_data)
        logger.info(f"Request data JSON: {request_data_json}")

        try:
            with requests.Session() as client:
                client.auth = (self.username, self.password)
                headers = {"Content-Type": "application/json", "Accept": "application/json"}
                logger.info(f"Sending request to: {self.api_url}")
                response = client.post(self.api_url, headers=headers, data=request_data_json, timeout=300)
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
                logger.info(f"API request successful. Status code: {response.status_code}")
                return response, request_data_json
        except requests.exceptions.RequestException as e:
            error_message = f"API request failed: {e}"
            logger.exception(error_message)
            raise Exception(error_message)

    
    def get_freeze_request_model(self, account_number: str):
        logger = self.run_item.logger
        try:
            timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:23]
            request_model  = {
                "TransactionId": timestamp,
                "AcctFreezeAddRequest":{
                    "AcctFreezeAddRq":{
                        "AcctId": account_number,
                        "FreezeCode": "D",
                        "FreezeReasonCode": "OTH",
                        "FreezeRemarks": "Test"
                    }
                }
            }
            logger.info(f"Freeze request model created for account: {account_number}")
            return request_model
        except Exception as e:
            logger.error(f"Error creating freeze modification model: {str(e)}")
            return None
    
    def get_unfreeze_request_model(self, account_number: str):
        logger = self.run_item.logger
        try:
            timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:23]
            request_model  = {
                "TransactionId": timestamp,
                "AcctUnFreezeAddRequest":{
                    "AcctUnFreezeAddRq":{
                        "AcctId": account_number,
                    }
                }
            }
            logger.warning(f"Unfreeze request model created for account: {account_number}")
            return request_model
        except Exception as e:
            logger.error(f"Error creating unfreeze modification model: {str(e)}")
            return None
    
    def get_freeze_modification_model(self, **kwargs) -> Tuple[bool, any]:
        logger = self.run_item.logger
        try:
            timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:23]
            account_info = kwargs["account_info"]
            api_data = kwargs['inquery_info']

            FreezeRemarks_new = account_info.get('remarks', '')
            FreezeReasonCode = account_info.get('reason_code') or 'OTH'
            account_number = account_info.get('account_num') or account_info.get('account_number', '')
            freeze_code = account_info.get('freeze_code', 'D') 

            if not account_number:
                return False, "Missing account number"
            
            if api_data.get('ACCT_STATUS') == 'ACTIVE':
                request_model = {
                    "TransactionId": timestamp,
                    "AcctFreezeAddRequest": {
                        "AcctFreezeAddRq": {
                            "AcctId": account_number,
                            "FreezeCode": freeze_code,  
                            "FreezeReasonCode": FreezeReasonCode,
                            "FreezeRemarks": FreezeRemarks_new,
                        }
                    }
                }
                return True, request_model

            else:
                freeze_code = api_data.get('FREZ_CODE', '')
                freeze_pairs = [
                    (api_data.get('FREZ_REASON_CODE', ''), api_data.get('FREEZE_RMKS', '')),
                    (api_data.get('FREZ_REASON_CODE_2', ''), api_data.get('FREEZE_RMKS2', '')),
                    (api_data.get('FREZ_REASON_CODE_3', ''), api_data.get('FREEZE_RMKS3', '')),
                    (api_data.get('FREZ_REASON_CODE_4', ''), api_data.get('FREEZE_RMKS4', '')),
                    (api_data.get('FREZ_REASON_CODE_5', ''), api_data.get('FREEZE_RMKS5', ''))
                ]
                
                for i, (reason, remarks) in enumerate(freeze_pairs, 1):
                    if bool(reason) != bool(remarks):  # XOR operation - one is filled but other is not
                        field_name = f" for field set {i}"
                        logger.error(f"Inconsistent freeze data{field_name}: reason_code present: {bool(reason)}, remarks present: {bool(remarks)}")
                        return False, f"Both freeze reason code and freeze remarks must be provided together{field_name}"
                
                filled_pairs = sum(1 for reason, remarks in freeze_pairs if reason and remarks)
                if filled_pairs >= 5:
                    return False, "Already 5 fields are freezed"
                    
                new_values = (FreezeReasonCode, FreezeRemarks_new)
                target_index = None
                
                for i, (reason, remarks) in enumerate(freeze_pairs):
                    if not reason and not remarks:
                        target_index = i
                        break
                        
                if target_index is None:
                    return False, "No available slots for additional freeze codes"
                    
                request_model = {
                    "TransactionId": timestamp,
                    "AcctFreezeAddRequest": {
                        "AcctFreezeAddRq": {
                            "AcctId": account_number,
                            "FreezeCode": freeze_code if freeze_code else "D",  
                            "ModFreezeInd": "Y",
                        }
                    }
                }
                
                field_names = ["", "2", "3", "4", "5"]
                for i, (reason, remarks) in enumerate(freeze_pairs):
                    if i == target_index:
                        reason, remarks = new_values
                        
                    if reason or remarks:
                        suffix = field_names[i]
                        key_prefix = "AcctFreezeAddRequest.AcctFreezeAddRq."
                        request_model["AcctFreezeAddRequest"]["AcctFreezeAddRq"][f"FreezeReasonCode{suffix}"] = reason
                        request_model["AcctFreezeAddRequest"]["AcctFreezeAddRq"][f"FreezeRemarks{suffix}"] = remarks

                return True, request_model
        except Exception as e:
            logger.error(f"Error creating freeze modification model: {str(e)}")
            return False, f"Exception: {str(e)}"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    def call_api(self, request_model, function_name):
        """
        Orchestrates the process of creating a signature, request data,
        and sending the request to the API.
        """
        try:
            logger = self.run_item.logger
            timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:23]
            signature, _ = self._create_signature(request_model, timestamp)
            request_data = self._create_request_data(request_model, signature, timestamp, function_name)
            logger.info(f"Request data prepared for {function_name}")

            response, request_data_json = self._send_request(request_data)
            logger.info(f"API call completed for function: {function_name}")
            
            response_data = response.json()
            if response_data:
                code = response_data.get('Code', '')
                message = response_data.get('Message', '')
                
                if code == '71603' and 'Couldn\'t get response from FI service' in message:
                    logger.warning(f"Received FI service error (code: {code}), retrying...")
                    raise Exception(f"FI service error - retrying: {message}")
                
                if code in ['0', '71604']:
                    logger.info(f"API call successful for function: {function_name}")
                    return "success", response
                else:
                    logger.error(f"API call failed with code: {code}, message: {message}")
                    return 'error', response
            else:
                logger.error("Empty response from API")
                return 'error', response       

        except Exception as e:
            error_message = f"Error occurred during API call for function {function_name}: {e}"
            logger.error(error_message)
            raise Exception(error_message)