import os
import traceback
import csv
import pandas as pd
from pathlib import Path
from typing import List, Optional, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import date, datetime, timedelta

from qrlib.QRUtils import display
from qrlib.QRRunItem import QRRunItem
from qrlib.QRProcess import QRProcess

from app.Variables import BotVariable
from app.components.CbsViewComponent import  SQLServerComponent
from app.Errors import DataNotFoundError
from app.components.FuzzyMatchComponent import FuzzyMatcherComponent
from app.components.QuickXtractAPIComponent import XtractApiComponent
from app.database.CBS_database import CbsDataSync, MatchingStatus
from qrlib.QRUtils import get_secret
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)



class WeightageProcess(QRProcess):
    """Process for extracting information from scanned documents"""

    def __init__(self):
        super().__init__()

        self.cbs_db = SQLServerComponent()
        self.cbs_sync = CbsDataSync()
        self.fuzzy_match_component = FuzzyMatcherComponent()
        self.xtract_component = XtractApiComponent()
        self.threshold = get_secret('fuzzy_config')['min_threshold']
        self.register(self.cbs_db)
        self.register(self.cbs_sync)
        self.register(self.fuzzy_match_component)
        self.register(self.xtract_component)

        run_item: QRRunItem = QRRunItem(is_ticket=True)
        self.notify(run_item)

        self.pending_tickets: List[Dict[str, Any]] = []
        self.error_count = 0
        self.max_retries = 3
        self.cbs_view_df = None
        self.institution_df = None

    def _load_cbs_view_data(self, logger):
        """Load CBS view data from either database or local file."""
        display("---------------------EXTRACTING CBS DETAILS-------------------")
        # file_path = os.path.join("", "individual_data.csv")
        # institution_file_path = os.path.join("", "institution_data.csv")
        file_path = "individual_data.csv"
        institution_file_path = "institution_data.csv"
        try:
            with self.cbs_sync as sync:
                # if sync.is_sync_needed():
                if False:   # SKIP SYNC FOR NOW
                    logger.info("SYNC IS NEEDED")
                    with self.cbs as od:
                        results = od.fetch_individual_data()
                        institution = od.fetch_institution_data()
                        if results:
                            df = pd.DataFrame(results)
                            institution_df = pd.DataFrame(institution)
                            df.to_csv(
                                file_path, index=False, quoting=csv.QUOTE_NONNUMERIC
                            )
                            institution_df.to_csv(
                                institution_file_path,
                                index=False,
                                quoting=csv.QUOTE_NONNUMERIC,
                            )
                            self.cbs_sync.update_last_sync_time()
                            return df, institution_df
                        else:
                            raise DataNotFoundError("No data found in the database.")
                else:
                    logger.info("SYNC NOT NEEDED, LOADING FROM FILE")

            if os.path.exists(file_path):
                return pd.read_csv(file_path, dtype={"ACCT_NUMBER": str}), pd.read_csv(
                    institution_file_path, dtype="str" , low_memory=False
                )
            else:
                raise DataNotFoundError("CBS data files not found")

        except Exception as e:
            logger.error(f"Error while loading CBS data: {e}")
            if os.path.exists(file_path):
                try:
                    return pd.read_csv(file_path)
                except Exception as file_error:
                    logger.error(f"Failed to load CBS data from file: {file_error}")

            raise RuntimeError("CBS data could not be loaded from database or file.")

    def before_run(self, *args, **kwargs) -> None:
        """Setup before running the extraction process"""
        run_item: QRRunItem = QRRunItem(is_ticket=True)
        self.notify(run_item)
        logger = run_item.logger

        logger.info("Creating database tables for excel file and extracted data")
        display("Creating database tables for excel file and extracted data")

        try:
            with self.cbs_sync as sync:
                sync.create_table()

            self.cbs_view_df, self.institution_df = self._load_cbs_view_data(logger)
        except Exception as e:
            logger.error(f"Failed to create database table: {str(e)}")
            run_item.report_data["Task"] = "BeforeRun: Weightage Process"
            run_item.report_data["Reason"] = (
                f"Failed to create database table: {str(e)}"
            )
            run_item.set_error()
            run_item.post()
            raise
        
        try:
            if not self.xtract_component._access_token:
                token = self.xtract_component.get_access_token()
                if token:
                    logger.info("Creating database tables for excel file and extracted data")
                    display("Creating database tables for excel file and extracted data")
                else:
                    logger.warning("Could not Login")
                    display("Could not Login")
        except Exception as e:
            logger.info("Failed to Login", {e})
            display("Failed to Login", {e})

    def before_run_item(self, *args, **kwargs) -> None:
        """Setup before processing each ticket"""
        run_item: QRRunItem = QRRunItem(is_ticket=True)
        self.notify(run_item)
        logger = run_item.logger
        try:
            # Get the current ticket from kwargs
            current_ticket = kwargs.get("current_ticket")
            if not current_ticket:
                logger.warning("No current ticket provided in kwargs")
                return

            ticket_id = current_ticket.get("uuid")
            logger.info(f"Setting up for processing ticket ID: {ticket_id}")
            display(f">>>>>>>>>>>>>>>>>>>>>>>Preparing to process ticket ID: {ticket_id}<<<<<<<<<<<<<<<<<<<<<\n")

            # Pass ticket info in report_data for later stages
            run_item.report_data["ticket_id"] = ticket_id
            run_item.post()

        except Exception as e:
            logger.error(f"Error in before_run_item: {str(e)}")
            run_item.report_data["Task"] = "BeforeRunItem: Process Ticket"
            run_item.report_data["Reason"] = (
                f"Failed to setup ticket processing: {str(e)}"
            )
            run_item.set_error()
            run_item.post()

    def execute_run_item(self, *args, **kwargs):
        """Process a single ticket and generate matching results"""
        run_item: QRRunItem = QRRunItem(is_ticket=True)
        self.notify(run_item)
        logger = run_item.logger

        post_status = 'pending'

        try:
            # Get the required data from kwargs
            ticket_kwargs = kwargs.get("current_ticket")
            entity_type = ticket_kwargs.get('entity_type')
            display(f"entity type--{entity_type}")
            ticket_uuid = ticket_kwargs.get('uuid')

            logger.info(f"Processing ticket ID: {ticket_kwargs.get('id')} ")     
            if entity_type == 'institution':
                matched_status, matches_df, ticket_name = self.fuzzy_match_component.get_ticket_matches(
                    cbs_data=self.institution_df, 
                    source_data=ticket_kwargs, 
                    ticket_id=str(ticket_kwargs.get('uuid')),
                    final_threshold=float(self.threshold),
                    # chalani_number=ticket_kwargs.get('chalani_no'),
                )
            else:
                matched_status, matches_df, ticket_name = self.fuzzy_match_component.get_ticket_matches(
                    cbs_data=self.cbs_view_df, 
                    source_data=ticket_kwargs, 
                    ticket_id=str(ticket_kwargs.get('uuid')),
                    final_threshold=float(self.threshold),
                    # chalani_number=ticket_kwargs.get('chalani_no'),
                )

            # convert df list of dictionaries (records):
            matches_list = matches_df.to_dict('records')
            
            
            logger.info("MATCHES LIST---->", matches_list)
            display(f"Searched Matches from CBS : {matches_list[:2]}")


            logger.info(f"Processed ticket ID: {ticket_kwargs.get('id')} with status: {matched_status}")
            
            run_item.report_data = {
                "Task": "Execute Run Item",
                "Ticket_id": ticket_kwargs.get('id'),
                "Chalani Number": ticket_kwargs.get('chalani_no'),
                "Status": matched_status,
                # "matches": matches_list,
                "Number of Matches Found": len(matches_list),
            }
            run_item.set_success()
            run_item.post()
            
            # POST MATCHES TO XTRACT API
            if matches_list:
                try:
                    response = self.xtract_component._post_matches(matches_list, ticket_uuid)
                    if response.status_code == 200:
                        post_status = 'success'
                        logger.info(f"Successfully posted matches to Xtract API: {response.text}")
                        display(f"Successfully posted matches to Xtract API: {response.text}")
                    else:
                        post_status = 'failed'
                        # logger.error(f"Failed to post matches, status code: {response.status_code}, response: {response.text}")
                        logger.error(f"Failed to post matches, status code: {response.status_code}")
                        # display(f"Failed to post matches, status code: {response.status_code}, response: {response.text}" )
                        display(f"Failed to post matches, status code: {response.status_code}" )
                    
                except Exception as e:
                    logger.error(f"Could not Connect to Xtract API: {str(e)}")
                    run_item.report_data["Task"] = "ExecuteRunItem: Post Matches"
                    run_item.report_data["Reason"] = f"Failed to post matches: {str(e)}"
                    run_item.set_error()
                    run_item.post()

                return {
                    'ticket_id': ticket_kwargs.get('id'),
                    'matched_status': matched_status,
                    "chalani_no": ticket_kwargs.get('chalani_no'),
                    "matches": matches_list,
                    "post_status": post_status,
                    "Number of Matches Found": len(matches_list),
                    'ticket_name': ticket_name,
                }
        
        except Exception as e:
            logger.error(f"Error in execute_run_item: {str(e)}")
            logger.error(traceback.format_exc())
            run_item.report_data["Task"] = "ExecuteRunItem: Process Ticket"
            run_item.report_data["Reason"] = f"Failed to process ticket: {str(e)}"
            run_item.set_error()
            run_item.post()
            
            return {
                'ticket_id': kwargs.get('current_ticket', {}).get('id', 'unknown'),
                'matched_status': 'Error',
                'ticket_filename': '',
                'ticket_name': f"error_ticket_{kwargs.get('current_ticket', {}).get('id', 'unknown')}",
            }

    def after_run_item(self, *args, **kwargs) -> None:
        """Handle results after processing a ticket"""
        run_item: QRRunItem = QRRunItem(is_ticket=True)
        self.notify(run_item)
        logger = run_item.logger

        # Get required data from kwargs
        ticket_id = kwargs.get("ticket_id")
        matched_status = kwargs.get("matched_status")
        ticket_filename = kwargs.get("ticket_filename")

        display(f"ticket id : {ticket_id}, match status: {matched_status}, ticket filename: {ticket_filename}")

        if not all([ticket_id, matched_status, ticket_filename]):
            logger.warning("Missing data in kwargs for after_run_item")
            run_item.report_data["Task"] = "AfterRunItem: Finalize Ticket"
            run_item.report_data["Reason"] = "Missing required data in kwargs"
            run_item.set_error()
            run_item.post()
            return

    def execute_run(self, *args, **kwargs) -> None:
        """Execute the complete process workflow"""
        run_item: QRRunItem = QRRunItem(is_ticket=True)
        self.notify(run_item)
        logger = run_item.logger

        display("EXECUTE RUN: Weightage Process")
        logger.info(f"STARTING EXECUTE RUN:::")

        try:
            # Step 1: Get all pending tickets for processing
            today = date.today()
            one_month_ago = today - timedelta(days=10)
            date_to = today.strftime("%Y-%m-%d")
            date_from = one_month_ago.strftime("%Y-%m-%d")
            params = {
                "processing_status": "pending",
                "date_to": date_to,
                "date_from": date_from,
            }
            response = self.xtract_component._fetch_tickets(params=params)
            pending_tickets = response.json().get('results')

            individual_tickets = [t for t in pending_tickets if t.get("entity_type") == "individual"]
            institution_tickets = [t for t in pending_tickets if t.get("entity_type") == "institution"]

            pending_tickets = individual_tickets[:2] + institution_tickets[:2]
            display(f"pending tickets{pending_tickets}")
            logger.info("pending_tickets", pending_tickets)

            logger.info(f"Found {len(pending_tickets)} tickets to process")
            display(f"Found {len(pending_tickets)} tickets to process")

            # Step 2: Process each pending ticket using run_item methods
            for ticket in pending_tickets:
                ticket_kwargs = kwargs.copy()
                ticket_kwargs["current_ticket"] = ticket

                # display(f"TICKET KWARGS ---> , {ticket_kwargs}")
                # logger.info("TICKET KWARGS ---> ", ticket_kwargs)

                # Process the ticket using the individual run item methods
                self.before_run_item(**ticket_kwargs)
                updated_kwargs = self.execute_run_item(**ticket_kwargs)
                if updated_kwargs:
                    ticket_kwargs = updated_kwargs
                    # ticket_kwargs.update(updated_kwargs)


                self.after_run_item(**ticket_kwargs)

            run_item.report_data["status"] = "Process Completed"
            run_item.report_data["tickets_processed"] = len(pending_tickets)
            run_item.post()

        except Exception as e:
            logger.error(f"Error in execute_run: {str(e)}")
            run_item.report_data["Task"] = "ExecuteRun: Weightage Process"
            run_item.report_data["Reason"] = f"Failed to complete process: {str(e)}"
            run_item.set_error()
            run_item.post()
            print(traceback.print_exc())

    def after_run(self, *args, **kwargs) -> None:
        """Cleanup after all processing is complete"""
        run_item: QRRunItem = QRRunItem(is_ticket=True)
        self.notify(run_item)
        logger = run_item.logger

        try:
            logger.info("Completing Weightage Process")
            display("AFTER RUN: Weightage Process Completed")

            # Add any final cleanup or reporting code here

            run_item.report_data["status"] = "Process Finalized"
            run_item.post()

        except Exception as e:
            logger.error(f"Error in after_run: {str(e)}")
            run_item.report_data["Task"] = "AfterRun: Weightage Process"
            run_item.report_data["Reason"] = f"Failed to finalize process: {str(e)}"
            run_item.set_error()
            run_item.post()
