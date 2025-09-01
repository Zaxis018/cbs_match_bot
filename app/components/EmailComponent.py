import os
import traceback
from pathlib import Path
from datetime import datetime
from RPA.Email.ImapSmtp import ImapSmtp

from qrlib.QRUtils import get_secret
from qrlib.QRComponent import QRComponent

class EmailComponent(QRComponent):
    def __init__(self):
        super().__init__()
        self.subject = ''
        self.body = ''
        self.recipients = []
        self.sending_file_list = []
        self.emailcred = get_secret('emailcred')
        self.account = None
        self.password = None
        self.server = None
        self.port = None

    def set_smtp_creds(self):
        logger = self.run_item.logger
        logger.info("Setting required creds")
        self.account = self.emailcred.get('email')
        self.server = self.emailcred.get('server')
        self.port = self.emailcred.get('port')
        try:
            self.password = self.emailcred.get('password')
        except:
            self.password = None

    def get_reciepents(self):
        logger = self.run_item.logger
        logger.info("Getting recievers email")
        self.recipients = str(self.emailcred.get('recipients')).split(',')
        logger.info(self.recipients)

    def authmail_and_send(self):
        logger = self.run_item.logger
        """Call when send mail only"""
        logger.info('SMTP connection started')
        mail = ImapSmtp()
        mail.authorize_smtp(account=self.account, password=self.password, smtp_server=self.server, smtp_port=self.port)
        logger.info(f"SMTP connection established.")
        logger.info(f"Mail sent process started.")
        for i in self.recipients:
            mail.send_message(
                sender=self.account,
                recipients=i,
                subject=self.subject,
                body=self.body,
                html=True,
                attachments=self.sending_file_list,
            )
            logger.info("Mail Sent Successfully.")

    def initiate_connection(self):
        logger = self.run_item.logger
        logger.info("Initiating connection setting")
        self.set_smtp_creds()
        self.get_reciepents()

    def send_extraction_mail(self, nrb_filename: str, excel_filepath: str):
        try:
            logger = self.run_item.logger
            logger.info("Sending Extraction Mail")
            self.initiate_connection()
            basename = os.path.basename(Path(excel_filepath))

            self.subject = f"Extraction Completed for {nrb_filename}"
            self.sending_file_list = []

            self.sending_file_list.append(excel_filepath)
            self.body = f'''
                <p>Dear All,</p>

                <p>The following file has been successfully downloaded and extracted:</p>

                <ul>
                    <li><strong>Extracted File:</strong> {nrb_filename}</li>
                    <li><strong>Generated Excel File:</strong> {basename}</li>
                </ul>

                <p>You can find the extracted data in the attachment/FTP Folder(/letteraction/excel): <strong>{basename}</strong>.</p>

                <p><em>This is an auto-generated email. Please do not reply.</em></p>

                <p><strong>Thank you!</strong></p>
            '''
            self.authmail_and_send()
        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            return False

    def send_fuzzymatched_mail(self, summary_data: dict, fuzzy_matched_excel: str):
        try:
            logger = self.run_item.logger
            logger.info("Sending Fuzzy Matching Summary Mail")

            self.sending_file_list = []

            self.sending_file_list.append(fuzzy_matched_excel)
            self.initiate_connection()
            summary_data.update({
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            
            table_rows = "".join(
                f"<tr><td>{ticket_id}</td><td>{result['matched_status']}</td><td>{result['chalani_number']}</td><td>{result['ticket_name']}</td></tr>"
                for ticket_id, result in summary_data["group_results"].items()
            )

            table_html = f"""
                <table border="1" cellspacing="0" cellpadding="5" style="border-collapse: collapse; width: 100%;">
                    <thead>
                        <tr style="background-color: #f2f2f2;">
                            <th>Ticket ID</th>
                            <th>Chalani Number</th>
                            <th>Matched Status</th>
                            <th>Ticket Name</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows}
                    </tbody>
                </table>
            """ if table_rows else "<p>No matching results found.</p>"

            self.subject = f"Matching Process Completed: {summary_data['patra_number']}"

            self.body = f'''
                <p>Dear All,</p>

                <p>Please find the summary of the fuzzy matching process:</p>

                <ul>
                    <li><strong>Patra Number:</strong> {summary_data["patra_number"]}</li>
                    <li><strong>Total Tickets:</strong> {summary_data["total_tickets"]}</li>
                    <li><strong>Matched Tickets:</strong> {summary_data["matched_tickets"]}</li>
                    <li><strong>Unmatched Tickets:</strong> {summary_data["unmatched_tickets"]}</li>
                    <li><strong>Match Percentage:</strong> {summary_data["match_percentage"]}</li>
                    <li><strong>Output File:</strong> {summary_data["output_filename"]}</li>
                    <li><strong>Timestamp:</strong> {summary_data["timestamp"]}</li>
                </ul>

                <p>You can find the matching results in the attached file: <strong>{summary_data["output_filename"]}</strong>.</p>

                <p>Below is the detailed match result:</p>
                {table_html}

                <p><em>This is an auto-generated email. Please do not reply.</em></p>

                <p><strong>Thank you!</strong></p>
            '''
            self.authmail_and_send()
        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            return False
        
    def send_letteraction_summary(self, summary_data):
        """
        Send email summary of letter actions that were processed
        """
        try:
            logger = self.run_item.logger
            logger.info("Sending Letter Action Summary Mail")

            self.initiate_connection()
            
            action_table_rows = ""
            success_count = 0
            failed_count = 0
            
            action_name_map = {
                'b': 'Block Account', 
                't': 'Total Freeze',
                'r': 'Release (Unfreeze)',
                'c': 'Credit Restriction',
                'i': 'Information'
            }
            
            if summary_data and isinstance(summary_data, list):
                for result in summary_data:
                    chalani_number = result.get('chalani_number', 'N/A')
                    account_number = result.get('account_number', 'N/A')
                    action_code = result.get('action', '').lower()
                    action_name = action_name_map.get(action_code, action_code if action_code else 'Unknown')
                    status = "Success" if result.get('message') == 'success' else "Failed"
                    response = result.get('response', 'N/A')
                    
                    if status == "Success":
                        success_count += 1
                        row_color = "#f0fff0" 
                    else:
                        failed_count += 1
                        row_color = "#fff0f0"  
                    
                    if response and len(str(response)) > 100:
                        response = str(response)[:100] + "..."
                    
                    action_table_rows += f"""<tr style="background-color: {row_color}">
                        <td>{chalani_number}</td>
                        <td>{account_number}</td>
                        <td>{action_name}</td>
                        <td>{status}</td>
                        <td>{response}</td>
                    </tr>"""
            
            total_actions = success_count + failed_count
            success_rate = (success_count / total_actions * 100) if total_actions > 0 else 0
            summary_html = f"""
                <p><strong>Summary:</strong></p>
                <ul>
                    <li>Total actions processed: {total_actions}</li>
                    <li>Successful actions: {success_count}</li>
                    <li>Failed actions: {failed_count}</li>
                    <li>Success rate: {success_rate:.2f}%</li>
                    <li>Processed at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</li>
                </ul>
            """
            
            action_table_html = f"""
                <table border="1" cellspacing="0" cellpadding="5" style="border-collapse: collapse; width: 100%;">
                    <thead>
                        <tr style="background-color: #f2f2f2;">
                            <th>Chalani Number</th>
                            <th>Account Number</th>
                            <th>Action</th>
                            <th>Status</th>
                            <th>Response</th>
                        </tr>
                    </thead>
                    <tbody>
                        {action_table_rows}
                    </tbody>
                </table>
            """ if action_table_rows else "<p>No account actions were performed.</p>"

            self.subject = f"Letter Action Process Completed - {success_count} Success, {failed_count} Failed"

            self.body = f'''
                <p>Dear Team,</p>

                <h3>Letter Action Report</h3>
                
                {summary_html}
                
                <h3>Detailed Action Results:</h3>
                {action_table_html}

                <p><em>This is an automated email. Please do not reply.</em></p>

                <p><strong>Thank you!</strong><br>
                Automated Letter Action System</p>
            '''
            
            logger.info(f"Sending letter action summary with {total_actions} actions")
            self.authmail_and_send()
            logger.info("Letter action summary email sent successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to send letter action summary email: {str(e)}")
            logger.error(traceback.format_exc())
            return False
        
    def send_aml_mail(self, aml_filename, aml_filepath):
        try:
            logger = self.run_item.logger
            logger.info("Sending AML REPORT Mail")
            self.initiate_connection()
            basename = os.path.basename(Path(aml_filepath))
            today_date =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            self.subject = f"AML REPORT: {aml_filename}"
            self.sending_file_list = []
            self.sending_file_list.append(aml_filepath)
            self.body = f'''
                <p>Dear All,</p>
                <p>The following <strong>AML report</strong> has been generated for date: <strong>{today_date}</strong></p>
 
                <p>You can find the report in the attachment/FTP Folder(/letteraction/reports/aml): <strong>{basename}</strong>.</p>

                <p><em>This is an auto-generated email. Please do not reply.</em></p>

                <p><strong>Thank you!</strong></p>
            '''
            self.authmail_and_send()
        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            return False
        
    def send_sis_mail(self, sis_filename: str, sis_filepath: str):
        try:
            logger = self.run_item.logger
            logger.info("Sending SIS REPORT Mail")
            self.initiate_connection()
            basename = os.path.basename(Path(sis_filepath))
            today_date =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            self.subject = f"SIS REPORT: {sis_filename}"
            self.sending_file_list = []
            self.sending_file_list.append(sis_filepath)
            self.body = f'''
                <p>Dear All,</p>
                <p>The following <strong>SIS report</strong> has been generated for date: <strong>{today_date}</strong></p>
 
                <p>You can find the report in the attachment/FTP Folder(/letteraction/reports/sis): <strong>{basename}</strong>.</p>

                <p><em>This is an auto-generated email. Please do not reply.</em></p>

                <p><strong>Thank you!</strong></p>
            '''
            self.authmail_and_send()
        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            return False
        
    def send_todays_report(self, sis_filename: str, sis_filepath: str):
        try:
            logger = self.run_item.logger
            logger.info("Sending TODAYS REPORT Mail")
            self.initiate_connection()
            basename = os.path.basename(Path(sis_filepath))
            today_date =  datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            self.subject = f"TODAYS REPORT: {sis_filename}"
            self.sending_file_list = []
            self.sending_file_list.append(sis_filepath)
            self.body = f'''
                <p>Dear All,</p>
                <p>The following <strong>TODAYS report</strong> has been generated for date: <strong>{today_date}</strong></p>
 
                <p>You can find the report in the attachment/FTP Folder(/letteraction/reports/todays): <strong>{basename}</strong>.</p>

                <p><em>This is an auto-generated email. Please do not reply.</em></p>

                <p><strong>Thank you!</strong></p>
            '''
            self.authmail_and_send()
        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            return False
