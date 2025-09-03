import time
from urllib import response
import requests
import json
import os
from qrlib.QRComponent import QRComponent
from qrlib.QRUtils import get_secret
from datetime import date, datetime, timedelta

from app.Constants import QUICKXTRACT_BASE_URL, API_PREFIX

# QUICKXTRACT APIs

TICKET_FETCH_API = "letteraction/tickets/"
TICKET_DETAIL_API = "letteraction/tickets/{ticket_uuid}/"

MATCH_FETCH_API = "letteraction/tickets/{ticket_uuid}/matches/"
MATCH_POST_API = "letteraction/tickets/{ticket_uuid}/matches/create/"


class XtractApiComponent(QRComponent):
    def __init__(self):
        vault = get_secret("xtract_credentials")
        self.username = vault["username"]
        self.password = vault["password"]
        self._access_token = None
        self._last_login_time = None
        self.login_url = QUICKXTRACT_BASE_URL + API_PREFIX + "users/token-auth/"

    def get_access_token(self):
        self._login()
        return self._access_token

    def _login(self):
        if self._last_login_time is None or (
            datetime.now() - self._last_login_time
        ) > timedelta(minutes=5):
            payload = {"email": self.username, "password": self.password}
            retries = 5
            for attempt in range(retries):
                try:
                    response = requests.post(self.login_url, data=payload)
                    if response.status_code == 200:
                        self._access_token = response.json()["access_token"]
                        self._last_login_time = datetime.now()
                        return
                    else:
                        print(f"Login failed with status code: {response.status_code}")
                        raise Exception("Login failed")
                except requests.exceptions.RequestException as e:
                    print(f"Error logging in (attempt {attempt + 1}/{retries}): {e}")
                    if attempt < retries - 1:
                        time.sleep(2**attempt)
                    else:
                        raise

    def _fetch_tickets(self, params=None):
        """By Default fetch on pending Tickets of Last 30 days"""
        if params is None:
            today = date.today()
            one_month_ago = today - timedelta(days=30)

            # Format dates into 'YYYY-MM-DD' string format, standard for APIs.
            date_to = today.strftime("%Y-%m-%d")
            date_from = one_month_ago.strftime("%Y-%m-%d")
            params = {
                "processing_status": "pending",
                "date_to": date_to,
                "date_from": date_from,
            }

        url = QUICKXTRACT_BASE_URL + API_PREFIX + TICKET_FETCH_API
        headers = {"Authorization": f"Bearer {self._access_token}"}
        response = requests.request("GET", url, headers=headers, params=params)
        response.raise_for_status()
        if response.status_code == 200:
            print("Successfully fetched Ticket Details")

        return response

    def _fetch_ticket_detail(self, ticket_uuid):
        url = (
            QUICKXTRACT_BASE_URL
            + API_PREFIX
            + TICKET_DETAIL_API.format(ticket_uuid=ticket_uuid)
        )
        headers = {"Authorization": f"Bearer {self._access_token}"}
        response = requests.request("GET", url, headers=headers)
        if response.status_code == 200:
            print("Ticket Fetch Successful")

    def _fetch_matches(self, ticket_uuid):

        url = (
            QUICKXTRACT_BASE_URL
            + API_PREFIX
            + MATCH_FETCH_API.format(ticket_uuid=ticket_uuid)
        )
        payload = {}
        headers = {"Authorization": f"Bearer {self._access_token}"}
        response = requests.request("GET", url, headers=headers)
        if response.status_code == 200:
            print(" Successfully fetched Matches for ticket : ", ticket_uuid)

    def _post_matches(self, payload, ticket_uuid):

        url = (
            QUICKXTRACT_BASE_URL
            + API_PREFIX
            + MATCH_POST_API.format(ticket_uuid=ticket_uuid)
        )
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }
        # response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
        response = requests.request("POST", url, headers=headers, json=payload)

        if response.status_code == 200:
            print(" Successfully posted matches for ticket : ", ticket_uuid)

        return response
