from typing import List

import requests

from qrlib.QREnv import QREnv
from qrlib.QRQueueItem import QRQueueItem, QueueItemStatus
from qrlib.queue.queue_exceptions import (BaseUrlNotSetException,
                                          IdentifierNotSetException)


class QRQueue:

    def __init__(self, name: str) -> None:
        self.name = name
        self.id = None
        self.get_queue_info()

    def get_queue_info(self):
        if (QREnv.NO_PLATFORM):
            return {"id": 0, "name": "Test Queue"}
        else:
            """
            retrieve queue with name
            {base_uri}/api/v1/bot/queue/{queue_name}/
            """
            uri = self.gen_uri() + f"/{self.name}/"
            headers = self.gen_headers()
            response = requests.get(
                url=uri,
                headers=headers,
            )
            if response.status_code == 200:
                queue_data = response.json()
                id = queue_data.get('id')
                name = queue_data.get('name')
                setattr(self, 'id', id)
                setattr(self, 'name', name)
            else:
                raise Exception(response.text)
            return response.json()

    @staticmethod
    def gen_uri(params: dict = None):
        if hasattr(QREnv, 'BASE_URL'):
            base_url = QREnv.BASE_URL
        else:
            raise BaseUrlNotSetException()

        path = f"/bot/queue/"
        uri = f"{base_url}{path}"

        """Not Implemented"""
        if params:
            pass

        return uri

    @staticmethod
    def gen_headers():
        identifier = QREnv.IDENTIFIER
        if not identifier:
            raise IdentifierNotSetException

        return {
            "Accept": "application/json",
            "Authorization": f"identifier {identifier}"
        }

    def get_items(self, count: int = 1, order: str = "asc") -> List[QRQueueItem]:
        if (QREnv.NO_PLATFORM):
            # Read from sample file
            return [QRQueueItem(id=0, status="New", queue=0, input={"key": "Test"}), QRQueueItem(id=2, status="New", queue=0, input={"key": "Test1"})]
        else:
            # Hit api to get items. Create queueitems with the response
            response = requests.get(
                url=self.gen_uri(),
                headers=self.gen_headers(),
                params={"name": f"{self.name}", "item_size": count}
            )
            if response.status_code == 200:
                queue_data = response.json()

                id = queue_data.get("id")
                name = queue_data.get('name')
                queue_items = queue_data.get('queue_items')
                setattr(self, 'id', id)
                setattr(self, 'name', name)
                queue_items_list = []
                for queue_item in queue_items:
                    item = QRQueueItem(queue=id, **queue_item)
                    queue_items_list.append(item)
                return queue_items_list
            else:
                raise Exception(response.text)

    def create_new_items_from_list(self, inputs: list) -> None:
        if (QREnv.NO_PLATFORM):
            return True
        else:
            if not self.id and not self.name:
                raise Exception("queue id and name not set")
            new_items = []

            # validate
            if not isinstance(inputs, list):
                raise Exception('inputs must me instance of list')
            #
            queue_item_list = []
            for input in inputs:
                if not isinstance(input, dict):
                    raise Exception('input must me instance of dict')
                #
                queue = self.id
                queue_item = QRQueueItem(
                    queue=queue,
                    status=QueueItemStatus.NEW,
                    input=input
                )
                queue_item_json = queue_item.dict()
                queue_item_json.pop('id', None)
                queue_item_json.pop('queue', None)
                queue_item_list.append(queue_item_json)

            response = requests.post(
                uri=self.gen_uri(),
                headers=self.gen_headers(),
                json=queue_item_list
            )

            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(response.text)
