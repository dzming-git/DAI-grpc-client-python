import grpc
from typing import List, Dict, Set
import logging

from generated.protos.target_detection import target_detection_pb2, target_detection_pb2_grpc

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TargetDetectionClient:
    """
    Client for interacting with the Target Detection service.
    Provides functionalities to filter detection results and query them by image ID.
    """

    class Filter:
        """
        A filter to manage included label IDs for detection results.
        """
        def __init__(self, label_cnt: int):
            """
            Initializes the filter with all possible label IDs.
            
            Parameters:
                label_cnt (int): The count of all possible labels.
            """
            self.filter: Set[int] = set(range(label_cnt))
        
        def add(self, label_id: int) -> None:
            """
            Adds a label ID to the filter.
            
            Parameters:
                label_id (int): Label ID to be added.
            """
            self.filter.add(label_id)
        
        def remove(self, label_id: int) -> None:
            """
            Removes a label ID from the filter.
            
            Parameters:
                label_id (int): Label ID to be removed.
            """
            if label_id in self.filter:
                self.filter.remove(label_id)
        
        def clear(self) -> None:
            """
            Clears all label IDs from the filter.
            """
            self.filter.clear()
        
        def check(self, label_id: int) -> bool:
            """
            Checks if a label ID is in the filter.
            
            Parameters:
                label_id (int): Label ID to check.
            
            Returns:
                bool: True if label ID is in the filter, False otherwise.
            """
            return label_id in self.filter
    
    class Result:
        """
        Represents a single detection result.
        """
        def __init__(self, label_id: int, x1: float, y1: float, x2: float, y2: float, confidence: float = 1.0):
            """
            Initializes a detection result with bounding box coordinates and confidence score.
            
            Parameters:
                label_id (int): Label ID of the detected object.
                x1 (float): X-coordinate of the top-left corner.
                y1 (float): Y-coordinate of the top-left corner.
                x2 (float): X-coordinate of the bottom-right corner.
                y2 (float): Y-coordinate of the bottom-right corner.
                confidence (float): Confidence score of the detection.
            """
            self.label_id = label_id
            self.x1 = x1
            self.y1 = y1
            self.x2 = x2
            self.y2 = y2
            self.confidence = confidence
        
        def __str__(self) -> str:
            """
            Returns a string representation of the detection result.
            
            Returns:
                str: String representation of the result.
            """
            return f'Label ID: {self.label_id}, Coordinates: ({self.x1}, {self.y1}), ({self.x2}, {self.y2}), Confidence: {self.confidence}'
    
    def __init__(self, ip: str, port: str, task_id: int):
        """
        Initializes the Target Detection client with the service's IP address, port, and the task ID.
        
        Parameters:
            ip (str): IP address of the Target Detection service.
            port (str): Port of the Target Detection service.
            task_id (int): Task ID for which detections are queried.
        """
        self.__ip = ip
        self.__port = port
        self.__task_id = task_id
        self.__conn = grpc.insecure_channel(f'{ip}:{port}')
        self.__client = target_detection_pb2_grpc.CommunicateStub(channel=self.__conn)
        logging.info("Initialized TargetDetectionClient with Task ID: {}".format(task_id))
        self.__result_mapping_table: Dict[int, str] = self.get_result_mapping_table()
        self.filter = self.Filter(len(self.__result_mapping_table))
        
    def convert_id_to_label(self, label_id: int) -> str:
        """
        Converts a label ID to its corresponding string label.

        Parameters:
            label_id (int): The ID of the label to convert.

        Returns:
            str: The string representation of the label.
        
        Raises:
            Exception: If the label ID does not exist in the result mapping table.
        """
        try:
            label = self.__result_mapping_table[label_id]
            logging.info(f"Converted label ID {label_id} to label '{label}'.")
            return label
        except KeyError:
            error_msg = f"TargetDetectionClient Error: Failed to find label for ID {label_id}"
            logging.error(error_msg)
            raise Exception(error_msg)

    def query_label_id(self, label: str) -> int:
        """
        Queries the ID of a given label.

        Parameters:
            label (str): The label whose ID is queried.

        Returns:
            int: The ID of the label.
        
        Raises:
            Exception: If the label does not exist in the result mapping table.
        """
        logging.info(f"Querying ID for label '{label}'.")
        for k, v in self.__result_mapping_table.items():
            if v == label:
                logging.info(f"Found ID {k} for label '{label}'.")
                return k
        error_msg = f"TargetDetectionClient Error: Failed to find ID for label '{label}'"
        logging.error(error_msg)
        raise Exception(error_msg)

    def get_result_mapping_table(self) -> Dict[int, str]:
        """
        Fetches the result mapping table from the server, mapping label IDs to their string representations.

        Returns:
            Dict[int, str]: A dictionary where keys are label IDs and values are their string labels.
        """
        logging.info("Fetching result mapping table.")
        try:
            request = target_detection_pb2.GetResultMappingTableRequest(taskId=self.__task_id)
            response = self.__client.getResultMappingTable(request)
            if response.response.code != 200:
                raise(Exception('\n'.join(
                    [
                        'TargetDetectionClient Error: Failed to get result mapping table',
                        f'   ip:   {self.__ip}',
                        f'   port: {self.__port}',
                        'Error:',
                        response.response.message
                    ]
                )))
            self.__result_mapping_table = {i: label for i, label in enumerate(response.labels)}
            logging.info("Successfully fetched result mapping table.")
            return self.__result_mapping_table
        except grpc.RpcError as e:
            logging.error(f"GRPC error in fetching result mapping table: {str(e)}")
            raise e

    def get_result_by_image_id(self, image_id: int) -> List['Result']:
        """
        Queries detection results for a given image ID, filtering out results not in the filter set.

        Parameters:
            image_id (int): The ID of the image for which detection results are queried.

        Returns:
            List[Result]: A list of detection results that passed the filter.
        """
        logging.info(f"Querying results for image ID: {image_id}")
        try:
            request = target_detection_pb2.GetResultIndexByImageIdRequest(taskId=self.__task_id, imageId=image_id, wait=True)
            response = self.__client.getResultIndexByImageId(request)
            if response.response.code != 200:
                raise(Exception('\n'.join(
                    [
                        'TargetDetectionClient Error: Failed to get result',
                        f'   ip:   {self.__ip}',
                        f'   port: {self.__port}',
                        'Error:',
                        response.response.message
                    ]
                )))
            results = [self.Result(label_id=result.labelId, x1=result.x1, y1=result.y1, x2=result.x2, y2=result.y2, confidence=result.confidence)
                        for result in response.results if self.filter.check(result.labelId)]
            logging.info(f"Retrieved {len(results)} filtered results for image ID: {image_id}")
            return results
        except grpc.RpcError as e:
            logging.error(f"GRPC error in querying results: {str(e)}")
            raise e
