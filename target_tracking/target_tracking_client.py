import grpc
from typing import List, Dict, Set
import logging

from generated.protos.target_tracking import target_tracking_pb2, target_tracking_pb2_grpc

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TargetTrackingClient:
    """
    Client to interact with Target Tracking service.
    
    Attributes:
        __ip (str): Service IP.
        __port (str): Service port.
        __task_id (int): Unique task identifier.
        filter (Filter): Filter for tracking results.
    """
    
    class Filter:
        """
        Manages included labels for results filtering.
        
        Attributes:
            filter (Set[str]): Set of included label IDs.
        """
        def __init__(self):
            """
            Initializes the filter with all possible label IDs.
            
            Parameters:
                label_cnt (int): The count of all possible labels.
            """
            self.filter: Set[str] = set()
        
        def add(self, label: str) -> None:
            """
            Adds a label ID to the filter.
            
            Parameters:
                label_id (int): Label ID to be added.
            """
            self.filter.add(label)
        
        def remove(self, label: str) -> None:
            """
            Removes a label ID from the filter.
            
            Parameters:
                label_id (int): Label ID to be removed.
            """
            if label in self.filter:
                self.filter.remove(label)
        
        def clear(self) -> None:
            """
            Clears all label IDs from the filter.
            """
            self.filter.clear()
        
        def check(self, label: str) -> bool:
            """
            Checks if a label ID is in the filter.
            
            Parameters:
                label_id (int): Label ID to check.
            
            Returns:
                bool: True if label ID is in the filter, False otherwise.
            """
            return label in self.filter
        
    class BBox:
        def __init__(self, x1: float, y1: float, x2: float, y2: float):
            """
            Represents a bounding box.
            
            Attributes:
                x1, y1, x2, y2 (float): Coordinates of the bounding box.
            """
            self.x1: float = x1
            self.y1: float = y1
            self.x2 = x2
            self.y2: float = y2
    
    def __init__(self, ip: str, port: str, task_id: int):
        """
        Initializes the client with service IP, port, and task ID.
        
        Parameters:
            ip (str): IP address of the Target Detection service.
            port (str): Port of the Target Detection service.
            task_id (int): Task ID for which detections are queried.
        """
        self.__ip = ip
        self.__port = port
        self.__task_id = task_id
        self.__conn = grpc.insecure_channel(f'{ip}:{port}')
        self.__client = target_tracking_pb2_grpc.CommunicateStub(channel=self.__conn)
        logging.info("Initialized TargetTrackingClient with Task ID: {}".format(task_id))
        self.filter = self.Filter()

    def get_result_by_image_id(self, image_id: int, only_the_last: bool) -> Dict[int, List[BBox]]:
        """
        Queries tracking results for an image ID.
        
        Parameters:
            image_id (int): Image ID for query.
            only_the_last (bool): If True, returns only the latest result.
        
        Returns:
            Dict[int, List[BBox]]: Tracking results filtered by labels.
        
        Raises:
            Exception: On failure to get results or GRPC error.
        """
        logging.info(f"Querying results for image ID: {image_id}")
        try:
            request = target_tracking_pb2.GetResultByImageIdRequest(
                taskId=self.__task_id, 
                imageId=image_id, 
                wait=True,
                onlyTheLatest=only_the_last
            )
            response = self.__client.getResultByImageId(request)
            if response.response.code != 200:
                raise(Exception('\n'.join(
                    [
                        'TargetTrackingClient Error: Failed to get result',
                        f'   ip:   {self.__ip}',
                        f'   port: {self.__port}',
                        'Error:',
                        response.response.message
                    ]
                )))
            results: Dict[int, List[TargetTrackingClient.BBox]] = {}
            for result in response.results:
                if not self.filter.check(result.label):
                    continue
                results[result.id] = []
                bboxes = result.bboxs
                for bbox in bboxes:
                    results[result.id].append(TargetTrackingClient.BBox(
                        bbox.x1,
                        bbox.y1,
                        bbox.x2,
                        bbox.y2
                    ))
            logging.info(f"Retrieved {len(results)} filtered results for image ID: {image_id}")
            return results
        except grpc.RpcError as e:
            logging.error(f"GRPC error in querying results: {str(e)}")
            raise e
