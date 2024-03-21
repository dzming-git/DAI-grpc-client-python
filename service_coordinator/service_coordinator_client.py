from generated.protos.service_coordinator import service_coordinator_pb2, service_coordinator_pb2_grpc
import grpc
import logging
from typing import Dict, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ServiceCoordinatorClient:
    """
    A client to communicate with the Service Coordinator service over gRPC.
    """

    def __init__(self, ip: str, port: int):
        """
        Initializes the client with the service IP address and port.

        Parameters:
            ip (str): IP address of the Service Coordinator service.
            port (int): Port number of the Service Coordinator service.
        """
        self.__conn = grpc.insecure_channel(f'{ip}:{port}')
        self.__client = service_coordinator_pb2_grpc.CommunicateStub(channel=self.__conn)
        logging.info("Service Coordinator client initialized.")

    def inform_previous_service_info(self, task_id: str, pre_service_name: str, pre_service_ip: str, pre_service_port: str, args: Dict[str, str] = {}) -> bool:
        """
        Informs the Service Coordinator about the information of the previous service.

        Parameters:
            task_id (str): The ID of the task.
            pre_service_name (str): The name of the previous service.
            pre_service_ip (str): The IP address of the previous service.
            pre_service_port (str): The port number of the previous service.
            args (Dict[str, str]): Additional arguments.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        try:
            request = service_coordinator_pb2.InformPreviousServiceInfoRequest(
                taskId=task_id,
                preServiceName=pre_service_name,
                preServiceIp=pre_service_ip,
                preServicePort=pre_service_port
            )
            if isinstance(args, dict):
                for key, value in args.items():
                    arg = request.args.add()
                    arg.key = key
                    arg.value = value
            response = self.__client.informPreviousServiceInfo(request)
            if response.response.code != 200:
                logging.error(f'Failed to inform previous service info: {response.response.message}, Task ID: {task_id}')
                return False
            logging.info(f'Successfully informed previous service info, Task ID: {task_id}')
            return True
        except grpc.RpcError as e:
            logging.error(f'gRPC error in inform_previous_service_info: {e}, Task ID: {task_id}')
            return False

    def inform_current_service_info(self, task_id: str, args: Dict[str, str] = {}) -> Tuple[bool, Dict[str, str]]:
        """
        Informs the Service Coordinator about the configuration and state of the current service.
        This method also returns any output arguments provided by the Service Coordinator, 
        which could include dynamically generated configuration data or status information.

        Parameters:
            task_id (str): The unique identifier of the task for which the service info is being informed.
            args (Dict[str, str]): A dictionary of arguments describing the current service's state or configuration. 
                These could include details such as the version of the service, runtime parameters, etc.

        Returns:
            Tuple[bool, Dict[str, str]]: A tuple containing a boolean flag indicating whether the operation was successful,
                and a dictionary of output arguments returned by the Service Coordinator. The dictionary is empty if the 
                operation was unsuccessful or if no output arguments were provided by the Service Coordinator.
        """
        try:
            request = service_coordinator_pb2.InformCurrentServiceInfoRequest(taskId=task_id)
            # Convert the input args dictionary to the protobuf message format
            if isinstance(args, dict):
                for key, value in args.items():
                    arg = request.args.add()
                    arg.key = key
                    arg.value = value
            
            # Make the gRPC call to inform the Service Coordinator
            response = self.__client.informCurrentServiceInfo(request)
            
            # Check the response code to determine if the operation was successful
            if response.response.code != 200:
                logging.error(f'Failed to inform current service info: {response.response.message}, Task ID: {task_id}')
                return False, {}
            
            # Extract output arguments from the response
            args_output: Dict[str, str] = {}
            for arg in response.args:
                args_output[arg.key] = arg.value
            
            logging.info(f'Successfully informed current service info, Task ID: {task_id}')
            return True, args_output
        except grpc.RpcError as e:
            logging.error(f'gRPC error in inform_current_service_info: {e}, Task ID: {task_id}')
            return False, {}
        
    def start(self, task_id: str) -> bool:
        """
        Requests the Service Coordinator to start a service or task based on the given task ID.

        Parameters:
            task_id (str): The unique identifier of the task to start.

        Returns:
            bool: True if the request to start the service or task was successful, False otherwise.
        """
        try:
            request = service_coordinator_pb2.StartRequest(taskId=task_id)
            response = self.__client.start(request)
            if response.response.code != 200:
                logging.error(f'Failed to start task: {response.response.message}, Task ID: {task_id}')
                return False
            logging.info(f'Successfully started task, Task ID: {task_id}')
            return True
        except Exception as e:
            logging.error(f'Error in starting task: {str(e)}, Task ID: {task_id}')
            return False

    def stop(self, task_id: str) -> bool:
        """
        Requests the Service Coordinator to stop a service or task based on the given task ID.

        Parameters:
            task_id (str): The unique identifier of the task to stop.

        Returns:
            bool: True if the request to stop the service or task was successful, False otherwise.
        """
        try:
            request = service_coordinator_pb2.StopRequest(taskId=task_id)
            response = self.__client.stop(request)
            if response.response.code != 200:
                logging.error(f'Failed to stop task: {response.response.message}, Task ID: {task_id}')
                return False
            logging.info(f'Successfully stopped task, Task ID: {task_id}')
            return True
        except Exception as e:
            logging.error(f'Error in stopping task: {str(e)}, Task ID: {task_id}')
            return False
