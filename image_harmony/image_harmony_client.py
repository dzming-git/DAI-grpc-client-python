from generated.protos.image_harmony import image_harmony_pb2, image_harmony_pb2_grpc
import grpc
import cv2
from typing import Tuple, List
import numpy as np
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ImageHarmonyClient:
    """
    A client for communicating with the Image Harmony service using gRPC.

    Attributes:
        __conn (grpc.Channel): The gRPC channel for communication.
        __client (CommunicateStub): The gRPC stub for the Communicate service.
        __connection_id (int): The ID of the current connection to the Image Harmony service.
    """
    def __init__(self, ip: str, port: str):
        """
        Initializes the Image Harmony client with the specified IP address and port.

        Parameters:
            ip (str): The IP address of the Image Harmony service.
            port (str): The port number of the Image Harmony service.
        """
        options = [('grpc.max_receive_message_length', 1024 * 1024 * 1024)]
        self.__conn = grpc.insecure_channel(f'{ip}:{port}', options=options)
        self.__client = image_harmony_pb2_grpc.CommunicateStub(channel=self.__conn)
        self.__connection_id = 0
        logging.info("Image Harmony client initialized")

    def connect_image_loader(self, loader_args_hash: int) -> Tuple[bool, str]:
        """
        Connects to the image loader using the provided loader arguments hash.

        Parameters:
            loader_args_hash (int): The hash value of the loader arguments.

        Returns:
            A tuple of (bool, str), where the bool indicates success (True) or failure (False),
            and the str provides a relevant message.
        """
        try:
            request = image_harmony_pb2.ConnectImageLoaderRequest(loaderArgsHash=loader_args_hash)
            response = self.__client.connectImageLoader(request)
            self.__connection_id = response.connectionId
            logging.info(f'Connected to image loader: {response.response.message}')
            return True, response.response.message
        except grpc.RpcError as e:
            logging.error(f'Failed to connect image loader: {e}')
            return False, f'gRPC error: {str(e)}'

    def disconnect_image_loader(self) -> Tuple[bool, str]:
        """
        Disconnects the current connection to the image loader.

        Returns:
            A tuple of (bool, str), where the bool indicates success (True) or failure (False),
            and the str provides a relevant message.
        """
        if self.__connection_id:
            try:
                request = image_harmony_pb2.DisconnectImageLoaderRequest(connectionId=self.__connection_id)
                response = self.__client.disconnectImageLoader(request)
                self.__connection_id = 0
                logging.info(f'Disconnected from image loader: {response.response.message}')
                return True, 'Disconnected from image loader'
            except grpc.RpcError as e:
                logging.error(f'Failed to disconnect image loader: {e}')
                return False, f'gRPC error: {str(e)}'
        return True, 'Not connected to image loader'

    def get_image_buffer_by_image_id(
        self, 
        image_id: int, 
        width: int, 
        height: int, 
        format: str = '.jpg', 
        params: List[int] = [cv2.IMWRITE_JPEG_QUALITY, 80]
    ) -> Tuple[int, bytes]:
        """
        Retrieves the image buffer for the specified image ID.

        Parameters:
            image_id (int): The ID of the image to retrieve.
            width (int): The desired width of the image.
            height (int): The desired height of the image.
            format (str): The format of the image. Defaults to '.jpg'.
            params (List[int]): Compression parameters for the image format. Defaults to [cv2.IMWRITE_JPEG_QUALITY, 80].

        Returns:
            A tuple of (int, bytes), where the int is the image ID, and the bytes are the image buffer.
        """
        request = image_harmony_pb2.GetImageByImageIdRequest(
            connectionId=self.__connection_id,
            imageRequest=image_harmony_pb2.CustomImageRequest(
                imageId=image_id,
                noImageBuffer=False,
                format=format,
                params=params,
                expectedW=width,
                expectedH=height
            )
        )
        response = self.__client.getImageByImageId(request)
        if response.response.code != 200:
            return 0, b''
        
        image_id = response.imageResponse.imageId
        buffer = response.imageResponse.buffer
        return image_id, buffer

    def get_image_by_image_id(
        self, 
        image_id: int, 
        width: int, 
        height: int, 
        format: str = '.jpg', 
        params: List[int] = [cv2.IMWRITE_JPEG_QUALITY, 80]
    ) -> Tuple[int, np.ndarray]:
        """
        Parameters:
            image_id (int): The ID of the image to retrieve.
            width (int): The desired width of the image.
            height (int): The desired height of the image.
            format (str): The format of the image. Defaults to '.jpg'.
            params (List[int]): Compression parameters for the image format. Defaults to [cv2.IMWRITE_JPEG_QUALITY, 80].

        Returns:
            A tuple of (int, np.ndarray), where the int is the image ID, and the np.ndarray is the decoded OpenCV image.
        """
        try:
            image_id, buffer = self.get_image_buffer_by_image_id(image_id, width, height, format, params)
            if not buffer:
                logging.warning(f'No buffer received for image ID: {image_id}')
                return 0, np.array([])
            nparr = np.frombuffer(buffer, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if image is None:
                logging.error(f'Failed to decode image for image ID: {image_id}')
                return image_id, np.array([])
            logging.info(f'Successfully decoded image for image ID: {image_id}')
            return image_id, image
        except Exception as e:
            logging.error(f'Error in get_image_by_image_id: {str(e)}')
            return 0, np.array([])

    def get_image_size_by_image_id(self, image_id: int) -> Tuple[int, int]:
        """
        Retrieves the width and height of the specified image ID without downloading the image buffer.

        Parameters:
            image_id (int): The ID of the image to retrieve its size.

        Returns:
            A tuple of (int, int), where the first int is the width and the second int is the height of the image.
        """
        try:
            request = image_harmony_pb2.GetImageByImageIdRequest(
                connectionId=self.__connection_id,
                imageRequest=image_harmony_pb2.CustomImageRequest(
                    imageId=image_id,
                    noImageBuffer=True
                )
            )
            response = self.__client.getImageByImageId(request)
            if response.response.code != 200:
                logging.warning(f'Failed to retrieve image size: {response.response.message}')
                return 0, 0
            width = response.imageResponse.width
            height = response.imageResponse.height
            logging.info(f'Successfully retrieved image size: {width}x{height} for image ID: {image_id}')
            return width, height
        except Exception as e:
            logging.error(f'Error in get_image_size_by_image_id: {str(e)}')
            return 0, 0
