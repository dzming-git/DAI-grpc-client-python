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
        self.__ip: str = ip
        self.__port: str = port
        options = [('grpc.max_receive_message_length', 1024 * 1024 * 1024)]
        self.__conn = grpc.insecure_channel(f'{ip}:{port}', options=options)
        self.__client = image_harmony_pb2_grpc.CommunicateStub(channel=self.__conn)
        self.__connection_id = 0
        logging.info("Image Harmony client initialized")

    def connect_image_loader(self, loader_args_hash: int) -> None:
        """
        Connects to the image loader using the provided loader arguments hash.

        Parameters:
            loader_args_hash (int): The hash value of the loader arguments.
        """
        try:
            request = image_harmony_pb2.ConnectImageLoaderRequest(loaderArgsHash=loader_args_hash)
            response = self.__client.connectImageLoader(request)
            # Check the response code to determine if the operation was successful
            if response.response.code != 200:
                raise(Exception('\n'.join(
                    [
                        'Failed to connect image loader',
                       f'Loader args hash: {loader_args_hash}',
                        'Image Hramony gRPC Info:',
                       f'   ip:   {self.__ip}',
                       f'   port: {self.__port}',
                        'Error:',
                        response.response.message
                    ]
                )))
            self.__connection_id = response.connectionId
            logging.info(f'Connected to image loader: {response.response.message}')
        except grpc.RpcError as e:
            raise(f'gRPC error in connect_image_loader: \n{e}') from e

    def disconnect_image_loader(self) -> None:
        """
        Disconnects the current connection to the image loader.
        """
        if 0 == self.__connection_id:
            return
        try:
            request = image_harmony_pb2.DisconnectImageLoaderRequest(connectionId=self.__connection_id)
            response = self.__client.disconnectImageLoader(request)
            # Check the response code to determine if the operation was successful
            if response.response.code != 200:
                raise(Exception('\n'.join(
                    [
                        'Failed to disconnect image loader',
                        'Image Hramony gRPC Info:',
                       f'   ip:   {self.__ip}',
                       f'   port: {self.__port}',
                        'Error:',
                        response.response.message
                    ]
                )))
            self.__connection_id = 0
            logging.info(f'Disconnected from image loader: {response.response.message}')
            return True, 'Disconnected from image loader'
        except grpc.RpcError as e:
            raise(f'gRPC error in disconnect_image_loader: \n{e}') from e

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
        try:
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
            # Check the response code to determine if the operation was successful
            if response.response.code != 200:
                raise(Exception('\n'.join(
                    [
                        'Failed to retrieve image buffer',
                        'Image Hramony gRPC Info:',
                       f'   ip:   {self.__ip}',
                       f'   port: {self.__port}',
                        'Error:',
                        response.response.message
                    ]
                )))
            image_id = response.imageResponse.imageId
            buffer = response.imageResponse.buffer
            return image_id, buffer
        except grpc.RpcError as e:
            raise(f'gRPC error in get_image_buffer_by_image_id: \n{e}') from e

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
        image_id, buffer = self.get_image_buffer_by_image_id(image_id, width, height, format, params)
        nparr = np.frombuffer(buffer, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if image is None:
            raise(Exception('\n'.join(
                [
                    f'Failed to decode image for image ID: {image_id}',
                    'Image Hramony gRPC Info:',
                    f'   ip:   {self.__ip}',
                    f'   port: {self.__port}',
                ]
            )))
        logging.info(f'Successfully decoded image for image ID: {image_id}')
        return image_id, image


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
                raise(Exception('\n'.join(
                    [
                        'Failed to retrieve image size',
                        'Image Hramony gRPC Info:',
                        f'   ip:   {self.__ip}',
                        f'   port: {self.__port}',
                        'Error:',
                        response.response.message
                    ]
                )))
            width = response.imageResponse.width
            height = response.imageResponse.height
            logging.info(f'Successfully retrieved image size: {width}x{height} for image ID: {image_id}')
            return width, height
        except grpc.RpcError as e:
            raise(f'gRPC error in get_image_size_by_image_id: \n{e}') from e
