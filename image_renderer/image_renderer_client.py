from generated.protos.image_renderer import image_renderer_pb2, image_renderer_pb2_grpc
import grpc
import cv2
import numpy as np
import logging
from typing import Tuple, List


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ImageRendererClient:
    """
    A client to communicate with the Image Renderer service over gRPC to fetch images.
    """

    def __init__(self, ip: str, port: int):
        """
        Initializes the client with the service IP address and port.

        Parameters:
            ip (str): IP address of the Image Renderer service.
            port (int): Port number of the Image Renderer service.
        """
        options = [('grpc.max_receive_message_length', 1024 * 1024 * 1024)]
        self.__conn = grpc.insecure_channel(f'{ip}:{port}', options=options)
        self.__client = image_renderer_pb2_grpc.CommunicateStub(channel=self.__conn)
        logging.info("Image Renderer client initialized.")

    def get_image_buffer_by_image_id(
        self, 
        task_id: int,
        image_id: int, 
        width: int, 
        height: int, 
        format: str = '.jpg', 
        params: List[int] = [cv2.IMWRITE_JPEG_QUALITY, 80]
    ) -> Tuple[int, bytes]:
        """
        Retrieves the image buffer for the specified image ID.

        Parameters:
            task_id (int): The ID of the task.
            image_id (int): The ID of the image to retrieve.
            width (int): The desired width of the image.
            height (int): The desired height of the image.
            format (str): The format of the image. Defaults to '.jpg'.
            params (List[int]): Compression parameters for the image format. Defaults to [cv2.IMWRITE_JPEG_QUALITY, 80].

        Returns:
            A tuple of (int, bytes), where the int is the image ID, and the bytes are the image buffer.
        """
        try:
            request = image_renderer_pb2.GetImageByImageIdRequest(
                taskId=task_id,
                imageRequest=image_renderer_pb2.CustomImageRequest(
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
                logging.warning(f'Failed to retrieve image buffer: {response.response.message}, task ID: {task_id}')
                return 0, b''
            image_id = response.imageResponse.imageId
            buffer = response.imageResponse.buffer
            return image_id, buffer
        except Exception as e:
            logging.error(f'Error in get_image_buffer_by_image_id: {str(e)}')
            return 0, b''
        
    def get_image_by_image_id(
        self, 
        task_id: int,
        image_id: int, 
        width: int, 
        height: int, 
        format: str = '.jpg', 
        params: List[int] = [cv2.IMWRITE_JPEG_QUALITY, 80]
    ) -> Tuple[int, np.ndarray]:
        """
        Fetches an image by its ID with specified parameters.

        Parameters:
            task_id (int): The ID of the task.
            image_id (int): ID of the image to fetch.
            width (int): Desired width of the image.
            height (int): Desired height of the image.
            format (str): Image format (default is JPEG).
            params (List[int]): Encoding parameters (default is [cv2.IMWRITE_JPEG_QUALITY, 80]).

        Returns:
            Tuple[int, np.ndarray]: Image ID and the image as an ndarray. Returns (0, empty array) on failure.
        """
        try:
            image_id, buffer = self.get_image_buffer_by_image_id(task_id, image_id, width, height, format, params)
            if not buffer:
                logging.warning(f'No buffer received for image ID: {image_id}, task ID: {task_id}')
                return 0, np.array([])
            nparr = np.frombuffer(buffer, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if image is None:
                logging.error(f'Failed to decode image for image ID: {image_id}, task ID: {task_id}')
                return image_id, np.array([])
            logging.info(f'Successfully decoded image for image ID: {image_id}, task ID: {task_id}')
            return image_id, image
        except Exception as e:
            logging.error(f'Error in get_image_by_image_id: {str(e)}')
            return 0, np.array([])

    def get_image_size_by_image_id(self, task_id: int, image_id: int) -> Tuple[int, int]:
        """
        Retrieves the width and height of the specified image ID without downloading the image buffer.

        Parameters:
            task_id (int): The ID of the task.
            image_id (int): The ID of the image to retrieve its size.

        Returns:
            A tuple of (int, int), where the first int is the width and the second int is the height of the image.
        """
        try:
            request = image_renderer_pb2.GetImageByImageIdRequest(
                task_id=task_id,
                imageRequest=image_renderer_pb2.CustomImageRequest(
                    imageId=image_id,
                    noImageBuffer=True
                )
            )
            response = self.__client.getImageByImageId(request)
            if response.response.code != 200:
                logging.warning(f'Failed to retrieve image size: {response.response.message}, task ID: {task_id}')
                return 0, 0
            width = response.imageResponse.width
            height = response.imageResponse.height
            logging.info(f'Successfully retrieved image size: {width}x{height} for image ID: {image_id}, task ID: {task_id}')
            return width, height
        except Exception as e:
            logging.error(f'Error in get_image_size_by_image_id: {str(e)}')
            return 0, 0
