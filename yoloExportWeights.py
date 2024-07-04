from ultralytics import YOLO
from PIL import Image

# Load the YOLOv8 model
model = YOLO("yolov8n-cls.pt")
model.cpu()

results = model(Image.open("bus.jpg"))
print(results)
# Export the model to ONNX format
model.export(format="onnx")  # creates 'yolov8n.onnx'

# Load the exported ONNX model
model = YOLO("yolov8n-cls.onnx")