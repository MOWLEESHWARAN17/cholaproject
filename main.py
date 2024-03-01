from fastapi import FastAPI
import numpy as np
from pypdfium2 import PdfDocument
from PIL import Image
import base64, io
from fastapi.responses import JSONResponse
from schema import CKYCSchema
 
 
app = FastAPI()
 
 
def process_pdf_base64(base64_string):
    try:
        # Decode the base64 string to binary content
        pdf_content = base64.b64decode(base64_string)
    
        # Load the PDF document using pypdfium
        pdf = PdfDocument(pdf_content)
    
        details_base64 = None
        photo_base64 = None
    
        # Loop over pages and render
        for i in range(len(pdf)):
            page = pdf[i]
            image = page.render(scale=4).to_pil()
    
            # Example cropping, adjust as per your requirements
            if i == 0:
                img_details = image.crop((40, 471, 2332, 2248))  # Cropping image details
                # img_details.save("img_details.jpg")
                details_buffer = io.BytesIO()
                img_details.save(details_buffer, format="JPEG")
                details_base64 = base64.b64encode(details_buffer.getvalue()).decode('utf-8')
    
                img_photo = image.crop((1549, 1029, 2275, 1836))  # Cropping photo
                # img_photo.save("img_photo.jpg")
                photo_buffer = io.BytesIO()
                img_photo.save(photo_buffer, format="JPEG")
                photo_base64 = base64.b64encode(photo_buffer.getvalue()).decode('utf-8')
    
                return {
                    "ckycImage": details_base64,
                    "photo": photo_base64
                }, 200
                    
    except Exception as Err:
        return {
            'error': str(Err)
        }, 400   
        
           
@app.post("/ckyc/image")
def extract_pdf_details(request: CKYCSchema):
    statusCode = 400
    response = {'message': 'Failed', 'result': {}}
    try:
 
        # Extract details from the PDF
        result, statusCode = process_pdf_base64(request.dataBase)
        response['result'] = result
        if statusCode == 200:
            response['message'] = 'Success'
    except Exception as Err:
        response['result']['error'] = str(Err)
    return JSONResponse(status_code=statusCode, content=result)
 