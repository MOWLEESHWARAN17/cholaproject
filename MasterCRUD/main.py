
#--------------------LAST MODIFIED : 29/02/2024--------------------#


# Import necessary modules and libraries
from typing import List, Dict, Any, Union, Optional, Type
from fastapi import FastAPI, HTTPException, Body, Query,File, UploadFile
from pydantic import BaseModel, create_model
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from bson import ObjectId
import pandas as pd
import json
from fastapi.responses import FileResponse
import tempfile

# Initialize FastAPI app
app = FastAPI(title="MASTERLIST")

# Initialize MongoDB client and database
client = AsyncIOMotorClient("mongodb://localhost:27017/")
db = client["databasename"]
collection = db["masterlist"]

# Add CORS middleware for cross-origin resource sharing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

#--------------Basemodels--------------#

# Define Pydantic models for database schema and fields
class FieldModel(BaseModel):
    col_name: str
    type: Union[str, int, float, bool, Dict[str, Any]]
    unique: Optional[bool]
    allowed_values: Optional[List[str]] = None
    dict_keys: Optional[Dict[str, str]] = None

class SchemaModel(BaseModel):
    schema_name: str
    fields: List[FieldModel]

class FilterData(BaseModel):
    filter: str

class FilterItem:
    def __init__(self, field: str, value: str):
        self.field = field
        self.value = value

    

#--------------Adding a New Schema--------------#


@app.post("/add-schema/", tags=["Common routes"])
async def add_schema(schema_data: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    # Check if 'schema_name' and 'fields' are present in the request body
    if "schema_name" not in schema_data or "fields" not in schema_data:
        raise HTTPException(status_code=400, detail="Schema data is missing in the request body")

    # Retrieve schema_name and fields from the request body
    schema_name = schema_data["schema_name"].lower()

    # Check for spaces in schema_name
    if " " in schema_name:
        raise HTTPException(status_code=400, detail="Schema name cannot contain spaces")

    fields = schema_data["fields"]

    # Check if schema with the same name already exists
    existing_schema = await collection.find_one({"schema_name": schema_name})
    if existing_schema:
        raise HTTPException(status_code=400, detail="Schema with the same name already exists")

    # Prepare fields for insertion
    processed_fields = []
    for field in fields:
        field_info = {"col_name": field["col_name"], "type": field["type"]}
        if "unique" in field:
            field_info["unique"] = field["unique"]
        if "allowed_values" in field:
            field_info["allowed_values"] = field["allowed_values"]
        if "dict_keys" in field:
            field_info["dict_keys"] = field["dict_keys"]
        processed_fields.append(field_info)

    # Insert the schema into the collection
    schema_dict = {
        "schema_name": schema_name,
        "fields": processed_fields,
        "created_at": datetime.now().strftime("%d/%m/%Y")
    }
    await collection.insert_one(schema_dict)

    return {"message": "Schema added successfully"}



#--------------Replacing fields in schema--------------#

# Route to replace fields in a schema
@app.put("/replacefields/{schema_name}", tags=["Common routes"])
async def replace_schema_fields(schema_name: str, new_fields: List[Dict[str, Any]]) -> Dict[str, str]:
    # Check if the schema exists
    existing_schema = await collection.find_one({"schema_name": schema_name})
    if not existing_schema:
        # If schema does not exist, raise an HTTPException
        raise HTTPException(status_code=404, detail="Schema not found")
    # Prepare the new schema data
    new_schema_data = {
        "schema_name": schema_name,
        "created_at": datetime.now().strftime("%d/%m/%Y"),
        "fields": new_fields
    }
    # Replace the existing schema with the new schema data
    await collection.replace_one(
        {"schema_name": schema_name},
        new_schema_data
    )
    # Return a success message
    return {"message": f"Schema '{schema_name}' fields replaced successfully"}


#--------------Generate routing for adding data inside schema--------------#

# Function to retrieve all schemas from the database
async def get_schemas() -> List[SchemaModel]:
    schemas = []
    # Iterate over documents in the collection
    async for document in collection.find({}):
        # Convert each document to a SchemaModel object and append to the list
        schema = SchemaModel(**document)
        schemas.append(schema)
    return schemas

# Function to set up routes for each schema
async def setup_routes():
    # Retrieve all schemas from the database
    schemas = await get_schemas()
    # Generate routes for each schema
    for schema in schemas:
        await generate_routes_from_schema(schema)

# Add event handler to set up routes on startup
app.add_event_handler("startup", setup_routes)

# Function to find an existing item in a schema
async def find_existing_item(schema_name: str, col_name: str, value: Any) -> Optional[Dict[str, Any]]:
    # Find an item in the schema collection by column name and value
    item = await collection.find_one({col_name: value})
    return item

# Function to generate routes for a given schema
async def generate_routes_from_schema(schema: SchemaModel):
    # Extract schema name and fields
    schema_name = schema.schema_name
    fields = {field.col_name: field for field in schema.fields}

    # Dynamically create a Pydantic model for the schema
    CustomModel = create_model(schema_name, **{field.col_name: (field.type, ...) for field in fields.values()})


    @app.get(f"/{schema_name}/", response_model=List[Dict[str, Any]], tags=[schema_name])
    async def get_items(page: int = Query(0, ge=0), page_size: int = Query(10, gt=0)) -> List[Dict[str, Any]]:
        # Pagination parameters
        skip = (page - 1) * page_size
        # Retrieve items from the schema collection
        items_cursor = db[schema_name].find({}).skip(skip).limit(page_size)
        # Convert cursor to list of items
        items = await items_cursor.to_list(length=None)
        # Convert ObjectId to string and include it in the response
        for item in items:
            item['_id'] = str(item['_id'])
        return items



    # Route to get an item by ID for the specified schema
    @app.get(f"/{schema_name}/{{id}}", response_model=CustomModel, tags=[schema_name])
    async def get_item_by_id(id: str) -> CustomModel:
        # Find item by ID in the schema collection
        item = await db[schema_name].find_one({"_id": ObjectId(id)})
        if item:
            return item
        else:
            raise HTTPException(status_code=404, detail=f"Item not found for ID: {id}")


    @app.post(f"/{schema_name}/filters/", response_model=List[Dict[str, Any]], tags=[schema_name])
    async def get_items_by_fields(filter_data: FilterData = Body(...)) -> List[Dict[str, Any]]:
        filter_str = filter_data.filter
        filter_items = parse_filter_string(filter_str)
        
        query = {}
        for item in filter_items:
            query[item.field] = item.value

        items = []
        async for document in db[schema_name].find(query):
            document["_id"] = str(document["_id"])
            items.append(document)

        return items

    @app.post(f"/{schema_name}/", tags=[schema_name])
    async def add_item(item_data: CustomModel = Body(...)) -> Dict[str, Any]:
            async def fetch_schema_definition(schema_name: str) -> SchemaModel:
                # Fetch the schema definition from the database
                schema_definition = await collection.find_one({"schema_name": schema_name})
                if schema_definition:
                    return SchemaModel(**schema_definition)
                else:
                    raise HTTPException(status_code=404, detail="Schema not found")

            # Retrieve the schema definition
            schema_definition = await fetch_schema_definition(schema_name)

            # Add the "modified_date" field with the current date
            modified_date = datetime.now().strftime("%d/%m/%Y")
            item_data_dict = item_data.dict()
            item_data_dict["modified_date"] = modified_date

            # Validate uniqueness constraints, allowed values, and dict field keys
            for field in schema_definition.fields:
                if field.unique:
                    existing_item = await db[schema_name].find_one({field.col_name: item_data_dict[field.col_name]})
                    if existing_item:
                        raise HTTPException(status_code=400, detail=f"{field.col_name} must be unique")

                if field.allowed_values:
                    if item_data_dict[field.col_name] not in field.allowed_values:
                        raise HTTPException(status_code=400, detail=f"Invalid value for {field.col_name}")

                if field.type == "dict" and field.dict_keys:
                    field_value = item_data_dict.get(field.col_name, {})
                    for key in field_value.keys():
                        if key not in field.dict_keys:
                            raise HTTPException(status_code=400, detail=f"Invalid key for {field.col_name}: {key}")

            # Insert the item data into the collection
            await db[schema_name].insert_one(item_data_dict)
            return {"message": "Item added successfully"}



    @app.post(f"/{schema_name}/import", tags=[schema_name])
    async def import_data(file: UploadFile = File(...)):
        # Retrieve the schema definition
        schema_definition = await collection.find_one({"schema_name": schema_name})
        if not schema_definition:
            raise HTTPException(status_code=404, detail="Schema not found")

        # Read Excel or CSV file
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file.file)
        elif file.filename.endswith('.xlsx'):
            df = pd.read_excel(file.file)
        else:
            raise HTTPException(status_code=400, detail="Only Excel (xlsx) or CSV files are allowed")

        # Check if "modified_date" column exists in DataFrame
        if "modified_date" in df.columns:
            # Replace values in "modified_date" column with today's date
            df["modified_date"] = datetime.now().strftime('%d/%m/%Y')

        # Validate data against schema
        valid_data = []
        invalid_data = []
        for _, row in df.iterrows():
            item_data = {}
            invalid_item = False
            error_details = []

            # Validate each field against schema
            for field in schema_definition['fields']:
                col_name = field['col_name']
                if col_name not in row:
                    invalid_item = True
                    error_details.append(f"Missing column: {col_name}")
                    continue

                # Check if the field is a nested dictionary
                if field['type'] == 'dict':
                    try:
                        item_data[col_name] = json.loads(row[col_name].replace("'", '"'))
                    except json.JSONDecodeError:
                        invalid_item = True
                        error_details.append(f"Invalid JSON format for column: {col_name}")
                else:
                    # Validate field with allowed values if the key exists
                    if 'allowed_values' in field and row[col_name] not in field['allowed_values']:
                        invalid_item = True
                        error_details.append(f"Invalid value for {col_name}")

                    # Validate field with unique constraint
                    if field.get('unique'):
                        existing_item = await db[schema_name].find_one({col_name: row[col_name]})
                        if existing_item:
                            invalid_item = True
                            error_details.append(f"{col_name} must be unique")

                    # Validate dict field keys
                    if field['type'] == 'dict' and field['dict_keys']:
                        field_value = json.loads(row[col_name].replace("'", '"'))
                        for key in field_value.keys():
                            if key not in field['dict_keys']:
                                invalid_item = True
                                error_details.append(f"Invalid key for {col_name}: {key}")

                    item_data[col_name] = row[col_name]

            # Add the "modified_date" field with the current date
            item_data["modified_date"] = datetime.now().strftime("%d/%m/%Y")

            # If the item is valid, add it to valid_data, otherwise to invalid_data
            if invalid_item:
                invalid_data.append({"data": row.to_dict(), "errors": error_details})
            else:
                valid_data.append(item_data)

        # Insert valid data into the collection
        if valid_data:
            await db[schema_name].insert_many(valid_data)
        if len(invalid_data) == 0:
            s = "All"
        elif len(valid_data) == 0:
            s = "No"
        else:
            s = "Some"
        return {"message": f"{s} datas imported ", "invalid_data": invalid_data}



            
    @app.post(f"/export/{schema_name}/", tags=[schema_name])
    async def export_csv(date: str = Query(..., title="Date", description="Date in the format DD/MM/YYYY")):
        try:
            # Call the export_data_to_csv function to export data to CSV
            csv_filename = await export_data_to_csv(schema_name, date)
            
            # Read the contents of the CSV file
            with open(csv_filename, 'r') as file:
                csv_content = file.read()
            
            # Create a temporary file to write the CSV content
            with tempfile.NamedTemporaryFile(mode='w+t', suffix='.csv', delete=False) as temp_file:
                temp_file.write(csv_content)
                temp_filename = temp_file.name
            
            # Return the temporary file as a FileResponse
            return FileResponse(temp_filename, media_type="text/csv", filename=f"{schema_name}_{date}.csv")
        except Exception as e:
            # Handle any exceptions and return appropriate response
            return {"error": str(e)}


    @app.put(f"/{schema_name}/{{id}}", tags=[schema_name])
    async def update_schema_item(id: str, updated_fields: Dict[str, Any]) -> Dict[str, str]:
        try:
            # Convert ID to ObjectId
            object_id = ObjectId(id)
        except Exception as e:
            raise HTTPException(status_code=400, detail="Invalid ObjectId")

        # Get the collection for the schema
        lcollection = db[schema_name]
        # Find the schema definition
        schema_definition = await collection.find_one({"schema_name": schema_name})

        if schema_definition:
            for field_name, updated_value in updated_fields.items():
                # Check if the field exists in the schema's fields
                field_exists = any(field["col_name"] == field_name for field in schema_definition["fields"])
                if not field_exists:
                    raise HTTPException(status_code=400, detail=f"Field '{field_name}' not found in schema '{schema_name}'")

                # Validate the field value against allowed values if the field is a string
                for field in schema_definition["fields"]:
                    if field["col_name"] == field_name and field["type"] == "str" and "allowed_values" in field:
                        if updated_value not in field["allowed_values"]:
                            raise HTTPException(status_code=400, detail=f"Invalid value for {field_name}. Allowed values are: {', '.join(field['allowed_values'])}")

                # Check if the field is marked as unique
                for field in schema_definition["fields"]:
                    if field["col_name"] == field_name and field.get("unique", False):
                        existing_item_with_value = await lcollection.find_one({field_name: updated_value})
                        if existing_item_with_value and existing_item_with_value["_id"] != object_id:
                            raise HTTPException(status_code=400, detail=f"{field_name} must be unique")
                        break

                # For list and dict types, validate against allowed_values and dict_keys
                for field in schema_definition["fields"]:
                    if field["col_name"] == field_name and field["type"] in ["list", "dict"]:
                        if "allowed_values" in field and updated_value not in field["allowed_values"]:
                            raise HTTPException(status_code=400, detail=f"Invalid value for {field_name}. Allowed values are: {', '.join(field['allowed_values'])}")
                        if "dict_keys" in field and not all(key in updated_value for key in field["dict_keys"]):
                            raise HTTPException(status_code=400, detail=f"Missing keys for {field_name}. Required keys are: {', '.join(field['dict_keys'])}")

                # Update the field
                await lcollection.update_one({"_id": object_id}, {"$set": {field_name: updated_value}})

            return {"message": f"Fields updated successfully for item with ID '{id}' in collection '{schema_name}'"}
        else:
            return {"message": f"Schema '{schema_name}' not found"}

    # @app.post(f"/export/{schema_name}/", tags=[schema_name])
    # async def export_csv(date: str = Query(..., title="Date", description="Date in the format DD/MM/YYYY")):
    #     try:
    #         # Call the export_data_to_csv function to export data to CSV
    #         csv_filename = await export_data_to_csv(schema_name, date)
            
    #         # Read the contents of the CSV file
    #         with open(csv_filename, 'rb') as file:
    #             csv_content = file.read()
            
    #         # Return the CSV content as a response
    #         return Response(content=csv_content, media_type="application/vnd.ms-excel", headers={"Content-Disposition": f"attachment; filename={schema_name}_{date}.xlsx"})
    #     except Exception as e:
    #         # Handle any exceptions and raise an HTTPException
    #         raise HTTPException(status_code=500, detail=str(e))


# Route to get fields of a schema
@app.get("/getfields/{schema_name}/", tags=["Common routes"])
async def get_schema_field(schema_name: str) -> Dict[str, Any]:
    # Find the schema in the collection
    schema_data = await collection.find_one({"schema_name": schema_name})
    if not schema_data:
        # If schema not found, raise an HTTPException
        raise HTTPException(status_code=404, detail="Schema not found")
    
    # Convert ObjectId to string
    schema_data["_id"] = str(schema_data["_id"])

    return schema_data
  
#--------------Get schema names with date--------------#
@app.get("/get-schema-names-with-date/", tags=["Common routes"])
async def get_schema_names_with_date(page: int = Query(1, gt=0), page_size: int = Query(10, gt=0)) -> Dict[str, Any]:
    skip = (page - 1) * page_size
    schemas_cursor = collection.find({}, {"schema_name": 1, "created_at": 1, "_id": 0}).skip(skip).limit(page_size)
    schemas = await schemas_cursor.to_list(length=None)
    total_schemas = await collection.count_documents({})
    total_pages = -(-total_schemas // page_size)  # Ceiling division to calculate total pages
    return {
        "schemas": schemas,
        "total_schemas": total_schemas,
        "total_pages": total_pages,
        "current_page": page
    }


#--------------Function for exporting data as csv,xlsx,xls--------------#
async def export_data_to_csv(schema_name: str, date: str) -> str:
    try:
        lcollection = db[schema_name]
        data_cursor = lcollection.find({"modified_date": date})

        # Convert the cursor to a list of documents
        data = await data_cursor.to_list(length=None)

        if not data:
            raise HTTPException(status_code=404, detail="No data found for the provided date")

        # Convert the data to a DataFrame
        df = pd.DataFrame(data)

        # Format date to ensure a valid file name
        formatted_date = date.replace('/', '_')

        # Generate file name based on schema name and date
        csv_filename = f"{schema_name}_{formatted_date}.csv"

        # Export the DataFrame to a CSV file
        df.to_csv(csv_filename, index=False)

        return csv_filename
    except Exception as e:
        # Handle any exceptions and raise an HTTPException
        raise HTTPException(status_code=500, detail=str(e))




def parse_filter_string(filter_str: str) -> List[FilterItem]:
    filters = filter_str.split(',')
    filter_items = []
    for f in filters:
        field, value = f.split(':')
        filter_items.append(FilterItem(field=field, value=value))
    return filter_items



