
#--------------------LAST MODIFIED : 19/02/2024--------------------#


# Import necessary modules and libraries
from typing import List, Dict, Any, Union, Optional, Type
from fastapi import FastAPI, HTTPException, Body, Query
from pydantic import BaseModel, create_model
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from bson import ObjectId

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
    type: Union[str, Type[int], Type[str], Type[bool], Type[float], Type[Dict]]
    unique: Optional[bool]
    allowed_values: Optional[List[str]] = None
    dict_keys: Optional[Dict[str, str]] = None

class SchemaModel(BaseModel):
    schema_name: str
    fields: List[FieldModel]

class QueryItem(BaseModel):
    field: str
    value: str

#--------------Adding a New Schema--------------#

# Route to add a new schema
@app.post("/add-schema/", tags=["Common routes"])
async def add_schema(schema: SchemaModel = Body(...)) -> Dict[str, Any]:
    # Get the current date
    current_date = datetime.now().strftime("%d/%m/%Y")
    # Convert the schema to a dictionary
    schema_dict = schema.dict()
    # Add the creation date to the schema dictionary
    schema_dict["created_at"] = current_date
    # Get the name of the schema
    schema_name = schema_dict["schema_name"]
    # Check if a schema with the same name already exists
    existing_schema = await collection.find_one({"schema_name": schema_name})
    if existing_schema:
        # If schema already exists, raise an HTTPException
        raise HTTPException(status_code=400, detail="Schema with the same name already exists")
    # Prepare fields for insertion
    fields = []
    # Loop through fields in the schema
    for field in schema.fields:
        # Create a dictionary to represent the field
        field_info = {"col_name": field.col_name, "type": field.type}
        # Add uniqueness information if present
        if field.unique:
            field_info["unique"] = True
        # Add allowed values if present
        if field.allowed_values:
            field_info["allowed_values"] = field.allowed_values
        # Add dictionary keys if present
        if field.dict_keys:
            field_info["dict_keys"] = field.dict_keys
        # Append field information to the list of fields
        fields.append(field_info)
    # Update the schema dictionary with the processed fields
    schema_dict["fields"] = fields
    # Insert the schema into the collection
    await collection.insert_one(schema_dict)
    # Return a success message
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

    # Route to get all items for the specified schema
    @app.get(f"/{schema_name}/", response_model=List[CustomModel], tags=[schema_name])
    async def get_items(page: int = Query(1, gt=0), page_size: int = Query(10, gt=0)) -> List[CustomModel]:
        # Pagination parameters
        skip = (page - 1) * page_size
        # Retrieve items from the schema collection
        items_cursor = db[schema_name].find({}).skip(skip).limit(page_size)
        # Convert cursor to list of items
        items = await items_cursor.to_list(length=None)
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

    # Helper function to parse query string into QueryItem objects
    def parse_query_string(query_string: str) -> List[QueryItem]:
        query_pairs = query_string.split("&")
        query_items = []
        for pair in query_pairs:
            field, value = pair.split(":")
            query_items.append(QueryItem(field=field, value=value))
        return query_items

    # Route to get items by specified fields for the schema
    @app.get(f"/{schema_name}/fields/", response_model=List[Dict[str, Any]], tags=[schema_name])
    async def get_items_by_fields(query_string: str) -> List[Dict[str, Any]]:
        try:
            # Parse the query string into QueryItem objects
            query_items = parse_query_string(query_string)
        except Exception as e:
            raise HTTPException(status_code=400, detail="Invalid query string format")

        # Construct query dictionary
        query = {}
        for item in query_items:
            field = item.field
            value = item.value
            if field in fields:
                query[field] = value

        # Fetch documents from the database matching the query
        items = []
        async for document in db[schema_name].find(query):
            # Convert ObjectId to string
            document["_id"] = str(document["_id"])
            items.append(document)

        return items

    # Route to add an item inside any schema
    @app.post(f"/{schema_name}/", tags=[schema_name])
    async def add_item(item_data: CustomModel = Body(...)) -> Dict[str, Any]:
        async def fetch_schema_definition(schema_name: str) -> SchemaModel:
            # Fetch the schema definition from the database
            schema_definition = await collection.find_one({"schema_name": schema_name})
            if schema_definition:
                return SchemaModel(**schema_definition)
            else:
                return None

        # Retrieve the schema definition
        schema_definition = await fetch_schema_definition(schema_name)
        if not schema_definition:
            raise HTTPException(status_code=404, detail="Schema not found")

        # Validate uniqueness constraints for fields with unique=True
        for field in schema_definition.fields:
            if field.unique:
                # Check if the value already exists in the collection for fields with unique constraint
                existing_item = await db[schema_name].find_one({field.col_name: item_data.dict().get(field.col_name)})
                if existing_item:
                    raise HTTPException(status_code=400, detail=f"{field.col_name} must be unique")

        # Validate allowed values for specific fields
        for field in schema_definition.fields:
            if field.allowed_values:
                field_value = item_data.dict().get(field.col_name)
                if field_value not in field.allowed_values:
                    raise HTTPException(status_code=400, detail=f"Invalid value for {field.col_name}")

        # Validate dict field keys against specified dict_keys
        for field in schema_definition.fields:
            if field.type == "dict" and field.dict_keys:
                field_value = item_data.dict().get(field.col_name, {})
                for key in field_value.keys():
                    if key not in field.dict_keys:
                        raise HTTPException(status_code=400, detail=f"Invalid key for {field.col_name}: {key}")

        # Insert the item data into the collection
        await db[schema_name].insert_one(item_data.dict())
        return {"message": "Item added successfully"}

    # Route to update an item in a schema
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
