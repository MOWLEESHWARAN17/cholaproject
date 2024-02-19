from typing import List, Dict, Any, Union, Optional, Type
from fastapi import FastAPI, HTTPException, Body, Query
from pydantic import BaseModel, create_model
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from bson import json_util

app = FastAPI(title="MASTERLIST")
client = AsyncIOMotorClient("mongodb://localhost:27017/")
db = client["databasename"]
collection = db["masterlist"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

#--------------Base Models--------------#
class FieldModel(BaseModel):
    col_name: str
    type: Union[str, Type[int], Type[str], Type[bool], Type[float], Type[List], Type[Dict[str, Any]]]
    unique: Optional[bool]
    selected_value: Optional[str] = None
    allowed_values: Optional[List[str]] = None
    dict_keys: Optional[Dict[str, Any]] = None

class SchemaModel(BaseModel):
    schema_name: str
    fields: List[FieldModel]

#--------------Adding a New Schema--------------#
@app.post("/add-schema/")
async def add_schema(schema: SchemaModel = Body(...)) -> Dict[str, Any]:
    current_date = datetime.now().strftime("%d/%m/%Y")
    schema_dict = schema.dict()
    schema_dict["created_at"] = current_date
    schema_name = schema_dict["schema_name"]
    existing_schema = await collection.find_one({"schema_name": schema_name})
    if existing_schema:
        raise HTTPException(status_code=400, detail="Schema with the same name already exists")
    fields = []
    for field in schema.fields:
        field_info = {"col_name": field.col_name}
        if field.type == "list":
            field_info["type"] = "list"
            field_info["allowed_values"] = field.allowed_values if field.allowed_values else []
        elif field.type == "dict":
            field_info["type"] = "dict"
            field_info["dict_keys"] = field.dict_keys if field.dict_keys else {}
        else:
            field_info["type"] = field.type
            field_info["unique"] = field.unique
        fields.append(field_info)
    schema_dict["fields"] = fields
    await collection.insert_one(schema_dict)
    return {"message": "Schema added successfully"}

#--------------Replacing fields in schema--------------#
@app.put("/replace-schema-fields/{schema_name}")
async def replace_schema_fields(schema_name: str, new_fields: List[Dict[str, Any]]) -> Dict[str, str]:
    existing_schema = await collection.find_one({"schema_name": schema_name})
    if not existing_schema:
        raise HTTPException(status_code=404, detail="Schema not found")
    new_schema_data = {
        "schema_name": schema_name,
        "created_at": datetime.now().strftime("%d/%m/%Y"),
        "fields": new_fields
    }
    for field in new_schema_data["fields"]:
        if field.get("unique") == "true":
            field["unique"] = True
        elif field.get("unique") == "false":
            field["unique"] = False
    await collection.replace_one(
        {"schema_name": schema_name},
        new_schema_data
    )
    return {"message": f"Schema '{schema_name}' fields replaced successfully"}

#--------------Generate routing for adding data inside schema--------------#
async def get_schemas() -> List[SchemaModel]:
    schemas = []
    async for document in collection.find({}):
        schema = SchemaModel(**document)
        schemas.append(schema)
    return schemas

async def setup_routes():
    schemas = await get_schemas()
    for schema in schemas:
        generate_routes_from_schema(schema)

app.add_event_handler("startup", setup_routes)

def generate_routes_from_schema(schema: SchemaModel):
    schema_name = schema.schema_name
    fields = {field.col_name: field for field in schema.fields}
    CustomModel = create_model(schema_name, **{field.col_name: (field.type, ...) for field in schema.fields})

    #--------------Adding an item inside any schema--------------#
    @app.post(f"/{schema_name}/")
    async def add_item(item_data: CustomModel = Body(...)) -> Dict[str, Any]:
        # Fetch the schema definition from the database based on the provided schema_name
        schema_definition = await collection.find_one({"schema_name": schema_name})
        if not schema_definition:
            raise HTTPException(status_code=404, detail="Schema not found")

        # Validate uniqueness constraints for fields with unique=True
        for field in schema_definition["fields"]:
            if field["unique"]:
                existing_item = await collection.find_one({field["col_name"]: item_data.get(field["col_name"])})
                if existing_item:
                    raise HTTPException(status_code=400, detail=f"{field['col_name']} must be unique")

        # Validate list fields against allowed values
        for field in schema_definition["fields"]:
            if field["type"] == "list" and "allowed_values" in field:
                allowed_values = field["allowed_values"]
                field_value = item_data.get(field["col_name"])
                if field_value not in allowed_values:
                    raise HTTPException(status_code=400, detail=f"Invalid value for {field['col_name']}")

        # Validate dict field keys against specified dict_keys
        for field in schema_definition["fields"]:
            if field["type"] == "dict" and "dict_keys" in field:
                dict_keys = field["dict_keys"]
                field_value = item_data.get(field["col_name"], {})
                for key in field_value.keys():
                    if key not in dict_keys:
                        raise HTTPException(status_code=400, detail=f"Invalid key for {field['col_name']}: {key}")

        # Insert the item data into the collection
        lc = db[schema_name]
        await lc.insert_one(item_data)
        return {"message": "Item added successfully"}

#--------------Get all schemas--------------#
@app.get("/get-schemas/", response_model=List[SchemaModel])
async def get_schemas() -> List[SchemaModel]:
    schemas = []
    async for document in collection.find({}):
        schema = SchemaModel(**document)
        schemas.append(schema)
    return schemas

#--------------Get schema by name--------------#
@app.get("/get-schema/{schema_name}/", response_model=SchemaModel)
async def get_schema_by_name(schema_name: str) -> SchemaModel:
    schema = await collection.find_one({"schema_name": schema_name})
    if not schema:
        raise HTTPException(status_code=404, detail="Schema not found")
    return SchemaModel(**schema)

#--------------Get schema with date--------------#
@app.get("/get-schema-names-with-date/")
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