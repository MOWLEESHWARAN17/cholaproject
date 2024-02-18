from typing import List, Dict, Any, Union, Optional
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel, create_model
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from bson import ObjectId

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

class FieldModel(BaseModel):
    col_name: str
    type: Union[int, str, bool, float, List, Dict[str, Any]]
    unique: bool
    selected_value: Optional[str] = None
    allowed_values: Optional[List[str]] = None
    dict_keys: Optional[Dict[str, Any]] = None

class SchemaModel(BaseModel):
    schema_name: str
    fields: List[FieldModel]

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

def generate_routes_from_schema(schema: SchemaModel):
    schema_name = schema.schema_name
    fields = {field.col_name: field for field in schema.fields}

    CustomModel = create_model(schema_name, **{field.col_name: (field.type, ...) for field in schema.fields})

    @app.post(f"/{schema_name}/")
    async def add_item(item: CustomModel = Body(...)) -> Dict[str, Any]:
        item_data = item.dict()
        item_data["created_at"] = datetime.now().strftime("%d/%m/%Y")

        schema_definition = await collection.find_one({"schema_name": schema_name})
        if schema_definition:
            for field_name, field in fields.items():
                if field.unique:
                    existing_item = await db[schema_name].find_one({field_name: item_data[field_name]})
                    if existing_item:
                        raise HTTPException(status_code=400, detail=f"{field_name} must be unique")
                if field.allowed_values is not None:
                    field_value = item_data[field_name]
                    if field_value not in field.allowed_values:
                        raise HTTPException(status_code=400, detail=f"{field_name} must be one of {', '.join(field.allowed_values)}")
                    else:
                        item_data[field_name] = field_value

                if field.dict_keys is not None:
                    item_data[field_name] = {k: v for k, v in item_data[field_name].items() if k in field.dict_keys}

        await db[schema_name].insert_one(item_data)
        return {"message": "Schema added successfully with creation date"}

    @app.put(f"/{schema_name}/{{item_id}}")
    async def update_schema_item(item_id: str, item: Dict[str, Any]) -> Dict[str, str]:
        try:
            object_id = ObjectId(item_id)
        except Exception as e:
            raise HTTPException(status_code=400, detail="Invalid ObjectId")

        lcollection = db[schema_name]
        schema_definition = await collection.find_one({"schema_name": schema_name})

        if schema_definition:
            field_to_update = None

            # Find the field to update
            for field_name, field in fields.items():
                if field_name in item:
                    field_to_update = field_name
                    break

            if not field_to_update:
                raise HTTPException(status_code=400, detail="No valid field provided for update")

            # Check if the field exists in the schema's collection
            existing_item = await lcollection.find_one({"_id": object_id})
            if existing_item:
                # Check uniqueness if the field is marked as unique
                for field_name, field in fields.items():
                    if field_name == field_to_update and field.unique:
                        existing_item_with_value = await lcollection.find_one({field_to_update: item[field_to_update]})
                        if existing_item_with_value and existing_item_with_value["_id"] != object_id:
                            raise HTTPException(status_code=400, detail=f"{field_to_update} must be unique")
                        break

                # Check allowed values if specified
                if field.allowed_values is not None:
                    field_value = item[field_to_update]
                    if field_value not in field.allowed_values:
                        raise HTTPException(status_code=400, detail=f"{field_to_update} must be one of {', '.join(field.allowed_values)}")
                    else:
                        item[field_to_update] = field_value

                # Check and update dict keys
                if field.dict_keys is not None:
                    item[field_to_update] = {k: v for k, v in item[field_to_update].items() if k in field.dict_keys}

                # Update the field
                await lcollection.update_one({"_id": object_id}, {"$set": {field_to_update: item[field_to_update]}})
                return {"message": f"Field '{field_to_update}' updated successfully for item with ID '{item_id}'"}
            else:
                return {"message": f"No item found with ID '{item_id}' in collection '{schema_name}'"}
        else:
            return {"message": f"Schema '{schema_name}' not found"}

    app.add_event_handler("startup", setup_routes)

@app.post("/add-schema/")
async def add_schema(schema: SchemaModel = Body(...)) -> Dict[str, Any]:
    current_date = datetime.now().strftime("%d/%m/%y")
    schema_dict = schema.dict()
    schema_dict["created_at"] = current_date

    schema_name = schema_dict["schema_name"]
    existing_schema = await collection.find_one({"schema_name": schema_name})
    if existing_schema:
        raise HTTPException(status_code=400, detail="Schema with the same name already exists")

    await collection.insert_one(schema_dict)

    schema_fields = {field.col_name: 1 for field in schema.fields if field.col_name != "created_at"}
    schema_collection = db[schema_name]
    await schema_collection.create_index(list(schema_fields.items()), unique=True)

    return {"message": "Schema added successfully"}

@app.get("/get-schemas/")
async def get_schemas_endpoint():
    schemas = await get_schemas()
    return schemas

@app.put("/update-schema/{schema_name}")
async def replace_schema_fields(schema_name: str, new_fields: List[Dict[str, Any]]) -> Dict[str, str]:
    # Check if the schema exists
    existing_schema = await collection.find_one({"schema_name": schema_name})
    if not existing_schema:
        raise HTTPException(status_code=404, detail="Schema not found")

    # Add created_at field with current date
    new_schema_data = {
        "schema_name": schema_name,
        "created_at": datetime.now().strftime("%d/%m/%Y"),
        "fields": new_fields
    }

    # Replace existing schema with new schema data
    await collection.replace_one(
        {"schema_name": schema_name},
        new_schema_data
    )

    return {"message": f"Schema '{schema_name}' fields replaced successfully"}
    
async def get_schema_fields(schema_name: str) -> Optional[SchemaModel]:
    schema_document = await collection.find_one({"schema_name": schema_name})

    if schema_document:
        schema_fields = {
            "schema_name": schema_document["schema_name"],
            "fields": []
        }
        for field_doc in schema_document["fields"]:
            field_model = FieldModel(
                col_name=field_doc["col_name"],
                type=field_doc["type"],
                unique=field_doc["unique"],
                selected_value=field_doc.get("selected_value"),
                allowed_values=field_doc.get("allowed_values"),
                dict_keys=field_doc.get("dict_keys")
            )
            schema_fields["fields"].append(field_model)

        return SchemaModel(**schema_fields)
    else:
        return None

@app.get("/get-schema-fields/{schema_name}")
async def read_schema_fields(schema_name: str) -> Dict[str, Any]:
    schema_fields = await get_schema_fields(schema_name)
    if schema_fields:
        return schema_fields
    else:
        raise HTTPException(status_code=404, detail="Schema not found")


# #### SCHEMA STRUCTURE######
# {
#   "_id": {
#     "$oid": "65cf5cf2d07bf6214b045557"
#   },
#   "schema_name": "college_details",
#   "created_at": "18/02/2024",
#   "fields": [
#     {
#       "col_name": "name",
#       "type": "str",
#       "unique": true
#     },
#     {
#       "col_name": "established_year",
#       "type": "int",
#       "unique": true
#     },
#     {
#       "col_name": "is_public",
#       "type": "bool",
#       "unique": false
#     },
#     {
#       "col_name": "tuition_fee",
#       "type": "float",
#       "unique": false
#     },
#     {
#       "col_name": "courses_offered",
#       "type": "list",
#       "unique": false,
#       "allowed_values": [
#         "Computer Science",
#         "Engineering",
#         "Business",
#         "Medicine",
#         "Arts"
#       ]
#     },
#     {
#       "col_name": "departments",
#       "type": "list",
#       "unique": false,
#       "allowed_values": [
#         "Computer Science",
#         "Engineering",
#         "Business",
#         "Medicine",
#         "Arts"
#       ]
#     },
#     {
#       "col_name": "campus_location",
#       "type": "str",
#       "unique": false
#     },
#     {
#       "col_name": "website",
#       "type": "str",
#       "unique": false
#     },
#     {
#       "col_name": "contact_info",
#       "type": "dict",
#       "unique": false,
#       "dict_keys": {
#         "email": "str",
#         "phone": "str",
#         "address": "str"
#       }
#     },
#     {
#       "col_name": "accreditation_status",
#       "type": "str",
#       "unique": false
#     },
#     {
#       "col_name": "total_students",
#       "type": "int",
#       "unique": false
#     },
#     {
#       "col_name": "faculty_count",
#       "type": "int",
#       "unique": false
#     },
#     {
#       "col_name": "student_to_faculty_ratio",
#       "type": "float",
#       "unique": false
#     },
#     {
#       "col_name": "campus_size",
#       "type": "str",
#       "unique": false
#     },
#     {
#       "col_name": "financial_aid_available",
#       "type": "bool",
#       "unique": false
#     },
#     {
#       "col_name": "library",
#       "type": "dict",
#       "unique": false,
#       "dict_keys": {
#         "location": "str",
#         "size": "str",
#         "collection": "str"
#       }
#     },
#     {
#       "col_name": "sports_facilities",
#       "type": "list",
#       "unique": false,
#       "allowed_values": [
#         "Football",
#         "Basketball",
#         "Tennis",
#         "Swimming",
#         "Track and Field"
#       ]
#     },
#     {
#       "col_name": "ranking",
#       "type": "int",
#       "unique": false
#     },
#     {
#       "col_name": "alumni_association",
#       "type": "dict",
#       "unique": false,
#       "dict_keys": {
#         "membership": "str",
#         "events": "str",
#         "fundraising": "str"
#       }
#     },
#     {
#       "col_name": "student_clubs",
#       "type": "list",
#       "unique": false,
#       "allowed_values": [
#         "Debate Club",
#         "Music Club",
#         "Dance Club",
#         "Coding Club",
#         "Sports Club"
#       ]
#     },
#     {
#       "col_name": "research_centers",
#       "type": "dict",
#       "unique": false,
#       "dict_keys": {
#         "name": "str",
#         "director": "str",
#         "focus_area": "str"
#       }
#     },
#     {
#       "col_name": "student_housing",
#       "type": "list",
#       "unique": false,
#       "allowed_values": [
#         "On-campus Dormitories",
#         "Off-campus Apartments",
#         "Student Residences"
#       ]
#     },
#     {
#       "col_name": "campus_events",
#       "type": "list",
#       "unique": false,
#       "allowed_values": [
#         "Orientation Week",
#         "Career Fair",
#         "Cultural Festival",
#         "Hackathon",
#         "Sports Day"
#       ]
#     },
#     {
#       "col_name": "faculty_profiles",
#       "type": "dict",
#       "unique": false,
#       "dict_keys": {
#         "name": "str",
#         "department": "str",
#         "education": "str"
#       }
#     },
#     {
#       "col_name": "student_feedback",
#       "type": "dict",
#       "unique": false,
#       "dict_keys": {
#         "feedback_date": "str",
#         "comment": "str",
#         "rating": "int"
#       }
#     },
#     {
#       "col_name": "international_programs",
#       "type": "list",
#       "unique": false,
#       "allowed_values": [
#         "Study Abroad",
#         "Exchange Programs",
#         "International Internships"
#       ]
#     },
#     {
#       "col_name": "campus_security",
#       "type": "dict",
#       "unique": false,
#       "dict_keys": {
#         "guards": "int",
#         "surveillance": "str",
#         "emergency_services": "str"
#       }
#     },
#     {
#       "col_name": "career_services",
#       "type": "dict",
#       "unique": false,
#       "dict_keys": {
#         "counselors": "int",
#         "workshops": "str",
#         "networking_events": "str"
#       }
#     },
#     {
#       "col_name": "gender_diversity",
#       "type": "dict",
#       "unique": false,
#       "dict_keys": {
#         "male_students": "int",
#         "female_students": "int",
#         "other": "int"
#       }
#     },
#     {
#       "col_name": "ethnic_diversity",
#       "type": "dict",
#       "unique": false,
#       "dict_keys": {
#         "white": "int",
#         "black": "int",
#         "hispanic": "int",
#         "asian": "int",
#         "other": "int"
#       }
#     },
#     {
#       "col_name": "religious_affiliation",
#       "type": "str",
#       "unique": false
#     },
#     {
#       "col_name": "school_mascot",
#       "type": "int",
#       "unique": false
#     },
#     {
#       "col_name": "student_life",
#       "type": "dict",
#       "unique": false,
#       "dict_keys": {
#         "events": "str",
#         "clubs": "str",
#         "services": "str"
#       }
#     },
#     {
#       "col_name": "graduation_rate",
#       "type": "float",
#       "unique": false
#     },
#     {
#       "col_name": "drop_out_rate",
#       "type": "float",
#       "unique": true
#     }
#   ]
# }