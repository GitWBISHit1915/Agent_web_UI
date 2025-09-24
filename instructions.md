THESE INSTRUCTIONS SERVE AS CONTEXT FOR THE BACKEND APPLICATION SQL SSMS DB + FASTAPI. USE THIS FOR CONTEXT TO WHAT WE ARE BUILDING OUR FRONT END ONTO
GENERAL INSTRUCTIONS DO THIS FOR EVERY PROMPT GET
    1.) Only do one code block at a time
    2.) Only do one step at a time
    3.) Currently skip working with migration folder elements if not necessary
    4.) Assume I am always using Visual Stuio {Microsoft Visual Studio Community 2022 (64-bit) - Current
            Version 17.14.15 (September 2025)
            as my IDE
            5.) Assume I am using Python 3.11.7
            6.) Assume I am using FastAPI for my web framework
            7.) Assume I am using SQLAlchemy for my ORM
            8.) Assume I am using SQLite for my database engine
            9.) Assume I am using Alembic for database migrations
            10.) Assume I am using Pydantic for data validation and serialization
            11.) Assume I am using Uvicorn as my ASGI server
            12.) Assume I am using pytest for testing
            13.) Assume I am using HTTPX for making HTTP requests in tests
            14.) Assume I am using Swagger UI (automatically provided by FastAPI) for API documentation and testing
            15.) Always confirm with me before making any assumptions about the project structure or requirements
            16.) Always confirm with me before adding any new dependencies or libraries to the project
        }
    5.) Always confirm with me before making any changes to the database schema or models
    6.) Always confirm with me before making any changes to the API endpoints or routes
    7.) Always confirm with me before making any changes to the project structure or organization
    8.) Always confirm with me before making any changes to the testing strategy or framework
    9.) Always confirm with me before making any changes to the deployment or hosting strategy
    10.) Always confirm with me before making any changes to the development workflow or processes
    11.) When suggesting code changes, always show me exactly where to add, delete, or modify in my scripts. If my script 
        is attached. The just show me what needs change and where. 

Working toward GOAL directory
CURRENT INTERPERTATION INSTRUCTIONS: Only build for objects related to building and entity database objects for now while we test with swagger UI/uvicorn API.
    
        C:\
    └── Projects\
        └── Agent_web_UI\
            ├── models\
            │   ├── __init__.py
            │   ├── building_model.py
            │   ├── building_contact_model.py
            │   ├── building_use_model.py
            │   ├── client_contact_model.py
            │   ├── entity_model.py
            │   ├── mortgagee_model.py
            │   ├── mortgagee_contact_model.py
            │   ├── policy_model.py
            │   ├── policy_building_model.py
            │   └── policy_contact_model.py
            ├── schemas\
            │   ├── __init__.py
            │   ├── building_schema.py
            │   ├── building_contact_schema.py
            │   ├── building_use_schema.py
            │   ├── client_contact_schema.py
            │   ├── entity_schema.py
            │   ├── mortgagee_schema.py
            │   ├── mortgagee_contact_schema.py
            │   ├── policy_schema.py
            │   ├── policy_building_schema.py
            │   └── policy_contact_schema.py
            ├── database.py
            ├── main.py
            ├── __init__.py
            ├── migrations\
            │   ├── 2023_09_21_initial.py  # Example migration file
            │   └── other_migrations.py  # Other migration files if needed
            ├── scripts\
            │   ├── __init__.py
            │   ├── setup_database.py  # Script to setup the database
            ├── controllers\
            │   ├── __init__.py
            │   ├── building_controller.py
            │   ├── building_contact_controller.py
            │   ├── building_use_controller.py
            │   ├── client_contact_controller.py
            │   ├── entity_controller.py
            │   ├── mortgagee_controller.py
            │   ├── mortgagee_contact_controller.py
            │   ├── policy_controller.py
            │   ├── policy_building_controller.py
            │   └── policy_contact_controller.py
            ├── tests\
            │   ├── __init__.py
            │   ├── test_building.py
            │   ├── test_building_contact.py
            │   ├── test_building_use.py
            │   ├── test_client_contact.py
            │   ├── test_entity.py
            │   ├── test_mortgagee.py
            │   ├── test_mortgagee_contact.py
            │   ├── test_policy.py
            │   ├── test_policy_building.py
            │   └── test_policy_contact.py
            └── database_scripts\
                ├── __init__.py
                ├── DATABASESCRIPT.sql  # Your SQL scripts

!START FRONT END USER INTERFACE INSTRUCTIONS HERE!

Use these instructions for the User Interface:
This must be scalable, not just for test development.
This must be such that my SSMS wbis_core database is the System of Record on my server instance. JOHN\SQLEXPRESS01
I want to work on building an user interface.
What I really need is a UI where I can pull all this data we made pull through to swagger UI, to a day to day userinterface that my employees and myself can use to update the database as a CRM. 
I do insurance for investment properties for most of our daily activities are effectively updating and adding to a database. 
What I really want is tables that pull all the tables as customizable fields in tables very similar to airtable but I only want to features I want to build into it. 
I was thinking in a similar was I built the very files that made this custom chatUI I made I am talking to you with I think would work. 
I had you make me a custom html file to make the window I am typing in. 
Im thinking making something webased like that is the way to go.

Language of Choice: React