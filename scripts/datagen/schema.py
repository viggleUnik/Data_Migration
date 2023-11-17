import sqlalchemy.types as type


regions = {
    'REGION_ID' : type.Numeric,
    'REGION_NAME' : type.VARCHAR(50)
}


locations = {
    'LOCATION_ID' : type.Numeric,
    'ADDRESS' : type.VARCHAR(255),
    'POSTAL_CODE' : type.VARCHAR(20),
    'CITY' : type.VARCHAR(50),
    'STATE' : type.VARCHAR(50),
    'COUNTRY_ID' :  type.CHAR(2)
}