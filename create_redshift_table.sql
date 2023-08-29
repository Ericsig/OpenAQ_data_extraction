create table if not exists ericsig_coderhouse.open_aq_data(
    id INT PRIMARY KEY,
    city VARCHAR,
    name VARCHAR,
    entity VARCHAR,
    country VARCHAR,
    sources VARCHAR,
    isMobile BOOLEAN,
    isAnalysis BOOLEAN,
    parameters VARCHAR, 
    sensorType VARCHAR,
    lastUpdated TIMESTAMP,
    firstUpdated TIMESTAMP,
    measurements VARCHAR, 
    bounds VARCHAR,  
    manufacturers VARCHAR,  
    coordinates_latitude FLOAT,
    coordinates_longitude FLOAT
)
DISTKEY(id)
SORTKEY(city, country, lastUpdated, coordinates_latitude, coordinates_longitude);