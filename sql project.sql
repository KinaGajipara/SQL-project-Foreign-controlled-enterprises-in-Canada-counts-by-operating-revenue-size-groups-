
     -- Project: Foreign-controlled enterprises in Canada, counts by operating revenue size groups

-- Creating a temporary table to load a dataset
CREATE TEMP TABLE temp_enterprises_data (
    REF_DATE INT,
    GEO VARCHAR(255),
    DGUID VARCHAR(255),
    NAICS VARCHAR(255),
    Enterprises_characteristics VARCHAR(255),
    country_control VARCHAR(255),
    Size_of_enterprise VARCHAR(255),
    UOM VARCHAR(255),
    UOM_ID INT,
    SCALAR_FACTOR VARCHAR(255),
    SCALAR_ID INT,
    VECTOR VARCHAR(255),
    COORDINATE VARCHAR(255),
    VALUE NUMERIC,
    STATUS VARCHAR(10),
    SYMBOL VARCHAR(10),
    TERMINATED VARCHAR(10),
    DECIMALS INT
);

-- Load data into the temporary table
COPY temp_enterprises_data 
FROM 'C:\Users\divya\Desktop\conestoga\Business Analytics\sem 1\sql\Foreign-controlled enterprises in Canada, counts by operating revenue size groups.csv' 
DELIMITER ',' 
CSV HEADER;

SELECT * FROM temp_enterprises_data;

-- Creating dimension tables 
-- Creating Enterprises characteristic table (dimension) 
CREATE TABLE Dim_Enterprise_Characteristics (
    Enterprise_Characteristics_ID SERIAL PRIMARY KEY,
    "Enterprises characteristics" VARCHAR(255)
);

-- Creating Country of Control table (dimension) 
CREATE TABLE Dim_Country_Control (
    Country_of_Control_ID SERIAL PRIMARY KEY,
    Country_Control VARCHAR(50)
);

-- Creating size of enterprises table (dimension)
CREATE TABLE Dim_Size_of_Enterprise (
    Size_of_Enterprise_ID SERIAL PRIMARY KEY,
    "Size of enterprise" VARCHAR(50)
);

-- Creating UOM table (dimension)
CREATE TABLE Dim_UOM (
    UOM_ID SERIAL PRIMARY KEY,
    UOM VARCHAR(50)
);

-- Creating Scalar table (dimension)
CREATE TABLE Dim_Scalar (
    SCALAR_ID SERIAL PRIMARY KEY,
    SCALAR_FACTOR VARCHAR(50)
);

-- Inserting data into all dimension tables that we have created
-- Inserting values into country control table
INSERT INTO Dim_Country_Control (Country_Control)
SELECT DISTINCT country_control FROM temp_enterprises_data;

-- Inserting values into enterprises characteristics table
INSERT INTO Dim_Enterprise_Characteristics ("Enterprises characteristics")
SELECT DISTINCT Enterprises_characteristics 
FROM temp_enterprises_data;

-- Inserting values into Scalar table
INSERT INTO Dim_Scalar (SCALAR_FACTOR)
SELECT DISTINCT SCALAR_FACTOR 
FROM temp_enterprises_data;

-- Inserting values into size of enterprises table
INSERT INTO Dim_Size_of_Enterprise ("Size of enterprise")
SELECT DISTINCT Size_of_enterprise 
FROM temp_enterprises_data;

-- Inserting values into UOM table
INSERT INTO Dim_UOM (UOM)
SELECT DISTINCT UOM 
FROM temp_enterprises_data;

-- Creating Enterprises table (Fact)
CREATE TABLE Fact_Enterprises (
    REF_DATE INT,
    GEO VARCHAR(255),
    DGUID VARCHAR(255),
    NAICS VARCHAR(255),  
    Enterprise_Characteristics_ID INT REFERENCES Dim_Enterprise_Characteristics(Enterprise_Characteristics_ID),
    Country_of_Control_ID INT REFERENCES Dim_Country_Control(Country_of_Control_ID),
    Size_of_Enterprise_ID INT REFERENCES Dim_Size_of_Enterprise(Size_of_Enterprise_ID),
    UOM_ID INT REFERENCES Dim_UOM(UOM_ID),
    SCALAR_ID INT REFERENCES Dim_Scalar(SCALAR_ID),
    VALUE NUMERIC,
    STATUS VARCHAR(10),
    SYMBOL VARCHAR(10),
    TERMINATED VARCHAR(10),
    DECIMALS INT,
    PRIMARY KEY (REF_DATE, NAICS, Enterprise_Characteristics_ID, Country_of_Control_ID, Size_of_Enterprise_ID, UOM_ID, SCALAR_ID)  -- Updated primary key to include NAICS
);

-- Inserting values into fact table using temporary table
INSERT INTO Fact_Enterprises (
    REF_DATE, GEO, DGUID, NAICS,  
    Enterprise_Characteristics_ID,
    Country_of_Control_ID, Size_of_Enterprise_ID, UOM_ID, SCALAR_ID, 
    VALUE, STATUS, SYMBOL, TERMINATED, DECIMALS
)
SELECT 
    t.REF_DATE,
    t.GEO,
    t.DGUID,
    t.NAICS,  
    dec.Enterprise_Characteristics_ID,
    dcc.Country_of_Control_ID,
    dsoe.Size_of_Enterprise_ID,
    duom.UOM_ID,
    ds.SCALAR_ID,
    t.VALUE,
    t.STATUS,
    t.SYMBOL,
    t.TERMINATED,
    t.DECIMALS
FROM temp_enterprises_data t
JOIN Dim_Enterprise_Characteristics dec ON t.Enterprises_characteristics = dec."Enterprises characteristics"
JOIN Dim_Country_Control dcc ON t.country_control = dcc.Country_Control
JOIN Dim_Size_of_Enterprise dsoe ON t.Size_of_enterprise = dsoe."Size of enterprise"
JOIN Dim_UOM duom ON t.UOM = duom.UOM
JOIN Dim_Scalar ds ON t.SCALAR_FACTOR = ds.SCALAR_FACTOR;

SELECT * FROM Fact_Enterprises;

-- Creating a view to analyze enterprises data grouped by Country of control and size of enterprises.
CREATE VIEW View_Enterprise_Summary_By_Country_Enterprises_Size AS
SELECT fe.REF_DATE AS Year,
       fe.GEO AS Geography,
       dcc.Country_Control AS Country,
       dsoe."Size of enterprise" AS Enterprise_Size,
       fe.NAICS AS Industry,  
       SUM(fe.VALUE) AS Total_Enterprises,
       COUNT(fe.VALUE) AS Enterprise_Count,
       AVG(fe.VALUE) AS Average_Enterprise_Value
FROM Fact_Enterprises fe
JOIN Dim_Country_Control dcc ON fe.Country_of_Control_ID = dcc.Country_of_Control_ID
JOIN Dim_Size_of_Enterprise dsoe ON fe.Size_of_Enterprise_ID = dsoe.Size_of_Enterprise_ID
GROUP BY fe.REF_DATE, 
         fe.GEO, 
         dcc.Country_Control, 
         dsoe."Size of enterprise", 
         fe.NAICS; 

SELECT * FROM View_Enterprise_Summary_By_Country_Enterprises_Size;

-- Get total enterprises for a specific geography and year
SELECT * 
FROM View_Enterprise_Summary_By_Country_Enterprises_Size
WHERE Geography = 'Canada'
AND Year = 2022;

-- Analyze data by geography and enterprises characteristics.
CREATE VIEW View_Enterprise_By_Geography_Characteristics AS
SELECT fe.REF_DATE AS Year,
       fe.GEO AS Geography,
       dec."Enterprises characteristics" AS Enterprise_Characteristic,
       SUM(fe.VALUE) AS Total_Value,
       COUNT(fe.VALUE) AS Number_of_Enterprises,
       AVG(fe.VALUE) AS Average_Value
FROM Fact_Enterprises fe
JOIN Dim_Enterprise_Characteristics dec ON fe.Enterprise_Characteristics_ID = dec.Enterprise_Characteristics_ID
GROUP BY fe.REF_DATE, 
         fe.GEO, 
         dec."Enterprises characteristics"
ORDER BY fe.REF_DATE DESC, 
         fe.GEO;

-- Get the enterprise characteristics by geography for 2022
SELECT * 
FROM View_Enterprise_By_Geography_Characteristics
WHERE Year = 2022;

-- Analyzing total enterprises with control category
SELECT Year, Country, Total_Enterprises,
    CASE 
        WHEN Total_Enterprises < 65500 THEN 'Low Control'
        WHEN Total_Enterprises BETWEEN 65501 AND 1000000 THEN 'Moderate Control'
        ELSE 'High Control'
    END AS Country_Control_Category,
    Enterprise_Size,
    Enterprise_Count,
    Average_Enterprise_Value
FROM View_Enterprise_Summary_By_Country_Enterprises_Size
WHERE Year = 2022 AND Geography = 'Canada';
