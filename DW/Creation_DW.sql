-- CREATION DATABASE

IF DB_ID('DW_Yazaki') IS NULL
    CREATE DATABASE DW_Yazaki;
GO

USE DW_Yazaki;
GO

-- DIMENSION DEPARTEMENT

CREATE TABLE Dim_Departement (
    DepartementID INT IDENTITY PRIMARY KEY,
    CodeDepartement NVARCHAR(50),
    NomDepartement NVARCHAR(50),
    DescriptionDepartement NVARCHAR(200)
);
GO

-- DIMENSION ROLE

CREATE TABLE Dim_Role (
    RoleID INT IDENTITY PRIMARY KEY,
    CodeRole NVARCHAR(50),
    NomRole NVARCHAR(50),
    DescriptionRole NVARCHAR(200)
);
GO

-- DIMENSION EMPLOYEE

CREATE TABLE Dim_Employee (
    EmployeeID INT IDENTITY PRIMARY KEY,
    CodeEmployee NVARCHAR(50),
    NumeroTelephone NVARCHAR(50)
);
GO

-- DIMENSION TEMPS

CREATE TABLE Dim_Temps (
    DateID INT PRIMARY KEY,
    DateComplete DATE,
    Annee INT,
    Mois INT,
    NomMois NVARCHAR(20),
    Trimestre INT,
    Jour INT,
    NomJour NVARCHAR(20)
);
GO

-- DIMENSION IMPRESSION

CREATE TABLE Dim_Impression (
    ImpressionID INT IDENTITY PRIMARY KEY,
    TypeImpression NVARCHAR(50),
    CouleurImpression NVARCHAR(50),
    FormatPapier NVARCHAR(50)
);
GO

-- FACT TELEPHONE

CREATE TABLE Fact_Telephone (
    ID INT IDENTITY PRIMARY KEY,
    DepartementID INT,
    EmployeeID INT,
    RoleID INT,
    DateID INT,
    ForfaitTND DECIMAL(10,2),
    -- Total calcul dans Power BI
    FOREIGN KEY (DepartementID) REFERENCES Dim_Departement(DepartementID),
    FOREIGN KEY (EmployeeID) REFERENCES Dim_Employee(EmployeeID),
    FOREIGN KEY (RoleID) REFERENCES Dim_Role(RoleID),
    FOREIGN KEY (DateID) REFERENCES Dim_Temps(DateID)
    
);
GO

CREATE INDEX IX_FactTel_Employee ON Fact_Telephone(EmployeeID);
CREATE INDEX IX_FactTel_Date ON Fact_Telephone(DateID);
CREATE INDEX IX_FactTel_Departement ON Fact_Telephone(DepartementID);
CREATE INDEX IX_FactTel_Role ON Fact_Telephone(RoleID);

-- FACT IMPRESSION

CREATE TABLE Fact_Impression (
    ID INT IDENTITY PRIMARY KEY,
    DepartementID INT,
    DateID INT,
    ImpressionID INT,
    NbPages INT,
    CoutUnitaire DECIMAL(10,4),
    -- Total calcul dans Power BI
    FOREIGN KEY (DepartementID) REFERENCES Dim_Departement(DepartementID),
    FOREIGN KEY (DateID) REFERENCES Dim_Temps(DateID),
    FOREIGN KEY (ImpressionID) REFERENCES Dim_Impression(ImpressionID)
);
GO

CREATE INDEX IX_FactImp_Departement ON Fact_Impression(DepartementID);
CREATE INDEX IX_FactImp_Date ON Fact_Impression(DateID);

-- Table Previsions

CREATE TABLE Previsions (
    PrevisionID INT PRIMARY KEY IDENTITY,
    DepartementID INT,
    Mois DATE,
    Type NVARCHAR(20),  -- 'Historique' ou 'Prevision'
    ChargeType NVARCHAR(20),  -- 'Telephonique' ou 'Impression'
    ChargeValue DECIMAL(10,2),
    Modele NVARCHAR(50),
    DateCreation DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (DepartementID) REFERENCES Dim_Departement(DepartementID)
);
GO

CREATE INDEX IX_Previsions_Dept_Mois ON Previsions(DepartementID, Mois, ChargeType);