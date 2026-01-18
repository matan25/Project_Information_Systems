DROP DATABASE IF EXISTS flytau;
CREATE DATABASE flytau;
USE flytau;


CREATE TABLE Airports (
    Airport_code        VARCHAR(10) PRIMARY KEY,
    Airport_Name        VARCHAR(60) NOT NULL,
    City                VARCHAR(60) NOT NULL,
    Country             VARCHAR(60) NOT NULL
);

CREATE TABLE Flight_Routes (
    Route_id                 VARCHAR(10) PRIMARY KEY,
    Duration_Minutes         INT NOT NULL,
    Origin_Airport_code      VARCHAR(10) NOT NULL,
    Destination_Airport_code VARCHAR(10) NOT NULL,
    CONSTRAINT fk_routes_origin
        FOREIGN KEY (Origin_Airport_code) REFERENCES Airports(Airport_code),
    CONSTRAINT fk_routes_dest
        FOREIGN KEY (Destination_Airport_code) REFERENCES Airports(Airport_code),
    CONSTRAINT uq_routes_origin_dest
        UNIQUE (Origin_Airport_code, Destination_Airport_code)
);


CREATE TABLE Aircrafts (
    Aircraft_id     VARCHAR(10) PRIMARY KEY,
	Manufacturer    ENUM('Boeing','Airbus','Dasso') NOT NULL,
    Model    VARCHAR(40) NOT NULL,
    Size            ENUM('Small','Large') NOT NULL,
    Purchase_Date   DATETIME NOT NULL
);

CREATE TABLE Seats (
    Seat_id         VARCHAR(10) PRIMARY KEY,
    Aircraft_id     VARCHAR(10) NOT NULL,
    Row_Num         INT NOT NULL,
    Col_Num         INT NOT NULL,
    Seat_Class      ENUM('Economy','Business') NOT NULL,
    CONSTRAINT fk_seats_aircraft
        FOREIGN KEY (Aircraft_id) REFERENCES Aircrafts(Aircraft_id),
    CONSTRAINT uq_seats_aircraft_row_col
        UNIQUE (Aircraft_id, Row_Num, Col_Num)
);


CREATE TABLE Register_Customers (
	Customer_Email               VARCHAR(80) PRIMARY KEY, 
    First_Name           VARCHAR(40) NOT NULL,
    Last_Name           VARCHAR(40) NOT NULL,
    Passport_No         VARCHAR(8)  NOT NULL,
    Registration_Date   DATETIME   NOT NULL,
    Birth_Date          DATETIME    NOT NULL,
    Customer_Password   VARCHAR(64) NOT NULL,           
   # CONSTRAINT uq_customers_email      UNIQUE (Email),
    CONSTRAINT uq_customers_passport   UNIQUE (Passport_No)
);

CREATE TABLE Register_Customers_Phones (
    Customer_Email     VARCHAR(80) NOT NULL,
    Phone_Number    VARCHAR(20) NOT NULL,
    PRIMARY KEY (Customer_Email, Phone_Number),
    CONSTRAINT fk_email_customer_register
        FOREIGN KEY (Customer_Email) REFERENCES Register_Customers(Customer_Email)
);

CREATE TABLE Guest_Customers (
	Customer_Email               VARCHAR(80) PRIMARY KEY, 
    First_Name           VARCHAR(40) NOT NULL,
    Last_Name           VARCHAR(40) NOT NULL
);

CREATE TABLE Guest_Customers_Phones (
    Customer_Email     VARCHAR(80) NOT NULL,
    Phone_Number    VARCHAR(20) NOT NULL,
    PRIMARY KEY (Customer_Email, Phone_Number),
    CONSTRAINT fk_email_customer_guest
        FOREIGN KEY (Customer_Email) REFERENCES  Guest_Customers(Customer_Email)
);


CREATE TABLE Managers (
    Manager_id          VARCHAR(9) PRIMARY KEY,
    First_Name          VARCHAR(40) NOT NULL,
    Last_Name           VARCHAR(40) NOT NULL,
    City                VARCHAR(60) NOT NULL,
    Street              VARCHAR(60) NOT NULL,
    House_Number        INT NOT NULL,
    Phone_Number        VARCHAR(20) NOT NULL,
    Start_Working_Date  DATETIME NOT NULL,
    Manager_Password    VARCHAR(64) NOT NULL
);

CREATE TABLE Pilots (
    Pilot_id            VARCHAR(9) PRIMARY KEY,
    First_Name          VARCHAR(40) NOT NULL,
    Last_Name           VARCHAR(40) NOT NULL,
    City                VARCHAR(60) NOT NULL,
    Street              VARCHAR(60) NOT NULL,
    House_Number        INT NOT NULL,
    Phone_Number        VARCHAR(20) NOT NULL,
    Start_Working_Date  DATETIME NOT NULL,
    Long_Haul_Certified BOOLEAN NOT NULL DEFAULT 0
);

CREATE TABLE FlightAttendants (
    Attendant_id        VARCHAR(9) PRIMARY KEY,
    First_Name          VARCHAR(40) NOT NULL,
    Last_Name           VARCHAR(40) NOT NULL,
    City                VARCHAR(60) NOT NULL,
    Street              VARCHAR(60) NOT NULL,
    House_Number        INT NOT NULL,
    Phone_Number        VARCHAR(20) NOT NULL,
    Start_Working_Date  DATETIME NOT NULL,
    Long_Haul_Certified BOOLEAN NOT NULL DEFAULT 0
);


CREATE TABLE Flights (
    Flight_id       VARCHAR(10) PRIMARY KEY,
    Dep_DateTime    DATETIME NOT NULL,
    Status          ENUM('Active', 'Full-Occupied','Completed','Cancelled') NOT NULL,
    Aircraft_id     VARCHAR(10) NOT NULL,
    Route_id        VARCHAR(10) NOT NULL,
    CONSTRAINT fk_flights_aircraft
        FOREIGN KEY (Aircraft_id) REFERENCES Aircrafts(Aircraft_id),
    CONSTRAINT fk_flights_route
        FOREIGN KEY (Route_id) REFERENCES Flight_Routes(Route_id)
);

CREATE TABLE FlightSeats (
    FlightSeat_id   VARCHAR(10) PRIMARY KEY,
    Flight_id       VARCHAR(10) NOT NULL,
    Seat_id         VARCHAR(10) NOT NULL,
    Seat_Price      DECIMAL(8,2)  NULL,
    Seat_Status     ENUM('Available','Sold','Blocked') NOT NULL,
    CONSTRAINT fk_fseat_flight
        FOREIGN KEY (Flight_id) REFERENCES Flights(Flight_id),
    CONSTRAINT fk_fseat_seat
        FOREIGN KEY (Seat_id) REFERENCES Seats(Seat_id),
    CONSTRAINT uq_fseat_flight_seat
        UNIQUE (Flight_id, Seat_id)
);

CREATE TABLE Orders (
    Order_code       VARCHAR(10) PRIMARY KEY,
    Order_Date      DATETIME NOT NULL,
    Status          ENUM('Active','Completed','Cancelled-Customer','Cancelled-System') NOT NULL,
    Cancel_Date     DATETIME NULL,
    Flight_id       VARCHAR(10) NOT NULL,
	Customer_Email     VARCHAR(80) NOT NULL,
    Customer_Type   ENUM('Register','Guest') NOT NULL    
);

CREATE TABLE Tickets (
    Ticket_id      INT AUTO_INCREMENT PRIMARY KEY,
    FlightSeat_id  VARCHAR(10) NOT NULL,
    Order_code     VARCHAR(10) NOT NULL,
    CONSTRAINT fk_tickets_fseat
        FOREIGN KEY (FlightSeat_id) REFERENCES FlightSeats(FlightSeat_id),
    CONSTRAINT fk_tickets_order
        FOREIGN KEY (Order_code) REFERENCES Orders(Order_code)
);


CREATE TABLE FlightCrew_Pilots (
    Pilot_id        VARCHAR(9) NOT NULL,
    Flight_id       VARCHAR(10) NOT NULL,
    PRIMARY KEY (Pilot_id, Flight_id),
    CONSTRAINT fk_fcp_pilot
        FOREIGN KEY (Pilot_id) REFERENCES Pilots(Pilot_id),
    CONSTRAINT fk_fcp_flight
        FOREIGN KEY (Flight_id) REFERENCES Flights(Flight_id)
);

CREATE TABLE FlightCrew_Attendants (
    Attendant_id    VARCHAR(9) NOT NULL,
    Flight_id       VARCHAR(10) NOT NULL,
    PRIMARY KEY (Attendant_id, Flight_id),
    CONSTRAINT fk_fca_attendant
        FOREIGN KEY (Attendant_id) REFERENCES FlightAttendants(Attendant_id),
    CONSTRAINT fk_fca_flight
        FOREIGN KEY (Flight_id) REFERENCES Flights(Flight_id)
);

CREATE TABLE  IdCounters (
  Name    VARCHAR(50) NOT NULL PRIMARY KEY,
  NextNum BIGINT NOT NULL
); 





INSERT INTO Airports (Airport_code, Airport_Name, City, Country) VALUES
('TLV', 'Ben Gurion',    'Tel Aviv', 'Israel'),
('LHR', 'Heathrow',      'London',   'UK'),
('JFK', 'John F Kennedy','New York', 'USA'),
('CDG', 'Charles de Gaulle','Paris', 'France');



INSERT INTO Flight_Routes (Route_id, Duration_Minutes, Origin_Airport_code, Destination_Airport_code) VALUES
('R001', 270, 'TLV', 'LHR'),
('R002', 240, 'TLV', 'CDG'),
('R003', 660, 'TLV', 'JFK'),
('R004', 420, 'JFK', 'LHR'),
('R005', 720, 'JFK', 'TLV'),
('R006', 300, 'LHR', 'TLV'),
('R007', 255, 'CDG', 'TLV'),
('R008', 420, 'LHR', 'JFK'),
('R009', 480, 'JFK', 'CDG'),
('R010', 510, 'CDG', 'JFK'),
('R011',  75, 'LHR', 'CDG'),
('R012',  90, 'CDG', 'LHR');


INSERT INTO Aircrafts (Aircraft_id, Manufacturer, Model, Size, Purchase_Date) VALUES
('ACB001', 'Boeing',   '787-9 Dreamliner', 'Large', '2015-01-10 00:00:00'),
('ACA002', 'Airbus',   'A320neo',          'Small', '2018-05-15 00:00:00'),
('ACD003', 'Dasso',    'Falcon 900EX',     'Large', '2016-03-20 00:00:00'),
('ACD004', 'Dasso',    'E190',             'Small', '2019-07-01 00:00:00'),
('ACA005', 'Airbus',   'A321-277',         'Large', '2014-11-11 00:00:00'),
('ACB006', 'Boeing',   '737-300',          'Small', '2020-02-02 00:00:00'),
('ACB007', 'Boeing',   '777-300ER',        'Large', '2013-09-18 00:00:00'),
('ACA008', 'Airbus',   'A220-300',         'Small', '2021-04-05 00:00:00'),
('ACD009', 'Dasso',    'E195-E2',          'Small', '2022-08-12 00:00:00'),
('ACB010', 'Boeing',   '787-10 Dreamliner','Large', '2017-12-01 00:00:00'),

-- 10 NEW aircrafts (5 Small + 5 Large)
('ACA011', 'Airbus', 'A320-200',          'Small', '2022-01-15 00:00:00'),
('ACA012', 'Airbus', 'A319',              'Small', '2020-06-20 00:00:00'),
('ACD013', 'Dasso',  'E190',              'Small', '2021-03-10 00:00:00'),
('ACB014', 'Boeing', '737-800',           'Small', '2019-09-01 00:00:00'),
('ACA015', 'Airbus', 'A220-100',          'Small', '2023-05-05 00:00:00'),

('ACB016', 'Boeing', '787-8 Dreamliner',  'Large', '2016-10-10 00:00:00'),
('ACA017', 'Airbus', 'A330-300',          'Large', '2015-04-04 00:00:00'),
('ACD018', 'Dasso',  'Falcon 7X',         'Large', '2017-07-07 00:00:00'),
('ACB019', 'Boeing', '777-200ER',         'Large', '2014-12-12 00:00:00'),
('ACA020', 'Airbus', 'A350-900',          'Large', '2018-08-08 00:00:00');


INSERT INTO Seats (Seat_id, Aircraft_id, Row_Num, Col_Num, Seat_Class) VALUES
('S001', 'ACB001', 1, 1, 'Business'),
('S002', 'ACB001', 1, 2, 'Business'),
('S003', 'ACB001', 2, 1, 'Economy'),
('S004', 'ACB001', 2, 2, 'Economy'),
('S005', 'ACB001', 3, 1, 'Economy'),
('S006', 'ACB001', 3, 2, 'Economy'),

('S007', 'ACA002', 1, 1, 'Economy'),
('S008', 'ACA002', 1, 2, 'Economy'),
('S009', 'ACA002', 2, 1, 'Economy'),
('S010', 'ACA002', 2, 2, 'Economy'),

('S011', 'ACD003', 1, 1, 'Business'),
('S012', 'ACD003', 1, 2, 'Business'),
('S013', 'ACD003', 2, 1, 'Economy'),
('S014', 'ACD003', 2, 2, 'Economy'),
('S015', 'ACD003', 3, 1, 'Economy'),
('S016', 'ACD003', 3, 2, 'Economy'),

('S017', 'ACD004', 1, 1, 'Economy'),
('S018', 'ACD004', 1, 2, 'Economy'),
('S019', 'ACD004', 2, 1, 'Economy'),
('S020', 'ACD004', 2, 2, 'Economy'),

('S021', 'ACA005', 1, 1, 'Business'),
('S022', 'ACA005', 1, 2, 'Business'),
('S023', 'ACA005', 2, 1, 'Economy'),
('S024', 'ACA005', 2, 2, 'Economy'),
('S025', 'ACA005', 3, 1, 'Economy'),
('S026', 'ACA005', 3, 2, 'Economy'),

('S027', 'ACB006', 1, 1, 'Economy'),
('S028', 'ACB006', 1, 2, 'Economy'),
('S029', 'ACB006', 2, 1, 'Economy'),
('S030', 'ACB006', 2, 2, 'Economy'),

('S031', 'ACB007', 1, 1, 'Business'),
('S032', 'ACB007', 1, 2, 'Business'),
('S033', 'ACB007', 2, 1, 'Economy'),
('S034', 'ACB007', 2, 2, 'Economy'),
('S035', 'ACB007', 3, 1, 'Economy'),
('S036', 'ACB007', 3, 2, 'Economy'),

('S037', 'ACA008', 1, 1, 'Economy'),
('S038', 'ACA008', 1, 2, 'Economy'),
('S039', 'ACA008', 2, 1, 'Economy'),
('S040', 'ACA008', 2, 2, 'Economy'),

('S041', 'ACD009', 1, 1, 'Economy'),
('S042', 'ACD009', 1, 2, 'Economy'),
('S043', 'ACD009', 2, 1, 'Economy'),
('S044', 'ACD009', 2, 2, 'Economy'),

('S047', 'ACB010', 1, 1, 'Business'),
('S048', 'ACB010', 1, 2, 'Business'),
('S049', 'ACB010', 2, 1, 'Economy'),
('S050', 'ACB010', 2, 2, 'Economy'),
('S051', 'ACB010', 3, 1, 'Economy'),
('S052', 'ACB010', 3, 2, 'Economy');

INSERT INTO Seats (Seat_id, Aircraft_id, Row_Num, Col_Num, Seat_Class) VALUES
('S053', 'ACA011', 1, 1, 'Economy'),
('S054', 'ACA011', 1, 2, 'Economy'),
('S055', 'ACA011', 2, 1, 'Economy'),
('S056', 'ACA011', 2, 2, 'Economy'),

('S057', 'ACA012', 1, 1, 'Economy'),
('S058', 'ACA012', 1, 2, 'Economy'),
('S059', 'ACA012', 2, 1, 'Economy'),
('S060', 'ACA012', 2, 2, 'Economy'),

('S061', 'ACD013', 1, 1, 'Economy'),
('S062', 'ACD013', 1, 2, 'Economy'),
('S063', 'ACD013', 2, 1, 'Economy'),
('S064', 'ACD013', 2, 2, 'Economy'),

('S065', 'ACB014', 1, 1, 'Economy'),
('S066', 'ACB014', 1, 2, 'Economy'),
('S067', 'ACB014', 2, 1, 'Economy'),
('S068', 'ACB014', 2, 2, 'Economy'),

('S069', 'ACA015', 1, 1, 'Economy'),
('S070', 'ACA015', 1, 2, 'Economy'),
('S071', 'ACA015', 2, 1, 'Economy'),
('S072', 'ACA015', 2, 2, 'Economy'),

('S073', 'ACB016', 1, 1, 'Business'),
('S074', 'ACB016', 1, 2, 'Business'),
('S075', 'ACB016', 2, 1, 'Economy'),
('S076', 'ACB016', 2, 2, 'Economy'),
('S077', 'ACB016', 3, 1, 'Economy'),
('S078', 'ACB016', 3, 2, 'Economy'),

('S079', 'ACA017', 1, 1, 'Business'),
('S080', 'ACA017', 1, 2, 'Business'),
('S081', 'ACA017', 2, 1, 'Economy'),
('S082', 'ACA017', 2, 2, 'Economy'),
('S083', 'ACA017', 3, 1, 'Economy'),
('S084', 'ACA017', 3, 2, 'Economy'),

('S085', 'ACD018', 1, 1, 'Business'),
('S086', 'ACD018', 1, 2, 'Business'),
('S087', 'ACD018', 2, 1, 'Economy'),
('S088', 'ACD018', 2, 2, 'Economy'),
('S089', 'ACD018', 3, 1, 'Economy'),
('S090', 'ACD018', 3, 2, 'Economy'),

('S091', 'ACB019', 1, 1, 'Business'),
('S092', 'ACB019', 1, 2, 'Business'),
('S093', 'ACB019', 2, 1, 'Economy'),
('S094', 'ACB019', 2, 2, 'Economy'),
('S095', 'ACB019', 3, 1, 'Economy'),
('S096', 'ACB019', 3, 2, 'Economy'),

('S097', 'ACA020', 1, 1, 'Business'),
('S098', 'ACA020', 1, 2, 'Business'),
('S099', 'ACA020', 2, 1, 'Economy'),
('S100', 'ACA020', 2, 2, 'Economy'),
('S101', 'ACA020', 3, 1, 'Economy'),
('S102', 'ACA020', 3, 2, 'Economy');


INSERT INTO Register_Customers
    (Customer_Email, First_Name, Last_Name, Passport_No,
     Registration_Date, Birth_Date, Customer_Password)
VALUES
    ('matan@flytau.com',  'Matan',  'Nijinsky', '20000001',
     '2025-01-10 09:00:00', '2000-05-15 00:00:00', 'matanPass1'),
    ('shira@flytau.com',  'Shira',  'Mutsafy',  '20000002',
     '2025-01-12 10:00:00', '1999-03-21 00:00:00', 'shiraPass1');

INSERT INTO Register_Customers_Phones (Customer_Email, Phone_Number) VALUES
('matan@flytau.com', '050-1111111'),
('shira@flytau.com', '050-2222222');

INSERT INTO Guest_Customers (Customer_Email, First_Name, Last_Name) VALUES
('daniel@flytau.com', 'Daniel', 'Messer'),
('roni@flytau.com',   'Roni',   'Levy');

INSERT INTO Guest_Customers_Phones (Customer_Email, Phone_Number) VALUES
('daniel@flytau.com', '050-3333333'),
('roni@flytau.com',   '050-4444444');



INSERT INTO Managers(Manager_id, First_Name, Last_Name, City, Street, House_Number, Phone_Number, Start_Working_Date, Manager_Password) VALUES
('300000001', 'אריאל',  'לוי',   'Tel Aviv', 'Herzl', 10, '03-1111111', '2018-01-01 08:00:00', 'managerPass1'),
('300000002', 'נעה',    'כהן',   'Haifa',    'Hagana', 5, '04-2222222', '2019-03-01 08:00:00', 'managerPass2');


INSERT INTO Pilots
(Pilot_id, First_Name, Last_Name, City, Street, House_Number, Phone_Number, Start_Working_Date, Long_Haul_Certified)
VALUES
('400000001', 'יוסי', 'כץ',      'Tel Aviv',   'Herzl',      15, '03-5000001', '2015-01-01 08:00:00', 1),
('400000002', 'דנה',  'בר',      'Tel Aviv',   'Herzl',      20, '03-5000002', '2016-02-01 08:00:00', 1),
('400000003', 'אבי',  'שלו',     'Jerusalem',  'King David',  5, '02-5000003', '2014-05-01 08:00:00', 1),
('400000004', 'ליאור','שרון',    'Haifa',      'Hagana',     8,  '04-5000004', '2017-07-01 08:00:00', 1),

('400000005', 'רון',  'טל',      'Beer Sheva', 'Rager',      12, '08-5000005', '2019-01-01 08:00:00', 0),
('400000006', 'ניר',  'אמיר',    'Tel Aviv',   'Dizengoff',  3,  '03-5000006', '2020-03-01 08:00:00', 0),
('400000007', 'טל',   'ירדן',    'Haifa',      'Hagana',     18, '04-5000007', '2021-04-01 08:00:00', 0),
('400000008', 'אדם',  'רונן',    'Jerusalem',  'Jabotinsky', 7,  '02-5000008', '2022-01-01 08:00:00', 0),
('400000009', 'מאיה', 'רגב',     'Tel Aviv',   'Ibn Gabirol',9,  '03-5000009', '2023-01-01 08:00:00', 0),
('400000010', 'עומר', 'שי',      'Rishon',     'Weizmann',   11, '03-5000010', '2023-06-01 08:00:00', 0),

('400000011','דודי', 'כהן',      'Tel Aviv',   'Herzl',      50, '03-5000011', '2023-08-01 08:00:00', 1),
('400000012','שרון', 'לוי',      'Haifa',      'Hagana',     33, '04-5000012', '2023-08-01 08:00:00', 1),
('400000013','רינה', 'אלון',     'Jerusalem',  'King David', 9,  '02-5000013', '2023-08-01 08:00:00', 1),

('400000014','כפיר', 'בן דוד',   'Tel Aviv',   'Dizengoff',  15, '03-5000014', '2023-08-01 08:00:00', 0),
('400000015','מור',  'אליאס',    'Haifa',      'Herzl',      22, '04-5000015', '2023-08-01 08:00:00', 0),
('400000016','שחר',  'כהן',      'Beer Sheva', 'Rager',      18, '08-5000016', '2023-08-01 08:00:00', 0),
('400000017','אילן', 'רוזן',     'Tel Aviv',   'Arlozorov',  21, '03-5000017', '2023-08-01 08:00:00', 0),
('400000018','אפרת', 'זהבי',     'Jerusalem',  'Jabotinsky', 16, '02-5000018', '2023-08-01 08:00:00', 0),
('400000019','חן',   'גל',       'Haifa',      'Hagana',     12, '04-5000019', '2023-08-01 08:00:00', 0),
('400000020','עדי',  'רום',      'Rishon',     'Weizmann',   25, '03-5000020', '2023-08-01 08:00:00', 0),

('400000021','איתן', 'לוי',      'Tel Aviv',   'Herzl',      61, '03-5000021', '2024-01-01 08:00:00', 1),
('400000022','מיכל', 'כהן',      'Haifa',      'Hagana',     41, '04-5000022', '2024-01-01 08:00:00', 1),
('400000023','נועם', 'רוזן',     'Jerusalem',  'King David', 17, '02-5000023', '2024-01-01 08:00:00', 1),

('400000024','לירון','ברק',      'Tel Aviv',   'Dizengoff',  12, '03-5000024', '2024-02-01 08:00:00', 1),
('400000025','דניאל','שחר',      'Haifa',      'Herzl',      19, '04-5000025', '2024-02-01 08:00:00', 1),
('400000026','שני',  'אמיר',     'Jerusalem',  'Jabotinsky', 8,  '02-5000026', '2024-02-01 08:00:00', 1),

('400000027','רועי', 'טל',       'Tel Aviv',   'Arlozorov',  33, '03-5000027', '2024-03-01 08:00:00', 0),
('400000028','ליה',  'גולד',     'Haifa',      'Hagana',     6,  '04-5000028', '2024-03-01 08:00:00', 0),
('400000029','אביב', 'פרץ',      'Jerusalem',  'Ben Yehuda', 21, '02-5000029', '2024-03-01 08:00:00', 0),
('400000030','שחר',  'מזרחי',    'Tel Aviv',   'Ibn Gabirol',14, '03-5000030', '2024-03-01 08:00:00', 0),

-- 10 NEW pilots (לא משובצים)
('400000031','אורי','לוי','Tel Aviv','Herzl',12,'03-5000031','2024-04-01 08:00:00',1),
('400000032','שירה','כהן','Haifa','Hagana',8,'04-5000032','2024-04-01 08:00:00',1),
('400000033','דניאל','ברק','Jerusalem','King David',5,'02-5000033','2024-04-01 08:00:00',1),
('400000034','נועה','פרץ','Rishon','Weizmann',9,'03-5000034','2024-04-01 08:00:00',1),
('400000035','איתן','רום','Beer Sheva','Rager',3,'08-5000035','2024-04-01 08:00:00',1),
('400000036','מאיה','טל','Tel Aviv','Dizengoff',20,'03-5000036','2024-05-01 08:00:00',0),
('400000037','לירון','שחר','Haifa','Herzl',14,'04-5000037','2024-05-01 08:00:00',0),
('400000038','רועי','מגן','Jerusalem','Jabotinsky',6,'02-5000038','2024-05-01 08:00:00',0),
('400000039','נטע','זיו','Ramat Gan','Jabotinsky',11,'03-5000039','2024-05-01 08:00:00',0),
('400000040','גיל','אלון','Tel Aviv','Ibn Gabirol',7,'03-5000040','2024-05-01 08:00:00',0);


INSERT INTO FlightAttendants
(Attendant_id, First_Name, Last_Name, City, Street, House_Number, Phone_Number, Start_Working_Date, Long_Haul_Certified)
VALUES
('500000001', 'עדי',   'לוי',     'Tel Aviv',   'Herzl',      3,  '03-6000001', '2017-02-01 08:00:00', 1),
('500000002', 'גל',    'שני',     'Haifa',      'Hagana',    14, '04-6000002', '2018-03-01 08:00:00', 1),
('500000003', 'נועם',  'ברק',     'Jerusalem',  'King David', 2,  '02-6000003', '2016-04-01 08:00:00', 1),
('500000004', 'רוני',  'טל',      'Tel Aviv',   'Dizengoff',  22, '03-6000004', '2015-05-01 08:00:00', 1),
('500000005', 'עדן',   'כהן',     'Tel Aviv',   'Arlozorov',  7,  '03-6000005', '2019-01-01 08:00:00', 1),
('500000006', 'איתי',  'מזרחי',   'Haifa',      'Herzl',      27, '04-6000006', '2019-06-01 08:00:00', 1),
('500000007', 'יעל',   'גרין',    'Beer Sheva', 'Rager',      19, '08-6000007', '2020-02-01 08:00:00', 1),
('500000008', 'נטע',   'אביב',    'Jerusalem',  'Ben Yehuda', 6,  '02-6000008', '2020-09-01 08:00:00', 1),

('500000009', 'ליה',   'שי',      'Tel Aviv',   'Herzl',      31, '03-6000009', '2021-01-01 08:00:00', 0),
('500000010', 'עידו',  'פרץ',     'Haifa',      'Hagana',     9,  '04-6000010', '2021-03-01 08:00:00', 0),
('500000011', 'ים',    'אורן',    'Ramat Gan',  'Jabotinsky', 13, '03-6000011', '2021-05-01 08:00:00', 0),
('500000012', 'אייל',  'רום',     'Tel Aviv',   'Dizengoff',  28, '03-6000012', '2021-07-01 08:00:00', 0),
('500000013', 'נועה',  'מור',     'Tel Aviv',   'Arlosorov',  16, '03-6000013', '2022-01-01 08:00:00', 0),
('500000014', 'עמית',  'רז',      'Haifa',      'Herzl',      21, '04-6000014', '2022-03-01 08:00:00', 0),
('500000015', 'שגיא',  'בר',      'Beer Sheva', 'Rager',      23, '08-6000015', '2022-05-01 08:00:00', 0),
('500000016', 'הילה',  'לוי',     'Jerusalem',  'King David', 4,  '02-6000016', '2022-07-01 08:00:00', 0),
('500000017', 'מאור',  'טל',      'Tel Aviv',   'Ibn Gabirol',5,  '03-6000017', '2023-01-01 08:00:00', 0),
('500000018', 'לילך',  'הראל',    'Haifa',      'Hagana',     30, '04-6000018', '2023-03-01 08:00:00', 0),
('500000019', 'רועי',  'גולד',    'Rishon',     'Weizmann',   20, '03-6000019', '2023-05-01 08:00:00', 0),
('500000020', 'דנה',   'שיר',     'Tel Aviv',   'Herzl',      40, '03-6000020', '2023-07-01 08:00:00', 0),

('500000021', 'אסף',   'לוין',    'Tel Aviv',   'Herzl',      45, '03-6000021', '2023-08-01 08:00:00', 1),
('500000022', 'רותם',  'כהן',     'Haifa',      'Hagana',     26, '04-6000022', '2023-08-01 08:00:00', 1),
('500000023', 'שקד',   'גיל',     'Jerusalem',  'Ben Yehuda', 11, '02-6000023', '2023-08-01 08:00:00', 1),
('500000024', 'אלון',  'ברק',     'Tel Aviv',   'Dizengoff',  35, '03-6000024', '2023-08-01 08:00:00', 1),
('500000025', 'נוי',   'זיו',     'Ramat Gan',  'Jabotinsky', 17, '03-6000025', '2023-08-01 08:00:00', 1),
('500000026', 'רז',    'אוחנה',   'Beer Sheva', 'Rager',      29, '08-6000026', '2023-08-01 08:00:00', 1),

('500000027', 'נטלי',  'כץ',      'Tel Aviv',   'Ibn Gabirol',13, '03-6000027', '2023-08-01 08:00:00', 0),
('500000028', 'עמית',  'בלום',    'Haifa',      'Herzl',      37, '04-6000028', '2023-08-01 08:00:00', 0),
('500000029', 'אורי',  'מגן',     'Jerusalem',  'King David', 19, '02-6000029', '2023-08-01 08:00:00', 0),
('500000030', 'גילי',  'דגן',     'Rishon',     'Weizmann',   27, '03-6000030', '2023-08-01 08:00:00', 0),

('500000031', 'רותם',  'לוי',     'Tel Aviv',   'Herzl',      52, '03-6000031', '2024-01-10 08:00:00', 1),
('500000032', 'אור',   'כהן',     'Haifa',      'Hagana',     24, '04-6000032', '2024-01-10 08:00:00', 1),
('500000033', 'מיה',   'בר',      'Jerusalem',  'King David', 13, '02-6000033', '2024-01-10 08:00:00', 1),
('500000034', 'עומר',  'גל',      'Tel Aviv',   'Dizengoff',  44, '03-6000034', '2024-01-10 08:00:00', 1),
('500000035', 'שירה',  'רום',     'Haifa',      'Herzl',      10, '04-6000035', '2024-01-10 08:00:00', 1),
('500000036', 'איתי',  'זיו',     'Jerusalem',  'Jabotinsky', 5,  '02-6000036', '2024-01-10 08:00:00', 1),

('500000037', 'גיל',   'דגן',     'Tel Aviv',   'Ibn Gabirol',22, '03-6000037', '2024-02-12 08:00:00', 1),
('500000038', 'דנה',   'אלון',    'Haifa',      'Hagana',     31, '04-6000038', '2024-02-12 08:00:00', 1),
('500000039', 'רוני',  'שני',     'Jerusalem',  'Ben Yehuda', 9,  '02-6000039', '2024-02-12 08:00:00', 1),
('500000040', 'נטע',   'מגן',     'Tel Aviv',   'Arlozorov',  26, '03-6000040', '2024-02-12 08:00:00', 1),
('500000041', 'אלון',  'פרץ',     'Haifa',      'Herzl',      17, '04-6000041', '2024-02-12 08:00:00', 1),
('500000042', 'הילה',  'ברק',     'Jerusalem',  'King David', 4,  '02-6000042', '2024-02-12 08:00:00', 1),

('500000043', 'אדם',   'לוי',     'Tel Aviv',   'Herzl',      18, '03-6000043', '2024-03-05 08:00:00', 0),
('500000044', 'לילך',  'כהן',     'Haifa',      'Hagana',     7,  '04-6000044', '2024-03-05 08:00:00', 0),
('500000045', 'שקד',   'בר',      'Jerusalem',  'Ben Yehuda', 16, '02-6000045', '2024-03-05 08:00:00', 0),
('500000046', 'נוי',   'רום',     'Tel Aviv',   'Dizengoff',  29, '03-6000046', '2024-03-05 08:00:00', 0),
('500000047', 'עמית',  'טל',      'Haifa',      'Herzl',      23, '04-6000047', '2024-03-05 08:00:00', 0),
('500000048', 'מאור',  'גולד',    'Jerusalem',  'Jabotinsky', 12, '02-6000048', '2024-03-05 08:00:00', 0),

('500000059','רון','לוי','Tel Aviv','Herzl',9,'03-6000059','2024-04-10 08:00:00',1),
('500000060','דנה','כהן','Haifa','Hagana',6,'04-6000060','2024-04-10 08:00:00',1),
('500000061','מיה','ברק','Jerusalem','King David',4,'02-6000061','2024-04-10 08:00:00',1),
('500000062','שקד','פרץ','Rishon','Weizmann',10,'03-6000062','2024-04-10 08:00:00',1),
('500000063','אסף','רום','Beer Sheva','Rager',2,'08-6000063','2024-04-10 08:00:00',1),
('500000064','ליה','טל','Tel Aviv','Dizengoff',18,'03-6000064','2024-05-12 08:00:00',0),
('500000065','עידו','שחר','Haifa','Herzl',12,'04-6000065','2024-05-12 08:00:00',0),
('500000066','אורי','מגן','Jerusalem','Jabotinsky',8,'02-6000066','2024-05-12 08:00:00',0),
('500000067','גילי','זיו','Ramat Gan','Jabotinsky',15,'03-6000067','2024-05-12 08:00:00',0),
('500000068','עמית','אלון','Tel Aviv','Ibn Gabirol',5,'03-6000068','2024-05-12 08:00:00',0);


INSERT INTO Flights(Flight_id, Dep_DateTime, Status, Aircraft_id, Route_id) VALUES
('FT001', '2025-06-01 08:00:00', 'Completed', 'ACA002', 'R001'),
('FT002', '2025-06-02 09:00:00', 'Completed', 'ACD004', 'R002'),

('FT003', '2025-06-10 07:00:00', 'Completed', 'ACB001', 'R003'),
('FT004', '2025-06-12 10:00:00', 'Completed', 'ACD003', 'R004'),

('FT005', '2025-06-15 14:00:00', 'Completed', 'ACA005', 'R003'),

('FT006', '2025-06-20 09:00:00', 'Completed', 'ACB006', 'R002'),

('FT007', '2025-06-22 08:00:00', 'Cancelled', 'ACD013', 'R001'),
('FT008', '2025-06-28 09:00:00', 'Cancelled', 'ACB014', 'R002'),
('FT009', '2025-07-20 07:00:00', 'Cancelled', 'ACB016', 'R005'),

('FT010', '2026-04-05 09:00:00', 'Active', 'ACB010', 'R003'),

('FT011', '2026-04-06 08:00:00', 'Active', 'ACB007', 'R009'),
('FT012', '2026-04-07 10:00:00', 'Active', 'ACA008', 'R011'),
('FT013', '2026-04-07 13:00:00', 'Active', 'ACD009', 'R012');


INSERT INTO FlightSeats (FlightSeat_id, Flight_id, Seat_id, Seat_Price, Seat_Status) VALUES
('FS000001', 'FT001', 'S007', 450.00, 'Sold'),
('FS000002', 'FT001', 'S008', 450.00, 'Sold'),
('FS000003', 'FT001', 'S009', 450.00, 'Available'),
('FS000004', 'FT001', 'S010', 450.00, 'Available'),

('FS000005', 'FT002', 'S017', 400.00, 'Sold'),
('FS000006', 'FT002', 'S018', 400.00, 'Available'),
('FS000007', 'FT002', 'S019', 400.00, 'Available'),
('FS000008', 'FT002', 'S020', 400.00, 'Available'),

('FS000009', 'FT003', 'S001', 1200.00, 'Sold'),
('FS000010','FT003', 'S002', 1200.00, 'Sold'),
('FS000011','FT003', 'S003', 800.00,  'Sold'),
('FS000012','FT003', 'S004', 800.00,  'Available'),
('FS000013','FT003', 'S005', 800.00,  'Available'),
('FS000014','FT003', 'S006', 800.00,  'Available'),

('FS000015','FT004', 'S011', 1000.00, 'Sold'),
('FS000016','FT004', 'S012', 1000.00, 'Sold'),
('FS000017','FT004', 'S013', 500.00,  'Sold'),
('FS000018','FT004', 'S014', 500.00,  'Sold'),
('FS000019','FT004', 'S015', 500.00,  'Available'),
('FS000020','FT004', 'S016', 500.00,  'Available'),

('FS000021','FT005', 'S021', 900.00,  'Sold'),
('FS000022','FT005', 'S022', 900.00,  'Sold'),
('FS000023','FT005', 'S023', 600.00,  'Available'),
('FS000024','FT005', 'S024', 600.00,  'Available'),
('FS000025','FT005', 'S025', 600.00,  'Available'),
('FS000026','FT005', 'S026', 600.00,  'Available'),

('FS000027','FT006', 'S027', 300.00,  'Sold'),
('FS000028','FT006', 'S028', 300.00,  'Sold'),
('FS000029','FT006', 'S029', 300.00,  'Available'),
('FS000030','FT006', 'S030', 300.00,  'Available'),

('FS000031','FT007', 'S007', 450.00,  'Available'),
('FS000032','FT007', 'S008', 450.00,  'Blocked'),
('FS000033','FT007', 'S009', 450.00,  'Available'),
('FS000034','FT007', 'S010', 450.00,  'Blocked'),

('FS000035','FT008', 'S001', 1200.00, 'Blocked'),
('FS000036','FT008', 'S002', 1200.00, 'Available'),
('FS000037','FT008', 'S003', 800.00,  'Blocked'),
('FS000038','FT008', 'S004', 800.00,  'Available'),
('FS000039','FT008', 'S005', 800.00,  'Available'),
('FS000040','FT008', 'S006', 800.00,  'Blocked'),

('FS000041','FT009', 'S021', 900.00,  'Blocked'),
('FS000042','FT009', 'S022', 900.00,  'Available'),
('FS000043','FT009', 'S023', 600.00,  'Available'),
('FS000044','FT009', 'S024', 600.00,  'Available'),
('FS000045','FT009', 'S025', 600.00,  'Available'),
('FS000046','FT009', 'S026', 600.00,  'Blocked'),

('FS000047','FT010', 'S047', 1400.00, 'Available'),
('FS000048','FT010', 'S048', 1400.00, 'Available'),
('FS000049','FT010', 'S049', 750.00,  'Available'),
('FS000050','FT010', 'S050', 750.00,  'Available'),
('FS000051','FT010', 'S051', 750.00,  'Available'),
('FS000052','FT010', 'S052', 750.00,  'Available'),

('FS000053','FT011', 'S031', 1200.00, 'Available'),
('FS000054','FT011', 'S032', 1200.00, 'Available'),
('FS000055','FT011', 'S033', 650.00,  'Available'),
('FS000056','FT011', 'S034', 650.00,  'Available'),
('FS000057','FT011', 'S035', 650.00,  'Available'),
('FS000058','FT011', 'S036', 650.00,  'Available'),

('FS000059','FT012', 'S037', 220.00,  'Available'),
('FS000060','FT012', 'S038', 220.00,  'Available'),
('FS000061','FT012', 'S039', 220.00,  'Available'),
('FS000062','FT012', 'S040', 220.00,  'Available'),

('FS000063','FT013', 'S041', 240.00,  'Available'),
('FS000064','FT013', 'S042', 240.00,  'Available'),
('FS000065','FT013', 'S043', 240.00,  'Available'),
('FS000066','FT013', 'S044', 240.00,  'Available');


INSERT INTO Orders(Order_code, Order_Date, Status, Cancel_Date, Customer_Email, Flight_id, Customer_Type) VALUES
('O000000001','2025-01-10 09:15:00','Completed',          NULL,'matan@flytau.com','FT001','Register'),
('O000000002','2025-01-22 16:40:00','Cancelled-Customer','2025-01-23 10:00:00','shira@flytau.com','FT001','Register'),

('O000000003','2025-02-05 11:20:00','Completed',          NULL,'roni@flytau.com','FT002','Guest'),
('O000000004','2025-02-18 14:00:00','Cancelled-Customer','2025-02-19 08:30:00','roni@flytau.com','FT002','Guest'),

('O000000005','2025-03-02 07:45:00','Completed',          NULL,'matan@flytau.com','FT003','Register'),
('O000000006','2025-03-15 12:10:00','Cancelled-Customer','2025-03-15 13:00:00','shira@flytau.com','FT003','Register'),

('O000000007','2025-04-01 10:30:00','Completed',          NULL,'daniel@flytau.com','FT004','Guest'),
('O000000008','2025-04-25 18:05:00','Cancelled-Customer','2025-04-15 19:00:00','roni@flytau.com','FT004','Guest'),

('O000000009','2025-05-03 09:00:00','Cancelled-Customer','2025-05-03 11:00:00','matan@flytau.com','FT005','Register'),
('O000000010','2025-05-20 15:45:00','Completed',          NULL,'shira@flytau.com','FT005','Register'),

('O000000011','2025-06-10 10:00:00','Completed',          NULL,'matan@flytau.com','FT006','Register'),
('O000000012','2025-06-18 16:30:00','Cancelled-Customer', NULL,'daniel@flytau.com','FT006','Guest');


INSERT INTO Tickets (FlightSeat_id, Order_code) VALUES
('FS000001', 'O000000001'),
('FS000002', 'O000000001'),

('FS000003', 'O000000002'),
('FS000004', 'O000000002'),

('FS000005', 'O000000003'),

('FS000006', 'O000000004'),
('FS000007', 'O000000004'),

('FS000009', 'O000000005'),
('FS000010', 'O000000005'),
('FS000011', 'O000000005'),

('FS000012', 'O000000006'),
('FS000013', 'O000000006'),

('FS000015', 'O000000007'),
('FS000016', 'O000000007'),
('FS000017', 'O000000007'),
('FS000018', 'O000000007'),

('FS000019', 'O000000008'),
('FS000020', 'O000000008'),

('FS000021', 'O000000010'),
('FS000022', 'O000000010'),

('FS000023', 'O000000009'),
('FS000024', 'O000000009'),

('FS000027', 'O000000011'),
('FS000028', 'O000000011'),

('FS000029', 'O000000012'),
('FS000030', 'O000000012');


INSERT INTO FlightCrew_Pilots (Pilot_id, Flight_id) VALUES
('400000004', 'FT001'),
('400000005', 'FT001'),

('400000006', 'FT002'),
('400000007', 'FT002'),

('400000001', 'FT003'),
('400000002', 'FT003'),
('400000003', 'FT003'),

('400000001', 'FT004'),
('400000002', 'FT004'),
('400000003', 'FT004'),

('400000011','FT005'),
('400000012','FT005'),
('400000013','FT005'),

('400000014','FT006'),
('400000015','FT006'),
('400000016','FT006'),

('400000017','FT007'),
('400000018','FT007'),

('400000019','FT008'),
('400000020','FT008'),

('400000011','FT009'),
('400000012','FT009'),
('400000013','FT009'),

('400000021', 'FT010'),
('400000022', 'FT010'),
('400000023', 'FT010'),

('400000024', 'FT011'),
('400000025', 'FT011'),
('400000026', 'FT011'),

('400000027', 'FT012'),
('400000028', 'FT012'),

('400000029', 'FT013'),
('400000030', 'FT013');


INSERT INTO FlightCrew_Attendants (Attendant_id, Flight_id) VALUES
('500000007', 'FT001'),
('500000008', 'FT001'),
('500000009', 'FT001'),

('500000010', 'FT002'),
('500000011', 'FT002'),
('500000012', 'FT002'),

('500000001', 'FT003'),
('500000002', 'FT003'),
('500000003', 'FT003'),
('500000004', 'FT003'),
('500000005', 'FT003'),
('500000006', 'FT003'),

('500000001', 'FT004'),
('500000002', 'FT004'),
('500000003', 'FT004'),
('500000004', 'FT004'),
('500000005', 'FT004'),
('500000006', 'FT004'),

('500000021', 'FT005'),
('500000022', 'FT005'),
('500000023', 'FT005'),
('500000024', 'FT005'),
('500000025', 'FT005'),
('500000026', 'FT005'),

('500000016', 'FT006'),
('500000017', 'FT006'),


('500000019', 'FT007'),
('500000020', 'FT007'),
('500000027', 'FT007'),

('500000028', 'FT008'),
('500000029', 'FT008'),
('500000030', 'FT008'),

('500000021', 'FT009'),
('500000022', 'FT009'),
('500000023', 'FT009'),
('500000024', 'FT009'),
('500000025', 'FT009'),
('500000026', 'FT009'),

('500000031', 'FT010'),
('500000032', 'FT010'),
('500000033', 'FT010'),
('500000034', 'FT010'),
('500000035', 'FT010'),
('500000036', 'FT010'),

-- original crews
('500000037', 'FT011'),
('500000038', 'FT011'),
('500000039', 'FT011'),
('500000040', 'FT011'),
('500000041', 'FT011'),
('500000042', 'FT011'),

('500000043', 'FT012'),
('500000044', 'FT012'),
('500000045', 'FT012'),

('500000046', 'FT013'),
('500000047', 'FT013'),
('500000048', 'FT013');



SELECT
    f.Flight_id, f.Dep_DateTime, DATE_ADD(f.Dep_DateTime, INTERVAL r.Duration_Minutes MINUTE) AS Arr_DateTime, CONCAT(r.Origin_Airport_code, ' -> ', r.Destination_Airport_code) AS Route,
    ao.City AS Origin_City, ad.City AS Destination_City,
    COUNT(fs.FlightSeat_id) AS Total_Seats, SUM(CASE WHEN fs.Seat_Status = 'Sold' THEN 1 ELSE 0 END) AS Sold_Seats,
    SUM(CASE WHEN fs.Seat_Status = 'Sold' THEN 1 ELSE 0 END) / COUNT(fs.FlightSeat_id) AS Load_Factor
FROM Flights f
JOIN Flight_Routes r ON f.Route_id = r.Route_id
JOIN Airports ao ON ao.Airport_code = r.Origin_Airport_code
JOIN Airports ad ON ad.Airport_code = r.Destination_Airport_code
JOIN FlightSeats fs ON f.Flight_id = fs.Flight_id
WHERE f.Status = 'Completed'
GROUP BY f.Flight_id, f.Dep_DateTime, r.Duration_Minutes, r.Origin_Airport_code, r.Destination_Airport_code, ao.City, ad.City 
ORDER BY f.Dep_DateTime;




    SELECT
    a.Size         AS Aircraft_Size,
    a.Manufacturer AS Aircraft_Manufacturer,
    s.Seat_Class   AS Seat_Class,
    COALESCE(SUM(CASE WHEN o.Status IN ('Active', 'Completed') THEN fs.Seat_Price 
            WHEN o.Status = 'Cancelled-Customer'
			AND o.Cancel_Date IS NOT NULL
			AND TIMESTAMPDIFF(HOUR, o.Cancel_Date, f.Dep_DateTime) >= 36 THEN 0.05 * fs.Seat_Price ELSE 0 END),0) AS Total_Revenue
FROM Aircrafts a
JOIN Seats s ON s.Aircraft_id = a.Aircraft_id
LEFT JOIN FlightSeats fs ON fs.Seat_id = s.Seat_id
JOIN Flights f ON f.Flight_id = fs.Flight_id AND f.Status <> 'Cancelled'     
LEFT JOIN Tickets t ON t.FlightSeat_id = fs.FlightSeat_id
LEFT JOIN Orders o ON o.Order_code = t.Order_code
GROUP BY a.Size, a.Manufacturer, s.Seat_Class
ORDER BY a.Size, a.Manufacturer, s.Seat_Class;





SELECT
    P.Pilot_id AS Employee_id,
    CONCAT(P.First_Name, ' ', P.Last_Name) AS Full_Name, 'Pilot' AS Employee_Type,
    SUM(CASE WHEN R.Duration_Minutes > 360 THEN R.Duration_Minutes ELSE 0 END) / 60.0 AS Long_Hours,
    SUM(CASE WHEN R.Duration_Minutes <= 360 THEN R.Duration_Minutes ELSE 0 END) / 60.0 AS Short_Hours
FROM Pilots P
JOIN FlightCrew_Pilots CP   ON P.Pilot_id   = CP.Pilot_id
JOIN Flights F              ON CP.Flight_id = F.Flight_id
JOIN Flight_Routes R        ON F.Route_id   = R.Route_id
WHERE F.Status = 'Completed'
GROUP BY P.Pilot_id, Full_Name

UNION ALL

SELECT
    A.Attendant_id AS Employee_id,
    CONCAT(A.First_Name, ' ', A.Last_Name) AS Full_Name, 'FlightAttendant' AS Employee_Type,
    SUM(CASE WHEN R.Duration_Minutes > 360 THEN R.Duration_Minutes ELSE 0 END) / 60.0 AS Long_Hours, 
	SUM(CASE WHEN R.Duration_Minutes <= 360 THEN R.Duration_Minutes ELSE 0 END) / 60.0 AS Short_Hours
FROM FlightAttendants A
JOIN FlightCrew_Attendants CA ON A.Attendant_id = CA.Attendant_id
JOIN Flights F                ON CA.Flight_id   = F.Flight_id
JOIN Flight_Routes R          ON F.Route_id     = R.Route_id
WHERE F.Status = 'Completed'
GROUP BY A.Attendant_id, Full_Name

ORDER BY Employee_Type, Full_Name;



SELECT
    DATE_FORMAT(Order_Date, '%Y-%m') AS YearMonth,         
    COUNT(*) AS Total_Orders,                               
    SUM(CASE WHEN Status IN ('Cancelled-Customer','Cancelled-System') THEN 1 ELSE 0 END) AS Cancelled_Orders,                                  
    ROUND(SUM(CASE WHEN Status IN ('Cancelled-Customer','Cancelled-System') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS Cancellation_Rate_Percent   
FROM Orders
GROUP BY DATE_FORMAT(Order_Date, '%Y-%m')
ORDER BY YearMonth;




WITH per_flight AS (
    SELECT f.Flight_id, f.Aircraft_id, DATE_FORMAT(f.Dep_DateTime, '%Y-%m-01') AS MonthStart, fr.Origin_Airport_code, fr.Destination_Airport_code,
           fr.Duration_Minutes, f.Status
    FROM Flights f
    JOIN Flight_Routes fr
      ON f.Route_id = fr.Route_id
),

agg_base AS (
    SELECT Aircraft_id, MonthStart, COUNT(*) AS Total_Flights, SUM(CASE WHEN Status = 'Completed' THEN 1 ELSE 0 END)  AS Flights_Completed,
           SUM(CASE WHEN Status = 'Cancelled' THEN 1 ELSE 0 END) AS Flights_Cancelled,
           SUM(CASE WHEN Status = 'Completed'THEN Duration_Minutes ELSE 0 END) AS Total_Flight_Minutes
    FROM per_flight
    GROUP BY Aircraft_id, MonthStart
),

route_counts AS (
    SELECT
        Aircraft_id, MonthStart, Origin_Airport_code, Destination_Airport_code, COUNT(*) AS Route_Flights,
        ROW_NUMBER() OVER (PARTITION BY Aircraft_id, MonthStart ORDER BY COUNT(*) DESC, Origin_Airport_code, Destination_Airport_code) AS rn
    FROM per_flight
    GROUP BY Aircraft_id, MonthStart, Origin_Airport_code, Destination_Airport_code
)

SELECT
    ab.Aircraft_id, ac.Manufacturer, ac.Model,
    DATE_FORMAT(ab.MonthStart, '%Y-%m') AS Month, ab.Flights_Completed, ab.Flights_Cancelled, ab.Total_Flights,
    ROUND(ab.Total_Flight_Minutes / (30 * 24 * 60) * 100,2) AS Utilization_Percent,CONCAT(rc.Origin_Airport_code,'-', rc.Destination_Airport_code) AS Dominant_Route
FROM agg_base AS ab
JOIN Aircrafts AS ac ON ac.Aircraft_id = ab.Aircraft_id
LEFT JOIN route_counts AS rc ON rc.Aircraft_id = ab.Aircraft_id AND rc.MonthStart = ab.MonthStart AND rc.rn = 1
ORDER BY ab.MonthStart, ab.Aircraft_id;




WITH flight_monthly AS (
    SELECT f.Flight_id, f.Aircraft_id, DATE_FORMAT(f.Dep_DateTime, '%Y-%m-01') AS MonthStart, fr.Origin_Airport_code,
        fr.Destination_Airport_code, fr.Duration_Minutes, f.Status
    FROM Flights f
    JOIN Flight_Routes fr ON f.Route_id = fr.Route_id
),

aircraft_month_summary AS (
    SELECT Aircraft_id, MonthStart, COUNT(*) AS Total_Flights, 
        SUM(CASE WHEN Status = 'Completed' THEN 1 ELSE 0 END)  AS Flights_Completed,
        SUM(CASE WHEN Status = 'Cancelled' THEN 1 ELSE 0 END)  AS Flights_Cancelled,
        SUM(CASE WHEN Status = 'Completed' THEN Duration_Minutes ELSE 0 END) AS Total_Flight_Minutes
    FROM flight_monthly
    GROUP BY Aircraft_id, MonthStart
),

top_route_rank AS (
    SELECT Aircraft_id, MonthStart, Origin_Airport_code, Destination_Airport_code, COUNT(*) AS Route_Completed_Flights,
        DENSE_RANK() OVER (PARTITION BY Aircraft_id, MonthStart ORDER BY COUNT(*) DESC) AS rk
    FROM flight_monthly
    WHERE Status = 'Completed'
    GROUP BY Aircraft_id, MonthStart, Origin_Airport_code, Destination_Airport_code
),

top_routes AS (
    SELECT Aircraft_id, MonthStart, Origin_Airport_code, Destination_Airport_code
    FROM top_route_rank
    WHERE rk = 1
),

top_routes_concat AS (
    SELECT Aircraft_id, MonthStart,
        GROUP_CONCAT(CONCAT(Origin_Airport_code, '-', Destination_Airport_code)ORDER BY Origin_Airport_code, Destination_Airport_code
		SEPARATOR ', ') AS Dominant_Routes
    FROM top_routes
    GROUP BY Aircraft_id, MonthStart
)

SELECT ms.Aircraft_id, ac.Manufacturer, ac.Model, DATE_FORMAT(ms.MonthStart, '%Y-%m') AS Month, ms.Flights_Completed,ms.Flights_Cancelled, ms.Total_Flights,
       ROUND(ms.Total_Flight_Minutes / (30 * 24 * 60) * 100, 2) AS Utilization_Percent, COALESCE(trc.Dominant_Routes, '-') AS Dominant_Routes
FROM aircraft_month_summary AS ms
JOIN Aircrafts AS ac ON ac.Aircraft_id = ms.Aircraft_id
LEFT JOIN top_routes_concat AS trc ON trc.Aircraft_id = ms.Aircraft_id AND trc.MonthStart  = ms.MonthStart
ORDER BY ms.MonthStart, ms.Aircraft_id;



/* ============================================================
   דוח 5 — פעילות חודשית לכל מטוס (ניצולת לפי "ימי פעילות")

   מטרת השאילתה:
   להציג לכל מטוס ולכל חודש:
   - סך טיסות (Total_Flights)
   - טיסות שבוצעו (Flights_Completed)
   - טיסות שבוטלו (Flights_Cancelled)
   - אחוז ניצולת חודשי: אחוז הימים בחודש בהם המטוס היה "בפעילות"
     (יום פעילות = קיים לפחות אירוע טיסה אחד שאינו Cancelled באותו יום)
   - מסלול דומיננטי בחודש (על בסיס טיסות Completed בלבד; בשוויון מציגים את כולם)

   הנחות/הגדרות:
   1) יום פעילות נספר אם קיימת לפחות טיסה אחת במצב:
      Active / Full-Occupied / Completed (כלומר: Status <> 'Cancelled').
   2) שיוך טיסה לחודש נעשה לפי תאריך ההמראה (Dep_DateTime).
   3) מעבר חודשים (טיסה שמתחילה בחודש ומסתיימת בחודש אחר):
      - ברמת הנתונים הקיימת (יש רק Dep_DateTime, והגעה נגזרת מה־Duration),
        אנו מתייחסים ליום(ים) הפעילות באופן הבא:
        * יום ההמראה תמיד נספר (אם הטיסה לא Cancelled).
        * כדי לספור גם את יום/ימי ההמשך בחודש הבא, אנו מחשבים Arrival_DateTime
          ומייצרים רשומת "יום פעילות" נוספת ליום ההגעה (אם שונה מיום ההמראה).
        כך, טיסה החוצה חצות/חודש יכולה לתרום יום פעילות גם לחודש הבא.
   4) אחוז הניצולת מחושב ביחס ל־30 ימים (כפי שנדרש בתרגיל),
      ולכן: Utilization_Percent = Active_Days / 30 * 100.
   ============================================================ */

WITH flight_base AS (
    SELECT f.Flight_id, f.Aircraft_id,  DATE_FORMAT(f.Dep_DateTime, '%Y-%m-01') AS MonthStart, fr.Origin_Airport_code, fr.Destination_Airport_code,
        fr.Duration_Minutes, f.Status, DATE(f.Dep_DateTime) AS DepDay,
        DATE_ADD(f.Dep_DateTime, INTERVAL fr.Duration_Minutes MINUTE) AS ArrDT,
        DATE(DATE_ADD(f.Dep_DateTime, INTERVAL fr.Duration_Minutes MINUTE)) AS ArrDay
    FROM Flights f
    JOIN Flight_Routes fr ON f.Route_id = fr.Route_id
),

flight_days AS (
    SELECT
        Aircraft_id, DATE_FORMAT(DepDay, '%Y-%m-01') AS MonthStart, DepDay AS ActivityDay
    FROM flight_base
    WHERE Status <> 'Cancelled'
    UNION ALL
    SELECT
        Aircraft_id, DATE_FORMAT(ArrDay, '%Y-%m-01') AS MonthStart, ArrDay AS ActivityDay
    FROM flight_base
    WHERE Status <> 'Cancelled' AND ArrDay <> DepDay
),

flight_monthly AS (
    SELECT
        Flight_id, Aircraft_id, MonthStart, Origin_Airport_code, Destination_Airport_code, Duration_Minutes, Status
    FROM flight_base
),

aircraft_month_summary AS (
    SELECT
        fm.Aircraft_id, fm.MonthStart, COUNT(*) AS Total_Flights,
        SUM(CASE WHEN fm.Status = 'Completed' THEN 1 ELSE 0 END) AS Flights_Completed,
        SUM(CASE WHEN fm.Status = 'Cancelled' THEN 1 ELSE 0 END) AS Flights_Cancelled, 
        COALESCE(fd.Active_Days, 0) AS Active_Days
    FROM flight_monthly fm
    LEFT JOIN (SELECT Aircraft_id, MonthStart, COUNT(DISTINCT ActivityDay) AS Active_Days
        FROM flight_days
        GROUP BY Aircraft_id, MonthStart) fd ON fd.Aircraft_id = fm.Aircraft_id AND fd.MonthStart  = fm.MonthStart
    GROUP BY fm.Aircraft_id, fm.MonthStart, fd.Active_Days
),

top_route_rank AS (
    SELECT
        Aircraft_id, MonthStart, Origin_Airport_code, Destination_Airport_code, COUNT(*) AS Route_Completed_Flights,
        DENSE_RANK() OVER (PARTITION BY Aircraft_id, MonthStart Order BY COUNT(*) DESC) AS rk
    FROM flight_monthly
    WHERE Status = 'Completed' 
    GROUP BY Aircraft_id, MonthStart, Origin_Airport_code, Destination_Airport_code
),

top_routes_concat AS (
    SELECT
        Aircraft_id, MonthStart, 
        GROUP_CONCAT(CONCAT(Origin_Airport_code, '→', Destination_Airport_code) ORDER BY Origin_Airport_code, Destination_Airport_code SEPARATOR ',') AS Dominant_Routes
    FROM top_route_rank
    WHERE rk = 1
    GROUP BY Aircraft_id, MonthStart
)

SELECT
    ms.Aircraft_id, ac.Manufacturer, ac.Model,DATE_FORMAT(ms.MonthStart, '%Y-%m') AS Month, ms.Flights_Completed, ms.Flights_Cancelled, ms.Total_Flights,
    ROUND(ms.Active_Days / 30 * 100, 2) AS Utilization_Percent, COALESCE(trc.Dominant_Routes, '-') AS Dominant_Routes
FROM aircraft_month_summary AS ms
JOIN Aircrafts AS ac ON ac.Aircraft_id = ms.Aircraft_id
LEFT JOIN top_routes_concat AS trc ON trc.Aircraft_id = ms.Aircraft_id AND trc.MonthStart  = ms.MonthStart
WHERE ms.MonthStart < DATE_FORMAT(CURDATE(), '%Y-%m-01')
ORDER BY ms.MonthStart, ms.Aircraft_id;





