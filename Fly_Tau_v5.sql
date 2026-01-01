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
('R001', 270, 'TLV', 'LHR'),   -- 4.5 hours
('R002', 240, 'TLV', 'CDG'),   -- 4  hours
('R003', 660, 'TLV', 'JFK'),   -- 11 hours (LONG)
('R004', 420, 'JFK', 'LHR'),   -- 7  hours (LONG)
('R005', 720, 'JFK', 'TLV'),	-- 12  hours (LONG)
('R006', 300, 'LHR', 'TLV'),   -- 5  hours
('R007', 255, 'CDG', 'TLV'),   -- 4.25 hours
('R008', 420, 'LHR', 'JFK'),   -- 7  hours (LONG)
('R009', 480, 'JFK', 'CDG'),   -- 8  hours (LONG)
('R010', 510, 'CDG', 'JFK'),   
('R011',  75, 'LHR', 'CDG'),   
('R012',  90, 'CDG', 'LHR');   

INSERT INTO Aircrafts (Aircraft_id, Manufacturer, Model, Size, Purchase_Date) VALUES
('ACB001', 'Boeing',   '787-9 Dreamliner', 'Large', '2015-01-10 00:00:00'),
('ACA002', 'Airbus',   'A320neo',          'Small', '2018-05-15 00:00:00'),
('ACD003', 'Dasso', 'Falcon 900EX',    'Large', '2016-03-20 00:00:00'),
('ACD004', 'Dasso',  'E190',            'Small', '2019-07-01 00:00:00'),
('ACA005', 'Airbus',   'A321-277',         'Large', '2014-11-11 00:00:00'),
('ACB006', 'Boeing',      '737-300',          'Small', '2020-02-02 00:00:00');

INSERT INTO Aircrafts (Aircraft_id, Manufacturer, Model, Size, Purchase_Date) VALUES
('ACB007', 'Boeing',   '777-300ER',         'Large', '2013-09-18 00:00:00'),
('ACA008', 'Airbus',   'A220-300',          'Small', '2021-04-05 00:00:00'),
('ACD009', 'Dasso',    'E195-E2',           'Small', '2022-08-12 00:00:00'),
('ACB010', 'Boeing',   '787-10 Dreamliner', 'Large', '2017-12-01 00:00:00');

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
('S030', 'ACB006', 2, 2, 'Economy');

-- AC007 (Large, 6 seats: 3x2)
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

INSERT INTO Guest_Customers
    (Customer_Email, First_Name, Last_Name)
VALUES
    ('daniel@flytau.com', 'Daniel', 'Messer'),
    ('roni@flytau.com',   'Roni',   'Levy');

INSERT INTO Guest_Customers_Phones (Customer_Email, Phone_Number) VALUES
    ('daniel@flytau.com', '050-3333333'),
    ('roni@flytau.com',   '050-4444444');


INSERT INTO Managers(Manager_id, First_Name, Last_Name, City, Street, House_Number,Phone_Number, Start_Working_Date, Manager_Password) VALUES
('300000001', 'אריאל',  'לוי',   'Tel Aviv', 'Herzl',        10,
 '03-1111111', '2018-01-01 08:00:00', 'managerPass1'),
('300000002', 'נעה',    'כהן',  'Haifa',    'Hagana',       5,
 '04-2222222', '2019-03-01 08:00:00', 'managerPass2');

INSERT INTO Pilots
(Pilot_id, First_Name, Last_Name, City, Street, House_Number,
 Phone_Number, Start_Working_Date, Long_Haul_Certified)
VALUES
('400000001', 'יוסי',   'כץ',      'Tel Aviv',   'Herzl',       15, '03-5000001', '2015-01-01 08:00:00', 1),
('400000002', 'דנה',    'בר',      'Tel Aviv',   'Herzl',       20, '03-5000002', '2016-02-01 08:00:00', 1),
('400000003', 'אבי',    'שלו',    'Jerusalem',  'King David',   5, '02-5000003', '2014-05-01 08:00:00', 1),
('400000004', 'ליאור',  'שרון',    'Haifa',      'Hagana',       8, '04-5000004', '2017-07-01 08:00:00', 1),
('400000005', 'רון',    'טל',      'Beer Sheva', 'Rager',       12, '08-5000005', '2019-01-01 08:00:00', 0),
('400000006', 'ניר',    'אמיר',    'Tel Aviv',   'Dizengoff',    3, '03-5000006', '2020-03-01 08:00:00', 0),
('400000007', 'טל',     'ירדן',    'Haifa',      'Hagana',      18, '04-5000007', '2021-04-01 08:00:00', 0),
('400000008', 'אדם',    'רונן',    'Jerusalem',  'Jabotinsky',   7, '02-5000008', '2022-01-01 08:00:00', 0),
('400000009', 'מאיה',   'רגב',    'Tel Aviv',   'Ibn Gabirol',  9, '03-5000009', '2023-01-01 08:00:00', 0),
('400000010','עומר',    'שי',      'Rishon',     'Weizmann',    11, '03-5000010', '2023-06-01 08:00:00', 0),

('400000011','דודי',    'כהן',    'Tel Aviv',   'Herzl',       50, '03-5000011', '2023-08-01 08:00:00', 1),
('400000012','שרון',    'לוי',    'Haifa',      'Hagana',      33, '04-5000012', '2023-08-01 08:00:00', 1),
('400000013','רינה',    'אלון',   'Jerusalem',  'King David',   9, '02-5000013', '2023-08-01 08:00:00', 1),

('400000014','כפיר',    'בן דוד', 'Tel Aviv',   'Dizengoff',   15, '03-5000014', '2023-08-01 08:00:00', 0),
('400000015','מור',     'אליאס',  'Haifa',      'Herzl',       22, '04-5000015', '2023-08-01 08:00:00', 0),
('400000016','שחר',    'כהן',    'Beer Sheva', 'Rager',       18, '08-5000016', '2023-08-01 08:00:00', 0),
('400000017','אילן',    'רוזן',   'Tel Aviv',   'Arlozorov',   21, '03-5000017', '2023-08-01 08:00:00', 0),
('400000018','אפרת',   'זהבי',   'Jerusalem',  'Jabotinsky',  16, '02-5000018', '2023-08-01 08:00:00', 0),
('400000019','חן',      'גל',     'Haifa',      'Hagana',      12, '04-5000019', '2023-08-01 08:00:00', 0),
('400000020','עדי',     'רום',    'Rishon',     'Weizmann',    25, '03-5000020', '2023-08-01 08:00:00', 0);


INSERT INTO FlightAttendants
(Attendant_id, First_Name, Last_Name, City, Street, House_Number,
 Phone_Number, Start_Working_Date, Long_Haul_Certified)
VALUES
('500000001', 'עדי',   'לוי',     'Tel Aviv',   'Herzl',       3,  '03-6000001', '2017-02-01 08:00:00', 1),
('500000002', 'גל',    'שני',     'Haifa',      'Hagana',     14,  '04-6000002', '2018-03-01 08:00:00', 1),
('500000003', 'נועם',  'ברק',     'Jerusalem',  'King David',  2,  '02-6000003', '2016-04-01 08:00:00', 1),
('500000004', 'רוני',  'טל',      'Tel Aviv',   'Dizengoff',  22,  '03-6000004', '2015-05-01 08:00:00', 1),
('500000005', 'עדן',   'כהן',     'Tel Aviv',   'Arlozorov',   7,  '03-6000005', '2019-01-01 08:00:00', 1),
('500000006', 'איתי',  'מזרחי',   'Haifa',      'Herzl',      27,  '04-6000006', '2019-06-01 08:00:00', 1),
('500000007', 'יעל',   'גרין',    'Beer Sheva', 'Rager',      19,  '08-6000007', '2020-02-01 08:00:00', 1),
('500000008', 'נטע',   'אביב',    'Jerusalem',  'Ben Yehuda',  6,  '02-6000008', '2020-09-01 08:00:00', 1),

('500000009', 'ליה',   'שי',      'Tel Aviv',   'Herzl',      31,  '03-6000009', '2021-01-01 08:00:00', 0),
('500000010','עידו',   'פרץ',     'Haifa',      'Hagana',      9,  '04-6000010', '2021-03-01 08:00:00', 0),
('500000011','ים',     'אורן',    'Ramat Gan',  'Jabotinsky', 13,  '03-6000011', '2021-05-01 08:00:00', 0),
('500000012','אייל',   'רום',     'Tel Aviv',   'Dizengoff',  28,  '03-6000012', '2021-07-01 08:00:00', 0),
('500000013','נועה',   'מור',     'Tel Aviv',   'Arlosorov',  16,  '03-6000013', '2022-01-01 08:00:00', 0),
('500000014','עמית',   'רז',      'Haifa',      'Herzl',      21,  '04-6000014', '2022-03-01 08:00:00', 0),
('500000015','שגיא',   'בר',      'Beer Sheva', 'Rager',      23,  '08-6000015', '2022-05-01 08:00:00', 0),
('500000016','הילה',   'לוי',     'Jerusalem',  'King David',  4,  '02-6000016', '2022-07-01 08:00:00', 0),
('500000017','מאור',   'טל',      'Tel Aviv',   'Ibn Gabirol', 5,  '03-6000017', '2023-01-01 08:00:00', 0),
('500000018','לילך',   'הראל',    'Haifa',      'Hagana',     30,  '04-6000018', '2023-03-01 08:00:00', 0),
('500000019','רועי',   'גולד',    'Rishon',     'Weizmann',   20,  '03-6000019', '2023-05-01 08:00:00', 0),
('500000020','דנה',    'שיר',     'Tel Aviv',   'Herzl',      40,  '03-6000020', '2023-07-01 08:00:00', 0),

('500000021','אסף',    'לוין',    'Tel Aviv',   'Herzl',      45, '03-6000021', '2023-08-01 08:00:00', 1),
('500000022','רותם',   'כהן',     'Haifa',      'Hagana',     26, '04-6000022', '2023-08-01 08:00:00', 1),
('500000023','שקד',    'גיל',     'Jerusalem',  'Ben Yehuda', 11, '02-6000023', '2023-08-01 08:00:00', 1),
('500000024','אלון',   'ברק',     'Tel Aviv',   'Dizengoff',  35, '03-6000024', '2023-08-01 08:00:00', 1),
('500000025','נוי',    'זיו',     'Ramat Gan',  'Jabotinsky', 17, '03-6000025', '2023-08-01 08:00:00', 1),
('500000026','רז',     'אוחנה',   'Beer Sheva', 'Rager',      29, '08-6000026', '2023-08-01 08:00:00', 1),

('500000027','נטלי',  'כץ',      'Tel Aviv',   'Ibn Gabirol',13, '03-6000027', '2023-08-01 08:00:00', 0),
('500000028','עמית',  'בלום',    'Haifa',      'Herzl',      37, '04-6000028', '2023-08-01 08:00:00', 0),
('500000029','אורי',  'מגן',     'Jerusalem',  'King David', 19, '02-6000029', '2023-08-01 08:00:00', 0),
('500000030','גילי',  'דגן',     'Rishon',     'Weizmann',   27, '03-6000030', '2023-08-01 08:00:00', 0);


    
   INSERT INTO Flights(Flight_id, Dep_DateTime, Status, Aircraft_id, Route_id) VALUES
    ('FT001', '2025-06-01 08:00:00', 'Completed', 'ACA002', 'R001'),
    ('FT002', '2025-06-02 09:00:00', 'Completed', 'ACD004', 'R002'),

    ('FT003', '2025-06-10 07:00:00', 'Completed', 'ACB001', 'R003'),
    ('FT004', '2025-06-12 10:00:00', 'Completed', 'ACD003', 'R004'),

    ('FT005', '2025-06-15 14:00:00', 'Completed', 'ACA005', 'R001'),
    
    ('FT006', '2025-06-20 09:00:00', 'Completed', 'ACB006', 'R002'), 
    
    ('FT007', '2025-06-22 08:00:00','Cancelled', 'ACA002', 'R001'),

    ('FT008', '2025-06-28 09:00:00','Cancelled', 'ACD004', 'R002'),

    ('FT009', '2025-07-20 07:00:00','Cancelled', 'ACB001', 'R005');

INSERT INTO FlightSeats
    (FlightSeat_id, Flight_id, Seat_id, Seat_Price, Seat_Status)
VALUES
    -- F001 on AC002 (Seats S007–S010)
    ('FS000001', 'FT001', 'S007', 450.00, 'Sold'),
    ('FS000002', 'FT001', 'S008', 450.00, 'Sold'),
    ('FS000003', 'FT001', 'S009', 450.00, 'Available'),
    ('FS000004', 'FT001', 'S010', 450.00, 'Available'),

    ('FS000005', 'FT002', 'S017', 400.00, 'Sold'),
    ('FS000006', 'FT002', 'S018', 400.00, 'Available'),
    ('FS000007', 'FT002', 'S019', 400.00, 'Available'),
    ('FS000008', 'FT002', 'S020', 400.00, 'Available'),

    ('FS000009',  'FT003', 'S001', 1200.00, 'Sold'),
    ('FS000010',  'FT003', 'S002', 1200.00, 'Sold'),
    ('FS000011',  'FT003', 'S003', 800.00, 'Sold'),
    ('FS000012',  'FT003', 'S004', 800.00, 'Available'),
    ('FS000013',  'FT003', 'S005', 800.00, 'Available'),
    ('FS000014',  'FT003', 'S006', 800.00, 'Available'),

    ('FS000015',  'FT004', 'S011', 1000.00, 'Sold'),
    ('FS000016',  'FT004', 'S012', 1000.00, 'Sold'),
    ('FS000017',  'FT004', 'S013', 500.00, 'Sold'),
    ('FS000018',  'FT004', 'S014', 500.00, 'Sold'),
    ('FS000019',  'FT004', 'S015', 500.00, 'Available'),
    ('FS000020',  'FT004', 'S016', 500.00, 'Available'),

    ('FS000021',  'FT005', 'S021', 900.00, 'Sold'),
    ('FS000022',  'FT005', 'S022', 900.00, 'Sold'),
    ('FS000023',  'FT005', 'S023', 600.00, 'Available'),
    ('FS000024',  'FT005', 'S024', 600.00, 'Available'),
    ('FS000025',  'FT005', 'S025', 600.00, 'Available'),
    ('FS000026',  'FT005', 'S026', 600.00, 'Available'),
    
    ('FS000027', 'FT006', 'S027', 300.00, 'Sold'),
    ('FS000028', 'FT006', 'S028', 300.00, 'Sold'),
    ('FS000029', 'FT006', 'S029', 300.00, 'Available'),
    ('FS000030', 'FT006', 'S030', 300.00, 'Available'),
    
    ('FS000031', 'FT007', 'S007', 450.00, 'Blocked'),
    ('FS000032', 'FT007', 'S008', 450.00, 'Blocked'),
    ('FS000033', 'FT007', 'S009', 450.00, 'Blocked'),
    ('FS000034', 'FT007', 'S010', 450.00, 'Blocked'),

    ('FS000035', 'FT008', 'S001', 1200.00, 'Blocked'),
    ('FS000036', 'FT008', 'S002', 1200.00, 'Blocked'),
    ('FS000037', 'FT008', 'S003',  800.00, 'Blocked'),
    ('FS000038', 'FT008', 'S004',  800.00, 'Blocked'),
    ('FS000039', 'FT008', 'S005',  800.00, 'Blocked'),
    ('FS000040', 'FT008', 'S006',  800.00, 'Blocked'),

    ('FS000041', 'FT009', 'S021', 900.00, 'Blocked'),
    ('FS000042', 'FT009', 'S022', 900.00, 'Blocked'),
    ('FS000043', 'FT009', 'S023', 600.00, 'Blocked'),
    ('FS000044', 'FT009', 'S024', 600.00, 'Blocked'),
    ('FS000045', 'FT009', 'S025', 600.00, 'Blocked'),
    ('FS000046', 'FT009', 'S026', 600.00, 'Blocked');


INSERT INTO Orders(Order_code,  Order_Date, Status, Cancel_Date, Customer_Email,  Flight_id, Customer_Type) VALUES
    -- January 2025
    ('O000000001','2025-01-10 09:15:00','Completed',          NULL,
     'matan@flytau.com','FT001','Register'),

    ('O000000002','2025-01-22 16:40:00','Cancelled-Customer','2025-01-23 10:00:00',
     'shira@flytau.com','FT001','Register'),

    -- February 2025
    ('O000000003','2025-02-05 11:20:00','Completed',          NULL,
     'roni@flytau.com','FT002','Guest'),

    ('O000000004','2025-02-18 14:00:00','Cancelled-Customer','2025-02-19 08:30:00',
     'roni@flytau.com','FT002','Guest'),

    -- March 2025
    ('O000000005','2025-03-02 07:45:00','Completed',          NULL,
     'matan@flytau.com','FT003','Register'),

    ('O000000006','2025-03-15 12:10:00','Cancelled-Customer','2025-03-15 13:00:00',
     'shira@flytau.com','FT003','Register'),

    -- April 2025
    ('O000000007','2025-04-01 10:30:00','Completed',          NULL,
     'daniel@flytau.com','FT004','Guest'),

    ('O000000008','2025-04-25 18:05:00','Cancelled-Customer','2025-04-15 19:00:00',
     'roni@flytau.com','FT004','Guest'),

    -- May 2025
    ('O000000009','2025-05-03 09:00:00','Cancelled-Customer','2025-05-03 11:00:00',
     'matan@flytau.com','FT005','Register'),

    ('O000000010','2025-05-20 15:45:00','Completed',          NULL,
     'shira@flytau.com','FT005','Register'),
    
    -- June 2025
    ('O000000011','2025-06-10 10:00:00','Completed',          NULL,
     'matan@flytau.com','FT006','Register'),

    ('O000000012','2025-06-18 16:30:00','Cancelled-Customer', NULL,
     'daniel@flytau.com','FT006','Guest');


-- INSERT INTO Tickets (FlightSeat_id, Order_id, Ticket_Status) VALUES
-- ('FS009', 'O000000001', 'Active'),
-- ('FS010', 'O000000001', 'Active'),
-- ('FS001', 'O000000002', 'Active'),
-- ('FS013', 'O000000003', 'Cancelled');

INSERT INTO Tickets
    (FlightSeat_id, Order_code)
VALUES
    -- F001 (Completed order)
    ('FS000001', 'O000000001'),
    ('FS000002', 'O000000001'),

    -- F001 (Cancelled-Customer order) – seats now Available
    ('FS000003', 'O000000002'),
    ('FS000004', 'O000000002'),

    -- F002 (Completed order)
    ('FS000005', 'O000000003'),

    -- F002 (Cancelled-Customer order)
    ('FS000006', 'O000000004'),
    ('FS000007', 'O000000004'),

    -- F003 (Completed order)
    ('FS000009', 'O000000005'),
    ('FS000010', 'O000000005'),
    ('FS000011', 'O000000005'),

    -- F003 (Cancelled-Customer order)
    ('FS000012', 'O000000006'),
    ('FS000013', 'O000000006'),

    -- F004 (Completed order)
    ('FS000015', 'O000000007'),
    ('FS000016', 'O000000007'),
    ('FS000017', 'O000000007'),
    ('FS000018', 'O000000007'),

    -- F004 (Cancelled-Customer order)
    ('FS000019', 'O000000008'),
    ('FS000020', 'O000000008'),

    -- F005 (Completed order)
    ('FS000021', 'O000000010'),
    ('FS000022', 'O000000010'),

    -- F005 (Cancelled-Customer order)
    ('FS000023', 'O000000009'),
    ('FS000024', 'O000000009'),

    -- F006 (Completed order)
    ('FS000027', 'O000000011'),
    ('FS000028', 'O000000011'),
    
    ('FS000029', 'O000000012'),
    ('FS000030', 'O000000012');

    
INSERT INTO IdCounters (Name, NextNum)
SELECT 'FlightSeat', COALESCE(MAX(CAST(SUBSTRING(FlightSeat_id, 3) AS UNSIGNED)), 0) + 1
FROM FlightSeats
ON DUPLICATE KEY UPDATE NextNum =
(
  SELECT COALESCE(MAX(CAST(SUBSTRING(FlightSeat_id, 3) AS UNSIGNED)), 0) + 1
  FROM FlightSeats
);

INSERT INTO FlightCrew_Pilots (Pilot_id, Flight_id) VALUES
-- F001 (TLV→LHR, small) – 2 pilots, כל אחד טיסה אחת
('400000004', 'FT001'),
('400000005', 'FT001'),

-- F002 (TLV→CDG, small)
('400000006', 'FT002'),
('400000007', 'FT002'),

-- F003 (TLV→JFK, long-haul, Large) – 3 long-haul pilots
('400000001', 'FT003'),
('400000002', 'FT003'),
('400000003', 'FT003'),

-- F004 (JFK→LHR, long-haul, Large) – המשך ישיר של F003 לאותם טייסים
('400000001', 'FT004'),
('400000002', 'FT004'),
('400000003', 'FT004'),

-- F005 (TLV→LHR, Large short) – טייסים ייחודיים לטיסה הזו
('400000008', 'FT005'),
('400000009', 'FT005'),
('400000010','FT005'),

-- F006 (TLV→CDG, Large short)
('400000014','FT006'),
('400000015','FT006'),
('400000016','FT006'),

-- F007 (TLV→LHR, small, Cancelled) – כל אחד טיסה אחת בלבד
('400000017','FT007'),
('400000018','FT007'),

-- F008 (TLV→CDG, small, Cancelled)
('400000019','FT008'),
('400000020','FT008'),

-- F009 (TLV→JFK, long-haul, Cancelled) – long-haul certified pilots, טיסה ראשונה ויחידה שלהם
('400000011','FT009'),
('400000012','FT009'),
('400000013','FT009');


INSERT INTO FlightCrew_Attendants (Attendant_id, Flight_id) VALUES
-- F001 (small) – 3 attendants
('500000007', 'FT001'),
('500000008', 'FT001'),
('500000009', 'FT001'),

-- F002 (small)
('500000010', 'FT002'),
('500000011', 'FT002'),
('500000012', 'FT002'),

-- F003 (long-haul, Large) – 6 certified attendants (1–6)
('500000001', 'FT003'),
('500000002', 'FT003'),
('500000003', 'FT003'),
('500000004', 'FT003'),
('500000005', 'FT003'),
('500000006', 'FT003'),

-- F004 (long-haul, Large) – המשך ישיר של F003 לאותם 6
('500000001', 'FT004'),
('500000002', 'FT004'),
('500000003', 'FT004'),
('500000004', 'FT004'),
('500000005', 'FT004'),
('500000006', 'FT004'),

-- F005 (Large short) – 3 attendants
('500000013', 'FT005'),
('500000014', 'FT005'),
('500000015', 'FT005'),

-- F006 (Large short)
('500000016', 'FT006'),
('500000017', 'FT006'),
('500000018', 'FT006'),

-- F007 (small, Cancelled)
('500000019', 'FT007'),
('500000020', 'FT007'),
('500000027', 'FT007'),

-- F008 (small, Cancelled)
('500000028', 'FT008'),
('500000029', 'FT008'),
('500000030', 'FT008'),

-- F009 (long-haul, Cancelled) – 6 new certified attendants (21–26), טיסה יחידה לכל אחד
('500000021', 'FT009'),
('500000022', 'FT009'),
('500000023', 'FT009'),
('500000024', 'FT009'),
('500000025', 'FT009'),
('500000026', 'FT009');


/* Report 1: Average load factor of flights that actually took place */
SELECT
    f.Flight_id,
    f.Dep_DateTime,
    #f.Arr_DateTime,
    COUNT(fs.FlightSeat_id) AS Total_Seats,
    SUM(CASE WHEN fs.Seat_Status = 'Sold' THEN 1 ELSE 0 END) AS Sold_Seats,
    SUM(CASE WHEN fs.Seat_Status = 'Sold' THEN 1 ELSE 0 END) / COUNT(fs.FlightSeat_id) AS Load_Factor
FROM Flights f
JOIN FlightSeats fs ON f.Flight_id = fs.Flight_id
WHERE f.Status = 'Completed'
GROUP BY f.Dep_DateTime, f.Flight_id;
#GROUP BY f.Dep_DateTime, f.Arr_DateTime, f.Flight_id;

/* Report 2: Revenue by Aircraft Size, Manufacturer and Seat Class */

/*SELECT
    a.Size         AS Aircraft_Size,
    a.Manufacturer AS Aircraft_Manufacturer,
    s.Seat_Class   AS Seat_Class,
    COALESCE(SUM(fs.Seat_Price), 0) AS Total_Revenue    -- avoid null(instead put 0)
FROM Aircrafts a
JOIN Seats s ON s.Aircraft_id = a.Aircraft_id            -- all seats in all flights
LEFT JOIN Flights f ON f.Aircraft_id = a.Aircraft_id AND f.Status <> 'Cancelled'                  -- only not cancelled flights
LEFT JOIN FlightSeats fs ON fs.Flight_id = f.Flight_id AND fs.Seat_id   = s.Seat_id AND fs.Seat_Status = 'Sold'                  -- only sold tickets
LEFT JOIN Tickets t  ON t.FlightSeat_id = fs.FlightSeat_id
LEFT JOIN Orders o ON o.Order_code = t.Order_code AND o.Status IN ('Completed','Active')       -- only active/completed orders
GROUP BY a.Size, a.Manufacturer, s.Seat_Class
ORDER BY a.Size, a.Manufacturer, s.Seat_Class;*/


/* Report 2:
   Revenue by aircraft size, manufacturer, and seat class.

   Rules:
   - Active / Completed orders: 100% of ticket price.
   - Cancelled-Customer orders:
       * if cancelled at least 36 hours before departure → 5% fee.
       * otherwise → 0 revenue.
   - Cancelled-System orders → 0 revenue.
   - Cancelled flights (Flights.Status = 'Cancelled') are excluded.
*/

/*SELECT
    a.Size         AS Aircraft_Size,
    a.Manufacturer AS Aircraft_Manufacturer,
    s.Seat_Class   AS Seat_Class,
    COALESCE(SUM(CASE WHEN o.Status IN ('Active', 'Completed') THEN fs.Seat_Price
				 WHEN o.Status = 'Cancelled-Customer' AND o.Cancel_Date IS NOT NULL
				 AND TIMESTAMPDIFF(HOUR,o.Cancel_Date,f.Dep_DateTime) >= 36 THEN 0.05 * fs.Seat_Price 
				 ELSE 0 END),0) AS Total_Revenue
FROM Aircrafts a
JOIN Seats s ON s.Aircraft_id = a.Aircraft_id
LEFT JOIN FlightSeats fs ON fs.Seat_id = s.Seat_id
LEFT JOIN Flights f ON f.Flight_id    = fs.Flight_id AND f.Status <> 'Cancelled'
LEFT JOIN Tickets t ON t.FlightSeat_id = fs.FlightSeat_id
LEFT JOIN Orders o ON o.Order_code = t.Order_code
GROUP BY
    a.Size, a.Manufacturer, s.Seat_Class
ORDER BY
    a.Size, a.Manufacturer,s.Seat_Class;*/
    
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

/* Report 3: Cumulative flight hours per employee, separated into long and short flights(long flight = DurationMinutes > 360) */
#PILOTS
/*SELECT
    P.Pilot_id AS Employee_id,
    CONCAT(P.First_Name, ' ', P.Last_Name) AS Full_Name,
    'Pilot' AS Employee_Type,
    SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, F.Dep_DateTime, F.Arr_DateTime) > 360 THEN TIMESTAMPDIFF(MINUTE, F.Dep_DateTime, F.Arr_DateTime) ELSE 0 END) / 60.0 AS Long_Hours,
    SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, F.Dep_DateTime, F.Arr_DateTime) <= 360 THEN TIMESTAMPDIFF(MINUTE, F.Dep_DateTime, F.Arr_DateTime) ELSE 0 END) / 60.0 AS Short_Hours
FROM Pilots P
JOIN FlightCrew_Pilots CP   ON P.Pilot_id = CP.Pilot_id
JOIN Flights F              ON CP.Flight_id = F.Flight_id
WHERE F.Status = 'Completed'
GROUP BY P.Pilot_id, Full_Name
UNION ALL
# ATTENDANTS
SELECT
    A.Attendant_id AS Employee_id,
    CONCAT(A.First_Name, ' ', A.Last_Name) AS Full_Name,
    'FlightAttendant' AS Employee_Type,
    SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, F.Dep_DateTime, F.Arr_DateTime) > 360 THEN TIMESTAMPDIFF(MINUTE, F.Dep_DateTime, F.Arr_DateTime) ELSE 0 END) / 60.0 AS Long_Hours,
    SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, F.Dep_DateTime, F.Arr_DateTime) <= 360 THEN TIMESTAMPDIFF(MINUTE, F.Dep_DateTime, F.Arr_DateTime)ELSE 0 END) / 60.0 AS Short_Hours
FROM FlightAttendants A
JOIN FlightCrew_Attendants CA ON A.Attendant_id = CA.Attendant_id
JOIN Flights F                ON CA.Flight_id = F.Flight_id
WHERE F.Status = 'Completed'
GROUP BY A.Attendant_id, Full_Name
ORDER BY Employee_Type, Full_Name;*/




/* Report 3: Cumulative flight hours per employee,
   separated into long and short flights
   (long flight = Duration_Minutes > 360) */

-- PILOTS
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

-- ATTENDANTS
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



/* Report 4: Cancellation rate of purchases per month */
SELECT
    DATE_FORMAT(Order_Date, '%Y-%m') AS YearMonth,          -- e.g. 2025-01
    COUNT(*) AS Total_Orders,                               -- all orders in that month
    SUM(CASE WHEN Status IN ('Cancelled-Customer','Cancelled-System') THEN 1 ELSE 0 END) AS Cancelled_Orders,                                  -- only cancelled orders
    ROUND(SUM(CASE WHEN Status IN ('Cancelled-Customer','Cancelled-System') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS Cancellation_Rate_Percent   -- percentage
FROM Orders
GROUP BY DATE_FORMAT(Order_Date, '%Y-%m')
ORDER BY YearMonth;

/*  Report 5 - Monthly activity per aircraft
    - Flights_Completed  : # of flights actually flown
    - Flights_Cancelled  : # of cancelled flights
    - Utilization_Percent: share of the month the aircraft was in the air
                           (only completed flights, assuming 30 days / month)
    - Dominant_Route     : most common Origin–Destination pair in that month
*/

/*WITH per_flight AS (
    SELECT
        f.Flight_id,
        f.Aircraft_id,
        -- normalize to the first day of the month
        DATE_FORMAT(f.Dep_DateTime, '%Y-%m-01') AS MonthStart,
        fr.Origin_Airport_code,
        fr.Destination_Airport_code,
        fr.Duration_Minutes,
        f.Status
    FROM Flights f
    JOIN Flight_Routes fr ON f.Route_id = fr.Route_id
),

agg_base AS (
     Aggregate per aircraft & month: counts and total performed hours 
    SELECT
        Aircraft_id,
        MonthStart,
        COUNT(*) AS Total_Flights,
        SUM(CASE WHEN Status = 'Completed' THEN 1 ELSE 0 END) AS Flights_Completed,
        SUM(CASE WHEN Status = 'Cancelled' THEN 1 ELSE 0 END) AS Flights_Cancelled,
        -- only completed flights count for utilization
        SUM(CASE WHEN Status = 'Completed' THEN Duration_Minutes ELSE 0 END) AS Total_Flight_Minutes
    FROM per_flight
    GROUP BY Aircraft_id, MonthStart
),

route_counts AS (
   For each aircraft & month, rank routes by number of performed flights
       so we can pick the dominant origin–destination pair (rn = 1). 
    SELECT
        Aircraft_id,
        MonthStart,
        Origin_Airport_code,
        Destination_Airport_code,
        COUNT(*) AS Route_Flights,
        ROW_NUMBER() OVER (PARTITION BY Aircraft_id, MonthStart ORDER BY COUNT(*) DESC, Origin_Airport_code, Destination_Airport_code) AS rn
    FROM per_flight
    GROUP BY Aircraft_id, MonthStart, Origin_Airport_code, Destination_Airport_code
)*/

/*  Report 5 - Monthly activity per aircraft
    - Flights_Completed   : # of flights actually flown in that month
    - Flights_Cancelled   : # of flights that were planned to depart in that month
                            and ended up with Status = 'Cancelled'
    - Utilization_Percent : share of the month the aircraft was in the air
                            (only completed flights, assuming 30 days / month)
    - Dominant_Route      : most common Origin–Destination pair in that month
                            (based on planned departures in that month)*/


WITH per_flight AS (
    SELECT f.Flight_id, f.Aircraft_id, DATE_FORMAT(f.Dep_DateTime, '%Y-%m-01') AS MonthStart, fr.Origin_Airport_code, fr.Destination_Airport_code,
           fr.Duration_Minutes, f.Status
    FROM Flights f
    JOIN Flight_Routes fr
      ON f.Route_id = fr.Route_id
),

agg_base AS (
    /* Aggregate per aircraft & planned month of departure */
    SELECT Aircraft_id, MonthStart, COUNT(*) AS Total_Flights, SUM(CASE WHEN Status = 'Completed' THEN 1 ELSE 0 END)  AS Flights_Completed,
           SUM(CASE WHEN Status = 'Cancelled' THEN 1 ELSE 0 END) AS Flights_Cancelled,
           SUM(CASE WHEN Status = 'Completed'THEN Duration_Minutes ELSE 0 END) AS Total_Flight_Minutes
    FROM per_flight
    GROUP BY Aircraft_id, MonthStart
),

route_counts AS (
   /* For each aircraft & month, pick the dominant route (rn = 1) */
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




SELECT ab.Aircraft_id, ac.Manufacturer, ac.Model, DATE_FORMAT(ab.MonthStart, '%Y-%m') AS Month, ab.Flights_Completed, ab.Flights_Cancelled, ab.Total_Flights,
       ROUND( ab.Total_Flight_Minutes / (30 * 24 * 60) * 100,2) AS Utilization_Percent, CONCAT(rc.Origin_Airport_code, '-',rc.Destination_Airport_code) AS Dominant_Route
FROM(SELECT pf.Aircraft_id, pf.MonthStart, COUNT(*) AS Total_Flights,
            SUM(CASE WHEN pf.Status = 'Completed' THEN 1 ELSE 0 END) AS Flights_Completed,
            SUM(CASE WHEN pf.Status = 'Cancelled' THEN 1 ELSE 0 END) AS Flights_Cancelled,
            SUM(CASE WHEN pf.Status = 'Completed' THEN pf.Duration_Minutes ELSE 0 END) AS Total_Flight_Minutes
        FROM(SELECT f.Flight_id, f.Aircraft_id, DATE_FORMAT(f.Dep_DateTime, '%Y-%m-01') AS MonthStart, fr.Origin_Airport_code,
					fr.Destination_Airport_code, fr.Duration_Minutes,f.Status
			  FROM Flights f
			  JOIN Flight_Routes fr ON f.Route_id = fr.Route_id) AS pf GROUP BY pf.Aircraft_id,pf.MonthStart) AS ab
			  JOIN Aircrafts AS ac ON ac.Aircraft_id = ab.Aircraft_id
			  LEFT JOIN(SELECT x.Aircraft_id, x.MonthStart, x.Origin_Airport_code, x.Destination_Airport_code
						FROM(SELECT pf2.Aircraft_id, pf2.MonthStart, pf2.Origin_Airport_code, pf2.Destination_Airport_code,COUNT(*) AS Route_Flights,
						ROW_NUMBER() OVER (PARTITION BY pf2.Aircraft_id, pf2.MonthStart ORDER BY COUNT(*) DESC, pf2.Origin_Airport_code, pf2.Destination_Airport_code) AS rn
						FROM(SELECT f2.Flight_id, f2.Aircraft_id, DATE_FORMAT(f2.Dep_DateTime, '%Y-%m-01') AS MonthStart, fr2.Origin_Airport_code,
									fr2.Destination_Airport_code, fr2.Duration_Minutes, f2.Status
							 FROM Flights f2 JOIN Flight_Routes fr2 ON f2.Route_id = fr2.Route_id) AS pf2
						GROUP BY pf2.Aircraft_id, pf2.MonthStart, pf2.Origin_Airport_code, pf2.Destination_Airport_code) AS x WHERE x.rn = 1) AS rc
			  ON rc.Aircraft_id = ab.Aircraft_id AND rc.MonthStart = ab.MonthStart
			  ORDER BY ab.MonthStart, ab.Aircraft_id;

