# FlyTAU — Flight Management & Booking System (Project_Information_Systems)

FlyTAU is a Flask + MySQL web application that simulates an airline management and booking platform.  
It supports **customers (registered)**, **guests**, and **managers**, including flight scheduling, seat inventory, ticketing, and analytical reports.

---

## Project Structure (Folder Tree)

Project_Information_Systems/
├─ main.py
├─ config.py
├─ db.py
├─ auth_routes.py
├─ Fly_Tau_v5.sql
├─ .gitignore
├─ main_routes/
│ ├─ init.py
│ ├─ home.py
│ ├─ booking.py
│ ├─ flights.py
│ ├─ seats.py
│ ├─ aircrafts.py
│ ├─ flights_crew.py
│ ├─ flytau_staff.py
│ ├─ manager_reports.py
│ └─ manager_view_orders.py
├─ templates/
│ ├─ base.html
│ ├─ home.html
│ ├─ login.html
│ ├─ register.html
│ ├─ search_flights.html
│ ├─ booking_seats.html
│ ├─ booking_review.html
│ ├─ booking_confirmation.html
│ ├─ guest_order_lookup.html
│ ├─ customer_home.html
│ ├─ customer_orders.html
│ ├─ manager_home.html
│ ├─ manager_aircrafts_list.html
│ ├─ manager_aircrafts_form.html
│ ├─ manager_aircraft_seats_form.html
│ ├─ manager_flights_list.html
│ ├─ manager_flights_form.html
│ ├─ manager_flight_view.html
│ ├─ manager_flight_seats.html
│ ├─ manager_flight_crew.html
│ ├─ manager_crew_list.html
│ ├─ manager_crew_form.html
│ ├─ manager_orders.html
│ ├─ manager_reports.html
│ ├─ report_load_factor.html
│ ├─ report_revenue_by_aircraft.html
│ ├─ report_employee_hours.html
│ ├─ report_cancellation_rate.html
│ └─ report_aircraft_monthly_activity.html
└─ static/
├─ styles.css
└─ img/
├─ flytau-logo.png
├─ client-welcome.png
├─ welcomeaboard.png
├─ seatmap.png
└─ graph.png


---

## Roles & Capabilities

### Manager
Managers can:
- Create and manage **flights** (assign aircraft, route, departure time).
- Edit/cancel flights (with system validation rules).
- Assign **crew** (pilots + attendants) to flights.
- Manage **seat pricing** and seat status.
- View system **reports** (analytics dashboard).

**Notes & business rules (manager views):**
- **Flight history & cancelled flights:** When a flight is cancelled, its crew assignments are **cleared/removed** from that flight (to keep data consistent and free resources). Therefore, in the **history view** of a cancelled flight you may see no crew listed, even if it was assigned before cancellation.
- **Seat pricing (default vs. actual):** In the flight edit screen, the displayed seat price starts as a **default/base price**. The manager can change it, and the shown price should be treated as an initial value—not a fixed value.
- **Crew replacement based on availability:** When editing a flight, the system shows:
  - the **currently assigned attendants** for that flight, and  
  - a list of **available attendants** for the same flight time window.  
  Managers can replace attendants only from the **available list**, ensuring no scheduling conflicts and allowing safe swaps directly within the edit flow.
  

### Registered Customer
Registered customers can:
- Search future active flights with available seats.
- Select seats and place orders.
- View their personal area (**My area**).
- View **all orders** in their personal area.
- View **order history** (past/locked orders such as completed/cancelled), including order and ticket details.

### Guest
Guests can:
- Search and book flights similarly to registered customers.
- Retrieve booking information via **Guest booking lookup** using an **Order Code**

## Order code format (Order_Code)

Each order is identified by a unique **Order_Code** in the format:

- **O########** (example: `O00000001`)

This code is shown to the user after checkout and is used for:
- tracking an order in the customer area
- **guest order lookup** (a guest can view their order/payment details by entering the `O########` code)

---

## Order lifecycle: Active vs Completed + cancellation rules (UTC)

### When an order is **Active**
An order is considered **Active** from the moment it is created **until 36 hours before the flight departure time**.
Seats are not reserved and the order is not considered Active until the customer performs final confirmation and payment;
 **If confirmation/payment is not completed, the system treats it as no order, and no seats are taken.**
 Seat reservation policy: Seats are not reserved during the Review step; 
 At this stage the selection is stored only temporarily (pending_booking in the session) and no database records are created.
 Seats are reserved only after the user confirms the booking and completes payment, when the system creates the Order (purchase of Tickets - FlightSeats) 
 and updates the selected FlightSeats to Sold.
 
### When an order is **Completed (no refund)**
Starting **36 hours before departure** (and after), the order is treated as **Completed** for refund purposes:
- the order is **not refundable**
- the customer cannot cancel for a refund

### Customer cancellation (fee policy)
If the customer cancels **while the order is still Active** (i.e., **at least 36 hours before departure**):
- a **5% cancellation fee** is charged
- the remaining amount is refunded (per implementation)

### 72-hour operational cancellation rule (system policy)
Separately from customer refunds, the system enforces a **72-hour rule** for operational flight cancellation:
- a flight may be cancelled by management only **≥ 72 hours before departure** (if implemented in the manager flows)
---

## Timezone / Date-Time Handling (UTC-0)

This project intentionally keeps all server-side timestamps in **UTC (timezone = UTC+0)**.  
This includes:
- `datetime.now()` in Python
- `NOW()` / `CURDATE()` in MySQL (as configured on PythonAnywhere / DB session)

### Why we keep UTC
- **Consistency**: one unified time reference across environments (local machine, PythonAnywhere, DB).
- **No daylight-saving issues**: avoids DST edge-cases (Israel DST changes).
- **Clear debugging**: logs and DB times match the server clock exactly.

### What the user sees (Israel time)
The UI may show times as stored (UTC) unless explicitly converted.  
Therefore, **when demonstrating the system, we treat all displayed times as UTC**

### Concrete example (Israel vs UTC)
Israel is usually **UTC+2** (and **UTC+3** during daylight saving time).

Example:
- Flight departure saved in DB: `2026-01-18 12:00:00` (UTC)
- In Israel (winter, UTC+2) this corresponds to: `2026-01-18 14:00:00` (Asia/Jerusalem)

So if the screen shows `12:00`, it is correct **in UTC**, and the equivalent Israel time is **14:00**.


### Business rules and UTC
All business rules that depend on time are computed in UTC as well.
For example, the cancellation rule "up to 36 hours before departure" is checked using UTC times:
- If `dep_utc` is the departure time (UTC)
- `now_utc = datetime.now()` (UTC)
- Cancellation allowed if: `dep_utc - now_utc >= 36 hours`



---

## Project Structure — `main/` Routes Package (Flask Blueprint)

The `main/` folder is the **core Flask package** that defines the application’s main logic and routes.
It contains the `main_bp` Blueprint and imports submodules so their route decorators are registered.

### Key file: `main/__init__.py`
Responsibilities:
- Defines the Blueprint: `main_bp = Blueprint("main", __name__)`
- Holds shared constants and helper functions used across multiple route modules, for example:
  - `LONG_FLIGHT_THRESHOLD_MINUTES` (short vs long flight profile)
  - `CREW_REQUIREMENTS` (crew size policy per flight profile)
  - Default seat prices by class (`Economy`, `Business`)
  - Access control helpers:
    - `_require_manager()` — manager-only access guard
    - `_require_customer()` — registered customer-only access guard
- Imports all route modules to ensure their `@main_bp.route(...)` decorators run:
  - `home`, `flights`, `flights_crew`, `booking`, `manager_view_orders`, `manager_reports`, `aircrafts`, `flytau_staff`, `seats`

### Route modules overview (what each file is responsible for)
Typical responsibilities inside `main/` submodules:

- **`home.py`**
  - Landing pages and navigation endpoints
  - Main menus for customer/manager flows

- **`flights.py`**
  - Flight listing and flight details logic
  - Filtering and presentation of flight data
  - Any general flight operations not specific to booking checkout

- **`booking.py`**
  - Customer/guest booking flow:
    - Search flights (future, active, seats available)
    - Seat selection
    - Booking review
    - Booking confirmation
  - Seat status synchronization logic (based on Orders/Tickets)
  - Rules such as:
    - flight status updates (Active vs Full-Occupied)
    - cancellation restrictions (36h policy, 5% fee)

- **`flights_crew.py`**
  - Crew assignment and crew viewing logic for flights
  - Applies crew requirement policies based on flight duration

- **`manager_reports.py`**
  - Manager reports endpoints
  - Report queries execution and rendering (tables/graphs export logic if exists)

- **`manager_view_orders.py`**
  - Manager view of orders / tickets and monitoring logic

- **`aircrafts.py`**
  - Aircraft management and aircraft-related views (if enabled)

- **`flytau_staff.py`**
  - Staff-related pages and interactions (as implemented in the project)

- **`seats.py`**
  - Seat catalog / seat templates and seat-related utilities (as implemented)


## Authentication & Authorization (auth_routes.py)

The `auth_routes.py` module is responsible for **login, registration, logout, and role-based session handling**.

### Login
- The login screen (`templates/login.html`) supports role-based access:
  - **Customer login** (registered customers)
  - **Manager login**
- On successful login:
  - The system stores the user identity in the session (e.g., role + identifying fields).
  - The user is redirected to the correct home screen:
    - Customer → `main.customer_home`
    - Manager → `main.manager_home`

### Registration (Customers)
- New users can register using `templates/register.html`.
- Registration creates a new customer record in the database (and any required related data).
- After registration, the customer can log in and access the personal area.

### Logout
- Logout clears the session and returns the user to the login screen.
- Static routes are excluded from navigation tracking to avoid redirect loops.

### Access Control
- Pages are protected using role checks (e.g., `_require_manager()` or customer session checks).
- Manager pages are blocked from customers/guests.
- Customer personal pages require a logged-in customer session.

---

## Flight Search (Customer Side)

The public flight search screen (`templates/search_flights.html`) shows:
- Only **future flights**
- Only flights with `Status = Active`
- Only flights with **Available** seats

It supports filtering by:
- Origin airport
- Destination airport
- Date filter type: **Departure date** or **Arrival date**
- Specific date (optional)

The table shows:
- Remaining available seats
- Starting (minimum) seat price per flight
- Link to seat selection flow

Navigation behavior:
- If a registered customer is logged in → **Back to my area**
- Otherwise → **Back to login**

---

## Core Database Entities (High Level)

- `Airports` — Airport definitions.
- `Flight_Routes` — Routes (origin, destination, duration).
- `Aircrafts` — Aircraft metadata.
- `Seats` — Seats per aircraft (row/col/class).
- `Flights` — Scheduled flight instance (aircraft + route + departure + status).
- `FlightSeats` — Seat inventory per flight (price + status).
- `Orders` — Order header (customer/guest, flight, status).
- `Tickets` — Tickets per seat purchase (linked to FlightSeats and Orders).
- `Pilots`, `FlightAttendants`, `Managers` — Staff entities.
- `FlightCrew_Pilots`, `FlightCrew_Attendants` — Crew assignment per flight.
- `IdCounters` — Counters for generating formatted IDs (if used by the application).

---

## Orders, Cancellation & Refund Policy

- An **order is considered Active** until **36 hours before the flight departure**.
- Starting **36 hours before departure**, the order is treated as **Completed**:
  - It is **locked** (cannot be cancelled),
  - and the customer **is not eligible for a refund**.

### Cancellation Fee
- When a cancellation is allowed (before the 36-hour cutoff), a **5% cancellation fee** is applied.
- The refund amount is calculated after the fee deduction.

---

## Guest Booking Lookup (Order Code)

Guests can retrieve and view their booking details (including payment summary) using the **Guest booking lookup** screen.

- The guest enters an **Order Code** in the format: `O......` (e.g., `O123456`).
- The lookup screen allows the guest to view:
  - Orders that are **Active**, **Completed**, or **Cancelled**,
  - including orders the guest **cancelled** or that were **cancelled by the system**,
  - and the **payment / charge summary** for that order.

---

## Manager Reports

The manager dashboard includes the following analytics reports:

1. **Flight load factor** — Seat occupancy for completed flights.  
2. **Revenue by aircraft** — Income breakdown by aircraft size, manufacturer, and seat class.  
3. **Employee flight hours** — Long vs. short flight hours per pilot and attendant.  
4. **Cancellation rate** — Monthly cancellation share of all orders.  
5. **Aircraft monthly activity** — Completion, cancellations, and utilization per aircraft.

---

## Setup (DB)

1) Create a MySQL database (example: `flytau`) and import the schema:

mysql flytau -  Fly_Tau_v5.sql
