PRAGMA foreign_keys = ON;

CREATE TABLE industry_types (
    industry_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
    industry_type_name TEXT UNIQUE NOT NULL
);

CREATE TABLE industries (
    industry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    industry_name TEXT UNIQUE NOT NULL,
    industry_type_id INTEGER NOT NULL,
    FOREIGN KEY (industry_type_id) REFERENCES industry_types(industry_type_id)
);

CREATE TABLE car_types (
    car_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
    car_type_name TEXT UNIQUE NOT NULL
);

CREATE TABLE car_spots (
    spot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    spot_name TEXT NOT NULL,
    industry_id INTEGER NOT NULL,
    capacity INTEGER NOT NULL,
    service_frequency REAL,
    FOREIGN KEY (industry_id) REFERENCES industries(industry_id)
);

CREATE TABLE spot_allowed_car_types (
    spot_id INTEGER NOT NULL,
    car_type_id INTEGER NOT NULL,
    PRIMARY KEY (spot_id, car_type_id),
    FOREIGN KEY (spot_id) REFERENCES car_spots(spot_id),
    FOREIGN KEY (car_type_id) REFERENCES car_types(car_type_id)
);

CREATE TABLE cars (
    car_number TEXT PRIMARY KEY,
    car_type_id INTEGER NOT NULL,
    build_year INTEGER,
    road_name TEXT,
    status TEXT NOT NULL,
    spot_id INTEGER,
    FOREIGN KEY (car_type_id) REFERENCES car_types(car_type_id),
    FOREIGN KEY (spot_id) REFERENCES car_spots(spot_id)
);
