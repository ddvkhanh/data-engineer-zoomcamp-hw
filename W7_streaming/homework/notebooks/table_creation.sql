CREATE TABLE processed_events (
    PULocationID INTEGER,
    DOLocationID INTEGER,
    trip_distance DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    tip_amount DOUBLE PRECISION
);

CREATE TABLE processed_q4_window_trips (
    window_start TIMESTAMP,
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);

SELECT PULocationID, num_trips
FROM processed_q4_window_trips
ORDER BY num_trips DESC
LIMIT 3;


CREATE TABLE processed_q5_session_window (
    window_start TIMESTAMP,
    window_end TIMESTAMP(3),
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);



CREATE TABLE processed_q6_largest_tip (
    window_start TIMESTAMP(3) PRIMARY KEY,
    total_tip_amount DOUBLE PRECISION
);

SELECT *
FROM processed_q6_largest_tip
ORDER BY total_tip_amount DESC
LIMIT 1;