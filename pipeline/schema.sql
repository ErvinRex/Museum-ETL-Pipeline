-- This file should contain all code required to create & seed database tables.

DROP TABLE IF EXISTS exhibition;
DROP TABLE IF EXISTS floor;
DROP TABLE IF EXISTS department;
DROP TABLE IF EXISTS rating_instance;
DROP TABLE IF EXISTS rating_type;
DROP TABLE IF EXISTS support_instance;
DROP TABLE IF EXISTS support_type;

CREATE TABLE IF NOT EXISTS floor (
    floor_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    floor VARCHAR(20) UNIQUE NOT NULL,
    PRIMARY KEY (floor_id)
);

CREATE TABLE IF NOT EXISTS department (
    department_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    department_name VARCHAR(20),
    PRIMARY KEY (department_id)
);

CREATE TABLE IF NOT EXISTS exhibition (
    exhibition_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    exhibition_name VARCHAR(30) UNIQUE NOT NULL,
    exhibition_start_date TIMESTAMPTZ NOT NULL,
    exhibition_description VARCHAR(150),
    floor_id SMALLINT NOT NULL,
       FOREIGN KEY (floor_id) REFERENCES floor(floor_id)
       ON DELETE CASCADE,
    department_id SMALLINT NOT NULL,
        FOREIGN KEY (department_id) REFERENCES department(department_id)
        ON DELETE CASCADE,
    PRIMARY KEY (exhibition_id)
);

CREATE TABLE IF NOT EXISTS rating_type (
    rating_type_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    rating_type_value SMALLINT NOT NULL,
    rating_description VARCHAR(30) NOT NULL,
    PRIMARY KEY (rating_type_id)
);

CREATE TABLE IF NOT EXISTS rating_instance (
    rating_instance_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    exhibition_id SMALLINT NOT NULL,
        FOREIGN KEY (exhibition_id) REFERENCES exhibition(exhibition_id)
        ON DELETE CASCADE,
    rating_type_id SMALLINT NOT NULL,
        FOREIGN KEY (rating_type_id) REFERENCES rating_type(rating_type_id)
        ON DELETE CASCADE,
    rating_created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (rating_instance_id)
);

CREATE TABLE IF NOT EXISTS support_type (
    support_type_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    support_type_value SMALLINT NOT NULL,
    support_description VARCHAR(30) NOT NULL,
    PRIMARY KEY (support_type_id)
);

CREATE TABLE IF NOT EXISTS support_instance (
    support_instance_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    instance_created_at TIMESTAMPTZ NOT NULL,
    support_type_id SMALLINT NOT NULL,
        FOREIGN KEY (support_type_id) REFERENCES support_type(support_type_id)
        ON DELETE CASCADE,
    exhibition_id SMALLINT NOT NULL,
        FOREIGN KEY (exhibition_id) REFERENCES exhibition(exhibition_id)
        ON DELETE CASCADE,
    PRIMARY KEY (support_instance_id)
);

INSERT INTO floor
        (floor)
    VALUES
        ('Vault'),
        ('1'),
        ('2'),
        ('3')
;

INSERT INTO department
        (department_name)
    VALUES
        ('Geology'),
        ('Entomology'),
        ('Zoology'),
        ('Ecology'),
        ('Paleontology')
;

INSERT INTO exhibition
        (exhibition_name, exhibition_start_date, exhibition_description, floor_id, department_id)
    VALUES
        ('Measureless to Man', '2021-08-23 00:00:00', 'An immersive 3D experience: delve deep into a previously-inaccessible cave system.', 2, 1),
        ('Adaptation', '2019-07-01 00:00:00', 'How insect evolution has kept pace with an industrialised world', 1, 2),
        ('The Crenshaw Collection', '2021-03-03 00:00:00', 'An exhibition of 18th Century watercolours, mostly focused on South American wildlife.', 3, 3),
        ('Cetacean Sensations', '2019-07-01 00:00:00', 'Whales: from ancient myth to critically endangered.', 2, 3),
        ('Our Polluted World', '2021-05-12 00:00:00', 'A hard-hitting exploration of humanity''s impact on the environment.', 4, 4),
        ('Thunder Lizards',  '2023-02-01 00:00:00', 'How new research is making scientists rethink what dinosaurs really looked like.', 2, 5)
;

INSERT INTO rating_type
        (rating_type_value, rating_description)
    VALUES
        (0, 'Terrible'),
        (1, 'Bad'),
        (2, 'Neutral'),
        (3, 'Good'),
        (4, 'Amazing')
;

INSERT INTO support_type
        (support_type_value, support_description)
    VALUES
        (0, 'Assistance'),
        (1, 'Emergency')