create table if not exists Musicians(
    id serial primary key,
    name varchar(20) not null
);
create table if not exists Genre(
    id serial primary key,
    name varchar(40) not null
);
create table if not exists Genre_muscians(
    id serial primary key,
    musicians_id integer not null references musicians(id),
    genre_id integer not null references genre(id)
);
create table if not exists albums_list(
    id serial primary key,
    name varchar(40) not null,
    year_release integer
);
create table if not exists album_musicians(
    id serial primary key,
    musicians_id integer not null references musicians(id),
    album_id integer not null references albums_list(id)
);
create table if not exists track_list(
    id serial primary key,
    album_id integer not null references albums_list(id),
    name varchar(40) not null,
    duration integer not null
);
create table if not exists collections(
    id serial primary key,
    name varchar(30) not null,
    year integer
);
create table if not exists track_in_collections(
    id serial primary key,
    track_id integer not null references track_list(id),
    collections_id integer not null references collections(id)
);