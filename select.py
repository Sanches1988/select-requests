import psycopg2
import sqlalchemy

engine = sqlalchemy.create_engine('postgresql://alexander:123456@localhost:5432/music_website_db')
connection = engine.connect()

# количество исполнителей в каждом жанре
connection.execute("""
SELECT gr.name, COUNT(m.name) quantity_musicians from musicians m 
JOIN genre_muscians gm ON gm.musicians_id = m.id
JOIN genre gr on gr.id = gm.genre_id 
GROUP BY gr.name
ORDER BY COUNT(m.name) DESC;
""").fetchall()

# количество треков, вошедших в альбомы 2019-2020 годов
connection.execute("""
SELECT al_ls.name, COUNT (tr_ls.name) from track_list tr_ls
JOIN albums_list al_ls ON tr_ls.album_id = al_ls.id
WHERE al_ls.year_release BETWEEN 2018 AND 2020
GROUP BY al_ls.name;
""").fetchall()

# средняя продолжительность треков по каждому альбому
connection.execute("""
SELECT al_ls.name, AVG(tr_ls.duration) avg_drt from track_list tr_ls
JOIN albums_list al_ls ON tr_ls.album_id = al_ls.id
group by al_ls.name;
""").fetchall()

# все исполнители, которые не выпустили альбомы в 2020 году
connection.execute("""
SELECT m.name FROM musicians m
LEFT JOIN album_musicians al_m ON al_m.musicians_id = m.id
LEFT JOIN albums_list al_ls ON al_ls.id = al_m.id
WHERE al_ls.year_release != (
     SELECT id FROM albums_list
     WHERE year_release = 2020)
GROUP BY m.name;
""").fetchall()

# названия сборников, в которых присутствует конкретный исполнитель (выберите сами)
connection.execute("""
SELECT c.name FROM collections c 
LEFT JOIN track_in_collections tic ON tic.collections_id = c.id 
LEFT JOIN track_list tl ON tl.id = tic.track_id 
LEFT JOIN albums_list al ON al.id = tl.album_id 
LEFT JOIN album_musicians am ON am.album_id = al.id 
LEFT JOIN musicians m ON m.id = am.musicians_id 
WHERE m.name = 'Prodigy'; 
""").fetchall()

# название альбомов, в которых присутствуют исполнители более 1 жанра
connection.execute("""
SELECT al_ls.name, mus.name FROM albums_list al_ls
LEFT JOIN album_musicians al_mus ON al_ls.id = al_mus.album_id    
LEFT JOIN musicians mus ON al_mus.musicians_id = mus.id 
LEFT JOIN genre_muscians gm ON gm.musicians_id = mus.id
LEFT JOIN genre gen ON gen.id = gm.genre_id
GROUP BY al_ls.name, mus.name
HAVING COUNT(gen.name) > 1; 
""").fetchall()

# наименование треков, которые не входят в сборники
connection.execute("""
SELECT tl.name FROM track_list tl
JOIN track_in_collections tic ON tic.track_id = tl.id 
JOIN collections c ON c.id = tic.collections_id 
GROUP BY tl.name
HAVING COUNT(c.name) = 0;
""").fetchall()

# исполнителя(-ей), написавшего самый короткий по продолжительности трек (теоретически таких треков может быть несколько)
connection.execute("""
SELECT DISTINCT m.name FROM musicians m
JOIN album_musicians am ON m.id = am.musicians_id 
JOIN albums_list al_ls ON am.album_id = al_ls.id
GROUP BY m.name, al_ls.id
HAVING al_ls.id IN (
    SELECT album_id FROM track_list tl 
    WHERE tl.duration = (
        SELECT MIN(track_list.duration) FROM track_list));
""").fetchall()

# название альбомов, содержащих наименьшее количество треков
connection.execute("""
SELECT al_ls.name, COUNT(tl.name) FROM albums_list al_ls
LEFT JOIN track_list tl ON tl.album_id = al_ls.id
GROUP BY al_ls.name
HAVING COUNT(tl.name) = (                                      
    SELECT MIN(qty) FROM (
        SELECT COUNT(tr_ls.name) qty FROM albums_list al
        LEFT JOIN track_list tr_ls ON tr_ls.album_id = al.id
        GROUP BY al.name) MinValue);
""").fetchall()
