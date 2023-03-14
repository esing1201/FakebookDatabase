-- Q4
create view selected_photo as
select PHOTO_ID
from  (
    select count(*) as times, p.PHOTO_ID
    from project2.PUBLIC_PHOTOS p, project2.PUBLIC_TAGS t
    where p.PHOTO_ID = t.TAG_PHOTO_ID
    group by p.PHOTO_ID
    order by times desc, p.PHOTO_ID
    )
where ROWNUM <= 10;

select s.PHOTO_ID, p.PHOTO_LINK, p.ALBUM_ID, a.ALBUM_NAME
from selected_photo s, project2.PUBLIC_PHOTOS p, project2.PUBLIC_ALBUMS a
where s.PHOTO_ID = p.PHOTO_ID and p.ALBUM_ID = a.ALBUM_ID;

select u.USER_ID, u.FIRST_NAME, u.LAST_NAME
from selected_photo s, project2.PUBLIC_TAGS t, project2.PUBLIC_USERS u
where s.PHOTO_ID = t.TAG_PHOTO_ID and t.TAG_SUBJECT_ID = u.USER_ID
order by u.USER_ID;

drop view selected_photo;