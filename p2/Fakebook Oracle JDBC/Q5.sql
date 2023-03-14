-- Q5
create view not_friend as
select u1.USER_ID as USER1_ID, u2.USER_ID as USER2_ID
from project2.PUBLIC_USERS u1, project2.PUBLIC_USERS u2
where u1.USER_ID < u2.USER_ID and u1.GENDER = u2.GENDER
  and (u1.YEAR_OF_BIRTH-u2.YEAR_OF_BIRTH <= 5 or u2.YEAR_OF_BIRTH-u1.YEAR_OF_BIRTH <= 5)
minus
select f.USER1_ID, f.USER2_ID
from project2.PUBLIC_FRIENDS f;

create view selected as
select distinct nf.USER1_ID, nf.USER2_ID, t1.TAG_PHOTO_ID
from not_friend nf, project2.PUBLIC_TAGS t1, project2.PUBLIC_TAGS t2
where nf.USER1_ID = t1.TAG_SUBJECT_ID and nf.USER2_ID = t2.TAG_SUBJECT_ID
  and t1.TAG_PHOTO_ID = t2.TAG_PHOTO_ID;

create view selected_pair as
select temp.USER1_ID, temp.USER2_ID from (
    select s.USER1_ID, s.USER2_ID, count(*) as common_tags
    from selected s
    group by (s.USER1_ID, s.USER2_ID)
    order by common_tags
    ) temp
where ROWNUM <= 5;

select u1.USER_ID, u2.USER_ID, u1.FIRST_NAME, u2.FIRST_NAME, u1.LAST_NAME, u2.LAST_NAME, u1.YEAR_OF_BIRTH, u2.YEAR_OF_BIRTH
from selected_pair sp, project2.PUBLIC_USERS u1, project2.PUBLIC_USERS u2
where sp.USER1_ID = u1.USER_ID and sp.USER2_ID = u2.USER_ID
order by u1.USER_ID, u2.USER_ID;

select s.TAG_PHOTO_ID, p.PHOTO_LINK, p.ALBUM_ID, a.ALBUM_NAME
from selected_pair sp, selected s, project2.PUBLIC_PHOTOS p, project2.PUBLIC_ALBUMS a
where sp.USER1_ID = s.USER1_ID and sp.USER2_ID = sp.USER2_ID and s.TAG_PHOTO_ID = p.PHOTO_ID
  and p.ALBUM_ID = a.ALBUM_ID
order by s.TAG_PHOTO_ID;

drop view not_friend;
drop view selected;
drop view selected_pair;