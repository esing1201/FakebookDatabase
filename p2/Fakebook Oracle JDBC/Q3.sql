-- Q3 empty
create view qualified_user as
select u.USER_ID, ucc.CURRENT_CITY_ID as cur, uhc.HOMETOWN_CITY_ID as home
from project2.PUBLIC_USERS u, project2.PUBLIC_USER_CURRENT_CITIES ucc, project2.PUBLIC_USER_HOMETOWN_CITIES uhc
where u.USER_ID = ucc.USER_ID and u.USER_ID = uhc.USER_ID
minus
select u.USER_ID, ucc.CURRENT_CITY_ID cur, ucc.CURRENT_CITY_ID as home
from project2.PUBLIC_USERS u, project2.PUBLIC_USER_CURRENT_CITIES ucc
where u.USER_ID = ucc.USER_ID;

select u.USER_ID, u.FIRST_NAME, u.LAST_NAME
from project2.PUBLIC_USERS u
where u.USER_ID in (
    select q.USER_ID
    from qualified_user q
    )
order by u.USER_ID;

drop view qualified_user;