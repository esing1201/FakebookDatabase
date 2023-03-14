-- Q2 pay attention to empty
select U.USER_ID, u.FIRST_NAME, u.LAST_NAME
from project2.PUBLIC_USERS u
where u.USER_ID not in (select f.USER1_ID from project2.PUBLIC_FRIENDS f)
  and u.USER_ID not in (select f.USER2_ID from project2.PUBLIC_FRIENDS f)
order by u.USER_ID;