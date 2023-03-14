-- Q1
create view first_name_length as
select length(u.FIRST_NAME) as LengthOfFirstName, u.FIRST_NAME
from project2.PUBLIC_USERS u;

select distinct f.FIRST_NAME
from first_name_length f
where f.LengthOfFirstName = (
    select max(f2.LengthOfFirstName)
    from first_name_length f2
    )
order by f.FIRST_NAME;

select distinct f.FIRST_NAME
from first_name_length f
where f.LengthOfFirstName = (
    select min(f2.LengthOfFirstName)
    from first_name_length f2
    )
order by f.FIRST_NAME;

create view name_times as
select count (*) as times, u.FIRST_NAME
from project2.PUBLIC_USERS u
group by u.FIRST_NAME
order by times desc, u.FIRST_NAME;

select max(nt.times)
from name_times nt;

select nt.FIRST_NAME
from name_times nt
where nt.times = (
select max(nt2.times)
from name_times nt2
    );

drop view first_name_length;
drop view name_times;