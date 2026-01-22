create table DateHolder(LastDate DATE);

select * from [dbo].[DateHolder]

select min(OrderDate) from [dbo].[SalesOrders]

insert into DateHolder(LastDate) values ('2023-06-22')

select max(OrderDate) from [dbo].[SalesOrders]

select * from [dbo].[SalesOrders] 
where OrderDate > '2023-06-22' and OrderDate <= '2025-06-22'


create procedure updateDateHolder
   @myvar Date 
as
begin
    begin transaction 
	   update DateHolder 
	   set LastDate = @myvar;
	commit transaction 
end;