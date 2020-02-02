rating = LOAD '/pig/u.data' USING PigStorage('\t') AS (userid:int,itemid:int,rating:int,time:long);

item = LOAD '/pig/u.item' USING PigStorage('|') AS  (movieid:int,movietitle:chararray,releasedate:chararray,videoreleasedate:chararray,url:chararray,unknown:chararray,action:int,adventure:int,animation:int,child:int,comedy:int,crime:int,docu:int,drama:int,fantasy:int,noir:int,horror:int,musical:int,mystery:int,romance:int,scifi:int,triller:int,war:int,western:int);

alldata = JOIN rating BY itemid, item BY movieid;

mydata = FOREACH alldata GENERATE movieid, movietitle, releasedate, url, rating;



groupdata = GROUP mydata BY (movieid,movietitle,releasedate,url);

finaldata = FOREACH groupdata GENERATE FLATTEN(group) as (movieid, movietitle, releasedate, url), AVG(mydata.rating) AS avgrating;

STORE finaldata INTO '/out';



