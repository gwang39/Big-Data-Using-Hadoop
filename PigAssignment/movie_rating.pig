--rating = LOAD '/pig/u.data' USING PigStorage('\t') AS (userid:int,itemid:int,rating:int,time:long);
--rating = LOAD 'hdfs://localhost:9000/pig/u.data' USING PigStorage('\t') as (userid:int, itemid:int, rating:int, time:long);
--item = LOAD '/pig/u.item' USING PigStorage('|') AS  (movieid:int,movietitle:chararray,releasedate:chararray,videoreleasedate:chararray,url:chararray,unknown:chararray,action:int,adventure:int,animation:int,child:int,comedy:int,crime:int,docu:int,drama:int,fantasy:int,noir:int,horror:int,musical:int,mystery:int,romance:int,scifi:int,triller:int,war:int,western:int);
--item = LOAD 'hdfs://localhost:9000/pig/u.item' USING PigStorage('|') as (movieid:int,movietitle:chararray,releasedate:chararray,videoreleasedate:chararray,url:chararray,unknown:chararray,action:int,adventure:int,animation:int,child:int,comedy:int,crime:int,docu:int,drama:int,fantasy:int,noir:int,horror:int,musical:int,mystery:int,romance:int,scifi:int,triller:int,war:int,western:int);
alldata = JOIN rating BY itemid, item BY movieid;
mydata = FOREACH alldata GENERATE movieid, movietitle, releasedate, url, rating;
--groupdata = GROUP mydata BY (movieid,movietitle,releasedate,url);
--finaldata = FOREACH groupdata GENERATE group AS movieid,  AVG(rating) AS avgrating;
--groupdata = GROUP mydata BY (movieid,movietitle,releasedate,url);
--finaldata = FOREACH groupdata GENERATE FLATTEN(group) AS (movieid,movietitle,releasedate,url),mydata.movieid,AVG(mydata.rating); 
STORE mydata INTO '/out';






